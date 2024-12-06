use backoff::ExponentialBackoffBuilder;
use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use kube::{
    api::DynamicObject,
    client::ClientBuilder,
    config::{KubeConfigOptions, Kubeconfig},
    Client, Config,
};
use miette::{IntoDiagnostic, Result};
use serde::Deserialize;
use serde_yaml::Deserializer;
use std::{fs::File, io, path::PathBuf, time::Duration};
use tower;
use tracing::level_filters::LevelFilter;
use tracing::{instrument, Level};

#[derive(Parser, Debug)]
#[command(about = "Apply Kubernetes manifests the dumb way.", long_about = None)]
struct Cli {
    /// The command to run
    #[command(subcommand)]
    command: Commands,

    #[command(flatten)]
    flags: GlobalFlags,
}

#[derive(Args, Debug)]
pub struct GlobalFlags {
    /// Path to the kubeconfig file to use for this CLI request
    #[arg(long, global = true, default_value = None)]
    kubeconfig: Option<PathBuf>,

    /// If present, the namespace scope for this CLI request
    #[arg(long, short, global = true, default_value = None)]
    namespace: Option<String>,

    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,

    /// Output format
    #[arg(long, short, global = true, value_enum, default_value_t = OutputFormat::Plain)]
    output: OutputFormat,

    /// Print internal debug info
    #[arg(long, short = 'D', global = true)]
    debug: bool,

    /// Limit the number of parallel requests. 0 to disable
    #[arg(long, short, global = true, default_value = "10")]
    parallelism: usize,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Logfmt,
    Plain,
    Pretty,
}

#[derive(Args, Debug)]
pub struct ApplyFlags {
    /// The file that contains the configuration to apply
    #[arg(long, short, required = true)]
    filename: PathBuf,

    /// Name of the manager used to track field ownership
    #[arg(long, default_value = "deka")]
    field_manager: String,

    /// The length of time to wait before giving up in seconds. 0 to wait indefinitely
    #[arg(long, default_value = "300")]
    timeout: u64,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Server-side apply manifests
    Apply {
        #[command(flatten)]
        flags: ApplyFlags,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    init_telemetry(
        cli.flags.verbose.tracing_level_filter(),
        cli.flags.output.clone(),
        cli.flags.debug,
    )?;

    match cli.command {
        Commands::Apply { flags } => apply(&cli.flags, &flags).await,
    }
}

#[instrument(skip_all, err)]
async fn apply(gflags: &GlobalFlags, flags: &ApplyFlags) -> Result<()> {
    let objects = read_objects(&flags.filename)?;
    let config = build_config(&gflags.kubeconfig).await?;
    let client = &build_client(config, gflags.parallelism)?;
    let backoff = &ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(400))
        .with_randomization_factor(0.5)
        .with_multiplier(5.0)
        .with_max_interval(Duration::from_secs(30))
        .with_max_elapsed_time(match flags.timeout {
            0 => None,
            t => Some(Duration::from_secs(t)),
        })
        .build();

    deka::apply_objects(
        objects,
        client,
        &flags.field_manager,
        gflags.namespace.as_deref(),
        backoff,
    )
    .await
    .into_diagnostic()
}

#[instrument(level = Level::DEBUG, skip_all, err)]
fn read_objects(path: &PathBuf) -> Result<Vec<DynamicObject>> {
    match path.to_string_lossy().as_ref() {
        "-" => Deserializer::from_reader(io::stdin().lock()),
        _ => Deserializer::from_reader(File::open(path).into_diagnostic()?),
    }
    .map(serde_yaml::Value::deserialize)
    .map(|v| v.and_then(serde_yaml::from_value))
    .map(IntoDiagnostic::into_diagnostic)
    .collect()
}

#[instrument(level = Level::DEBUG, skip_all, err)]
async fn build_config(path: &Option<PathBuf>) -> Result<Config> {
    match path {
        Some(p) => {
            let k = Kubeconfig::read_from(p).into_diagnostic()?;
            Config::from_custom_kubeconfig(k, &KubeConfigOptions::default())
                .await
                .into_diagnostic()
        }
        None => Config::infer()
            .await
            .map_err(kube::Error::InferConfig)
            .into_diagnostic(),
    }
}

#[instrument(level = Level::DEBUG, skip_all, err)]
fn build_client(config: Config, parallelism: usize) -> Result<Client> {
    let builder = ClientBuilder::try_from(config).into_diagnostic()?;
    let client = match parallelism {
        0 => builder.build(),
        p => builder
            .with_layer(&tower::limit::concurrency::ConcurrencyLimitLayer::new(p))
            .build(),
    };

    Ok(client)
}

pub fn init_telemetry(lvl: LevelFilter, output: OutputFormat, debug: bool) -> Result<()> {
    use tracing_subscriber::{
        filter::Targets, layer::SubscriberExt, util::SubscriberInitExt, Layer, Registry,
    };

    let stdout = match output {
        OutputFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        OutputFormat::Logfmt => tracing_logfmt::layer().boxed(),
        OutputFormat::Plain => tracing_subscriber::fmt::layer().boxed(),
        OutputFormat::Pretty => tracing_subscriber::fmt::layer().pretty().boxed(),
    }
    .with_filter(lvl);

    let stdout = if debug {
        stdout.boxed()
    } else {
        let only_crate = Targets::default().with_target(env!("CARGO_CRATE_NAME"), Level::TRACE);
        stdout.with_filter(only_crate).boxed()
    };

    Registry::default()
        .with(stdout)
        .try_init()
        .into_diagnostic()
}
