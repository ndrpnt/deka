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

    let tp = init_telemetry(
        cli.flags.verbose.tracing_level_filter(),
        cli.flags.output.clone(),
        cli.flags.debug,
    )?;

    let res = match cli.command {
        Commands::Apply { flags } => apply(&cli.flags, &flags).await,
    };

    tp.force_flush();
    res
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

pub fn init_telemetry(
    lvl: LevelFilter,
    output: OutputFormat,
    debug: bool,
) -> Result<opentelemetry_sdk::trace::TracerProvider> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::{KeyValue, StringValue, Value};
    use opentelemetry_sdk::Resource;
    use opentelemetry_semantic_conventions::resource;
    use std::{ffi::OsStr, path::Path, process};
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{
        filter::Targets, layer::SubscriberExt, util::SubscriberInitExt, Layer, Registry,
    };

    let resource = Resource::new([
        KeyValue::new(resource::HOST_ARCH, env::consts::ARCH),
        KeyValue::new(resource::OS_TYPE, env::consts::OS),
        KeyValue::new(resource::PROCESS_PID, i64::from(process::id())),
        KeyValue::new(
            resource::PROCESS_COMMAND,
            env::args().next().unwrap_or_default(),
        ),
        KeyValue::new(
            resource::PROCESS_COMMAND_ARGS,
            Value::Array(
                env::args()
                    .map(StringValue::from)
                    .collect::<Vec<_>>()
                    .into(),
            ),
        ),
        KeyValue::new(
            resource::PROCESS_EXECUTABLE_NAME,
            env::current_exe()
                .ok()
                .as_ref()
                .map(Path::new)
                .and_then(Path::file_name)
                .and_then(OsStr::to_str)
                .map(String::from)
                .unwrap_or_default(),
        ),
        KeyValue::new(
            resource::PROCESS_EXECUTABLE_PATH,
            env::current_exe()
                .ok()
                .as_ref()
                .map(Path::new)
                .map(Path::display)
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        ),
        KeyValue::new(resource::PROCESS_RUNTIME_NAME, "rustc"),
        KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        KeyValue::new(resource::SERVICE_NAME, env!("CARGO_BIN_NAME")),
    ]);

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

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .build()
                .into_diagnostic()?,
            opentelemetry_sdk::runtime::Tokio,
        )
        .build();

    let otlp = OpenTelemetryLayer::new(provider.tracer(env!("CARGO_BIN_NAME"))).with_filter(lvl);

    Registry::default()
        .with(otlp)
        .with(stdout)
        .try_init()
        .into_diagnostic()?;

    Ok(provider)
}
