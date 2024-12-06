# `deka`

> Apply Kubernetes manifests the dumb way.

`deka` is a proof-of-concept CLI and library offering an alternative to `kubectl apply`.

When deploying Kubernetes manifests, tools traditionally rely on ordering API calls to handle dependencies.
They are either "smart" (i.e. they enforce hard-coded priorities: [Helm][0], [Argo CD][1]),
"too dumb" (i.e. they leave that burden to the user: [kubectl][2]),
or both (e.g. [Kustomize][3]).
`deka` aims to be both simpler to reason about and more resilient by following a different, minimalist, approach:
it **applies manifests in an undefined order and retries automatically on errors**.

Note that `deka` is suitable for experimental use only.
It currently supports [Server-Side Apply (SSA)][4], and declarative deletion through the `deka.ndrpnt.dev/action: delete` annotation.

## Usage

To install the CLI using cargo, run:

```console
$ cargo install --git https://github.com/ndrpnt/deka

$ deka apply --help
Server-side apply manifests

Usage: deka apply [OPTIONS] --filename <FILENAME>

Options:
  -f, --filename <FILENAME>            The file that contains the configuration to apply
      --field-manager <FIELD_MANAGER>  Name of the manager used to track field ownership [default: deka]
      --kubeconfig <KUBECONFIG>        Path to the kubeconfig file to use for this CLI request
  -n, --namespace <NAMESPACE>          If present, the namespace scope for this CLI request
      --timeout <TIMEOUT>              The length of time to wait before giving up in seconds. 0 to wait indefinitely [default: 300]
  -v, --verbose...                     Increase logging verbosity
  -q, --quiet...                       Decrease logging verbosity
  -o, --output <OUTPUT>                Output format [default: plain] [possible values: json, logfmt, plain, pretty]
  -D, --debug                          Print internal debug info
  -p, --parallelism <PARALLELISM>      Limit the number of parallel requests. 0 to disable [default: 10]
  -h, --help                           Print help
```

[0]: https://github.com/helm/helm/blob/2cea1466d3c27491364eb44bafc7be1ca5461b2d/pkg/releaseutil/kind_sorter.go#L31-L68
[1]: https://github.com/argoproj/gitops-engine/blob/0371401803996f84bcd70a5f6bb2f0ecc7d7b5d2/pkg/sync/sync_tasks.go#L27-L62
[2]: https://kubernetes.io/docs/concepts/workloads/management/#organizing-resource-configurations
[3]: https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/sortoptions/
[4]: https://kubernetes.io/docs/reference/using-api/server-side-apply/
