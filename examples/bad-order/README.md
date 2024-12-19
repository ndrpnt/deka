## `kubectl`

`kubectl` partially fails on the first execution, and succeeds on the second one.
This happen because the Namespace does not yet exist when the Pod is applied.

```console
$ kubectl apply --filename manifests.yaml --server-side
namespace/foo serverside-applied
Error from server (NotFound): namespaces "foo" not found

$ kubectl apply --filename manifests.yaml --server-side
pod/foo serverside-applied
namespace/foo serverside-applied
```

## `deka`

`deka` succeeds on first execution, without any retry.
For some reason, the Namespace is consistently created before the Pod, possibly because the API Server processes that request more quickly (?).
One can observe the retry behavior by setting `--parallelism 1`, though it does not guarantee the order in which requests are sent.

```console
$ deka apply --filename manifests.yaml --output pretty
  2024-12-19T09:30:26.983329Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Namespace", object.name: "foo", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 2, field_manager: "deka", default_namespace: "default"
    in deka::apply

  2024-12-19T09:30:27.043860Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Pod", object.name: "foo", field_manager: "deka", namespace: "foo", action: "apply"
    in deka::apply_objects with objects.count: 2, field_manager: "deka", default_namespace: "default"
    in deka::apply
```
