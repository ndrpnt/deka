## `kubectl`

On a local KinD cluster, creating 135 Namespaces with `kubectl` takes ~19 seconds.

```console
$ time kubectl apply --filename manifests.yaml --server-side
namespace/foo-1 serverside-applied
namespace/foo-2 serverside-applied
namespace/foo-3 serverside-applied
[…]
namespace/foo-133 serverside-applied
namespace/foo-134 serverside-applied
namespace/foo-135 serverside-applied
kubectl apply --filename manifests.yaml --server-side  7.53s user 0.48s system 41% cpu 19.116 total
```

## `deka`

On a local KinD cluster, creating 135 Namespaces with `deka` takes ~3 seconds, with `--parallelism 10` (default).

```console
$ time deka apply --filename manifests.yaml --output pretty
  2024-12-19T09:44:39.297159Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Namespace", object.name: "foo-4", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 135, field_manager: "deka", default_namespace: "default"
    in deka::apply

  2024-12-19T09:44:39.297994Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Namespace", object.name: "foo-9", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 135, field_manager: "deka", default_namespace: "default"
    in deka::apply

[…]

  2024-12-19T09:44:41.729110Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Namespace", object.name: "foo-105", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 135, field_manager: "deka", default_namespace: "default"
    in deka::apply

  2024-12-19T09:44:41.829683Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "v1", object.kind: "Namespace", object.name: "foo-123", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 135, field_manager: "deka", default_namespace: "default"
    in deka::apply

deka apply --filename manifests.yaml --output pretty  0.14s user 0.07s system 7% cpu 2.773 total
```
