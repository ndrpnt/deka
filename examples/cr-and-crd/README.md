## `kubectl`

`kubectl` partially fails on the first execution, and succeeds on the second one.
This happen because the CRD is not yet registered when the CR is applied.
Note that this happens even though the resources in the `manifests.yaml` file are correctly ordered.

```console
$ kubectl apply --filename manifests.yaml --server-side
customresourcedefinition.apiextensions.k8s.io/foos.example.com serverside-applied
error: resource mapping not found for name: "my-foo" namespace: "" from "manifests.yaml": no matches for kind "Foo" in version "example.com/v1"
ensure CRDs are installed first

$ kubectl apply --filename manifests.yaml --server-side
customresourcedefinition.apiextensions.k8s.io/foos.example.com serverside-applied
foo.example.com/my-foo serverside-applied
```

## `deka`

`deka` succeeds on first execution, with an internal retry.

```console
$ deka apply --filename manifests.yaml --output pretty
  2024-12-18T17:31:32.490817Z  WARN deka: Failed to discover API, error: ApiError: "404 page not found\n": Failed to parse error data (ErrorResponse { status: "404 Not Found", message: "\"404 page not found\\n\"", reason: "Failed to parse error data", code: 404 })
    at src/lib.rs:133
    in deka::apply_object with object.api_version: "example.com/v1", object.kind: "Foo", object.name: "my-foo", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 2, field_manager: "deka", default_namespace: "default"
    in deka::apply

  2024-12-18T17:31:32.500119Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "apiextensions.k8s.io/v1", object.kind: "CustomResourceDefinition", object.name: "foos.example.com", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 2, field_manager: "deka", default_namespace: "default"
    in deka::apply

  2024-12-18T17:31:32.724190Z  INFO deka: Applied object
    at src/lib.rs:152
    in deka::apply_object with object.api_version: "example.com/v1", object.kind: "Foo", object.name: "my-foo", field_manager: "deka", namespace: "default", action: "apply"
    in deka::apply_objects with objects.count: 2, field_manager: "deka", default_namespace: "default"
    in deka::apply
```
