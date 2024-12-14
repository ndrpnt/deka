use backoff::backoff::Backoff;
use futures::StreamExt;
use kube::{
    api::{DeleteParams, DynamicObject, Patch, PatchParams},
    core::{gvk::ParseGroupVersionError, GroupVersionKind, TypeMeta},
    discovery::{self, Scope},
    error::DiscoveryError,
    Api, Client, Error as KubeError, Resource, ResourceExt,
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use strum_macros::{AsRefStr, EnumString};
use thiserror::Error;
use tracing::{debug_span, info, instrument, warn, Instrument, Span};

const ANNOTATION_ACTION: &str = "deka.ndrpnt.dev/action";

#[derive(EnumString, PartialEq, Default, AsRefStr)]
#[strum(serialize_all = "kebab-case")]
enum Action {
    #[default]
    Apply,
    Delete,
}

#[derive(Error, Debug)]
#[error("Error(s) while applying objects")]
pub struct ApplyErrors(Vec<ApplyError>);

#[derive(Error, Debug)]
pub enum ApplyError {
    #[error("KubeError: {0}")]
    Kube(#[from] KubeError),

    #[error("ParseGroupVersionError: {0}")]
    ParseGroupVersion(#[from] ParseGroupVersionError),

    #[error("SerdeError: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("StrumParseError: {0}")]
    StrumParse(#[from] strum::ParseError),
}

#[instrument(skip_all, fields(
    objects.count = objects.len(),
    field_manager = manager,
    default_namespace = namespace.unwrap_or(client.default_namespace()),
    objects.error_count,
), err)]
pub async fn apply_objects<B: Backoff + Clone>(
    objects: Vec<DynamicObject>,
    client: &Client,
    manager: &str,
    namespace: Option<&str>,
    backoff: &B,
) -> Result<(), ApplyErrors> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    futures::stream::iter(objects)
        .for_each_concurrent(None, |obj| {
            let c_errors = Arc::clone(&errors);
            async move {
                if let Err(e) = apply_object(&obj, client, manager, namespace, backoff).await {
                    c_errors.lock().unwrap().push(e);
                }
            }
        })
        .await;

    let errors = Arc::try_unwrap(errors)
        .expect("Arc should have only one reference")
        .into_inner()
        .unwrap();
    Span::current().record("objects.error_count", errors.len());

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApplyErrors(errors))
    }
}

#[instrument(skip_all, fields(
    object.api_version = object.types.clone().unwrap_or_default().api_version,
    object.kind = object.types.clone().unwrap_or_default().kind,
    object.name = object.name_any(),
    field_manager = manager,
    namespace,
    action,
), err)]
async fn apply_object<B: Backoff + Clone>(
    object: &DynamicObject,
    client: &Client,
    manager: &str,
    namespace: Option<&str>,
    backoff: &B,
) -> Result<(), ApplyError> {
    let namespace = object
        .meta()
        .namespace
        .as_deref()
        .or(namespace)
        .unwrap_or(client.default_namespace());
    Span::current().record("namespace", namespace);

    let action = &match object.annotations().get(ANNOTATION_ACTION) {
        Some(a) => Action::from_str(a)?,
        None => Action::default(),
    };
    Span::current().record("action", action.as_ref());

    let gvk = &GroupVersionKind::try_from(object.types.as_ref().unwrap_or(&TypeMeta::default()))?;
    let data = &Patch::Apply(serde_json::to_value(&object)?);

    backoff::future::retry(backoff.clone(), || async move {
        let (resource, capabilities) = match discovery::pinned_kind(client, gvk)
            .instrument(debug_span!("discover_api_resource").or_current())
            .await
        {
            Ok(v) => v,
            Err(KubeError::Discovery(DiscoveryError::MissingKind(_)))
                if action == &Action::Delete =>
            {
                info!("Object already deleted (kind not found)");
                return Ok(());
            }
            Err(e) => {
                warn!(error = %e, "Failed to discover API");
                return Err(backoff::Error::transient(e));
            }
        };

        let api: Api<DynamicObject> = match capabilities.scope {
            Scope::Cluster => Api::all_with(client.clone(), &resource),
            Scope::Namespaced => Api::namespaced_with(client.clone(), namespace, &resource),
        };

        match action {
            Action::Apply => {
                let params = PatchParams::apply(manager).force();
                let resp = api
                    .patch(object.name_any().as_ref(), &params, data)
                    .instrument(debug_span!("patch").or_current())
                    .await;
                match resp {
                    Ok(_) => {
                        info!("Applied object");
                        Ok(())
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to apply object");
                        Err(backoff::Error::transient(e))
                    }
                }
            }
            Action::Delete => {
                let resp = api
                    .delete(object.name_any().as_ref(), &DeleteParams::default())
                    .instrument(debug_span!("delete").or_current())
                    .await;
                match resp {
                    Ok(_) => {
                        info!("Deleted object");
                        Ok(())
                    }
                    Err(KubeError::Api(e)) if e.code == 404 => {
                        info!("Object already deleted (not found)");
                        Ok(())
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to delete object");
                        Err(backoff::Error::transient(e))
                    }
                }
            }
        }
    })
    .await
    .map_err(ApplyError::Kube)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Request, Response, StatusCode};
    use kube::client::Body;
    use serde_json::{json, Value};
    use std::cell::LazyCell;
    use std::{future::Future, time::Duration};
    use tower_test::mock;

    const API_RESOURCES: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "kind":"APIResourceList",
            "groupVersion":"v1",
            "resources":[
                {
                    "name":"pods",
                    "singularName":"pod",
                    "namespaced":true,
                    "kind":"Pod",
                    "verbs":[
                        "create",
                        "delete",
                        "deletecollection",
                        "get",
                        "list",
                        "patch",
                        "update",
                        "watch"
                    ],
                    "shortNames":[
                        "po"
                    ],
                    "categories":[
                        "all"
                    ]
                },
                {
                    "name": "services",
                    "singularName": "service",
                    "namespaced": true,
                    "kind": "Service",
                    "verbs": [
                        "create",
                        "delete",
                        "deletecollection",
                        "get",
                        "list",
                        "patch",
                        "update",
                        "watch"
                    ],
                    "shortNames": [
                        "svc"
                    ],
                    "categories": [
                        "all"
                    ]
                }
            ]
        })
    });

    const EMPTY_API_RESOURCES: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "kind":"APIResourceList",
            "groupVersion":"v1",
            "resources": []
        })
    });

    const POD: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "example"
            },
            "spec": {
                "containers": [{ "name": "example", "image": "example-image" }],
            }
        })
    });

    const POD_NOT_FOUND_ERROR: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "pods \"example\" not found",
            "reason": "NotFound",
            "details": {
                "name": "example",
                "kind": "pods"
            },
            "code": 404
        })
    });

    const POD_DELETED_RESPONSE: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Success",
            "details": {
                "name": "example",
                "kind": "pods",
                "uid": "e2e1d349-f96a-446f-9da5-f8239517bb79"
            }
        })
    });

    const SVC: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "example"
            },
            "spec": {}
        })
    });

    const INTERNAL_ERROR: LazyCell<Value> = LazyCell::new(|| {
        json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "Internal error occurred: unexpected response: 500",
            "reason": "InternalError",
            "code": 500,
            "details": {
                "causes": [{
                    "message": "etcd cluster is unavailable or misconfigured",
                    "reason": "EtcdError"
                }]
            }
        })
    });

    #[derive(Clone)]
    struct MockBackoff<B: Backoff + Clone> {
        inner: B,
        reset_calls: Arc<Mutex<usize>>,
        next_backoff_calls: Arc<Mutex<usize>>,
    }

    impl<B: Backoff + Clone> MockBackoff<B> {
        pub fn new(inner: B) -> Self {
            Self {
                inner,
                reset_calls: Default::default(),
                next_backoff_calls: Default::default(),
            }
        }
    }

    impl<B: Backoff + Clone> Backoff for MockBackoff<B> {
        fn reset(&mut self) {
            *self.reset_calls.lock().unwrap() += 1;
            self.inner.reset()
        }

        fn next_backoff(&mut self) -> Option<Duration> {
            *self.next_backoff_calls.lock().unwrap() += 1;
            self.inner.next_backoff()
        }
    }

    #[derive(Clone, Default)]
    struct LimitAndCount {
        interval: Duration,
        retries: usize,
        retry_limit: Option<usize>,
    }

    impl Backoff for LimitAndCount {
        fn next_backoff(&mut self) -> Option<Duration> {
            self.retries += 1;
            self.retry_limit
                .and_then(|l| (self.retries <= l).then_some(self.interval))
        }
    }

    fn unwrap_arc_mutex<T: std::fmt::Debug>(v: Arc<Mutex<T>>) -> T {
        Arc::try_unwrap(v)
            .expect("Arc should have only one reference")
            .into_inner()
            .unwrap()
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_1_object() {
        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value((*POD).clone()).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_1_object_with_explicit_action_annotation() {
        let mut pod = (*POD).clone();
        pod["metadata"]["annotations"][ANNOTATION_ACTION] = json!(Action::Apply.as_ref());

        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_1_object_with_explicit_namespace() {
        let mut pod = (*POD).clone();
        pod["metadata"]["namespace"] = json!("another_ns");

        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("another_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&pod).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_1_object_with_default_client_namespace() {
        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("default", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value((*POD).clone()).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                None,
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn delete_1_object() {
        let mut pod = (*POD).clone();
        pod["metadata"]["annotations"][ANNOTATION_ACTION] = json!(Action::Delete.as_ref());

        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::delete("/api/v1/namespaces/test_ns/pods/example?")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&json!({})).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(
                        serde_json::to_vec(&*POD_DELETED_RESPONSE).unwrap(),
                    ))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn delete_1_object_with_missing_kind() {
        let mut pod = (*POD).clone();
        pod["metadata"]["annotations"][ANNOTATION_ACTION] = json!(Action::Delete.as_ref());

        let expectations = vec![(
            Request::get("/api/v1").body(Body::empty()).unwrap(),
            Response::builder()
                .body(Body::from(
                    serde_json::to_vec(&*EMPTY_API_RESOURCES).unwrap(),
                ))
                .unwrap(),
        )];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn delete_1_object_already_absent() {
        let mut pod = (*POD).clone();
        pod["metadata"]["annotations"][ANNOTATION_ACTION] = json!(Action::Delete.as_ref());

        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::delete("/api/v1/namespaces/test_ns/pods/example?")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&json!({})).unwrap()))
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from(
                        serde_json::to_vec(&*POD_NOT_FOUND_ERROR).unwrap(),
                    ))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn invalid_action_annotation() {
        let mut pod = (*POD).clone();
        pod["metadata"]["annotations"][ANNOTATION_ACTION] = json!("invalid_action");

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(vec![], |s| async {
            apply_object(
                &serde_json::from_value(pod).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap_err();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            0,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_2_objects() {
        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "services", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*SVC).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*SVC).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_objects(
                vec![
                    serde_json::from_value((*POD).clone()).unwrap(),
                    serde_json::from_value((*SVC).clone()).unwrap(),
                ],
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            2,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn apply_0_objects() {
        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(vec![], |s| async {
            apply_objects(vec![], &Client::new(s, "default"), "test_manager", None, &b)
                .await
                .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            0,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            0,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn retry_apply_1_object_after_discovery_failure() {
        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(
                        serde_json::to_vec(&*EMPTY_API_RESOURCES).unwrap(),
                    ))
                    .unwrap(),
            ),
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount {
            retry_limit: Some(1),
            ..Default::default()
        });

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value((*POD).clone()).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            1,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn retry_apply_1_object_after_patch_failure() {
        let expectations = vec![
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::get("/api/v1").body(Body::empty()).unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*API_RESOURCES).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(serde_json::to_vec(&*INTERNAL_ERROR).unwrap()))
                    .unwrap(),
            ),
            (
                Request::patch(ssa_uri("test_ns", "pods", "example", "test_manager"))
                    .header("accept", "application/json")
                    .header("content-type", "application/apply-patch+yaml")
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
                Response::builder()
                    .body(Body::from(serde_json::to_vec(&*POD).unwrap()))
                    .unwrap(),
            ),
        ];

        let b = MockBackoff::new(LimitAndCount {
            retry_limit: Some(1),
            ..Default::default()
        });

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value((*POD).clone()).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            1,
            "unexpected number of next_backoff calls"
        );
    }

    #[test_log::test(tokio::test)]
    #[test_log(default_log_filter = "deka=trace")]
    async fn retry_limit_is_effective() {
        let expectations = vec![(
            Request::get("/api/v1").body(Body::empty()).unwrap(),
            Response::builder()
                .body(Body::from(
                    serde_json::to_vec(&*EMPTY_API_RESOURCES).unwrap(),
                ))
                .unwrap(),
        )];

        let b = MockBackoff::new(LimitAndCount::default());

        with_mock_service(expectations, |s| async {
            apply_object(
                &serde_json::from_value((*POD).clone()).unwrap(),
                &Client::new(s, "default"),
                "test_manager",
                Some("test_ns"),
                &b,
            )
            .await
            .unwrap_err();
        })
        .await;

        assert_eq!(
            unwrap_arc_mutex(b.reset_calls),
            1,
            "unexpected number of reset calls"
        );
        assert_eq!(
            unwrap_arc_mutex(b.next_backoff_calls),
            1,
            "unexpected number of next_backoff calls"
        );
    }

    /// Does not return before receiving an undue request, so this can safely
    /// be [`tokio::select!`]ed without returning before the client on
    /// successful applies.
    async fn mock_server(
        mut handle: mock::Handle<Request<Body>, Response<Body>>,
        expectations: Arc<Mutex<Vec<(Request<Body>, Response<Body>)>>>,
    ) {
        loop {
            let (request, send) = handle.next_request().await.expect("service not called");
            let (expected_request, response) = {
                let mut _expectations = expectations.lock().unwrap();
                _expectations
                    .iter()
                    .position(|e| {
                        e.0.method() == request.method()
                            && e.0.uri() == request.uri()
                            && e.0.headers() == request.headers()
                            && e.0.version() == request.version()
                    })
                    .map(|p| _expectations.remove(p))
                    .unwrap_or_else(|| panic!("unexpected request: {:#?}", request))
            };
            assert_eq!(
                request.into_body().collect_bytes().await.unwrap(),
                expected_request.into_body().collect_bytes().await.unwrap(),
                "body does not match"
            );
            send.send_response(response);
        }
    }

    async fn with_mock_service<F, Fut>(expectations: Vec<(Request<Body>, Response<Body>)>, f: F)
    where
        F: FnOnce(mock::Mock<Request<Body>, Response<Body>>) -> Fut,
        Fut: Future<Output = ()>,
    {
        let expectations = Arc::new(Mutex::new(expectations));
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        tokio::select! {
            _ = mock_server(handle, Arc::clone(&expectations)) => {}
            _ = f(service) => {
                let remaining_expectations = Arc::try_unwrap(expectations)
                    .expect("Arc should have only one reference")
                    .into_inner()
                    .unwrap();
                assert!(
                    remaining_expectations.is_empty(),
                    "unmet expectation(s): {:#?}",
                    remaining_expectations
                );
            }
        };
    }

    /// Encapsulates a long format string that causes code formatting issues
    /// when used inline.
    fn ssa_uri(namespace: &str, resource: &str, name: &str, manager: &str) -> String {
        format!(
            "/api/v1/namespaces/{}/{}/{}?&force=true&fieldManager={}",
            namespace, resource, name, manager
        )
    }
}
