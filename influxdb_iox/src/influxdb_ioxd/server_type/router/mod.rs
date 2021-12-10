use futures::{future::BoxFuture, FutureExt};
use hyper::{Body, Request, Response};
use metric::Registry;
use router::server::RouterServer;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use crate::influxdb_ioxd::{
    http::metrics::LineProtocolMetrics,
    rpc::RpcBuilderInput,
    server_type::{common_state::CommonServerState, RpcError, ServerType},
    serving_readiness::ServingReadiness,
};

mod http;
mod rpc;

pub use self::http::ApplicationError;

#[derive(Debug)]
pub struct RouterServerType {
    server: Arc<RouterServer>,
    serving_readiness: ServingReadiness,
    shutdown: CancellationToken,
    max_request_size: usize,
    lp_metrics: Arc<LineProtocolMetrics>,
}

impl RouterServerType {
    pub fn new(server: Arc<RouterServer>, common_state: &CommonServerState) -> Self {
        let lp_metrics = Arc::new(LineProtocolMetrics::new(server.metric_registry().as_ref()));

        Self {
            server,
            serving_readiness: common_state.serving_readiness().clone(),
            shutdown: CancellationToken::new(),
            max_request_size: common_state.run_config().max_http_request_size,
            lp_metrics,
        }
    }
}

impl ServerType for RouterServerType {
    type RouteError = ApplicationError;

    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(self.server.metric_registry())
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.server.trace_collector().clone()
    }

    fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> BoxFuture<'_, Result<Response<Body>, Self::RouteError>> {
        async move { self::http::route_request(self, req).await }.boxed()
    }

    fn server_grpc(
        self: Arc<Self>,
        builder_input: RpcBuilderInput,
    ) -> BoxFuture<'static, Result<(), RpcError>> {
        async move { self::rpc::server_grpc(self, builder_input).await }.boxed()
    }

    fn join(self: Arc<Self>) -> BoxFuture<'static, ()> {
        async move {
            self.shutdown.cancelled().await;
        }
        .boxed()
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
