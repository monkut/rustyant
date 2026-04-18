//! Lambda entry point for the API Gateway WebSocket transport.
//!
//! Wraps `rustyant::ws::handle` with:
//!   * the AWS SDK client for the API Gateway Management API (to post
//!     replies back on the same WebSocket connection), and
//!   * the Lambda runtime bridge that deserializes events.

use aws_lambda_events::apigw::{ApiGatewayProxyResponse, ApiGatewayWebsocketProxyRequest};
use aws_sdk_apigatewaymanagement::Client as MgmtClient;
use aws_sdk_apigatewaymanagement::primitives::Blob;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use tracing::{error, warn};

use rustyant::State;
use rustyant::ws::{WsOutcome, handle};

#[tokio::main]
async fn main() -> Result<(), Error> {
    rustyant::init_tracing();
    let state = State::from_env().await?;
    let aws_config = aws_config::load_from_env().await;

    lambda_runtime::run(service_fn(move |ev: LambdaEvent<ApiGatewayWebsocketProxyRequest>| {
        let state = state.clone();
        let aws_config = aws_config.clone();
        async move { handle_event(state, aws_config, ev.payload).await }
    }))
    .await
}

#[allow(clippy::similar_names)] // `state` vs `stage` is unavoidable here
async fn handle_event(
    state: State,
    aws_config: aws_config::SdkConfig,
    event: ApiGatewayWebsocketProxyRequest,
) -> Result<ApiGatewayProxyResponse, Error> {
    let domain_name = event.request_context.domain_name.clone();
    let stage = event.request_context.stage.clone();

    let outcome = match handle(&state, event).await {
        Ok(o) => o,
        Err(e) => {
            error!(error = %e, "ws handler transport error");
            return Ok(response(500));
        }
    };

    if let WsOutcome::Reply { connection_id, data } = outcome {
        let Some(domain) = domain_name else {
            warn!("reply requested but no domain_name in context");
            return Ok(response(500));
        };
        let Some(stage) = stage else {
            warn!("reply requested but no stage in context");
            return Ok(response(500));
        };
        let endpoint = format!("https://{domain}/{stage}");

        let mgmt_config =
            aws_sdk_apigatewaymanagement::config::Builder::from(&aws_config).endpoint_url(endpoint).build();
        let mgmt = MgmtClient::from_conf(mgmt_config);

        if let Err(e) = mgmt.post_to_connection().connection_id(connection_id).data(Blob::new(data)).send().await {
            error!(error = %e, "post_to_connection failed");
            return Ok(response(500));
        }
    }

    Ok(response(200))
}

fn response(status: i64) -> ApiGatewayProxyResponse {
    ApiGatewayProxyResponse { status_code: status, ..ApiGatewayProxyResponse::default() }
}
