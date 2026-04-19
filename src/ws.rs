//! API Gateway WebSocket event handler.
//!
//! Each inbound WS frame arrives here as an `ApiGatewayWebsocketProxyRequest`.
//! This module is transport-pure: it returns a [`WsOutcome`] describing what
//! the Lambda should do next. The binary (`src/bin/ws.rs`) owns the AWS SDK
//! client that actually posts replies back via the API Gateway Management API
//! — keeping this module testable without network calls.

use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequest;
use base64::Engine;

use crate::commands;
use crate::resp;
use crate::state::State;

/// What the caller should do with the result of handling one event.
#[derive(Debug)]
pub enum WsOutcome {
    /// Control-plane event ($connect, $disconnect) — nothing to send back.
    Empty,
    /// Data-plane event — post `data` back to `connection_id` via the
    /// API Gateway Management API.
    Reply { connection_id: String, data: Vec<u8> },
}

/// Route-key classification. API Gateway sets the route for $connect,
/// $disconnect, and $default (messages). Anything else is treated as a
/// message for forward compatibility.
fn is_control_route(route_key: Option<&str>) -> bool {
    matches!(route_key, Some("$connect" | "$disconnect"))
}

fn decode_body(body: Option<String>, is_base64: bool) -> Result<Vec<u8>, String> {
    let body = body.unwrap_or_default();
    if is_base64 {
        base64::engine::general_purpose::STANDARD.decode(body).map_err(|e| format!("base64 decode: {e}"))
    } else {
        Ok(body.into_bytes())
    }
}

/// Handle one WebSocket event.
///
/// Never returns an error for command-level failures — those are returned to
/// the client as a RESP `-ERR` reply. Only transport/infrastructure failures
/// (missing `connection_id`, invalid base64 body) produce `Err`, which the
/// binary converts to a 500 response.
pub async fn handle(state: &State, event: ApiGatewayWebsocketProxyRequest) -> Result<WsOutcome, String> {
    let ctx = event.request_context;

    if is_control_route(ctx.route_key.as_deref()) {
        return Ok(WsOutcome::Empty);
    }

    let connection_id = ctx.connection_id.ok_or_else(|| "missing connection_id in request context".to_string())?;

    let bytes = decode_body(event.body, event.is_base64_encoded)?;

    let reply = match resp::parse_command(&bytes) {
        Ok(argv) => commands::dispatch(state, argv).await,
        Err(e) => resp::RespReply::err(format!("PARSE {e}")),
    };

    let encoded = reply.encode().map_err(|e| format!("encode reply: {e}"))?;

    Ok(WsOutcome::Reply { connection_id, data: encoded.to_vec() })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aws_lambda_events::apigw::ApiGatewayWebsocketProxyRequestContext;

    use super::*;
    use crate::Settings;
    use crate::storage::InMemoryStorage;

    fn test_state() -> State {
        let settings = Settings {
            bucket: "test".to_string(),
            key_prefix: "t/".to_string(),
            aws_region: None,
            aws_endpoint_url: None,
            emf_namespace: None,
        };
        State::with_storage(settings, Arc::new(InMemoryStorage::new()))
    }

    fn ws_event(route: &str, body: Option<&str>, is_base64: bool) -> ApiGatewayWebsocketProxyRequest {
        let ctx = ApiGatewayWebsocketProxyRequestContext {
            route_key: Some(route.to_string()),
            connection_id: Some("conn-1".to_string()),
            domain_name: Some("example.execute-api.us-east-1.amazonaws.com".to_string()),
            stage: Some("prod".to_string()),
            ..ApiGatewayWebsocketProxyRequestContext::default()
        };
        ApiGatewayWebsocketProxyRequest {
            request_context: ctx,
            body: body.map(String::from),
            is_base64_encoded: is_base64,
            ..ApiGatewayWebsocketProxyRequest::default()
        }
    }

    #[tokio::test]
    async fn connect_route_returns_empty() {
        let state = test_state();
        let ev = ws_event("$connect", None, false);
        let out = handle(&state, ev).await.expect("ok");
        assert!(matches!(out, WsOutcome::Empty));
    }

    #[tokio::test]
    async fn disconnect_route_returns_empty() {
        let state = test_state();
        let ev = ws_event("$disconnect", None, false);
        let out = handle(&state, ev).await.expect("ok");
        assert!(matches!(out, WsOutcome::Empty));
    }

    #[tokio::test]
    async fn ping_yields_pong_reply() {
        let state = test_state();
        let ev = ws_event("$default", Some("*1\r\n$4\r\nPING\r\n"), false);
        let out = handle(&state, ev).await.expect("ok");
        match out {
            WsOutcome::Reply { connection_id, data } => {
                assert_eq!(connection_id, "conn-1");
                assert_eq!(&data, b"+PONG\r\n");
            }
            WsOutcome::Empty => panic!("expected reply"),
        }
    }

    #[tokio::test]
    async fn set_and_get_roundtrip_through_ws() {
        let state = test_state();

        let set_body = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        let set_out = handle(&state, ws_event("$default", Some(set_body), false)).await.expect("set ok");
        match set_out {
            WsOutcome::Reply { data, .. } => assert_eq!(&data, b"+OK\r\n"),
            WsOutcome::Empty => panic!("expected reply"),
        }

        let get_body = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let get_out = handle(&state, ws_event("$default", Some(get_body), false)).await.expect("get ok");
        match get_out {
            WsOutcome::Reply { data, .. } => assert_eq!(&data, b"$1\r\nv\r\n"),
            WsOutcome::Empty => panic!("expected reply"),
        }
    }

    #[tokio::test]
    async fn base64_body_is_decoded() {
        let state = test_state();
        // base64 of "*1\r\n$4\r\nPING\r\n"
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"*1\r\n$4\r\nPING\r\n");
        let out = handle(&state, ws_event("$default", Some(&b64), true)).await.expect("ok");
        match out {
            WsOutcome::Reply { data, .. } => assert_eq!(&data, b"+PONG\r\n"),
            WsOutcome::Empty => panic!("expected reply"),
        }
    }

    #[tokio::test]
    async fn malformed_body_returns_resp_error_not_http_failure() {
        let state = test_state();
        let out = handle(&state, ws_event("$default", Some("garbage"), false))
            .await
            .expect("should succeed at transport level");
        match out {
            WsOutcome::Reply { data, .. } => {
                assert!(data.starts_with(b"-"), "expected RESP error, got {:?}", String::from_utf8_lossy(&data));
            }
            WsOutcome::Empty => panic!("expected reply"),
        }
    }

    #[tokio::test]
    async fn unknown_command_returns_resp_error() {
        let state = test_state();
        let body = "*1\r\n$4\r\nNOPE\r\n";
        let out = handle(&state, ws_event("$default", Some(body), false)).await.expect("ok");
        match out {
            WsOutcome::Reply { data, .. } => {
                assert!(data.starts_with(b"-"));
                assert!(data.windows(3).any(|w| w == b"ERR"));
            }
            WsOutcome::Empty => panic!("expected reply"),
        }
    }

    #[tokio::test]
    async fn missing_connection_id_is_transport_error() {
        let state = test_state();
        let mut ev = ws_event("$default", Some("*1\r\n$4\r\nPING\r\n"), false);
        ev.request_context.connection_id = None;
        let err = handle(&state, ev).await.expect_err("should fail");
        assert!(err.contains("connection_id"));
    }
}
