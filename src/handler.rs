use lambda_http::{Body, Error, Request, Response};
use tracing::{error, info};

use crate::commands;
use crate::resp;
use crate::state::State;

pub async fn handle(state: State, event: Request) -> Result<Response<Body>, Error> {
    let body_bytes = match event.body() {
        Body::Empty => Vec::new(),
        Body::Text(s) => s.as_bytes().to_vec(),
        Body::Binary(b) => b.clone(),
    };

    let argv = match resp::parse_command(&body_bytes) {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "resp parse failed");
            return error_response(&format!("PARSE {e}"));
        }
    };

    if argv.is_empty() {
        return error_response("empty command");
    }

    info!(argc = argv.len(), "dispatching command");
    let reply = commands::dispatch(&state, argv).await;

    let encoded = reply.encode().map_err(|e| Error::from(format!("encode: {e}")))?;

    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/resp")
        .body(Body::Binary(encoded.to_vec()))?)
}

fn error_response(msg: &str) -> Result<Response<Body>, Error> {
    let reply = crate::resp::RespReply::err(msg);
    let encoded = reply.encode().map_err(|e| Error::from(format!("encode: {e}")))?;
    Ok(Response::builder()
        .status(400)
        .header("content-type", "application/resp")
        .body(Body::Binary(encoded.to_vec()))?)
}
