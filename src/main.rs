use lambda_http::{Error, run, service_fn};

#[tokio::main]
async fn main() -> Result<(), Error> {
    rustyant::init_tracing();
    let state = rustyant::State::from_env().await?;
    run(service_fn(move |event| {
        let state = state.clone();
        async move { rustyant::handler::handle(state, event).await }
    }))
    .await
}
