mod request_handler;
mod server;
use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    server::start_server("127.0.0.1:9092").await?;
    Ok(())
}
