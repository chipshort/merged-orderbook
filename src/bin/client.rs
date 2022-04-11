use clap::Parser;
use merged_orderbook::api;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let args: Cli = Cli::parse();
    let endpoint = args
        .endpoint
        .unwrap_or_else(|| "http://[::1]:50051".parse().unwrap());

    loop {
        let mut client = match api::orderbook_aggregator_client::OrderbookAggregatorClient::connect(
            endpoint.clone(),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                eprintln!("Error connecting to orderbook aggregator {}", e);
                continue;
            }
        };

        if let Ok(r) = client.book_summary(api::Empty {}).await {
            let mut stream = r.into_inner();
            while let Some(item) = stream.next().await {
                println!("{:?}", item);
            }
        }
    }
}

#[derive(Parser)]
#[clap(author, version, about = "A simple client that just calls the accompanying server", long_about = None)]
pub struct Cli {
    /// The endpoint to connect to (default: "http://[::1]:50051").
    endpoint: Option<tonic::transport::Endpoint>,
}
