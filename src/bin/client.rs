use merged_orderbook::api;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let mut client =
        api::orderbook_aggregator_client::OrderbookAggregatorClient::connect("http://[::1]:50051")
            .await
            .unwrap();

    loop {
        if let Ok(r) = client.book_summary(api::Empty {}).await {
            let mut stream = r.into_inner();
            while let Some(item) = stream.next().await {
                println!("{:?}", item);
            }
        }
    }
}
