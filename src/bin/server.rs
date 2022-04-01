use async_stream::stream;
use async_trait::async_trait;

use merged_orderbook::api;
use merged_orderbook::api::orderbook_aggregator_server::{
    OrderbookAggregator, OrderbookAggregatorServer,
};
use std::{error::Error, pin::Pin};
use tokio::select;
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Status};

#[derive(Default)]
struct Aggregator;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<api::Summary, Status>> + Send>>;

#[async_trait]
impl OrderbookAggregator for Aggregator {
    type BookSummaryStream = ResponseStream;

    async fn book_summary(
        &self,
        _request: tonic::Request<api::Empty>,
    ) -> Result<tonic::Response<Self::BookSummaryStream>, tonic::Status> {
        // TODO: actually return correct stream
        // let stream = Box::pin(tokio_stream::pending::<Result<api::Summary, _>>());

        let mut s1 = Box::pin(stream! {
            let mut i = 0f64;
            loop {
                yield i;
                i += 1f64;
            }
        });
        let mut s2 = Box::pin(stream! {
            let mut i = 0f64;
            loop {
                yield i;
                i -= 1f64;
            }
        });

        let combined = stream! {
            loop {
                select! {
                    Some(item) = s1.next() => {
                        yield Ok(api::Summary {
                            spread: item,
                            asks: vec![],
                            bids: vec![]
                        });
                    },
                    Some(item) = s2.next() => {
                        yield Ok(api::Summary {
                            spread: item,
                            asks: vec![],
                            bids: vec![]
                        });
                    }
                }
            }
        };

        Ok(tonic::Response::new(Box::pin(combined)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "[::1]:50051".parse()?;
    let aggregator = Aggregator::default();

    println!("Starting gRPC Server...");
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(aggregator))
        .serve(addr)
        .await?;

    Ok(())
}
