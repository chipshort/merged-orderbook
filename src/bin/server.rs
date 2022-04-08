use async_trait::async_trait;
use clap::Parser;

use merged_orderbook::api;
use merged_orderbook::api::orderbook_aggregator_server::{
    OrderbookAggregator, OrderbookAggregatorServer,
};
use merged_orderbook::exchange::binance::start_binance_task;
use merged_orderbook::exchange::exchange_order_book::ExchangeOrderBook;
use merged_orderbook::orderbook::{calculate_spread, merge};
use std::net::SocketAddr;
use std::{error::Error, pin::Pin};
use tokio::select;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Status};

struct Aggregator {
    receiver: watch::Receiver<Option<api::Summary>>,
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<api::Summary, Status>> + Send>>;

#[async_trait]
impl OrderbookAggregator for Aggregator {
    type BookSummaryStream = ResponseStream;

    async fn book_summary(
        &self,
        _request: tonic::Request<api::Empty>,
    ) -> Result<tonic::Response<Self::BookSummaryStream>, tonic::Status> {
        // every listener gets their own receiver
        let receiver = self.receiver.clone();
        let stream = WatchStream::new(receiver)
            .filter_map(|orderbook| orderbook)
            .map(|orderbook| Ok(orderbook));
        Ok(tonic::Response::new(Box::pin(stream)))
        // Err(tonic::Status::aborted(""))
    }
}

/// Spawns a task that merges the exchange orderbooks from two receivers and sends the result to the given sender.
fn start_merge_task(
    mut exchange0: watch::Receiver<Option<ExchangeOrderBook>>,
    mut exchange1: watch::Receiver<Option<ExchangeOrderBook>>,
    sender: watch::Sender<Option<api::Summary>>,
) {
    tokio::spawn(async move {
        loop {
            // wait for at least one of the receivers to get a new value
            select! {
                _ = exchange0.changed() => {}, // TODO: handle result, log error
                _ = exchange1.changed() => {}
            };

            let orderbook0 = exchange0.borrow(); // TODO: borrow_and_update?
            let orderbook1 = exchange1.borrow();

            // get asks and bids of both orderbook
            let asks0 = orderbook0.as_ref().map(|o| o.asks()).unwrap_or(&[]);
            let bids0 = orderbook0.as_ref().map(|o| o.bids()).unwrap_or(&[]);
            let asks1 = orderbook1.as_ref().map(|o| o.asks()).unwrap_or(&[]);
            let bids1 = orderbook1.as_ref().map(|o| o.bids()).unwrap_or(&[]);

            // extract top 10 asks / bids
            let asks = merge(
                asks0,
                asks1,
                |a1, a2| a1.price < a2.price, // asks are ordered from low to high
                10,
            );
            let bids = merge(
                bids0,
                bids1,
                |b1, b2| b1.price > b2.price, // bids from high to low
                10,
            );
            let spread = calculate_spread(&asks, &bids);

            sender
                .send(Some(api::Summary {
                    asks,
                    bids,
                    spread: spread.unwrap_or(f64::MAX),
                }))
                .expect("merge receiver should never be dropped");
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // setup cli
    let args: Cli = Cli::parse();
    let addr = args
        .listen
        .unwrap_or_else(|| "[::1]:50051".parse().unwrap());

    // start exchange websocket tasks
    let (binance_sender, binance_receiver) = watch::channel(None);
    let (bitstamp_sender, bitstamp_receiver) = watch::channel(None);
    start_binance_task(args.symbol.clone(), binance_sender);
    start_binance_task(args.symbol.clone(), bitstamp_sender); // TODO: implement bitstamp task instead

    // start task for merging both exchange orderbooks
    let (orderbook_sender, orderbook_receiver) = watch::channel(None);
    start_merge_task(binance_receiver, bitstamp_receiver, orderbook_sender);

    // start gRPC server
    let aggregator = Aggregator {
        receiver: orderbook_receiver,
    };
    println!("Starting gRPC Server...");
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(aggregator))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// The socket address and port to listen on (default: "[::1]:50051").
    #[clap(short, long)]
    listen: Option<SocketAddr>,

    /// The merged orderbook of this symbol will be provided by the gRPC server.
    symbol: String,
}
