use async_trait::async_trait;
use clap::Parser;

use log::*;
use merged_orderbook::api;
use merged_orderbook::api::orderbook_aggregator_server::{
    OrderbookAggregator, OrderbookAggregatorServer,
};
use merged_orderbook::exchange::binance::start_binance_task;
use merged_orderbook::exchange::bitstamp::start_bitstamp_task;
use merged_orderbook::orderbook::start_merge_task;
use std::net::SocketAddr;
use std::{error::Error, pin::Pin};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    // setup cli
    let args: Cli = Cli::parse();
    let addr = args
        .listen
        .unwrap_or_else(|| "[::1]:50051".parse().unwrap());

    // start exchange websocket tasks
    let (binance_sender, binance_receiver) = watch::channel(None);
    let (bitstamp_sender, bitstamp_receiver) = watch::channel(None);
    start_binance_task(args.symbol.clone(), binance_sender);
    start_bitstamp_task(args.symbol.clone(), bitstamp_sender);

    // start task for merging both exchange orderbooks
    let (orderbook_sender, orderbook_receiver) = watch::channel(None);
    start_merge_task(binance_receiver, bitstamp_receiver, orderbook_sender);

    // start gRPC server
    let aggregator = Aggregator {
        receiver: orderbook_receiver,
    };
    info!("Starting gRPC Server...");
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
