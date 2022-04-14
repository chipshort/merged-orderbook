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

    // one channel to shutdown exchange websockets
    let (shutdown_ws_tx, shutdown_ws_rx) = watch::channel(());
    // one channel to shutdown merge task
    let (shutdown_merge_tx, shutdown_merge_rx) = watch::channel(());

    // start exchange websocket tasks
    let (binance_sender, binance_receiver) = watch::channel(None);
    let (bitstamp_sender, bitstamp_receiver) = watch::channel(None);
    start_binance_task(args.symbol.clone(), binance_sender, shutdown_ws_rx.clone());
    start_bitstamp_task(args.symbol.clone(), bitstamp_sender, shutdown_ws_rx); // not cloning because no receiver should be left over

    // start task for merging both exchange orderbooks
    let (orderbook_sender, orderbook_receiver) = watch::channel(None);
    start_merge_task(
        binance_receiver,
        bitstamp_receiver,
        orderbook_sender,
        shutdown_merge_rx, // not cloning here, see above
    );

    // start gRPC server
    let aggregator = Aggregator {
        receiver: orderbook_receiver,
    };
    info!("Starting gRPC Server...");
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(aggregator))
        .serve_with_shutdown(addr, async {
            // wait for SIGINT
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!("Failed to listen for shutdown signal {}", e);
            }
            // in any case, end running tasks using shutdown listeners

            // shutdown websockets first, because they rely on their receiver not being dropped
            // (which would be the case if the merge task was dropped first)
            shutdown_ws_tx
                .send(())
                .expect("could not send shutdown message to exchange websockets");
            // wait for exchange websocket tasks to end
            shutdown_ws_tx.closed().await;

            // shutdown merge task now
            shutdown_merge_tx
                .send(())
                .expect("could not send shutdown message");

            // wait for task to end
            shutdown_merge_tx.closed().await;
        })
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
