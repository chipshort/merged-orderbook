use std::{fmt::Debug, time::Duration};
use tokio::{select, sync::watch};
use tokio_stream::{Stream, StreamExt};

use super::exchange_order_book::ExchangeOrderBook;

/// This function is used to process a stream of exchange orderbooks.
/// It will send the appropriate values to the given sender.
/// When this function returns, you should try to reconnect
pub async fn handle_exchange_stream<T, E, S>(
    mut stream: S,
    sender: &watch::Sender<Option<ExchangeOrderBook>>,
) where
    T: Into<ExchangeOrderBook> + Debug,
    E: Debug,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    // this loop is responsible for reading the stream
    loop {
        // this select implements a timeout for the next message.
        // This is needed in case of network connectivity loss
        select! {
            maybe_msg = stream.next() => {
                match maybe_msg {
                    Some(Ok(order_book)) => {
                        sender
                            .send(Some(order_book.into()))
                            .expect("websocket orderbook receiver should never be dropped");
                    }
                    e => {
                        eprintln!("Binance stream errored: {:?}", e);
                        break; // leave loop, caller will have to reconnect
                    }
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                eprintln!("Binance websocket timeout");
                break; // leave loop, caller will have to reconnect
            },
        };
    }
    // set value to None, so that the merge task does not work with stale data
    // and reconnect in next loop iteration
    sender
        .send(None)
        .expect("websocket orderbook receiver should never be dropped");
}
