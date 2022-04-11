use std::time::Duration;

use serde_json::from_str;
use tokio::sync::watch::Sender;
use tokio_stream::{Stream, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{error::Error as TError, error::ProtocolError, Message},
};

use super::{
    common::{handle_exchange_stream, OrderBook, SocketError},
    exchange_order_book::ExchangeOrderBook,
};

const BINANCE_WS_ENDPOINT: &str = "wss://stream.binance.com:9443/ws";

pub fn start_binance_task(symbol: String, sender: Sender<Option<ExchangeOrderBook>>) {
    tokio::spawn(async move {
        // this loop is responsible for reconnecting in case of error
        loop {
            // connect
            match binance_orderbook_stream(&symbol).await {
                Err(e) => {
                    eprintln!("Failed to connect to binance order book stream: {:?}", e);
                    // wait a bit before retrying
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    // TODO: exponential backoff?
                }
                Ok(stream) => {
                    handle_exchange_stream(
                        stream.map(|r| r.map(|o| (o, "binance"))),
                        &sender,
                        "binance",
                        Duration::from_secs(5),
                    )
                    .await;
                }
            }
        }
    });
}

/// Returns a stream of the top 10 bids and asks from binance or an error if the connection failed.
async fn binance_orderbook_stream(
    symbol: &str,
) -> Result<impl Stream<Item = Result<OrderBook, SocketError>>, TError> {
    let endpoint = format!(
        "{}/{}@depth10@100ms",
        BINANCE_WS_ENDPOINT,
        symbol.to_lowercase() // binance expects lower case symbol for websocket streams
    );
    let (ws_stream, _) = connect_async(endpoint).await?;

    Ok(ws_stream.filter_map(|maybe_msg| match maybe_msg {
        Ok(Message::Text(msg)) => {
            Some(from_str::<OrderBook>(&msg).map_err(|_| SocketError::Decode))
        }
        Ok(Message::Ping(_) | Message::Pong(_)) => None, // ignore ping and pong
        Ok(Message::Close(_))
        | Err(TError::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
            eprintln!("binance socket was closed. Restarting...");
            Some(Err(SocketError::Closed))
        }
        Err(e) => Some(Err(SocketError::Unexpected(Box::new(e)))),
        _ => {
            eprintln!("Unexpected message from binance {:?}", maybe_msg);
            None
        } // ignore other messages
    }))
}

#[cfg(test)]
mod test {
    use crate::exchange::common::Limit;

    use super::OrderBook;

    #[test]
    fn deserialize_orderbook_event() {
        // example from binance docs
        let json = serde_json::json!({
          "lastUpdateId": 160,  // Last update ID
          "bids": [             // Bids to be updated
            [
              "0.0024",         // Price level to be updated
              "10"              // Quantity
            ]
          ],
          "asks": [             // Asks to be updated
            [
              "0.0026",         // Price level to be updated
              "100"            // Quantity
            ]
          ]
        });
        // should be deserialized without error
        let event = serde_json::from_value::<OrderBook>(json).unwrap();

        assert_eq!(event.asks.len(), 1, "only one ask should be present");
        assert_eq!(
            event.bids[0],
            Limit {
                price: 0.0024f64,
                qty: 10f64
            },
            "the bid should be decoded correctly"
        )
    }
}
