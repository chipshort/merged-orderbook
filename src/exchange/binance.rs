use std::{error::Error, time::Duration};

use crate::api::*;
use serde::Deserialize;
use serde_json::from_str;
use tokio::sync::watch::Sender;
use tokio_stream::{Stream, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{error::Error as TError, error::ProtocolError, Message},
};

use super::{common::handle_exchange_stream, exchange_order_book::ExchangeOrderBook};

const BINANCE_WS_ENDPOINT: &str = "wss://stream.binance.com:9443/ws";

#[derive(Debug)]
pub enum BinanceSocketError {
    Decode,
    Closed,
    Unexpected(Box<dyn Error>),
}

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
                    handle_exchange_stream(stream, &sender).await;
                }
            }
        }
    });
}

/// Returns a stream of the top 10 bids and asks from binance or an error if the connection failed.
pub async fn binance_orderbook_stream(
    symbol: &str,
) -> Result<impl Stream<Item = Result<OrderBook, BinanceSocketError>>, TError> {
    let endpoint = format!(
        "{}/{}@depth10@100ms",
        BINANCE_WS_ENDPOINT,
        symbol.to_lowercase() // binance expects lower case symbol for websocket streams
    );
    let (ws_stream, _) = connect_async(endpoint).await?;

    Ok(ws_stream.filter_map(|maybe_msg| match maybe_msg {
        Ok(Message::Text(msg)) => {
            Some(from_str::<OrderBook>(&msg).map_err(|_| BinanceSocketError::Decode))
        }
        Ok(Message::Ping(_) | Message::Pong(_)) => None, // ignore ping and pong
        Ok(Message::Close(_))
        | Err(TError::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
            println!("binance socket was closed. Restarting...");
            Some(Err(BinanceSocketError::Closed))
        }
        Err(e) => Some(Err(BinanceSocketError::Unexpected(Box::new(e)))),
        _ => {
            println!("Unexpected message from binance {:?}", maybe_msg);
            None
        } // ignore other messages
    }))
}

#[derive(Debug, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<Limit>,
    pub asks: Vec<Limit>,
    // unnecessary fields omitted
}

#[derive(PartialEq, Debug)]
pub struct Limit {
    pub price: f64,
    pub qty: f64,
}

impl<'de> Deserialize<'de> for Limit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut vec = Vec::<String>::deserialize(deserializer)?;

        let len = vec.len();
        let invalid_length =
            || serde::de::Error::invalid_length(len, &"a vec of length 2 was expected");
        let invalid_value = |v: &str| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(v),
                &"a valid f64 string was expected",
            )
        };

        let qty = vec.pop().ok_or_else(invalid_length)?;
        let price = vec.pop().ok_or_else(invalid_length)?;

        let qty = qty.parse::<f64>().map_err(|_| invalid_value(&qty))?;
        let price = price.parse::<f64>().map_err(|_| invalid_value(&price))?;
        Ok(Limit { price, qty })
    }
}

impl Into<Level> for Limit {
    fn into(self) -> Level {
        Level {
            price: self.price,
            amount: self.qty,
            exchange: "binance".to_string(),
        }
    }
}

impl Into<ExchangeOrderBook> for OrderBook {
    fn into(self) -> ExchangeOrderBook {
        let asks: Vec<Level> = self.asks.into_iter().map(|ask| ask.into()).collect();
        let bids: Vec<Level> = self.bids.into_iter().map(|bid| bid.into()).collect();

        ExchangeOrderBook::new(asks, bids)
    }
}

#[cfg(test)]
mod test {
    use crate::{api, exchange::exchange_order_book::ExchangeOrderBook};

    use super::{Limit, OrderBook};

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

    #[test]
    fn convert_order_book() {
        let order_book = OrderBook {
            asks: vec![Limit {
                qty: 0.5,
                price: 10.6,
            }],
            bids: vec![Limit {
                qty: 1.56,
                price: 9.4,
            }],
        };

        let converted: ExchangeOrderBook = order_book.into();
        let asks = converted.asks();
        assert_eq!(
            asks[0],
            api::Level {
                amount: 0.5,
                price: 10.6,
                exchange: "binance".to_string()
            }
        );
    }
}
