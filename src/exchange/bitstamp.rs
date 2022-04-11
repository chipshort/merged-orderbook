use std::{error::Error, time::Duration};

use futures_util::SinkExt;
use serde::Deserialize;
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

const BITSTAMP_WS_ENDPOINT: &str = "wss://ws.bitstamp.net";

pub fn start_bitstamp_task(symbol: String, sender: Sender<Option<ExchangeOrderBook>>) {
    tokio::spawn(async move {
        // this loop is responsible for reconnecting in case of error
        loop {
            // connect
            match bitstamp_orderbook_stream(&symbol).await {
                Err(e) => {
                    eprintln!("Failed to connect to bitstamp order book stream: {:?}", e);
                    // wait a bit before retrying
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    // TODO: exponential backoff?
                }
                Ok(stream) => {
                    handle_exchange_stream(
                        stream.map(|r| r.map(|o| (o, "bitstamp"))),
                        &sender,
                        "bitstamp",
                        Duration::from_secs(10),
                    )
                    .await;
                }
            }
        }
    });
}

/// Returns a stream of the top 10 bids and asks from bitstamp or an error if the connection failed.
pub async fn bitstamp_orderbook_stream(
    symbol: &str,
) -> Result<impl Stream<Item = Result<OrderBook, SocketError>>, TError> {
    let (mut ws_stream, _) = connect_async(BITSTAMP_WS_ENDPOINT).await?;

    let channel_name = format!("order_book_{}", symbol.to_lowercase());
    // subscribe to order book
    let subscription = format!(
        "{{\"event\": \"bts:subscribe\", \"data\": {{ \"channel\": \"{}\" }} }}",
        channel_name
    );
    ws_stream.send(Message::text(subscription)).await?;

    Ok(ws_stream.filter_map(move |maybe_msg| {
        match maybe_msg {
            Ok(Message::Text(msg)) => match from_str::<BitstampMsg>(&msg) {
                Ok(BitstampMsg::Data { channel, data }) => {
                    // make sure the channel is the one we expect
                    if channel != channel_name {
                        eprintln!(
                            "Got bitstamp data with an unexpected channel name: {}",
                            channel
                        );
                        None
                    } else {
                        Some(Ok(data))
                    }
                }
                Ok(BitstampMsg::Reconnect) => Some(Err(SocketError::Closed)),
                Ok(_) => None, // ignore other messages
                Err(_) => {
                    println!("{}", msg);
                    Some(Err(SocketError::Decode))
                }
            },
            Ok(Message::Ping(_) | Message::Pong(_)) => None, // ignore ping and pong
            Ok(Message::Close(_))
            | Err(TError::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                println!("bitstamp socket was closed. Restarting...");
                Some(Err(SocketError::Closed))
            }
            Err(e) => Some(Err(SocketError::Unexpected(Box::new(e)))),
            _ => {
                println!("Unexpected message from bitstamp {:?}", maybe_msg);
                None
            } // ignore other messages
        }
    }))
}

#[derive(Deserialize, Debug)]
#[serde(tag = "event")]
enum BitstampMsg {
    #[serde(rename = "bts:request_reconnect")]
    Reconnect,
    #[serde(rename = "bts:subscription_succeeded")]
    SubscriptionSucceeded { channel: String },
    #[serde(rename = "data")]
    Data { data: OrderBook, channel: String },
}

#[derive(Deserialize, Debug)]
enum BitstampData {
    Str(String),
    Data(OrderBook),
}

#[cfg(test)]
mod test {
    use crate::exchange::{bitstamp::BitstampMsg, common::Limit};

    #[test]
    fn deserialize_orderbook_event() {
        // example from bitstamp websocket stream
        let json = serde_json::json!({
          "data": {
            "timestamp": "1649456282",
            "microtimestamp": "1649456282391896",
            "bids": [
              [
                "42391.82",
                "0.14340182"
              ],
              [
                "42389.30",
                "0.14718279"
              ]
            ],
            "asks": [
              [
                "42411.00",
                "0.14689979"
              ],
              [
                "42417.28",
                "0.05415000"
              ]
            ]
          },
          "channel": "order_book_btcusd",
          "event": "data"
        });
        // should be deserialized without error
        let event = serde_json::from_value::<BitstampMsg>(json).unwrap();

        if let BitstampMsg::Data { data, .. } = event {
            assert_eq!(data.asks.len(), 2, "two asks should be present");
            assert_eq!(
                data.bids[0],
                Limit {
                    price: 42391.82f64,
                    qty: 0.14340182f64
                },
                "the bid should be decoded correctly"
            )
        } else {
            panic!("Did not parse bitstamp message correctly");
        }
    }
}