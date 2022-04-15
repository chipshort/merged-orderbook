use log::error;
use serde::Deserialize;
use std::{error::Error, fmt::Debug, time::Duration};
use tokio::{select, sync::watch};
use tokio_stream::{Stream, StreamExt};

use crate::api::*;

use super::exchange_order_book::ExchangeOrderBook;

#[derive(Debug)]
pub enum SocketError {
    Decode,
    Closed,
    Unexpected(Box<dyn Error>),
}

/// This function is used to process a stream of exchange orderbooks.
/// It will send the appropriate values to the given sender.
/// When this function returns, you should try to reconnect
pub async fn handle_exchange_stream<E, S>(
    mut stream: S,
    sender: &watch::Sender<Option<ExchangeOrderBook>>,
    exchange_name: &'static str,
    timeout_after: Duration,
) where
    E: Debug,
    S: Stream<Item = Result<OrderBook, E>> + Unpin,
{
    // this loop is responsible for reading the stream
    loop {
        // this select implements a timeout for the next message.
        // This is needed in case of network connectivity loss
        select! {
            maybe_msg = stream.next() => {
                match maybe_msg {
                    Some(Ok(mut order_book)) => {
                        order_book.only_top(10);
                        sender
                            .send(Some((order_book, exchange_name).into()))
                            .expect("websocket orderbook receiver should never be dropped");
                    }
                    e => {
                        error!("{} stream errored: {:?}", exchange_name, e);
                        break; // leave loop, caller will have to reconnect
                    }
                }
            },
            _ = tokio::time::sleep(timeout_after) => {
                error!("{} websocket timeout", exchange_name);
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

// both binance and bitstamp messages have mostly the same structure, at least for the fields I am interested in
#[derive(Debug, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<Limit>,
    pub asks: Vec<Limit>,
    // unnecessary fields omitted
}

impl OrderBook {
    /// Sorts the bids and asks, such that the best price is on top.
    pub fn sort(&mut self) {
        // I expect asks and bids to be sorted already, so the timsort variation used in sort_by will perform well,
        // asks should be sorted from low to high, bids from high to low
        self.asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .expect("json values should never be NaN")
        });
        self.bids.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .expect("json values should never be NaN")
        });
    }

    /// Sorts the bids and asks and keeps only the top `n`.
    pub fn only_top(&mut self, n: usize) {
        self.sort();
        self.bids.drain(n.min(self.bids.len())..self.bids.len());
        self.asks.drain(n.min(self.asks.len())..self.asks.len());
    }
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

impl Into<Level> for (Limit, &'static str) {
    fn into(self) -> Level {
        Level {
            price: self.0.price,
            amount: self.0.qty,
            exchange: self.1.to_string(),
        }
    }
}

impl Into<ExchangeOrderBook> for (OrderBook, &'static str) {
    fn into(self) -> ExchangeOrderBook {
        let asks: Vec<Level> = self
            .0
            .asks
            .into_iter()
            .map(|ask| (ask, self.1).into())
            .collect();
        let bids: Vec<Level> = self
            .0
            .bids
            .into_iter()
            .map(|bid| (bid, self.1).into())
            .collect();

        ExchangeOrderBook::new(asks, bids)
    }
}

#[cfg(test)]
mod test {
    use crate::{api, exchange::exchange_order_book::ExchangeOrderBook};

    use super::{Limit, OrderBook};

    #[test]
    fn convert_orderbook() {
        let orderbook = OrderBook {
            asks: vec![Limit {
                qty: 0.5,
                price: 10.6,
            }],
            bids: vec![Limit {
                qty: 1.56,
                price: 9.4,
            }],
        };

        let converted: ExchangeOrderBook = (orderbook, "binance").into();
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

    #[test]
    fn orderbook_top_works_for_less() {
        let mut orderbook = OrderBook {
            asks: vec![Limit {
                qty: 0.78,
                price: 12.6,
            }],
            bids: vec![Limit {
                qty: 1.56,
                price: 7.4,
            }],
        };

        orderbook.only_top(10);
        assert_eq!(orderbook.asks.len(), 1);
        assert_eq!(orderbook.bids.len(), 1);
    }

    #[test]
    fn orderbook_top_works_for_more() {
        let mut orderbook = OrderBook {
            asks: vec![
                Limit {
                    qty: 0.78,
                    price: 12.6,
                },
                Limit {
                    qty: 0.67,
                    price: 1.7,
                },
            ],
            bids: vec![
                Limit {
                    qty: 1.56,
                    price: 7.4,
                },
                Limit {
                    qty: 6.7,
                    price: 3.4,
                },
            ],
        };

        orderbook.only_top(1);
        assert_eq!(orderbook.asks.len(), 1);
        assert_eq!(orderbook.bids.len(), 1);
    }
}
