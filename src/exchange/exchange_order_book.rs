use std::cmp::Ordering;

use crate::api;

/// Represents the order book of one exchange with correctly sorted bids and asks
#[derive(Debug)]
pub struct ExchangeOrderBook {
    asks: Vec<api::Level>,
    bids: Vec<api::Level>,
}

impl ExchangeOrderBook {
    /// Creates a new ExchangeOrderBook, making sure to sort the levels correctly.
    pub fn new(mut asks: Vec<api::Level>, mut bids: Vec<api::Level>) -> Self {
        // I expect asks and bids to be sorted already, so the timsort variation used in sort_by will perform well,
        // asks should be sorted from low to high, bids from high to low
        asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .expect("json values should never be NaN")
        });
        bids.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .expect("json values should never be NaN")
        });

        Self { asks, bids }
    }

    /// Provides read-only access to the asks of this order book. Sorted by price from low to high.
    #[inline]
    pub fn asks(&self) -> &[api::Level] {
        &self.asks
    }

    /// Provides read-only access to the bids of this order book. Sorted by price from high to low.
    #[inline]
    pub fn bids(&self) -> &[api::Level] {
        &self.bids
    }
}
