use std::cmp::Ordering;

use crate::api;

/// Represents the order book of one exchange with correctly sorted bids and asks
#[derive(Debug)]
pub struct ExchangeOrderBook {
    asks: Vec<api::Level>,
    bids: Vec<api::Level>,
}

impl ExchangeOrderBook {
    /// Creates a new ExchangeOrderBook.
    pub fn new(asks: Vec<api::Level>, bids: Vec<api::Level>) -> Self {
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
