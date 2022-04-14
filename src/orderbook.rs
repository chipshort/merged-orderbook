use tokio::{select, sync::watch};

use crate::{
    api::{self, Level},
    exchange::exchange_order_book::ExchangeOrderBook,
};

/// Merges two already sorted lists, keeping only `first_n` items.
/// `less_than` can be a strict order (<) or a partial order (<=).
/// ```rust
/// assert_eq!(merged_orderbook::orderbook::merge(&[0, 1, 6, 8], &[2, 3, 7], |a, b| a < b, 10), vec![0, 1, 2, 3, 6, 7, 8]);
/// ```
pub fn merge<T: Clone>(
    a: &[T],
    b: &[T],
    less_than: impl Fn(&T, &T) -> bool,
    first_n: usize,
) -> Vec<T> {
    // handle empty inputs
    if a.len() == 0 {
        return b.to_vec();
    }
    if b.len() == 0 {
        return a.to_vec();
    }

    let mut i = 0;
    let mut j = 0;
    let mut sorted = Vec::with_capacity(first_n);

    loop {
        if sorted.len() == first_n {
            // only interested in the top elements, so stop merging after we have them
            break;
        }
        if less_than(&a[i], &b[j]) {
            sorted.push(a[i].clone());
            i += 1;
            if i == a.len() {
                // done with a, push rest of b to the end
                let remaining_end = (j + first_n - sorted.len()).min(b.len());
                if remaining_end < j {
                    // no remaining elements
                    break;
                }
                let rest = &b[j..remaining_end];
                for elem in rest {
                    sorted.push(elem.clone());
                }
                break;
            }
        } else {
            sorted.push(b[j].clone());
            j += 1;
            if j == b.len() {
                // done with b, push the rest of a to the end
                let remaining_end = (i + first_n - sorted.len()).min(a.len());
                if remaining_end < i {
                    // no remaining elements
                    break;
                }
                let remaining = &a[i..remaining_end];
                for elem in remaining {
                    sorted.push(elem.clone());
                }
                break;
            }
        }
    }

    sorted
}

pub fn calculate_spread(asks: &[Level], bids: &[Level]) -> Option<f64> {
    let best_ask = asks.first()?.price;
    let best_bid = bids.first()?.price;

    Some(best_ask - best_bid)
}

/// Spawns a task that merges the exchange orderbooks from two receivers and sends the result to the given sender.
pub fn start_merge_task(
    mut exchange0: watch::Receiver<Option<ExchangeOrderBook>>,
    mut exchange1: watch::Receiver<Option<ExchangeOrderBook>>,
    sender: watch::Sender<Option<api::Summary>>,
    mut shutdown: watch::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            // wait for at least one of the receivers to get a new value, or stop if shutdown signal is received
            select! {
                _ = exchange0.changed() => {},
                _ = exchange1.changed() => {},
                _ = shutdown.changed() => {
                    return;
                }
            };

            let (asks, bids) = {
                let orderbook0 = exchange0.borrow();
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
                (asks, bids)
            };

            let spread = calculate_spread(&asks, &bids);

            // The receiver for this sender can only be dropped after this is no longer called, in case of shutdown.
            // Before the receiver is dropped, this task has to stop and drop it's `shutdown` receiver.
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

#[cfg(test)]
mod test {
    use crate::orderbook::merge;

    #[test]
    fn empty_remaining() {
        assert_eq!(
            merge(&[0, 2, 4, 6], &[1, 3, 5, 7], |a, b| a < b, 8),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn correct_length() {
        assert_eq!(
            merge(&[0, 2], &[3, 6, 8], |a, b| a < b, 8),
            vec![0, 2, 3, 6, 8]
        );

        assert_eq!(
            merge(&[0, 2, 8], &[3, 6, 7, 9, 27], |a, b| a < b, 5),
            vec![0, 2, 3, 6, 7]
        );

        assert_eq!(
            merge(&[0, 6, 8], &[4, 6, 7, 9, 27], |a, b| a < b, 2),
            vec![0, 4]
        );
    }

    #[test]
    fn generate_many() {
        let a = vec![
            2, 4, 6, 7, 8, 9, 12, 36, 57, 90, 123, 4576, 5786, 6789, 9809,
        ];
        let b = vec![
            3, 4, 9, 10, 76, 87, 97, 99, 123, 156, 257, 546, 587, 690, 852, 946, 1798,
        ];

        for i in 0..a.len() {
            let a = &a[0..i];
            for j in 0..b.len() {
                let b = &b[0..j];

                let merged = merge(a, b, |a, b| a < b, a.len() + b.len());
                for (item, next) in merged.iter().zip(merged.iter().skip(1)) {
                    assert!(item <= next);
                }

                let merged = merge(b, a, |a, b| a < b, a.len() + b.len());
                for (item, next) in merged.iter().zip(merged.iter().skip(1)) {
                    assert!(item <= next);
                }
            }
        }
    }
}
