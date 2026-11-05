//! A token bucket on the posted-body validates (DA-API-20): each one's verdicts tell a little
//! about the node's variables, so they are spent at a bounded rate, node-wide.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Posted-body validates taken at once before the rate applies.
pub const VALIDATE_BURST: u32 = 5;
/// Posted-body validates a node takes per second, once the burst is spent.
pub const VALIDATE_PER_SECOND: u32 = 1;

/// A token bucket: `burst` tokens, refilled at `per_second`.
#[derive(Debug)]
pub struct RateLimit {
    burst: f64,
    per_second: f64,
    /// The tokens left, as of the instant.
    state: Mutex<(f64, Instant)>,
}

impl RateLimit {
    /// Full: `burst` takes succeed at once.
    pub fn new(burst: u32, per_second: u32) -> Self {
        Self {
            burst: f64::from(burst),
            per_second: f64::from(per_second),
            state: Mutex::new((f64::from(burst), Instant::now())),
        }
    }

    /// The posted-body validates' limit: [`VALIDATE_BURST`] at [`VALIDATE_PER_SECOND`].
    pub fn validate() -> Self {
        Self::new(VALIDATE_BURST, VALIDATE_PER_SECOND)
    }

    /// Takes a token at `now`; `Err` with the wait until one is there.
    ///
    /// # Errors
    /// The bucket is empty: the wait until the next token.
    pub fn take(&self, now: Instant) -> Result<(), Duration> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (tokens, at) = *state;
        let refilled = now.saturating_duration_since(at).as_secs_f64() * self.per_second;
        let tokens = (tokens + refilled).min(self.burst);
        if tokens >= 1.0 {
            *state = (tokens - 1.0, now);
            Ok(())
        } else {
            *state = (tokens, now);
            Err(Duration::from_secs_f64((1.0 - tokens) / self.per_second))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_burst_is_taken_at_once_then_the_rate_applies() {
        let limit = RateLimit::new(5, 1);
        let t0 = Instant::now();
        for _ in 0..5 {
            limit.take(t0).unwrap();
        }
        let wait = limit.take(t0).unwrap_err();
        assert_eq!(wait, Duration::from_secs(1));
        let wait = limit.take(t0 + Duration::from_millis(400)).unwrap_err();
        assert!(
            wait > Duration::from_millis(590) && wait <= Duration::from_millis(600),
            "{wait:?}"
        );
        limit.take(t0 + Duration::from_secs(1)).unwrap();
        limit.take(t0 + Duration::from_secs(1)).unwrap_err();
        for _ in 0..5 {
            limit.take(t0 + Duration::from_secs(60)).unwrap();
        }
        limit
            .take(t0 + Duration::from_secs(60))
            .expect_err("refilled to the burst, no more");
    }
}
