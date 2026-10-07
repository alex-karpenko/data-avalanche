//! Exponential backoff with jitter (Data-Flow §Error Paths: initial 1s, max 60s, jitter).

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            first: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

impl Backoff {
    /// The wait before retry `attempt` (0 = the first retry): `first · 2^attempt`, capped at `max`,
    /// then a random point in its upper half, so workers that failed together don't retry together.
    pub fn delay(&self, attempt: u32) -> Duration {
        let ceiling = self
            .first
            .saturating_mul(1u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX))
            .min(self.max);
        let half = ceiling / 2;
        let nanos = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        let jitter = match nanos {
            0 => 0,
            n => getrandom::u64().expect("the OS random number generator is unavailable") % (n + 1),
        };
        half + Duration::from_nanos(jitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_double_up_to_the_cap_with_jitter_in_the_upper_half() {
        let b = Backoff::default();
        for (attempt, ceiling) in [(0, 1), (1, 2), (2, 4), (5, 32), (6, 60), (40, 60)] {
            let ceiling = Duration::from_secs(ceiling);
            for _ in 0..20 {
                let d = b.delay(attempt);
                assert!(
                    d >= ceiling / 2 && d <= ceiling,
                    "attempt {attempt}: {d:?} outside {:?}..={ceiling:?}",
                    ceiling / 2
                );
            }
        }
        let zero = Backoff {
            first: Duration::ZERO,
            max: Duration::ZERO,
        };
        assert_eq!(zero.delay(3), Duration::ZERO);
    }
}
