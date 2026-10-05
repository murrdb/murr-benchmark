use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Per-call latency distribution in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct LatencyStats {
    pub mean: f64,
    pub min: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

/// Collects the duration of every call made after the warmup window.
///
/// Criterion does not signal where warmup ends, so the window is time-based:
/// it opens at the first recorded call and lasts `warmup`.
pub struct LatencyRecorder {
    warmup: Duration,
    first_call: OnceLock<Instant>,
    samples: Mutex<Vec<u64>>,
}

impl LatencyRecorder {
    pub fn new(warmup: Duration) -> Self {
        LatencyRecorder {
            warmup,
            first_call: OnceLock::new(),
            samples: Mutex::new(Vec::new()),
        }
    }

    /// Records a call that began at `start` and took `elapsed`.
    pub fn record(&self, start: Instant, elapsed: Duration) {
        let first_call = *self.first_call.get_or_init(|| start);
        if start.duration_since(first_call) >= self.warmup {
            self.samples.lock().unwrap().push(elapsed.as_nanos() as u64);
        }
    }

    /// Number of calls recorded after warmup.
    pub fn len(&self) -> usize {
        self.samples.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Nearest-rank percentiles over the recorded calls; `None` if there are none.
    pub fn stats(&self) -> Option<LatencyStats> {
        let mut sorted = self.samples.lock().unwrap().clone();
        if sorted.is_empty() {
            return None;
        }
        sorted.sort_unstable();

        let percentile = |p: f64| {
            let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
            sorted[rank.clamp(1, sorted.len()) - 1]
        };
        let sum: u128 = sorted.iter().map(|&ns| ns as u128).sum();

        Some(LatencyStats {
            mean: sum as f64 / sorted.len() as f64,
            min: sorted[0],
            p50: percentile(50.0),
            p90: percentile(90.0),
            p99: percentile(99.0),
            p999: percentile(99.9),
            max: sorted[sorted.len() - 1],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let recorder = LatencyRecorder::new(Duration::ZERO);
        let start = Instant::now();
        // Recorded out of order: 1000, 999, ..., 1 ns.
        for ns in (1..=1000).rev() {
            recorder.record(start, Duration::from_nanos(ns));
        }

        let stats = recorder.stats().unwrap();

        assert_eq!(recorder.len(), 1000);
        assert_eq!(stats.min, 1);
        assert_eq!(stats.p50, 500);
        assert_eq!(stats.p90, 900);
        assert_eq!(stats.p99, 990);
        assert_eq!(stats.p999, 999);
        assert_eq!(stats.max, 1000);
        assert_eq!(stats.mean, 500.5);
    }

    #[test]
    fn calls_started_during_warmup_are_dropped() {
        let recorder = LatencyRecorder::new(Duration::from_secs(2));
        let start = Instant::now();

        recorder.record(start, Duration::from_nanos(10));
        recorder.record(start + Duration::from_secs(1), Duration::from_nanos(20));
        recorder.record(start + Duration::from_secs(2), Duration::from_nanos(30));
        recorder.record(start + Duration::from_secs(3), Duration::from_nanos(40));

        let stats = recorder.stats().unwrap();

        assert_eq!(recorder.len(), 2);
        assert_eq!(stats.min, 30);
        assert_eq!(stats.max, 40);
    }

    #[test]
    fn no_calls_yield_no_stats() {
        let recorder = LatencyRecorder::new(Duration::ZERO);

        assert!(recorder.is_empty());
        assert_eq!(recorder.stats(), None);
    }
}
