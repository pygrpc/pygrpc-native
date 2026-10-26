// SPDX-License-Identifier: Apache-2.0
//! Per-method call counts and latencies, recorded on the tokio threads.
//!
//! Each thread records into its own shard, so a call costs a few atomic adds
//! on cache lines no other thread writes. A snapshot sums the shards.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// gRFC A66's default latency buckets, in seconds.
pub const BOUNDS: [f64; 41] = [
    0.0, 0.00001, 0.00005, 0.0001, 0.0003, 0.0006, 0.0008, 0.001, 0.002, 0.003, 0.004, 0.005,
    0.006, 0.008, 0.01, 0.013, 0.016, 0.02, 0.025, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1, 0.13, 0.16,
    0.2, 0.25, 0.3, 0.4, 0.5, 0.65, 0.8, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0,
];
const BUCKETS: usize = BOUNDS.len() + 1;
const CODES: usize = 17;
const UNKNOWN: usize = 2;
const PER_LINE: usize = 16;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn bounds_ns() -> [u64; BOUNDS.len()] {
    BOUNDS.map(|seconds| (seconds * 1e9).round() as u64)
}

// Own cache lines, so shards never share one.
#[repr(align(128))]
struct Line([AtomicU64; PER_LINE]);

#[repr(align(128))]
struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    sum_ns: AtomicU64,
    min_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Histogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
        }
    }
}

struct Shard {
    started: Box<[Line]>,
    // (method, status code), allocated the first time that pair finishes.
    histograms: Box<[OnceLock<Box<Histogram>>]>,
}

pub struct Metrics {
    names: Vec<String>,
    bounds_ns: [u64; BOUNDS.len()],
    shards: Box<[Shard]>,
}

/// One method's totals; `other` gathers the paths not registered.
pub struct MethodSnapshot<'a> {
    pub name: Option<&'a str>,
    pub started: u64,
    pub statuses: Vec<StatusSnapshot>,
}

pub struct StatusSnapshot {
    pub code: i32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub buckets: Vec<u64>,
}

impl Metrics {
    /// `names` are the registered method paths in method-id order.
    pub fn new(names: Vec<String>, shards: usize) -> Self {
        let slots = names.len() + 1;
        let shards = (0..shards.max(1))
            .map(|_| Shard {
                started: (0..slots.div_ceil(PER_LINE))
                    .map(|_| Line(std::array::from_fn(|_| AtomicU64::new(0))))
                    .collect(),
                histograms: (0..slots * CODES).map(|_| OnceLock::new()).collect(),
            })
            .collect();
        Metrics {
            names,
            bounds_ns: bounds_ns(),
            shards,
        }
    }

    /// The slot of a method id, or of `other` for an unregistered path.
    pub fn slot(&self, method_id: Option<u32>) -> usize {
        match method_id {
            Some(id) if (id as usize) < self.names.len() => id as usize,
            _ => self.names.len(),
        }
    }

    fn shard(&self) -> &Shard {
        let index = SHARD.with(|shard| {
            let mut index = shard.get();
            if index == usize::MAX {
                index = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
                shard.set(index);
            }
            index
        });
        &self.shards[index % self.shards.len()]
    }

    pub fn started(&self, slot: usize) {
        self.shard().started[slot / PER_LINE].0[slot % PER_LINE].fetch_add(1, Ordering::Relaxed);
    }

    pub fn finished(&self, slot: usize, code: i32, elapsed: Duration) {
        let code = usize::try_from(code)
            .ok()
            .filter(|code| *code < CODES)
            .unwrap_or(UNKNOWN);
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let histogram =
            self.shard().histograms[slot * CODES + code].get_or_init(|| Box::new(Histogram::new()));
        histogram.sum_ns.fetch_add(ns, Ordering::Relaxed);
        histogram.min_ns.fetch_min(ns, Ordering::Relaxed);
        histogram.max_ns.fetch_max(ns, Ordering::Relaxed);
        // Release pairs with the snapshot's Acquire, so a snapshot that sees
        // this call finished also sees it started.
        let bucket = self.bounds_ns.partition_point(|bound| *bound < ns);
        histogram.buckets[bucket].fetch_add(1, Ordering::Release);
    }

    pub fn snapshot(&self) -> Vec<MethodSnapshot<'_>> {
        (0..=self.names.len())
            .map(|slot| {
                let statuses: Vec<StatusSnapshot> = (0..CODES)
                    .filter_map(|code| self.status_snapshot(slot, code))
                    .collect();
                // Read after the counts, so started is never behind them.
                let started = self
                    .shards
                    .iter()
                    .map(|shard| {
                        shard.started[slot / PER_LINE].0[slot % PER_LINE].load(Ordering::Relaxed)
                    })
                    .sum();
                MethodSnapshot {
                    name: self.names.get(slot).map(String::as_str),
                    started,
                    statuses,
                }
            })
            .collect()
    }

    fn status_snapshot(&self, slot: usize, code: usize) -> Option<StatusSnapshot> {
        let mut buckets = vec![0u64; BUCKETS];
        let (mut sum, mut min, mut max) = (0u64, u64::MAX, 0u64);
        let mut seen = false;
        for shard in self.shards.iter() {
            let Some(histogram) = shard.histograms[slot * CODES + code].get() else {
                continue;
            };
            seen = true;
            for (total, bucket) in buckets.iter_mut().zip(&histogram.buckets) {
                *total += bucket.load(Ordering::Acquire);
            }
            sum = sum.saturating_add(histogram.sum_ns.load(Ordering::Relaxed));
            min = min.min(histogram.min_ns.load(Ordering::Relaxed));
            max = max.max(histogram.max_ns.load(Ordering::Relaxed));
        }
        let count: u64 = buckets.iter().sum();
        if !seen || count == 0 {
            return None;
        }
        Some(StatusSnapshot {
            code: code as i32,
            count,
            sum: sum as f64 / 1e9,
            min: min as f64 / 1e9,
            max: max as f64 / 1e9,
            buckets,
        })
    }
}

/// Nanoseconds since the Unix epoch, for the snapshot's start time.
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_upper_inclusive() {
        let metrics = Metrics::new(vec!["/t.S/U".into()], 1);
        metrics.started(0);
        metrics.finished(0, 0, Duration::from_micros(10));
        metrics.finished(0, 0, Duration::from_nanos(10_001));
        metrics.finished(0, 0, Duration::from_secs(200));
        let snapshot = metrics.snapshot();
        let ok = &snapshot[0].statuses[0];
        assert_eq!(ok.count, 3);
        assert_eq!(ok.buckets[1], 1);
        assert_eq!(ok.buckets[2], 1);
        assert_eq!(ok.buckets[BUCKETS - 1], 1);
        assert_eq!(ok.min, 1e-5);
        assert_eq!(ok.max, 200.0);
    }

    /// cargo test --release --lib record_cost -- --ignored --nocapture
    #[test]
    #[ignore]
    fn record_cost() {
        const CALLS: u64 = 2_000_000;
        for threads in [1, 2, 8] {
            let metrics = Metrics::new(vec!["/t.S/U".into()], threads);
            let start = std::time::Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| {
                        for i in 0..CALLS {
                            let since = std::time::Instant::now();
                            metrics.started(0);
                            metrics.finished(
                                0,
                                0,
                                since.elapsed() + Duration::from_nanos(i % 4096),
                            );
                        }
                    });
                }
            });
            let per_call = start.elapsed().as_nanos() as f64 / CALLS as f64;
            println!("{threads} thread(s): {per_call:.1} ns per call, clock reads included");
        }
    }

    #[test]
    fn unknown_codes_and_paths_have_their_own_slots() {
        let metrics = Metrics::new(vec!["/t.S/U".into()], 2);
        let other = metrics.slot(None);
        assert_eq!(metrics.slot(Some(7)), other);
        metrics.started(other);
        metrics.finished(other, 99, Duration::ZERO);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot[1].name, None);
        assert_eq!(snapshot[1].started, 1);
        assert_eq!(snapshot[1].statuses[0].code, UNKNOWN as i32);
        assert!(snapshot[0].statuses.is_empty());
    }
}
