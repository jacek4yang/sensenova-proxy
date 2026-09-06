//! Small in-process circuit breaker for SenseNova upstream health.
//!
//! Three states:
//! - `Closed`: normal operation.
//! - `Open`: requests fail fast until the cooldown elapses (quota exhaustion
//!   opens until the parsed reset hint; sustained overload opens briefly).
//! - `HalfOpen`: one probe request may pass; success closes, failure reopens.
//!
//! Deliberately not a distributed framework: a `Mutex<State>` guarded without
//! ever awaiting while the lock is held.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

struct State {
    state: CircuitState,
    open_until: Instant,
    open_reason: &'static str,
    overload_timestamps: Vec<Instant>,
    half_open_probe_in_flight: bool,
}

pub struct CircuitBreaker {
    state: Mutex<State>,
    overload_threshold: u32,
    overload_window: Duration,
    overload_open: Duration,
}

#[derive(Debug)]
pub enum Admission {
    Allowed,
    /// Fail fast; the duration is how long the circuit remains open.
    Refused {
        remaining: Duration,
        reason: &'static str,
    },
}

impl CircuitBreaker {
    pub fn new(
        overload_threshold: u32,
        overload_window_secs: u64,
        overload_open_secs: u64,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                state: CircuitState::Closed,
                open_until: Instant::now(),
                open_reason: "",
                overload_timestamps: Vec::new(),
                half_open_probe_in_flight: false,
            }),
            overload_threshold: overload_threshold.max(1),
            overload_window: Duration::from_secs(overload_window_secs.max(1)),
            overload_open: Duration::from_secs(overload_open_secs.max(1)),
        }
    }

    /// Ask whether a request may proceed. In HalfOpen exactly one probe is
    /// admitted; the caller must report its outcome via `record_success` or
    /// `record_failure`.
    pub fn admit(&self) -> Admission {
        let mut state = self.lock();
        let now = Instant::now();
        match state.state {
            CircuitState::Closed => Admission::Allowed,
            CircuitState::Open => {
                if now >= state.open_until {
                    state.state = CircuitState::HalfOpen;
                    state.half_open_probe_in_flight = true;
                    Admission::Allowed
                } else {
                    Admission::Refused {
                        remaining: state.open_until.duration_since(now),
                        reason: state.open_reason,
                    }
                }
            }
            CircuitState::HalfOpen => {
                if state.half_open_probe_in_flight {
                    Admission::Refused {
                        remaining: Duration::from_secs(1),
                        reason: "half_open_probe_in_flight",
                    }
                } else {
                    state.half_open_probe_in_flight = true;
                    Admission::Allowed
                }
            }
        }
    }

    pub fn record_success(&self) {
        let mut state = self.lock();
        state.state = CircuitState::Closed;
        state.half_open_probe_in_flight = false;
        state.overload_timestamps.clear();
    }

    /// Record a quota/account exhaustion with a known reset horizon.
    pub fn record_quota_exhaustion(&self, open_for: Duration) {
        let open_for = open_for.min(Duration::from_secs(366 * 24 * 60 * 60));
        let mut state = self.lock();
        state.state = CircuitState::Open;
        state.open_until = Instant::now() + open_for;
        state.open_reason = "quota_exhausted";
        state.half_open_probe_in_flight = false;
    }

    /// Record overload (rate limit / transient server failure). A failure
    /// during a half-open probe reopens immediately; sustained overload in
    /// the closed state opens the circuit briefly.
    pub fn record_overload(&self) {
        let mut state = self.lock();
        let now = Instant::now();
        state
            .overload_timestamps
            .retain(|time| now.duration_since(*time) <= self.overload_window);
        state.overload_timestamps.push(now);
        let probe_failed = state.state == CircuitState::HalfOpen;
        if probe_failed || state.overload_timestamps.len() as u32 >= self.overload_threshold {
            state.state = CircuitState::Open;
            state.open_until = now + self.overload_open;
            state.open_reason = if probe_failed {
                "probe_failed"
            } else {
                "overload"
            };
            state.overload_timestamps.clear();
        }
        state.half_open_probe_in_flight = false;
    }

    /// A non-overload failure (e.g. invalid request) must not affect the
    /// overload window but does release the half-open probe slot.
    pub fn record_neutral_failure(&self) {
        let mut state = self.lock();
        state.half_open_probe_in_flight = false;
    }

    pub fn state(&self) -> CircuitState {
        let mut state = self.lock();
        if state.state == CircuitState::Open && Instant::now() >= state.open_until {
            // Reflect recovery lazily; admission still performs the real
            // HalfOpen transition.
            state.state = CircuitState::HalfOpen;
            state.half_open_probe_in_flight = false;
        }
        state.state
    }

    pub fn open_remaining(&self) -> Option<(Duration, &'static str)> {
        let state = self.lock();
        if state.state == CircuitState::Open {
            let now = Instant::now();
            if state.open_until > now {
                return Some((state.open_until.duration_since(now), state.open_reason));
            }
        }
        None
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(3, 60, 30)
    }

    #[test]
    fn closed_by_default_and_allows_all() {
        let breaker = breaker();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(matches!(breaker.admit(), Admission::Allowed));
    }

    #[test]
    fn quota_exhaustion_opens_until_reset() {
        let breaker = breaker();
        breaker.record_quota_exhaustion(Duration::from_secs(120));
        assert_eq!(breaker.state(), CircuitState::Open);
        match breaker.admit() {
            Admission::Refused { remaining, reason } => {
                assert!(remaining > Duration::from_secs(100));
                assert_eq!(reason, "quota_exhausted");
            }
            Admission::Allowed => panic!("must refuse while open"),
        }
    }

    #[test]
    fn sustained_overload_opens_briefly() {
        let breaker = breaker();
        for _ in 0..2 {
            breaker.record_overload();
        }
        assert_eq!(breaker.state(), CircuitState::Closed);
        breaker.record_overload();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn half_open_admits_one_probe_then_closes_or_reopens() {
        let breaker = breaker();
        breaker.record_quota_exhaustion(Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(20));
        assert!(matches!(breaker.admit(), Admission::Allowed));
        // Second concurrent request is refused while the probe is in flight.
        assert!(matches!(breaker.admit(), Admission::Refused { .. }));
        breaker.record_success();
        assert!(matches!(breaker.admit(), Admission::Allowed));

        breaker.record_quota_exhaustion(Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(20));
        assert!(matches!(breaker.admit(), Admission::Allowed));
        breaker.record_overload();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn neutral_failures_do_not_open_the_circuit() {
        let breaker = breaker();
        for _ in 0..10 {
            breaker.record_neutral_failure();
        }
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admission_is_race_free() {
        let breaker = Arc::new(breaker());
        breaker.record_quota_exhaustion(Duration::from_millis(5));
        let tasks = (0..16).map(|_| {
            let breaker = breaker.clone();
            tokio::spawn(async move { matches!(breaker.admit(), Admission::Allowed) })
        });
        let allowed = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|allowed| *allowed)
            .count();
        assert!(
            allowed <= 2,
            "at most the single probe may pass, got {allowed}"
        );
    }
}
