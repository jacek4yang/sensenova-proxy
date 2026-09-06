//! Bounded local admission control (concurrency shaping).
//!
//! Claude Code issues parallel requests and subagents; SenseNova serializes
//! work server-side under modest concurrency. A semaphore limits simultaneous
//! upstream exchanges, and a logically bounded wait queue rejects overflow
//! instead of growing without limit. No unbounded queue exists anywhere in
//! this module.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionFailure {
    /// Too many waiters already queued: fail fast.
    QueueFull,
    /// Waited longer than `queue_timeout_secs` without receiving a permit.
    QueueTimeout,
}

#[derive(Clone)]
pub struct AdmissionController {
    semaphore: Arc<Semaphore>,
    limit: usize,
    queue_capacity: usize,
    waiting: Arc<AtomicUsize>,
}

pub struct Permit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    pub queue_wait: Duration,
}

struct WaitingGuard(Arc<AtomicUsize>);

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AdmissionController {
    pub fn new(limit: usize, queue_capacity: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit.max(1))),
            limit: limit.max(1),
            queue_capacity: queue_capacity.max(1),
            waiting: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Acquire a permit, waiting at most `timeout` when the concurrency limit
    /// is saturated. Permits are returned automatically on drop; streaming
    /// handlers move the permit into the response body so the permit covers
    /// the whole upstream exchange.
    pub async fn acquire(&self, timeout: Duration) -> Result<Permit, AdmissionFailure> {
        let started = Instant::now();
        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            return Ok(Permit {
                _permit: permit,
                queue_wait: started.elapsed(),
            });
        }
        if self.waiting.load(Ordering::Acquire) >= self.queue_capacity {
            return Err(AdmissionFailure::QueueFull);
        }
        // The guard keeps the waiting counter correct even when this task is
        // cancelled mid-await.
        self.waiting.fetch_add(1, Ordering::AcqRel);
        let guard = WaitingGuard(self.waiting.clone());
        let result = tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()).await;
        drop(guard);
        match result {
            Ok(Ok(permit)) => Ok(Permit {
                _permit: permit,
                queue_wait: started.elapsed(),
            }),
            Ok(Err(_closed)) => Err(AdmissionFailure::QueueTimeout),
            Err(_elapsed) => Err(AdmissionFailure::QueueTimeout),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize as StdAtomicUsize;

    #[tokio::test]
    async fn semaphore_limit_is_respected() {
        let controller = AdmissionController::new(2, 8);
        let a = controller.acquire(Duration::from_secs(1)).await.unwrap();
        let b = controller.acquire(Duration::from_secs(1)).await.unwrap();
        let started = Instant::now();
        let c = controller.acquire(Duration::from_millis(80)).await;
        assert!(matches!(c, Err(AdmissionFailure::QueueTimeout)));
        assert!(started.elapsed() >= Duration::from_millis(60));
        drop((a, b));
        let _d = controller.acquire(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn queue_rejects_when_full() {
        let controller = AdmissionController::new(1, 2);
        let _held = controller.acquire(Duration::from_secs(1)).await.unwrap();
        let waiter1 = tokio::spawn({
            let controller = controller.clone();
            async move { controller.acquire(Duration::from_secs(5)).await }
        });
        let waiter2 = tokio::spawn({
            let controller = controller.clone();
            async move { controller.acquire(Duration::from_secs(5)).await }
        });
        // Busy-wait briefly until both waiters registered.
        for _ in 0..100 {
            if controller.waiting() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let rejected = controller.acquire(Duration::from_secs(5)).await;
        assert!(matches!(rejected, Err(AdmissionFailure::QueueFull)));
        // Release the held permit so pending waiters can finish.
        drop(_held);
        assert!(waiter1.await.unwrap().is_ok());
        assert!(waiter2.await.unwrap().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_acquisition_never_exceeds_limit() {
        let controller = Arc::new(AdmissionController::new(3, 16));
        let in_flight = Arc::new(StdAtomicUsize::new(0));
        let max_seen = Arc::new(StdAtomicUsize::new(0));
        let tasks = (0..12).map(|_| {
            let controller = controller.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            tokio::spawn(async move {
                let permit = controller.acquire(Duration::from_secs(5)).await.unwrap();
                let current = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                max_seen.fetch_max(current, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(20)).await;
                in_flight.fetch_sub(1, Ordering::AcqRel);
                drop(permit);
            })
        });
        for task in tasks {
            task.await.unwrap();
        }
        assert!(max_seen.load(Ordering::Acquire) <= 3);
    }

    #[tokio::test]
    async fn no_deadlock_when_permit_holders_are_cancelled() {
        let controller = Arc::new(AdmissionController::new(1, 4));
        for _ in 0..8 {
            let controller = controller.clone();
            let handle = tokio::spawn(async move {
                let permit = controller.acquire(Duration::from_millis(50)).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
                drop(permit);
            });
            handle.abort();
            let _ = handle.await;
        }
        let permit = controller.acquire(Duration::from_secs(2)).await;
        assert!(
            permit.is_ok(),
            "permits must be recovered from cancelled tasks"
        );
    }
}
