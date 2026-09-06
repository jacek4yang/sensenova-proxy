//! Concurrency-safe credential pool with quota-group semantics.
//!
//! Multiple SenseNova API keys from one account are NOT assumed to have
//! independent quota (there is no evidence they do). `quota_group` is the
//! failure domain: account-level exhaustion cools the whole group, while a
//! per-key rate limit only cools that key.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::config::SensenovaKeyConfig;

#[derive(Clone)]
pub struct KeyPool {
    inner: Arc<Inner>,
}

struct Inner {
    keys: Vec<KeyEntry>,
    active: AtomicUsize,
}

struct KeyEntry {
    configured_index: usize,
    name: Arc<str>,
    api_key: Arc<str>,
    quota_group: Arc<str>,
    runtime: Mutex<KeyRuntime>,
}

#[derive(Default)]
struct KeyRuntime {
    cooling_until: Option<Instant>,
    unusable: bool,
    /// Consecutive generic-429 (fallback-cooldown) events without an
    /// authoritative hint or an intervening success. Drives the progressive
    /// transient rate-limit backoff (5s → 10s → 20s → ...).
    rate_limit_streak: u32,
}

#[derive(Clone)]
pub struct SelectedKey {
    pub index: usize,
    pub configured_index: usize,
    pub name: Arc<str>,
    pub quota_group: Arc<str>,
    api_key: Arc<str>,
}

impl SelectedKey {
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

impl std::fmt::Debug for SelectedKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SelectedKey")
            .field("index", &self.index)
            .field("configured_index", &self.configured_index)
            .field("name", &self.name)
            .field("quota_group", &self.quota_group)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct KeyStateSnapshot {
    pub name: Arc<str>,
    pub quota_group: Arc<str>,
    pub cooling_remaining: Option<Duration>,
    pub unusable: bool,
}

impl KeyPool {
    pub fn new(configured: &[SensenovaKeyConfig]) -> Self {
        let keys = configured
            .iter()
            .enumerate()
            .filter(|(_, key)| key.enabled)
            .map(|(configured_index, key)| KeyEntry {
                configured_index,
                name: Arc::from(key.name.as_str()),
                api_key: Arc::from(key.api_key.as_str()),
                quota_group: Arc::from(key.quota_group.as_str()),
                runtime: Mutex::new(KeyRuntime::default()),
            })
            .collect();
        Self {
            inner: Arc::new(Inner {
                keys,
                active: AtomicUsize::new(0),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.keys.len()
    }

    /// Select the sticky active credential, or the next usable credential in
    /// configured order. `attempted` bounds the per-request failover walk.
    pub fn select(&self, attempted: &HashSet<usize>) -> Option<SelectedKey> {
        let count = self.len();
        if count == 0 {
            return None;
        }
        let active = self.inner.active.load(Ordering::Acquire) % count;
        let now = Instant::now();
        for offset in 0..count {
            let index = (active + offset) % count;
            if attempted.contains(&index) {
                continue;
            }
            let entry = self.inner.keys.get(index)?;
            let usable = {
                let mut runtime = lock(&entry.runtime);
                if runtime.unusable {
                    false
                } else {
                    clear_expired(&mut runtime, now);
                    runtime.cooling_until.is_none()
                }
            };
            if usable {
                if index != active {
                    let _ = self.inner.active.compare_exchange(
                        active,
                        index,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                return Some(SelectedKey {
                    index,
                    configured_index: entry.configured_index,
                    name: entry.name.clone(),
                    quota_group: entry.quota_group.clone(),
                    api_key: entry.api_key.clone(),
                });
            }
        }
        None
    }

    /// Mark one key cooling after a per-key rate limit. No other call site may
    /// mutate cooldown state.
    pub fn mark_key_cooling(&self, index: usize, cooldown: Duration) {
        let cooldown = cooldown.min(crate::rate_limit::MAX_PARSED_COOLDOWN);
        let Some(entry) = self.inner.keys.get(index) else {
            return;
        };
        {
            let mut runtime = lock(&entry.runtime);
            runtime.cooling_until = Instant::now().checked_add(cooldown);
        }
        self.advance_if_active(index);
    }

    /// Mark every credential in a quota group cooling: account-level quota is
    /// shared, so one exhaustion signal applies to the whole domain.
    pub fn mark_group_cooling(&self, group: &str, cooldown: Duration) {
        let cooldown = cooldown.min(crate::rate_limit::MAX_PARSED_COOLDOWN);
        let now = Instant::now();
        let active_index = self.inner.active.load(Ordering::Acquire);
        let mut active_cooled = false;
        for (index, entry) in self.inner.keys.iter().enumerate() {
            if entry.quota_group.as_ref() == group {
                let mut runtime = lock(&entry.runtime);
                runtime.cooling_until = now.checked_add(cooldown);
                active_cooled |= index == active_index % self.len().max(1);
            }
        }
        if active_cooled {
            self.advance_if_active(active_index % self.len().max(1));
        }
    }

    /// Record a generic 429 (no authoritative Retry-After) for the
    /// progressive transient-backoff ladder.
    pub fn note_rate_limit(&self, index: usize) {
        if let Some(entry) = self.inner.keys.get(index) {
            let mut runtime = lock(&entry.runtime);
            runtime.rate_limit_streak = runtime.rate_limit_streak.saturating_add(1);
        }
    }

    /// Current consecutive generic-429 count for one credential.
    pub fn rate_limit_streak(&self, index: usize) -> u32 {
        self.inner
            .keys
            .get(index)
            .map(|entry| lock(&entry.runtime).rate_limit_streak)
            .unwrap_or(0)
    }

    /// A successful exchange resets the transient rate-limit ladder.
    pub fn note_credential_success(&self, index: usize) {
        if let Some(entry) = self.inner.keys.get(index) {
            lock(&entry.runtime).rate_limit_streak = 0;
        }
    }

    /// An authoritative upstream hint supersedes the transient ladder.
    pub fn reset_rate_limit_streak(&self, index: usize) {
        if let Some(entry) = self.inner.keys.get(index) {
            lock(&entry.runtime).rate_limit_streak = 0;
        }
    }

    /// A 401/403 means this specific credential is wrong; it must never be
    /// selected again. Other credentials stay usable.
    pub fn mark_unusable(&self, index: usize) {
        if let Some(entry) = self.inner.keys.get(index) {
            lock(&entry.runtime).unusable = true;
        }
        self.advance_if_active(index);
    }

    /// Remaining cooldown of the credential that will become usable next.
    pub fn earliest_retry_after(&self) -> Option<Duration> {
        let now = Instant::now();
        self.inner
            .keys
            .iter()
            .filter(|entry| !lock(&entry.runtime).unusable)
            .filter_map(|entry| {
                let mut runtime = lock(&entry.runtime);
                clear_expired(&mut runtime, now);
                runtime
                    .cooling_until
                    .and_then(|until| until.checked_duration_since(now))
            })
            .min()
    }

    pub fn usable_count(&self) -> usize {
        let now = Instant::now();
        self.inner
            .keys
            .iter()
            .filter(|entry| {
                let mut runtime = lock(&entry.runtime);
                if runtime.unusable {
                    return false;
                }
                clear_expired(&mut runtime, now);
                runtime.cooling_until.is_none()
            })
            .count()
    }

    pub fn snapshots(&self) -> Vec<KeyStateSnapshot> {
        let now = Instant::now();
        self.inner
            .keys
            .iter()
            .map(|entry| {
                let mut runtime = lock(&entry.runtime);
                clear_expired(&mut runtime, now);
                KeyStateSnapshot {
                    name: entry.name.clone(),
                    quota_group: entry.quota_group.clone(),
                    cooling_remaining: runtime
                        .cooling_until
                        .and_then(|until| until.checked_duration_since(now)),
                    unusable: runtime.unusable,
                }
            })
            .collect()
    }

    fn advance_if_active(&self, rejected: usize) {
        let count = self.len();
        if count == 0 || self.inner.active.load(Ordering::Acquire) % count != rejected {
            return;
        }
        let now = Instant::now();
        for offset in 1..count {
            let candidate = (rejected + offset) % count;
            let entry = &self.inner.keys[candidate];
            let mut runtime = lock(&entry.runtime);
            if runtime.unusable {
                continue;
            }
            clear_expired(&mut runtime, now);
            if runtime.cooling_until.is_none() {
                drop(runtime);
                let _ = self.inner.active.compare_exchange(
                    rejected,
                    candidate,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return;
            }
        }
    }
}

fn clear_expired(runtime: &mut KeyRuntime, now: Instant) {
    if let Some(until) = runtime.cooling_until
        && until <= now
    {
        runtime.cooling_until = None;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(count: usize, group: &str) -> KeyPool {
        KeyPool::new(
            &(0..count)
                .map(|index| SensenovaKeyConfig {
                    name: format!("key-{index}"),
                    api_key: format!("secret-{index}"),
                    enabled: true,
                    quota_group: group.into(),
                })
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn selection_is_sticky_and_advances_after_cooling() {
        let pool = pool(3, "a");
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 0);
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 0);
        pool.mark_key_cooling(0, Duration::from_secs(60));
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 1);
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 1);
    }

    #[test]
    fn attempted_keys_are_never_selected_twice() {
        let pool = pool(3, "a");
        let mut attempted = HashSet::new();
        for expected in 0..3 {
            let selected = pool.select(&attempted).unwrap();
            assert_eq!(selected.index, expected);
            attempted.insert(selected.index);
        }
        assert!(pool.select(&attempted).is_none());
    }

    #[test]
    fn group_cooling_covers_all_keys_in_the_group() {
        let pool = pool(2, "shared");
        pool.mark_group_cooling("shared", Duration::from_secs(60));
        assert!(pool.select(&HashSet::new()).is_none());
        assert!(pool.earliest_retry_after().is_some());
    }

    #[test]
    fn group_cooling_only_affects_the_named_group() {
        let configured = vec![
            SensenovaKeyConfig {
                name: "a".into(),
                api_key: "secret-a".into(),
                enabled: true,
                quota_group: "group-1".into(),
            },
            SensenovaKeyConfig {
                name: "b".into(),
                api_key: "secret-b".into(),
                enabled: true,
                quota_group: "group-2".into(),
            },
        ];
        let pool = KeyPool::new(&configured);
        pool.mark_group_cooling("group-1", Duration::from_secs(60));
        let selected = pool.select(&HashSet::new()).unwrap();
        assert_eq!(selected.quota_group.as_ref(), "group-2");
    }

    #[test]
    fn cooling_keys_are_not_counted_as_usable() {
        let pool = pool(2, "a");
        assert_eq!(pool.usable_count(), 2);
        pool.mark_key_cooling(0, Duration::from_secs(60));
        assert_eq!(
            pool.usable_count(),
            1,
            "cooling key must not count as usable"
        );
        pool.mark_group_cooling("a", Duration::from_secs(60));
        assert_eq!(pool.usable_count(), 0);
    }

    #[test]
    fn unusable_keys_are_never_selected() {
        let pool = pool(2, "a");
        pool.mark_unusable(0);
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 1);
        assert_eq!(pool.usable_count(), 1);
        pool.mark_unusable(1);
        assert_eq!(pool.usable_count(), 0);
        assert!(pool.select(&HashSet::new()).is_none());
    }

    #[tokio::test]
    async fn expired_keys_become_eligible_again() {
        let pool = pool(2, "a");
        pool.mark_key_cooling(0, Duration::from_millis(5));
        // Selection advances to the next usable key and stays sticky there.
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Once the sticky key fails too, the expired key is eligible again.
        pool.mark_key_cooling(1, Duration::from_secs(60));
        assert_eq!(pool.select(&HashSet::new()).unwrap().index, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_updates_keep_state_valid() {
        let pool = pool(4, "a");
        let tasks = (0..32).map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                pool.mark_key_cooling(0, Duration::from_secs(1));
                pool.select(&HashSet::new()).map(|key| key.index)
            })
        });
        for task in tasks {
            let index = task.await.unwrap().unwrap();
            assert!((1..4).contains(&index));
        }
    }

    #[test]
    fn selected_key_debug_is_redacted() {
        let key = pool(1, "a").select(&HashSet::new()).unwrap();
        assert!(!format!("{key:?}").contains("secret-0"));
    }
}
