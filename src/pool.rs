//! Concurrency-safe credential pool.
//!
//! Multiple SenseNova API keys from one account are NOT assumed to have
//! independent quota (there is no evidence they do). `quota_group` is the
//! failure domain, and the authoritative routing state — key, quota-group,
//! `(model, quota_group)` and model circuits/cooldowns — lives in
//! [`crate::router`].
//!
//! This type therefore keeps only the configuration-derived credential set
//! (identity, credential material, quota group) used to report how many
//! credentials the proxy could dial. Every routing and health decision is made
//! by [`crate::router::RouteTable`], which owns its own state so the
//! `(model, quota_group)` and model-level failure domains can exist at all.

use std::sync::Arc;

use crate::config::SensenovaKeyConfig;

#[derive(Clone)]
pub struct KeyPool {
    inner: Arc<Inner>,
}

struct Inner {
    keys: Vec<KeyEntry>,
}

#[allow(dead_code)]
pub struct KeyEntry {
    /// Index in `sensenova_api_keys`, for diagnostics.
    pub configured_index: usize,
    pub name: Arc<str>,
    pub api_key: Arc<str>,
    pub quota_group: Arc<str>,
}

/// A fully chosen credential, ready to dial.
#[derive(Clone, PartialEq, Eq)]
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

    /// Build a credential handle.
    ///
    /// The routing layer keeps its own key table (so routing health is
    /// independent from this pool) but hands the transport this same type.
    pub fn new(index: usize, name: Arc<str>, quota_group: Arc<str>, api_key: Arc<str>) -> Self {
        Self {
            index,
            configured_index: index,
            name,
            quota_group,
            api_key,
        }
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
            })
            .collect();
        Self {
            inner: Arc::new(Inner { keys }),
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.keys.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.keys.is_empty()
    }

    #[allow(dead_code)]
    pub fn keys(&self) -> &[KeyEntry] {
        &self.inner.keys
    }

    /// Credentials available to dial. Routing health (cooldowns, 401
    /// disablement, model circuits) is tracked by
    /// [`crate::router::RouteTable::usable_credential_count`].
    pub fn usable_count(&self) -> usize {
        self.inner.keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured(count: usize, group: &str) -> Vec<SensenovaKeyConfig> {
        (0..count)
            .map(|index| SensenovaKeyConfig {
                name: format!("key-{index}"),
                api_key: format!("secret-{index}"),
                enabled: true,
                quota_group: group.into(),
            })
            .collect()
    }

    #[test]
    fn disabled_keys_are_not_part_of_the_pool() {
        let mut keys = configured(2, "a");
        keys[1].enabled = false;
        let pool = KeyPool::new(&keys);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.keys()[0].name.as_ref(), "key-0");
        assert_eq!(pool.keys()[0].configured_index, 0);
    }

    #[test]
    fn empty_pool_is_reported() {
        let pool = KeyPool::new(&[]);
        assert!(pool.is_empty());
        assert_eq!(pool.usable_count(), 0);
    }

    #[test]
    fn selected_key_debug_is_redacted() {
        let key = SelectedKey::new(
            0,
            Arc::from("primary"),
            Arc::from("account-a"),
            Arc::from("top-secret"),
        );
        let rendered = format!("{key:?}");
        assert!(!rendered.contains("top-secret"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn selected_key_exposes_credential_only_through_accessor() {
        let key = SelectedKey::new(
            3,
            Arc::from("primary"),
            Arc::from("account-a"),
            Arc::from("top-secret"),
        );
        assert_eq!(key.api_key(), "top-secret");
        assert_eq!(key.index, 3);
        assert_eq!(key.configured_index, 3);
        assert_eq!(key.quota_group.as_ref(), "account-a");
    }
}
