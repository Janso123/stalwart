/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

//! Bounded, **ordered** parallel blob reads.
//!
//! # Ordering (critical)
//! Always use [`futures::StreamExt::buffered`] via [`ordered_buffered`].
//! **Never** use `buffer_unordered` or `join_all` + sort for IMAP FETCH or JMAP
//! list construction — Apple Mail and other clients break on out-of-order
//! untagged FETCH within one command; JMAP `list` must match request `ids` order.
//!
//! # Fairness
//! Per-command [`ordered_buffered`] uses [`DEFAULT_MAX_CONCURRENT_BLOB_READS`] (8).
//! [`BlobReadLimiter`] adds process-global ([`DEFAULT_GLOBAL_MAX_CONCURRENT_BLOB_READS`] = 32)
//! and per-account ([`DEFAULT_ACCOUNT_MAX_CONCURRENT_BLOB_READS`] = 16) semaphores.
//! These are compile-time defaults (`BlobReadLimiter::with_defaults()`).
//! Wiring them into BlobStore registry schema (codegen) is optional future work.

use ahash::AHashMap;
use futures::{
    stream::{self, Stream, StreamExt},
    Future,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default maximum concurrent blob reads **per command/method**.
///
/// Peak memory ≈ `limit × full raw message size` (Stalwart loads `0..usize::MAX`
/// even for `bodyValues` / partial FETCH). Default 8 → e.g. 8 × 25 MB = 200 MB.
pub const DEFAULT_MAX_CONCURRENT_BLOB_READS: usize = 8;

/// Process-wide ceiling on concurrent blob GetObjects (all tenants/commands).
///
/// Prevents `maxConcurrentRequests (e.g. 15) × 8` from opening ~120 GetObjects.
pub const DEFAULT_GLOBAL_MAX_CONCURRENT_BLOB_READS: usize = 32;

/// Per-account ceiling on concurrent blob GetObjects.
pub const DEFAULT_ACCOUNT_MAX_CONCURRENT_BLOB_READS: usize = 16;

/// Process-global + per-account waiting semaphores for blob reads.
///
/// `accounts` grows with distinct `account_id`s and is **not** pruned (known
/// limitation). Each entry is one `Semaphore` (small). A racy idle-evict would
/// split one account across two semaphores and bypass the per-account cap.
pub struct BlobReadLimiter {
    global: Arc<Semaphore>,
    per_account_limit: usize,
    accounts: Mutex<AHashMap<u32, Arc<Semaphore>>>,
}

/// RAII permits held for the duration of one GetObject.
pub struct BlobReadPermit {
    _account: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl BlobReadLimiter {
    pub fn new(global_limit: usize, per_account_limit: usize) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global_limit.max(1))),
            per_account_limit: per_account_limit.max(1),
            accounts: Mutex::new(AHashMap::new()),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            DEFAULT_GLOBAL_MAX_CONCURRENT_BLOB_READS,
            DEFAULT_ACCOUNT_MAX_CONCURRENT_BLOB_READS,
        )
    }

    pub fn per_account_limit(&self) -> usize {
        self.per_account_limit
    }

    fn account_semaphore(&self, account_id: u32) -> Arc<Semaphore> {
        let mut map = self.accounts.lock();
        map.entry(account_id)
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_account_limit)))
            .clone()
    }

    /// Acquire account permit first (fairness), then global.
    pub async fn acquire(&self, account_id: u32) -> BlobReadPermit {
        let account = self
            .account_semaphore(account_id)
            .acquire_owned()
            .await
            .expect("blob account semaphore closed");
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("blob global semaphore closed");
        BlobReadPermit {
            _account: account,
            _global: global,
        }
    }
}

impl Default for BlobReadLimiter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl std::fmt::Debug for BlobReadLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReadLimiter")
            .field("per_account_limit", &self.per_account_limit)
            .finish_non_exhaustive()
    }
}

/// Maps `items` through `f` with at most `limit` concurrent futures, yielding
/// results in **input order** (`buffered`, not `buffer_unordered`).
pub fn ordered_buffered<'a, I, T, F, Fut, R>(
    items: I,
    limit: usize,
    f: F,
) -> impl Stream<Item = R> + 'a
where
    I: IntoIterator<Item = T> + 'a,
    T: Send + 'a,
    F: FnMut(T) -> Fut + Send + 'a,
    Fut: Future<Output = R> + Send + 'a,
    R: Send + 'a,
{
    stream::iter(items).map(f).buffered(limit.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Mutex as AsyncMutex;

    #[tokio::test]
    async fn ordered_buffered_preserves_order_under_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let items: Vec<u32> = (0..16).collect();
        let limit = 4usize;
        let delay = Duration::from_millis(40);

        let active2 = active.clone();
        let max_active2 = max_active.clone();

        let results: Vec<u32> = ordered_buffered(items, limit, move |n| {
            let active = active2.clone();
            let max_active = max_active2.clone();
            async move {
                let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(cur, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                active.fetch_sub(1, Ordering::SeqCst);
                // Finish slower for lower indices so unordered would scramble.
                tokio::time::sleep(delay.saturating_mul(16 - n as u32)).await;
                n
            }
        })
        .collect()
        .await;

        assert_eq!(results, (0..16).collect::<Vec<_>>());
        let peak = max_active.load(Ordering::SeqCst);
        assert!(
            peak <= limit,
            "peak concurrency {peak} exceeded limit {limit}"
        );
        assert!(peak > 1, "expected overlapping work, peak was {peak}");
    }

    #[tokio::test]
    async fn ordered_buffered_limit_one_is_sequential() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let active2 = active.clone();
        let max_active2 = max_active.clone();

        let _: Vec<u32> = ordered_buffered(0..8u32, 1, move |n| {
            let active = active2.clone();
            let max_active = max_active2.clone();
            async move {
                let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(cur, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                n
            }
        })
        .collect()
        .await;

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn blob_read_limiter_caps_global_and_account() {
        let limiter = Arc::new(BlobReadLimiter::new(3, 2));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(AsyncMutex::new(()));
        let hold = gate.lock().await;

        let mut joins = Vec::new();
        for account in [1u32, 1, 1, 2, 2, 2] {
            let limiter = limiter.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let gate = gate.clone();
            joins.push(tokio::spawn(async move {
                let _p = limiter.acquire(account).await;
                let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(cur, Ordering::SeqCst);
                let _g = gate.lock().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        let peak_while_held = max_active.load(Ordering::SeqCst);
        drop(hold);
        for j in joins {
            j.await.unwrap();
        }
        assert!(
            peak_while_held <= 3,
            "global cap exceeded: {peak_while_held}"
        );
        assert!(peak_while_held >= 2, "expected some concurrency");
    }

    #[test]
    fn blob_concurrency_defaults_match_documented_constants() {
        assert_eq!(DEFAULT_MAX_CONCURRENT_BLOB_READS, 8);
        assert_eq!(DEFAULT_GLOBAL_MAX_CONCURRENT_BLOB_READS, 32);
        assert_eq!(DEFAULT_ACCOUNT_MAX_CONCURRENT_BLOB_READS, 16);
        let limiter = BlobReadLimiter::with_defaults();
        assert_eq!(limiter.per_account_limit(), 16);
    }
}
