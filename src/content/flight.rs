//! Per-key local read coordination; entries live only while requests hold them.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{Content, ContentError};

const MAX_KEYS: usize = 272; // All active generations, including completed cohorts.
pub(super) const MAX_FOLLOWERS: usize = 256;

pub(super) struct Flights {
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    generations: Arc<Semaphore>,
}

impl Default for Flights {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            generations: Arc::new(Semaphore::new(MAX_KEYS)),
        }
    }
}

struct Entry {
    turn: Arc<Semaphore>,
    // Outcomes exist only for the cohort already registered at completion.
    // Returned bodies retain their shared delivery reservation; no extra copy is made.
    outcome: Mutex<Option<Result<Option<Content>, ContentError>>>,
    _generation: OwnedSemaphorePermit,
}

pub(super) struct Registration {
    registry: Arc<Flights>,
    key: String,
    entry: Option<Arc<Entry>>,
    pub joined: bool,
}

impl Flights {
    pub fn register(self: &Arc<Self>, key: String) -> Result<Registration, ContentError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ContentError::unavailable("local read coordination unavailable"))?;
        let joined = entries.contains_key(&key);
        let entry = if let Some(entry) = entries.get(&key) {
            entry.clone()
        } else {
            // Count completed generations still held by followers as well as map keys.
            let generation = self.generations.clone().try_acquire_owned().map_err(|_| {
                let mut error = ContentError::unavailable("local read coordination full");
                error.retry_after = Some(std::time::Duration::from_secs(1));
                error
            })?;
            let entry = Arc::new(Entry {
                turn: Arc::new(Semaphore::new(1)),
                outcome: Mutex::new(None),
                _generation: generation,
            });
            entries.insert(key.clone(), entry.clone());
            entry
        };
        Ok(Registration {
            registry: self.clone(),
            key,
            entry: Some(entry),
            joined,
        })
    }
}

impl Registration {
    pub fn turn(&self) -> Arc<Semaphore> {
        self.entry.as_ref().expect("live registration").turn.clone()
    }

    pub fn outcome(&self) -> Option<Result<Option<Content>, ContentError>> {
        self.entry
            .as_ref()
            .expect("live registration")
            .outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn complete(&self, result: &Option<Content>) {
        self.finish(Ok(result.clone()));
    }

    pub fn complete_busy(&self, error: &ContentError) {
        if super::local::cohort_busy(error) {
            self.finish(Err(error.clone()));
        }
    }

    fn finish(&self, result: Result<Option<Content>, ContentError>) {
        let entry = self.entry.as_ref().expect("live registration");
        let mut entries = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut outcome = entry.outcome.lock().unwrap_or_else(|e| e.into_inner());
        if outcome.is_some() {
            return;
        }
        *outcome = Some(result);
        // Late arrivals perform a new lookup; an earlier cohort keeps its own result.
        if entries
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(&self.key);
        }
        // Completed cohorts only read their result; none need an exclusive turn.
        entry.turn.add_permits(MAX_FOLLOWERS);
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut entries = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = self.entry.take().expect("live registration");
        // The decrement must occur under the same lock as the count/removal.
        // A finished cohort must never remove a newer generation of the same key.
        if Arc::strong_count(&entry) == 2
            && entries
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            entries.remove(&self.key);
        }
        drop(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_and_completed_registrations_release_keys_and_keep_followers() {
        let flights = Arc::new(Flights::default());
        let first = flights.register("same".into()).unwrap();
        let follower = flights.register("same".into()).unwrap();
        assert!(Arc::ptr_eq(&first.turn(), &follower.turn()));
        assert!(!first.joined);
        assert!(follower.joined);
        drop(first);
        assert_eq!(flights.entries.lock().unwrap().len(), 1);
        drop(follower);
        assert!(flights.entries.lock().unwrap().is_empty());
        assert!(!flights.register("same".into()).unwrap().joined);
        assert!(flights.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn full_registry_still_admits_existing_keys_and_recovers_after_release() {
        let flights = Arc::new(Flights::default());
        let mut held: Vec<_> = (0..MAX_KEYS)
            .map(|i| flights.register(i.to_string()).unwrap())
            .collect();
        assert!(flights.register("extra".into()).is_err());
        let follower = flights.register("0".into()).unwrap();
        assert!(follower.joined);
        held.pop();
        assert!(flights.register("extra".into()).is_ok());
        drop(held);
        assert_eq!(flights.entries.lock().unwrap().len(), 1);
        drop(follower);
        assert!(flights.entries.lock().unwrap().is_empty());
    }
    #[test]
    fn simultaneous_last_drops_do_not_leak_keys() {
        let flights = Arc::new(Flights::default());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let registration_failures = std::sync::atomic::AtomicUsize::new(0);
        let leaked_maps = std::sync::atomic::AtomicUsize::new(0);
        let leaked_permits = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let flights = flights.clone();
                let barrier = barrier.clone();
                let registration_failures = &registration_failures;
                let leaked_maps = &leaked_maps;
                let leaked_permits = &leaked_permits;
                scope.spawn(move || {
                    use std::sync::atomic::Ordering::Relaxed;
                    for _ in 0..100_000 {
                        let registration = flights.register("same".into());
                        if registration.is_err() {
                            registration_failures.fetch_add(1, Relaxed);
                        }
                        barrier.wait();
                        drop(registration);
                        if barrier.wait().is_leader() {
                            if !flights
                                .entries
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .is_empty()
                            {
                                leaked_maps.fetch_add(1, Relaxed);
                            }
                            if flights.generations.available_permits() != MAX_KEYS {
                                leaked_permits.fetch_add(1, Relaxed);
                            }
                        }
                        // Inspect every pair before another registration can repair a leak.
                        // Defer assertions so failures cannot strand the other thread.
                        barrier.wait();
                    }
                });
            }
        });
        assert_eq!(registration_failures.into_inner(), 0);
        assert_eq!(leaked_maps.into_inner(), 0);
        assert_eq!(leaked_permits.into_inner(), 0);
        assert!(flights.entries.lock().unwrap().is_empty());
        assert_eq!(flights.generations.available_permits(), MAX_KEYS);
    }

    #[test]
    fn completed_miss_only_reaches_existing_followers_and_old_drops_keep_new_generation() {
        let flights = Arc::new(Flights::default());
        let first = flights.register("same".into()).unwrap();
        let follower = flights.register("same".into()).unwrap();
        first.complete(&None);
        assert!(matches!(follower.outcome(), Some(Ok(None))));
        let late = flights.register("same".into()).unwrap();
        assert!(!late.joined);
        assert!(late.outcome().is_none());
        drop((first, follower));
        assert_eq!(flights.entries.lock().unwrap().len(), 1);
        drop(late);
        assert!(flights.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn completed_generations_remain_bounded_until_followers_release() {
        let flights = Arc::new(Flights::default());
        let mut held = Vec::new();
        for _ in 0..MAX_KEYS {
            let registration = flights.register("same".into()).unwrap();
            registration.complete(&None);
            held.push(registration);
        }
        assert!(flights.entries.lock().unwrap().is_empty());
        assert!(flights.register("same".into()).is_err());
        held.pop();
        assert!(flights.register("same".into()).is_ok());
        drop(held);
        assert_eq!(flights.generations.available_permits(), MAX_KEYS);
    }

    #[tokio::test]
    async fn completing_a_cohort_releases_every_follower_without_serial_handoffs() {
        let flights = Arc::new(Flights::default());
        let leader = flights.register("same".into()).unwrap();
        let held = leader.turn().acquire_owned().await.unwrap();
        let followers: Vec<_> = (0..MAX_FOLLOWERS)
            .map(|_| flights.register("same".into()).unwrap())
            .collect();
        leader.complete(&None);
        let permits: Vec<_> = followers
            .iter()
            .map(|follower| {
                assert!(matches!(follower.outcome(), Some(Ok(None))));
                follower
                    .turn()
                    .try_acquire_owned()
                    .expect("all followers ready")
            })
            .collect();
        // Repeated completion cannot grow the semaphore or overwrite the outcome.
        leader.complete(&None);
        assert_eq!(leader.turn().available_permits(), 0);
        let late = flights.register("same".into()).unwrap();
        assert!(!late.joined);
        assert_eq!(late.turn().available_permits(), 1);
        drop((permits, held, followers, leader, late));
        assert_eq!(flights.generations.available_permits(), MAX_KEYS);
    }
}
