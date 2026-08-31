use core::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::time::{Duration, Instant};

use crate::display_ext::DisplayInstantExt;

/// Stores an object along with its last update time and lease duration.
/// The lease duration specifies how long the object remains valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Leased<T, I = Instant> {
    data: T,
    last_update: Option<I>,
    lease: Duration,
    lease_enabled: bool,
}

impl<T: fmt::Display, I: DisplayInstantExt> fmt::Display for Leased<T, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let enabled = if self.lease_enabled {
            "enabled"
        } else {
            "disabled"
        };

        match &self.last_update {
            Some(utime) => write!(
                f,
                "{}@{}+{:?}({})",
                self.data,
                utime.display(),
                self.lease,
                enabled
            ),
            None => write!(f, "{}({})", self.data, enabled),
        }
    }
}

impl<T: Default, I> Default for Leased<T, I> {
    fn default() -> Self {
        Self {
            data: T::default(),
            last_update: None,
            lease: Default::default(),
            lease_enabled: true,
        }
    }
}

impl<T, I> Deref for Leased<T, I> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T, I> DerefMut for Leased<T, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> Leased<T, Instant> {
    /// Creates a new object that keeps track of the time when it was last updated.
    pub(crate) fn new(now: Instant, lease: Duration, data: T) -> Self {
        Self {
            data,
            last_update: Some(now),
            lease,
            lease_enabled: true,
        }
    }

    /// Creates a new object that has no last-updated time.
    pub(crate) fn without_last_update(data: T) -> Self {
        Self {
            data,
            last_update: None,
            lease: Duration::default(),
            lease_enabled: true,
        }
    }

    /// Return the last updated time of this object.
    pub(crate) fn last_update(&self) -> Option<Instant> {
        self.last_update
    }

    /// Return a Display instance that shows the last updated time and lease duration relative to
    /// `now`.
    pub(crate) fn display_lease_info(&self, now: Instant) -> impl fmt::Display + '_ {
        struct DisplayLeaseInfo<'a, T> {
            now: Instant,
            leased: &'a Leased<T, Instant>,
        }

        impl<T> fmt::Display for DisplayLeaseInfo<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match &self.leased.last_update {
                    Some(utime) => {
                        let expire_at = *utime + self.leased.lease;
                        write!(
                            f,
                            "now: {}, last_update: {}({:?} ago), lease: {:?}, expire at: {}({:?} since now)",
                            self.now.display(),
                            utime.display(),
                            self.now.saturating_duration_since(*utime),
                            self.leased.lease,
                            expire_at.display(),
                            expire_at.saturating_duration_since(self.now)
                        )
                    }
                    None => {
                        write!(f, "last_update: None")
                    }
                }
            }
        }

        DisplayLeaseInfo { now, leased: self }
    }

    /// Update the content of the object and the last updated time.
    pub(crate) fn update(&mut self, now: Instant, lease: Duration, data: T) {
        self.data = data;
        self.last_update = Some(now);
        self.lease = lease;
        self.lease_enabled = true;
    }

    /// Reset the lease duration so that the object expires at once.
    /// And until the next `update()`, [`Self::touch()`] won't update the lease.
    pub(crate) fn disable_lease(&mut self) {
        self.lease = Duration::default();
        self.lease_enabled = false;
    }

    /// Checks if the value is expired based on the provided `now` timestamp.
    /// An additional `timeout` parameter can be used to extend the lease under various situations.
    pub(crate) fn is_expired(&self, now: Instant, timeout: Duration) -> bool {
        match self.last_update {
            Some(utime) => now > utime + self.lease + timeout,
            None => true,
        }
    }

    /// Update the last updated time.
    pub(crate) fn touch(&mut self, now: Instant, lease: Duration) {
        debug_assert!(
            Some(now) >= self.last_update,
            "expect now: {}, must >= self.utime: {}, {:?}",
            now.display(),
            self.last_update.unwrap().display(),
            self.last_update.unwrap().saturating_duration_since(now),
        );
        self.last_update = Some(now);
        if self.lease_enabled {
            self.lease = lease;
        }
    }
}

#[cfg(test)]
impl<T> Leased<T, Instant> {
    pub(crate) fn lease_info(&self) -> (Option<Instant>, Duration, bool) {
        (self.last_update, self.lease, self.lease_enabled)
    }
}
