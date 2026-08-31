use std::fmt;
use std::time::{Instant, SystemTime};

use jiff::Timestamp;
use jiff::tz::TimeZone;

/// Display `Instant` in human readable format ("%H:%M:%S%.6f").
pub(crate) struct DisplayInstant<'a>(pub &'a Instant);

impl fmt::Display for DisplayInstant<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Convert Instant to SystemTime
        let sys_t = {
            let sys_now = SystemTime::now();
            let now = Instant::now();

            if &now >= self.0 {
                let d = now - *self.0;
                sys_now - d
            } else {
                let d = *self.0 - now;
                sys_now + d
            }
        };

        let ts = Timestamp::try_from(sys_t).unwrap_or(Timestamp::UNIX_EPOCH);
        let zoned = ts.to_zoned(TimeZone::system());
        let formatted = zoned.strftime("%H:%M:%S%.6f");
        write!(f, "{formatted}")
    }
}

pub(crate) trait DisplayInstantExt {
    fn display(&self) -> DisplayInstant<'_>;
}

impl DisplayInstantExt for Instant {
    fn display(&self) -> DisplayInstant<'_> {
        DisplayInstant(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::display_ext::DisplayInstantExt;
    use std::time::Instant;

    #[test]
    fn test_display_instant() {
        let now = Instant::now();
        let d = now.display();
        let s = format!("{d}");
        assert!(!s.is_empty());
    }
}
