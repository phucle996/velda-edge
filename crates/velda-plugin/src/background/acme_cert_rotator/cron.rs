//! Minimal 5-field cron engine (`min hour day-of-month month day-of-week`, evaluated in UTC).
//!
//! Supports `*`, `a`, `a-b`, `a,b,c`, and `/step` on `*` or ranges. Day-of-week accepts `0-7`
//! (both 0 and 7 mean Sunday). When both day-of-month and day-of-week are restricted, a day
//! matches if EITHER matches (classic Vixie cron semantics).

use crate::error::PluginError;

/// Upper bound of the forward search: 5 years covers every valid expression (e.g. `0 0 29 2 *`).
const MAX_SEARCH_DAYS: u64 = 366 * 5;

/// Matcher function pointer for Day of Month and Day of Week restrictions.
pub type DayMatcherFn = fn(dom_mask: u32, dom: u32, dow_mask: u8, dow: u32) -> bool;

#[inline(always)]
fn match_unrestricted(_dom_mask: u32, _dom: u32, _dow_mask: u8, _dow: u32) -> bool {
    true
}

#[inline(always)]
fn match_dom_only(dom_mask: u32, dom: u32, _dow_mask: u8, _dow: u32) -> bool {
    (dom_mask & (1 << dom)) != 0
}

#[inline(always)]
fn match_dow_only(_dom_mask: u32, _dom: u32, dow_mask: u8, dow: u32) -> bool {
    (dow_mask & (1 << dow)) != 0
}

#[inline(always)]
fn match_dom_or_dow(dom_mask: u32, dom: u32, dow_mask: u8, dow: u32) -> bool {
    (dom_mask & (1 << dom)) != 0 || (dow_mask & (1 << dow)) != 0
}

/// Parsed cron schedule stored as per-field bitmasks with direct function-pointer day dispatch.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes: u64,
    hours: u32,
    days_of_month: u32,
    months: u16,
    days_of_week: u8,
    day_matcher: DayMatcherFn,
}

impl PartialEq for CronSchedule {
    fn eq(&self, other: &Self) -> bool {
        self.minutes == other.minutes
            && self.hours == other.hours
            && self.days_of_month == other.days_of_month
            && self.months == other.months
            && self.days_of_week == other.days_of_week
            && (self.day_matcher as usize) == (other.day_matcher as usize)
    }
}

impl Eq for CronSchedule {}

fn parse_field(field: &str, min: u32, max: u32, name: &str) -> Result<(u64, bool), PluginError> {
    let err = |detail: &str| PluginError::Config(format!("cron {name} field '{field}': {detail}"));
    let mut mask = 0u64;
    let mut restricted = true;
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s.parse().map_err(|_| err("invalid step"))?;
                if step == 0 {
                    return Err(err("step must be > 0"));
                }
                (r, step)
            }
            None => (part, 1),
        };
        let (lo, hi) = if range == "*" {
            if step == 1 && part.len() == 1 && !field.contains(',') {
                restricted = false;
            }
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (
                a.parse().map_err(|_| err("invalid range start"))?,
                b.parse().map_err(|_| err("invalid range end"))?,
            )
        } else {
            let v: u32 = range.parse().map_err(|_| err("invalid value"))?;
            // `a/step` means "from a to max every step".
            (v, if part.contains('/') { max } else { v })
        };
        if lo < min || hi > max || lo > hi {
            return Err(err("value out of range"));
        }
        let mut v = lo;
        while v <= hi {
            mask |= 1u64 << v;
            v += step;
        }
    }
    Ok((mask, restricted))
}

impl CronSchedule {
    /// Parses a 5-field cron expression.
    pub fn parse(expr: &str) -> Result<Self, PluginError> {
        let f: Vec<&str> = expr.split_whitespace().collect();
        if f.len() != 5 {
            return Err(PluginError::Config(format!(
                "cron expression '{expr}' must have exactly 5 fields"
            )));
        }
        let (minutes, _) = parse_field(f[0], 0, 59, "minute")?;
        let (hours, _) = parse_field(f[1], 0, 23, "hour")?;
        let (dom, dom_restricted) = parse_field(f[2], 1, 31, "day-of-month")?;
        let (months, _) = parse_field(f[3], 1, 12, "month")?;
        let (mut dow, dow_restricted) = parse_field(f[4], 0, 7, "day-of-week")?;
        if dow & (1 << 7) != 0 {
            dow |= 1;
        }
        let day_matcher: DayMatcherFn = match (dom_restricted, dow_restricted) {
            (false, false) => match_unrestricted,
            (true, false) => match_dom_only,
            (false, true) => match_dow_only,
            (true, true) => match_dom_or_dow,
        };

        Ok(Self {
            minutes,
            hours: hours as u32,
            days_of_month: dom as u32,
            months: months as u16,
            days_of_week: (dow & 0x7f) as u8,
            day_matcher,
        })
    }

    /// Returns the first fire time (Unix seconds, minute-aligned) strictly after `after_unix`.
    ///
    /// Evaluates using direct function-pointer day matching and SWAR CTZ (`trailing_zeros`)
    /// register scans, eliminating the 1,440-minute scalar loop and division stalls.
    pub fn next_after(&self, after_unix: u64) -> Option<u64> {
        let start_day = after_unix / 86_400;
        let first_minute_of_day = (after_unix % 86_400) / 60 + 1; // strictly after
        for d in 0..MAX_SEARCH_DAYS {
            let day = start_day + d;
            let (_, month, dom) = civil_from_days(day as i64);
            if self.months & (1 << month) == 0 {
                continue;
            }
            let dow = ((day + 4) % 7) as u32; // 1970-01-01 was a Thursday
            if !(self.day_matcher)(self.days_of_month, dom, self.days_of_week, dow) {
                continue;
            }

            let start_h = if d == 0 {
                (first_minute_of_day / 60) as u32
            } else {
                0
            };
            let start_m = if d == 0 {
                (first_minute_of_day % 60) as u32
            } else {
                0
            };

            if start_h >= 24 {
                continue;
            }

            // SWAR CTZ Hour Search: find hours matching bits in self.hours starting from start_h
            let hour_mask =
                self.hours & !((1u32.checked_shl(start_h).unwrap_or(0)).wrapping_sub(1));
            let mut eligible_hours = hour_mask;
            while eligible_hours != 0 {
                let h = eligible_hours.trailing_zeros();
                eligible_hours &= eligible_hours - 1; // Clear lowest set bit

                let min_mask = if d == 0 && h == start_h {
                    if start_m >= 60 {
                        0
                    } else {
                        self.minutes & !((1u64.checked_shl(start_m).unwrap_or(0)).wrapping_sub(1))
                    }
                } else {
                    self.minutes
                };

                if min_mask != 0 {
                    let m = min_mask.trailing_zeros();
                    return Some(day * 86_400 + (h as u64) * 3600 + (m as u64) * 60);
                }
            }
        }
        None
    }
}

/// Converts days since 1970-01-01 to `(year, month 1-12, day 1-31)` (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-11-10 06:57:29 UTC (a Tuesday)
    const T: u64 = 1_794_380_249;

    #[test]
    fn daily_at_two() {
        let c = CronSchedule::parse("0 2 * * *").unwrap();
        let n = c.next_after(T).unwrap();
        assert_eq!(n % 86_400, 2 * 3600);
        assert!(n > T && n - T <= 86_400);
    }

    #[test]
    fn strictly_after() {
        let c = CronSchedule::parse("* * * * *").unwrap();
        let n = c.next_after(T).unwrap();
        assert_eq!(n % 60, 0);
        assert!(n > T && n - T <= 60);
        assert_eq!(c.next_after(n).unwrap(), n + 60);
    }

    #[test]
    fn steps_and_lists() {
        let c = CronSchedule::parse("*/15 3,4 * * *").unwrap();
        let n = c.next_after(0).unwrap();
        assert_eq!(n, 3 * 3600);
        assert_eq!(c.next_after(n).unwrap(), 3 * 3600 + 900);
    }

    #[test]
    fn day_of_week_sunday_alias() {
        let a = CronSchedule::parse("0 0 * * 0").unwrap();
        let b = CronSchedule::parse("0 0 * * 7").unwrap();
        assert_eq!(a, b);
        let n = a.next_after(T).unwrap();
        assert_eq!(((n / 86_400) + 4) % 7, 0);
    }

    #[test]
    fn leap_day_found() {
        let c = CronSchedule::parse("0 0 29 2 *").unwrap();
        let n = c.next_after(T).unwrap();
        let (y, m, d) = civil_from_days((n / 86_400) as i64);
        assert_eq!((y, m, d), (2028, 2, 29));
    }

    #[test]
    fn rejects_invalid() {
        for e in [
            "",
            "* * * *",
            "60 * * * *",
            "* 24 * * *",
            "*/0 * * * *",
            "a * * * *",
            "5-1 * * * *",
        ] {
            assert!(CronSchedule::parse(e).is_err(), "{e}");
        }
    }
}
