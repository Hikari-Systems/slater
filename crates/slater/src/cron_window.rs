//! A cron-style off-peak window for deferring the fraction-of-core auto-consolidation
//! (Phase 4d follow-up — the `delta.consolidateWindow` knob).
//!
//! Five space-separated fields `minute hour day-of-month month day-of-week`, evaluated
//! in **server-local** time. Each field is `*`, a single number, a range `a-b`, a
//! comma-list `a,c,e`, or a step `*/n` / `a-b/n`. Day-of-week uses `0`–`6` with `0` =
//! Sunday (`7` is also accepted as Sunday).
//!
//! The window has **hour granularity**: a consolidation deferral window is inherently
//! ≥ 1 hour, so the *minute* field is validated for cron compatibility but does **not**
//! narrow the window. Thus `"0 1-5 * * *"` and `"* 1-5 * * *"` both mean "01:00–05:59
//! daily". A due consolidation fires only when the current local time is inside the
//! window (or when no window is configured); the `deltaHardBytes` throttle is
//! unaffected and still fires anytime as the OOM backstop.

use anyhow::{bail, Context, Result};

/// A parsed cron-style consolidation window (hour granularity, server-local time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronWindow {
    hours: Field,  // 0–23
    doms: Field,   // 1–31 (day of month)
    months: Field, // 1–12
    dows: Field,   // 0–6 (0 = Sunday)
}

/// One cron field, precomputed as a membership set over `[0, max]`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    allowed: Vec<bool>,
}

impl Field {
    fn matches(&self, v: u32) -> bool {
        self.allowed.get(v as usize).copied().unwrap_or(false)
    }
}

/// Parse one cron field over the inclusive domain `[min, max]` into a membership set.
fn parse_field(spec: &str, min: u32, max: u32) -> Result<Field> {
    let mut allowed = vec![false; (max + 1) as usize];
    for part in spec.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (
                r,
                s.parse::<u32>()
                    .with_context(|| format!("step in {part:?} must be a number"))?,
            ),
            None => (part, 1),
        };
        if step == 0 {
            bail!("step must be ≥ 1 in {part:?}");
        }
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (
                a.parse::<u32>()
                    .with_context(|| format!("range low in {part:?}"))?,
                b.parse::<u32>()
                    .with_context(|| format!("range high in {part:?}"))?,
            )
        } else {
            let n = range
                .parse::<u32>()
                .with_context(|| format!("value {part:?}"))?;
            (n, n)
        };
        if lo < min || hi > max || lo > hi {
            bail!("{part:?} is out of range {min}-{max}");
        }
        let mut v = lo;
        while v <= hi {
            allowed[v as usize] = true;
            // A huge `step` (near `u32::MAX`) would overflow `v += step` — a panic in
            // debug, and in release a wrap back below `hi` that loops forever. Stop
            // cleanly once the next multiple leaves the `u32` range.
            v = match v.checked_add(step) {
                Some(next) => next,
                None => break,
            };
        }
    }
    Ok(Field { allowed })
}

impl CronWindow {
    /// Parse a 5-field cron-style window. An empty / whitespace-only spec yields
    /// `Ok(None)` (no window ⇒ no gating). The minute field is validated but unused
    /// (hour granularity — see the module docs).
    pub fn parse(spec: &str) -> Result<Option<CronWindow>> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Ok(None);
        }
        let fields: Vec<&str> = spec.split_whitespace().collect();
        if fields.len() != 5 {
            bail!(
                "a consolidate window needs 5 cron fields \
                 \"minute hour day-of-month month day-of-week\", got {}",
                fields.len()
            );
        }
        // Minute is validated for cron compatibility but not used (hour granularity).
        parse_field(fields[0], 0, 59).context("minute field")?;
        let hours = parse_field(fields[1], 0, 23).context("hour field")?;
        let doms = parse_field(fields[2], 1, 31).context("day-of-month field")?;
        let months = parse_field(fields[3], 1, 12).context("month field")?;
        let mut dows = parse_field(fields[4], 0, 7).context("day-of-week field")?;
        // 7 and 0 both mean Sunday; fold 7 onto 0 so `contains` can take 0–6.
        if dows.allowed.get(7).copied().unwrap_or(false) {
            dows.allowed[0] = true;
        }
        Ok(Some(CronWindow {
            hours,
            doms,
            months,
            dows,
        }))
    }

    /// Whether the given local time is inside the window: `hour` 0–23, `dom` 1–31,
    /// `month` 1–12, `dow` 0–6 with `0` = Sunday (`chrono`'s `num_days_from_sunday`).
    pub fn contains(&self, hour: u32, dom: u32, month: u32, dow: u32) -> bool {
        self.hours.matches(hour)
            && self.doms.matches(dom)
            && self.months.matches(month)
            && self.dows.matches(dow)
    }
}

/// The current **server-local** time as `(hour, day-of-month, month, day-of-week)` with
/// `day-of-week` 0–6 (`0` = Sunday) — the reads a [`CronWindow`] gates on. Isolated here
/// so the window logic ([`CronWindow::contains`]) stays a pure, clock-free function that
/// tests drive directly.
pub fn local_now_hms() -> (u32, u32, u32, u32) {
    use chrono::{Datelike, Local, Timelike};
    let now = Local::now();
    (
        now.hour(),
        now.day(),
        now.month(),
        now.weekday().num_days_from_sunday(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_spec_is_no_window() {
        assert_eq!(CronWindow::parse("").unwrap(), None);
        assert_eq!(CronWindow::parse("   ").unwrap(), None);
    }

    #[test]
    fn hour_range_window_matches_by_hour_only() {
        // "0 1-5 * * *" = 01:00–05:59 daily; the minute field is ignored.
        let w = CronWindow::parse("0 1-5 * * *").unwrap().unwrap();
        // Any minute of hours 1–5 is inside (hour granularity).
        assert!(w.contains(1, 15, 6, 3));
        assert!(w.contains(5, 1, 12, 0));
        // Hours outside 1–5 are outside.
        assert!(!w.contains(0, 15, 6, 3));
        assert!(!w.contains(6, 15, 6, 3));
        // A different minute spelling of the same window behaves identically.
        let w2 = CronWindow::parse("* 1-5 * * *").unwrap().unwrap();
        assert_eq!(w, w2);
    }

    #[test]
    fn lists_steps_and_weekday_restrict_the_window() {
        // Hours {1,2,3,22,23} on weekdays (Mon–Fri) in any month.
        let w = CronWindow::parse("0 1-3,22-23 * * 1-5").unwrap().unwrap();
        assert!(w.contains(2, 10, 6, 3)); // 02:xx Wednesday
        assert!(w.contains(23, 10, 6, 1)); // 23:xx Monday
        assert!(!w.contains(2, 10, 6, 0)); // Sunday excluded
        assert!(!w.contains(12, 10, 6, 3)); // noon excluded
                                            // Step: every 2nd hour from 0 → {0,2,4,…,22}.
        let s = CronWindow::parse("0 */2 * * *").unwrap().unwrap();
        assert!(s.contains(4, 1, 1, 1));
        assert!(!s.contains(5, 1, 1, 1));
    }

    #[test]
    fn day_of_week_seven_is_sunday() {
        let w = CronWindow::parse("0 0-23 * * 7").unwrap().unwrap();
        assert!(w.contains(3, 1, 1, 0), "dow 7 folds onto Sunday (0)");
        assert!(!w.contains(3, 1, 1, 1));
    }

    #[test]
    fn day_of_month_and_month_fields_apply() {
        // Only the 1st of January, any hour.
        let w = CronWindow::parse("0 * 1 1 *").unwrap().unwrap();
        assert!(w.contains(10, 1, 1, 4));
        assert!(!w.contains(10, 2, 1, 4)); // 2nd excluded
        assert!(!w.contains(10, 1, 2, 4)); // February excluded
    }

    #[test]
    fn malformed_specs_are_rejected() {
        assert!(CronWindow::parse("1-5 * * *").is_err(), "too few fields");
        assert!(CronWindow::parse("0 1 2 3 4 5").is_err(), "too many fields");
        assert!(
            CronWindow::parse("0 24 * * *").is_err(),
            "hour out of range"
        );
        assert!(CronWindow::parse("0 5-1 * * *").is_err(), "reversed range");
        assert!(CronWindow::parse("0 */0 * * *").is_err(), "zero step");
        assert!(
            CronWindow::parse("x 1 * * *").is_err(),
            "non-numeric minute"
        );
        assert!(CronWindow::parse("0 * 0 * *").is_err(), "day-of-month 0");
    }

    #[test]
    fn huge_step_does_not_overflow_or_hang() {
        // A step near u32::MAX must terminate after the first multiple instead of
        // overflowing (panic in debug / infinite wrap-around loop in release).
        let step = u32::MAX;
        let f = parse_field(&format!("0-23/{step}"), 0, 23).expect("parses");
        // Only the low endpoint is set; the next multiple is out of the u32 range.
        assert!(f.allowed[0]);
        assert!(f.allowed[1..].iter().all(|&b| !b));
    }
}
