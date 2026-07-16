//! Temporal value support — `date`, `localtime`, `localdatetime`, `duration`.
//!
//! Ported from FalkorDB's `src/datatypes/{date,time,datetime,duration}.c` and
//! `src/arithmetic/{time_funcs,temporal_arithmetic}/*.c`. The defining fact of
//! FalkorDB's temporal model is that **every** temporal value is a single
//! `time_t` (whole seconds since the Unix epoch, UTC) plus a type tag:
//!
//! - **Date** — seconds at UTC midnight of the day.
//! - **Time** (`localtime`) — seconds since midnight, in `[0, 86400)`. FalkorDB
//!   keeps it as a `time_t` anchored at 1900-01-01; only the time-of-day is ever
//!   observed (toString / components / compare), so Slater stores the
//!   seconds-of-day directly — observationally identical, and a clean
//!   `nanoOfDay` for the Bolt `LocalTime` struct.
//! - **DateTime** (`localdatetime`) — seconds since epoch.
//! - **Duration** — the `time_t` of *epoch + duration*; reading components back
//!   decomposes that absolute instant relative to the epoch (so e.g. `weeks`
//!   always folds into `days`). This is exactly FalkorDB's
//!   `duration_from_epoch_utc` / `duration_from_time_t_utc` round trip.
//!
//! Because the storage is whole seconds, sub-second precision (milli/micro/nano)
//! is dropped on construction — matching FalkorDB, whose components report `0`
//! for those fields and whose `toString` never prints a fraction.
//!
//! This module is deliberately `Val`-free (like [`crate::algo`] / [`crate::vector`]):
//! it works in primitive `i64`/`f64` and `chrono` calendar types; the executor
//! does the `Val` map-extraction + validation and wraps the results.

use chrono::{DateTime, Datelike, Days, NaiveDate, NaiveDateTime, Timelike, Utc};

const SECONDS_IN_DAY: i64 = 86_400;
const SECONDS_IN_HOUR: i64 = 3_600;
const SECONDS_IN_MINUTE: i64 = 60;

/// Which calendar fields a temporal carries — drives both component access and
/// the field selection in temporal ± duration. `Duration` is handled apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TKind {
    Date,
    Time,
    DateTime,
}

// ── seconds ↔ chrono ──────────────────────────────────────────────────────────

fn to_ndt(secs: i64) -> NaiveDateTime {
    DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|dt| dt.naive_utc())
        // The validated component/string ranges keep us well inside chrono's
        // proleptic-Gregorian domain; only an absurd out-of-range epoch second
        // could hit this, which our constructors never produce.
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap().naive_utc())
}

fn date_to_secs(d: NaiveDate) -> i64 {
    d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()
}

// ── Date construction (FalkorDB date.c / time_funcs.c AR_DATE) ─────────────────

/// `date({year, month, day})` — `timegm`-style normalisation: an out-of-range
/// `day` (e.g. 31 in a 30-day month) rolls forward, matching FalkorDB's
/// `DateTime_fromComponents`.
pub fn date_from_components(year: i32, month: u32, day: i64) -> i64 {
    date_to_secs(ymd_normalized(year, month, day))
}

/// `date({year, week, dayOfWeek})` — ISO-8601 week date, ported verbatim from
/// FalkorDB `DateTime_fromWeekDate`: anchor on Jan 4 (always in week 1), step
/// back to that week's Monday, then add `(week-1)*7 + (dayOfWeek-1)` days. The
/// arithmetic is **lenient** (week 53 of a 52-week year just runs on), unlike
/// `chrono::from_isoywd_opt`, so out-of-range weeks behave as FalkorDB does.
pub fn date_from_week(year: i32, week: i64, day_of_week: i64) -> i64 {
    date_to_secs(week_date(year, week, day_of_week))
}

/// `date({year, quarter, dayOfQuarter})` — FalkorDB `DateTime_fromQuarterDate`:
/// start at the quarter's first month, add `dayOfQuarter-1` days (may overflow
/// into later months).
pub fn date_from_quarter(year: i32, quarter: i64, day_of_quarter: i64) -> i64 {
    date_to_secs(quarter_date(year, quarter, day_of_quarter))
}

/// `localdatetime({...})` map forms — a date built one of three ways plus a
/// clock offset, exactly as FalkorDB layers `hour/minute/second` over the date.
pub fn datetime_from_components(year: i32, month: u32, day: i64, hms: (i64, i64, i64)) -> i64 {
    date_to_secs(ymd_normalized(year, month, day)) + hms_secs(hms)
}

pub fn datetime_from_week(year: i32, week: i64, dow: i64, hms: (i64, i64, i64)) -> i64 {
    date_to_secs(week_date(year, week, dow)) + hms_secs(hms)
}

pub fn datetime_from_quarter(year: i32, quarter: i64, doq: i64, hms: (i64, i64, i64)) -> i64 {
    date_to_secs(quarter_date(year, quarter, doq)) + hms_secs(hms)
}

/// `localtime({hour, minute, second})` → seconds since midnight (sub-second
/// fields are validated by the caller but dropped, matching FalkorDB's `time_t`).
pub fn time_from_components(hour: i64, minute: i64, second: i64) -> i64 {
    (hms_secs((hour, minute, second))).rem_euclid(SECONDS_IN_DAY)
}

fn hms_secs((h, m, s): (i64, i64, i64)) -> i64 {
    h * SECONDS_IN_HOUR + m * SECONDS_IN_MINUTE + s
}

/// `NaiveDate` from y/m/d allowing `day` to overflow its month (timegm-style):
/// build the first of the month, then add `day-1` days.
fn ymd_normalized(year: i32, month: u32, day: i64) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_else(epoch_date);
    add_days(first, day - 1)
}

fn week_date(year: i32, week: i64, day_of_week: i64) -> NaiveDate {
    // Jan 4 is always in ISO week 1.
    let jan4 = NaiveDate::from_ymd_opt(year, 1, 4).unwrap_or_else(epoch_date);
    let iso_wday = jan4.weekday().number_from_monday() as i64; // Mon=1 .. Sun=7
    let monday_w1 = add_days(jan4, -(iso_wday - 1));
    add_days(monday_w1, (week - 1) * 7 + (day_of_week - 1))
}

fn quarter_date(year: i32, quarter: i64, day_of_quarter: i64) -> NaiveDate {
    let base_month = (quarter - 1) * 3 + 1; // 1-based: Q1→Jan, Q2→Apr, …
    let start = NaiveDate::from_ymd_opt(year, base_month as u32, 1).unwrap_or_else(epoch_date);
    add_days(start, day_of_quarter - 1)
}

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

/// Signed day offset (chrono's `Days` is unsigned, so split on the sign).
fn add_days(d: NaiveDate, n: i64) -> NaiveDate {
    let r = if n >= 0 {
        d.checked_add_days(Days::new(n as u64))
    } else {
        d.checked_sub_days(Days::new((-n) as u64))
    };
    r.unwrap_or(d)
}

// ── String parsing ────────────────────────────────────────────────────────────

/// `date('…')` — the ISO-8601 subset FalkorDB's `Date_fromString` accepts.
/// `None` on a malformed string (→ Cypher `null`).
pub fn date_from_string(s: &str) -> Option<i64> {
    parse_date(s).map(date_to_secs)
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    // Week date: YYYY-Www or YYYY-Www-D (checked first — contains both '-' and 'W').
    if let Some(wpos) = s.find('W') {
        let year: i32 = s.get(0..4)?.parse().ok()?;
        let rest = &s[wpos + 1..];
        let mut parts = rest.split('-');
        let week: i64 = parts.next()?.parse().ok()?;
        let dow: i64 = match parts.next() {
            Some(d) => d.parse().ok()?,
            None => 1,
        };
        if !(1..=53).contains(&week) || !(1..=7).contains(&dow) {
            return None;
        }
        return Some(week_date(year, week, dow));
    }
    if s.contains('-') {
        // YYYY-MM-DD then YYYY-MM.
        let parts: Vec<&str> = s.split('-').collect();
        let year: i32 = parts.first()?.parse().ok()?;
        let month: u32 = parts.get(1)?.parse().ok()?;
        let day: i64 = match parts.get(2) {
            Some(d) => d.parse().ok()?,
            None => 1,
        };
        if !(1..=12).contains(&month) {
            return None;
        }
        return Some(ymd_normalized(year, month, day));
    }
    // Compact, length-disambiguated.
    if !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match s.len() {
        8 => {
            // YYYYMMDD
            let year: i32 = s[0..4].parse().ok()?;
            let month: u32 = s[4..6].parse().ok()?;
            let day: i64 = s[6..8].parse().ok()?;
            if !(1..=12).contains(&month) {
                return None;
            }
            Some(ymd_normalized(year, month, day))
        }
        7 => {
            // YYYYDDD ordinal date
            let year: i32 = s[0..4].parse().ok()?;
            let ord: u32 = s[4..7].parse().ok()?;
            if !(1..=366).contains(&ord) {
                return None;
            }
            NaiveDate::from_yo_opt(year, ord)
        }
        6 => {
            // YYYYMM
            let year: i32 = s[0..4].parse().ok()?;
            let month: u32 = s[4..6].parse().ok()?;
            if !(1..=12).contains(&month) {
                return None;
            }
            NaiveDate::from_ymd_opt(year, month, 1)
        }
        4 => {
            let year: i32 = s.parse().ok()?;
            NaiveDate::from_ymd_opt(year, 1, 1)
        }
        _ => None,
    }
}

/// `localtime('…')` — FalkorDB `Time_fromString`: colon (`HH:MM[:SS]`) and
/// compact (`H`/`HH`/`HMM`/`HHMM`/`HMMSS`/`HHMMSS`) forms, fractional seconds
/// parsed-then-dropped. `None` on malformed input.
pub fn time_from_string(s: &str) -> Option<i64> {
    let (h, m, sec) = parse_clock(s)?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) || !(0..=59).contains(&sec) {
        return None;
    }
    Some(time_from_components(h, m, sec))
}

/// Parse a clock string into `(hour, minute, second)`; the fractional part (if
/// any) is consumed but discarded (whole-second storage).
fn parse_clock(s: &str) -> Option<(i64, i64, i64)> {
    let int_part = s.split('.').next().unwrap_or(s);
    if int_part.contains(':') {
        let mut it = int_part.split(':');
        let h: i64 = it.next()?.parse().ok()?;
        let m: i64 = it.next()?.parse().ok()?;
        let sec: i64 = match it.next() {
            Some(x) => x.parse().ok()?,
            None => 0,
        };
        return Some((h, m, sec));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let v: i64 = int_part.parse().ok()?;
    match int_part.len() {
        1 | 2 => Some((v, 0, 0)),                             // H / HH
        3 | 4 => Some((v / 100, v % 100, 0)),                 // HMM / HHMM
        5 | 6 => Some((v / 10000, (v / 100) % 100, v % 100)), // HMMSS / HHMMSS
        _ => None,
    }
}

/// `localdatetime('…')` — FalkorDB `_parse_iso8601`: an optional `T`-separated
/// date and time, each in the compact or extended forms above. `None` on failure.
pub fn datetime_from_string(s: &str) -> Option<i64> {
    let (date_str, time_str) = match s.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let date = parse_date(date_str)?;
    let secs = date_to_secs(date);
    match time_str {
        None => Some(secs),
        Some(t) => {
            let (h, m, sec) = parse_clock(t)?;
            if !(0..=23).contains(&h) || !(0..=59).contains(&m) || !(0..=59).contains(&sec) {
                return None;
            }
            Some(secs + hms_secs((h, m, sec)))
        }
    }
}

// ── String rendering (toString) ───────────────────────────────────────────────

pub fn date_to_string(secs: i64) -> String {
    let d = to_ndt(secs).date();
    format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day())
}

pub fn time_to_string(secs: i64) -> String {
    let s = secs.rem_euclid(SECONDS_IN_DAY);
    format!(
        "{:02}:{:02}:{:02}",
        s / SECONDS_IN_HOUR,
        (s / SECONDS_IN_MINUTE) % SECONDS_IN_MINUTE,
        s % SECONDS_IN_MINUTE
    )
}

pub fn datetime_to_string(secs: i64) -> String {
    let dt = to_ndt(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// `toString(duration)` — FalkorDB `Duration_toString`: `PnYnMnDTnHnMnS`, each
/// part omitted when zero (`weeks` always folds into `days`). Components are
/// whole numbers after the `time_t` round trip, so `%​.9g` reduces to a plain
/// integer — no float formatting is involved.
pub fn duration_to_string(secs: i64) -> String {
    let d = duration_components(secs);
    let mut out = String::from("P");
    if d.years != 0 {
        out.push_str(&format!("{}Y", d.years));
    }
    if d.months != 0 {
        out.push_str(&format!("{}M", d.months));
    }
    if d.days != 0 {
        out.push_str(&format!("{}D", d.days));
    }
    if d.hours != 0 || d.minutes != 0 || d.seconds != 0 {
        out.push('T');
        if d.hours != 0 {
            out.push_str(&format!("{}H", d.hours));
        }
        if d.minutes != 0 {
            out.push_str(&format!("{}M", d.minutes));
        }
        if d.seconds != 0 {
            out.push_str(&format!("{}S", d.seconds));
        }
    }
    out
}

// ── Component access (property reads) ─────────────────────────────────────────

/// Date / DateTime component (FalkorDB `Date_getComponent` / `DateTime_getComponent`),
/// keyed case-insensitively. `None` → the caller raises "unknown … component".
/// `with_time` selects the DateTime variant (clock components + millis-as-0).
pub fn date_component(secs: i64, key: &str, with_time: bool) -> Option<i64> {
    let dt = to_ndt(secs);
    let d = dt.date();
    let month = d.month() as i64;
    match key.to_ascii_lowercase().as_str() {
        "year" => Some(d.year() as i64),
        "month" => Some(month),
        "day" => Some(d.day() as i64),
        "dayofweek" => Some(d.weekday().num_days_from_sunday() as i64), // Sun=0 .. Sat=6
        "weekday" => Some(d.weekday().number_from_monday() as i64),     // Mon=1 .. Sun=7
        "ordinalday" => Some(d.ordinal() as i64),
        "quarter" => Some((month - 1) / 3 + 1),
        // Standard ISO-8601 week / week-year. FalkorDB hand-rolls these (and
        // `date.c` and `datetime.c` disagree in edge cases); they agree with the
        // ISO calendar on every tested vector, so we use chrono's ISO week.
        "week" => Some(d.iso_week().week() as i64),
        "weekyear" => Some(d.iso_week().year() as i64),
        // FalkorDB `Date_getComponent`'s dayOfQuarter is its own (off-by-one,
        // non-leap-table) formula; ported verbatim so the documented vector
        // (1984-10-21 → 23) matches.
        "dayofquarter" | "quarterday" => Some(day_of_quarter(d.ordinal() as i64, month)),
        // Clock components — only on DateTime; `0` for sub-second (no precision).
        "hour" if with_time => Some(dt.hour() as i64),
        "minute" if with_time => Some(dt.minute() as i64),
        "second" if with_time => Some(dt.second() as i64),
        "millisecond" | "microsecond" | "nanosecond" if with_time => Some(0),
        _ => None,
    }
}

/// FalkorDB `Date_getComponent` dayOfQuarter, ported including its quirks.
fn day_of_quarter(ordinal: i64, month: i64) -> i64 {
    const DAYS_UNTIL_QUARTER: [i64; 5] = [0, 0, 90, 181, 273];
    let q = (month - 1) / 3 + 1;
    let doq = if q > 1 {
        ordinal - DAYS_UNTIL_QUARTER[q as usize]
    } else {
        ordinal
    };
    doq + 1
}

/// Time component (FalkorDB `Time_getComponent`): hour / minute / second.
pub fn time_component(secs: i64, key: &str) -> Option<i64> {
    let s = secs.rem_euclid(SECONDS_IN_DAY);
    match key.to_ascii_lowercase().as_str() {
        "hour" => Some(s / SECONDS_IN_HOUR),
        "minute" => Some((s / SECONDS_IN_MINUTE) % SECONDS_IN_MINUTE),
        "second" => Some(s % SECONDS_IN_MINUTE),
        _ => None,
    }
}

/// Duration component (FalkorDB `Duration_getComponent`) — returns a float to
/// mirror FalkorDB (`SI_DoubleVal`); `weeks` always reads back `0`.
pub fn duration_component(secs: i64, key: &str) -> Option<f64> {
    let d = duration_components(secs);
    Some(match key.to_ascii_lowercase().as_str() {
        "years" => d.years as f64,
        "months" => d.months as f64,
        "weeks" => 0.0,
        "days" => d.days as f64,
        "hours" => d.hours as f64,
        "minutes" => d.minutes as f64,
        "seconds" => d.seconds as f64,
        _ => return None,
    })
}

// ── Duration encode / decode (FalkorDB duration.c) ────────────────────────────

/// The broken-down duration FalkorDB stores and reads back. `weeks` is never
/// produced by the decode (it folds into `days`); kept off the struct for that
/// reason.
#[derive(Debug, Clone, Copy)]
pub struct DurationParts {
    pub years: i64,
    pub months: i64,
    pub days: i64,
    pub hours: i64,
    pub minutes: i64,
    pub seconds: i64,
}

/// A `duration(…)` the engine cannot represent as a `time_t`.
///
/// Carried as a typed error so callers classify it by *type*
/// (`err.downcast_ref::<DurationOutOfRange>()`), never by matching the message
/// text (house rule; `slater_scalar::ArithmeticOverflow` is the same shape on
/// integer arithmetic).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DurationOutOfRange {
    /// `duration({years: 1e400})` — the literal parses to `f64` `INFINITY`
    /// (`parse::<f64>` never errors on overflow), and `NaN` can arrive the same
    /// way. Both saturate on an `as` cast, so they are rejected up front.
    #[error("duration component `{0}` is not a finite number")]
    NonFinite(&'static str),
    /// Finite, but with no whole `i64` counterpart — e.g. `duration({years:
    /// 1e19})`, which is past `i64::MAX`.
    #[error("duration component `{0}` is too large to represent")]
    Component(&'static str),
    /// The components are individually representable but their fold is not.
    #[error("duration is outside the representable range")]
    Overflow,
}

/// A user-supplied `f64` duration component as a whole `i64`, or `None` if it
/// has no `i64` counterpart.
///
/// Rust's float→int `as` cast **saturates**: `1e19 as i64` is `i64::MAX`, and so
/// is `f64::INFINITY as i64`. That turns an absurd request into a
/// plausible-looking number which the arithmetic downstream then wraps — the
/// shape of this module's past bug — so every cast of a user-supplied float goes
/// through here instead.
fn whole_i64(v: f64) -> Option<i64> {
    if !v.is_finite() {
        return None;
    }
    let t = v.trunc();
    // 2^63 is exactly representable as an f64; `i64::MAX` is not (it rounds *up*
    // to 2^63), so a bound spelled `i64::MAX as f64` would wrongly admit 2^63
    // itself — which is precisely the input that used to answer `P-1Y`. The
    // range is half-open for the same reason: `-2^63` *is* exactly `i64::MIN`.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0; // 2^63
    if !(-LIMIT..LIMIT).contains(&t) {
        return None;
    }
    Some(t as i64)
}

/// `duration_from_epoch_utc`: fold component amounts (calendar years/months by
/// the calendar, everything else as fixed seconds) into the `time_t` of
/// *epoch + duration*. Fractional years/months use FalkorDB's 365.25 / 30.44
/// averages; the final conversion to seconds truncates (matching the C
/// `(time_t)`).
///
/// Every component is user-supplied and unbounded, so anything unrepresentable
/// is a clean [`DurationOutOfRange`] rather than a saturated cast followed by
/// wrapping arithmetic.
pub fn duration_to_timet(
    years: f64,
    months: f64,
    weeks: f64,
    days: f64,
    hours: f64,
    minutes: f64,
    seconds: f64,
) -> Result<i64, DurationOutOfRange> {
    for (name, v) in [
        ("years", years),
        ("months", months),
        ("weeks", weeks),
        ("days", days),
        ("hours", hours),
        ("minutes", minutes),
        ("seconds", seconds),
    ] {
        if !v.is_finite() {
            return Err(DurationOutOfRange::NonFinite(name));
        }
    }
    let years_int = whole_i64(years).ok_or(DurationOutOfRange::Component("years"))?;
    let months_int = whole_i64(months).ok_or(DurationOutOfRange::Component("months"))?;

    // 1970-01-01 + whole years + whole months, by the calendar.
    let total_months = years_int
        .checked_mul(12)
        .and_then(|m| m.checked_add(months_int))
        .ok_or(DurationOutOfRange::Overflow)?;
    let base_date =
        add_calendar_months(epoch_date(), total_months).ok_or(DurationOutOfRange::Overflow)?;
    let base_time = date_to_secs(base_date);

    let extra_days = (years - years_int as f64) * 365.25 + (months - months_int as f64) * 30.44;
    let mut total_seconds = 0.0_f64;
    total_seconds += (weeks * 7.0 + days + extra_days) * SECONDS_IN_DAY as f64;
    total_seconds += hours * SECONDS_IN_HOUR as f64;
    total_seconds += minutes * SECONDS_IN_MINUTE as f64;
    total_seconds += seconds;

    // The seconds side is the same trap one fold down: `days: 1e18` is a
    // perfectly representable component but 8.64e22 seconds, which saturates to
    // `i64::MAX` and then overflows the add whenever `base_time` is non-zero.
    let offset = whole_i64(total_seconds).ok_or(DurationOutOfRange::Overflow)?;
    let t = base_time
        .checked_add(offset)
        .ok_or(DurationOutOfRange::Overflow)?;

    // The `time_t` must be an instant chrono can decode. `duration_components`
    // reads every duration back through chrono, and `to_ndt` answers an
    // undecodable `time_t` with the *epoch* — so the entire span reappears as
    // `days` (`duration({days: 1e14})` decodes to 1e14 days), which then
    // overflows `(day - 1 + s * d.days) * SECONDS_IN_DAY` in `add_duration`.
    // Every `Val::Duration` in the engine is built by this function (Bolt
    // refuses a struct parameter, and durations are not persisted), so this
    // check is what keeps the decode side total.
    if DateTime::<Utc>::from_timestamp(t, 0).is_none() {
        return Err(DurationOutOfRange::Overflow);
    }
    Ok(t)
}

/// `duration_from_time_t_utc`: decompose an absolute `time_t` back into
/// duration-since-epoch components — calendar year/month difference from the
/// epoch, then the remaining seconds split into days/hours/minutes/seconds.
pub fn duration_components(secs: i64) -> DurationParts {
    let target = to_ndt(secs);
    let mut year_diff = target.year() as i64 - 1970;
    let mut month_diff = target.month0() as i64; // epoch month0 = 0

    if month_diff < 0 {
        year_diff -= 1;
        month_diff += 12;
    }

    // `year_diff`/`month_diff` are read straight back out of a real
    // `NaiveDateTime`, so the anchor is always in range; the fallback only
    // satisfies the type.
    let anchor =
        add_calendar_months(epoch_date(), year_diff * 12 + month_diff).unwrap_or_else(epoch_date);
    let anchor_time = date_to_secs(anchor);

    let mut delta = secs - anchor_time;
    let days = delta / SECONDS_IN_DAY;
    delta -= days * SECONDS_IN_DAY;
    let hours = delta / SECONDS_IN_HOUR;
    delta -= hours * SECONDS_IN_HOUR;
    let minutes = delta / SECONDS_IN_MINUTE;
    delta -= minutes * SECONDS_IN_MINUTE;

    DurationParts {
        years: year_diff,
        months: month_diff,
        days,
        hours,
        minutes,
        seconds: delta,
    }
}

/// `duration('P…')` — FalkorDB `_parse_duration` (integer designators only),
/// then encode to a `time_t`.
///
/// `Ok(None)` is a malformed string (→ Cypher `null`, FalkorDB's behaviour);
/// `Err` is a well-formed string naming a duration we cannot represent, e.g.
/// `duration('P9999999999999999999Y')`. The two are distinct: a `null` for the
/// second would be another way of quietly not answering the question.
pub fn duration_from_string(s: &str) -> Result<Option<i64>, DurationOutOfRange> {
    let Some((years, months, weeks, days, hours, minutes, seconds)) = parse_duration_parts(s)
    else {
        return Ok(None);
    };
    duration_to_timet(years, months, weeks, days, hours, minutes, seconds).map(Some)
}

/// The `P…` designator scan itself. `None` on a malformed string.
fn parse_duration_parts(s: &str) -> Option<(f64, f64, f64, f64, f64, f64, f64)> {
    let mut bytes = s.bytes().peekable();
    if bytes.next() != Some(b'P') {
        return None;
    }
    let (mut years, mut months, mut weeks, mut days) = (0.0, 0.0, 0.0, 0.0);
    let (mut hours, mut minutes, mut seconds) = (0.0, 0.0, 0.0);
    let mut in_time = false;
    let chars: Vec<char> = s[1..].chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == 'T' {
            in_time = true;
            i += 1;
            continue;
        }
        // Parse a (possibly signed) integer.
        let start = i;
        if chars[i] == '-' || chars[i] == '+' {
            i += 1;
        }
        let digit_start = i;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
        if i == digit_start || i >= chars.len() {
            return None; // no digits, or no trailing designator
        }
        let val: f64 = chars[start..i].iter().collect::<String>().parse().ok()?;
        match chars[i] {
            'Y' => years = val,
            'M' if in_time => minutes = val,
            'M' => months = val,
            'W' => weeks = val,
            'D' => days = val,
            'H' => hours = val,
            'S' => seconds = val,
            _ => return None,
        }
        i += 1;
    }
    Some((years, months, weeks, days, hours, minutes, seconds))
}

// ── Arithmetic (FalkorDB temporal_arithmetic.c) ───────────────────────────────

/// `temporal ± duration` → a temporal of the same kind. The duration's
/// components are applied to the temporal's broken-down fields and re-normalised
/// `timegm`-style: only the calendar parts touch Date, only clock parts touch
/// Time, both touch DateTime. `sub` negates the duration.
pub fn add_duration(kind: TKind, secs: i64, dur_secs: i64, sub: bool) -> i64 {
    let d = duration_components(dur_secs);
    let s = if sub { -1 } else { 1 };

    match kind {
        TKind::Time => {
            let delta = s * (d.hours * SECONDS_IN_HOUR + d.minutes * SECONDS_IN_MINUTE + d.seconds);
            (secs + delta).rem_euclid(SECONDS_IN_DAY)
        }
        TKind::Date | TKind::DateTime => {
            let dt = to_ndt(secs);
            let (y, mo0) = (dt.year() as i64, dt.month0() as i64);
            let (day, hh, mm, ss) = (
                dt.day() as i64,
                dt.hour() as i64,
                dt.minute() as i64,
                dt.second() as i64,
            );

            // Calendar carry: fold the month overflow into the year.
            let total_month = y * 12 + mo0 + s * (d.years * 12 + d.months);
            let y2 = total_month.div_euclid(12) as i32;
            let mo2 = (total_month.rem_euclid(12) + 1) as u32;
            let start = NaiveDate::from_ymd_opt(y2, mo2, 1).unwrap_or_else(epoch_date);

            let mut out = date_to_secs(start);
            out += (day - 1 + s * d.days) * SECONDS_IN_DAY;
            if matches!(kind, TKind::DateTime) {
                out += (hh + s * d.hours) * SECONDS_IN_HOUR
                    + (mm + s * d.minutes) * SECONDS_IN_MINUTE
                    + (ss + s * d.seconds);
            }
            out
        }
    }
}

/// `duration ± duration` — component-wise (FalkorDB `_AddDurations` /
/// `_SubDurations`), re-encoded through the `time_t` (which normalises any
/// overflow, e.g. 66 minutes → 1h6m).
///
/// Both operands decode back out of a stored `time_t`, so every component is
/// already bounded by chrono's calendar and the re-encode cannot realistically
/// fail — but it is fallible, so the error is propagated rather than assumed
/// away.
pub fn add_durations(a: i64, b: i64, sub: bool) -> Result<i64, DurationOutOfRange> {
    let x = duration_components(a);
    let y = duration_components(b);
    let s = if sub { -1 } else { 1 };
    duration_to_timet(
        (x.years + s * y.years) as f64,
        (x.months + s * y.months) as f64,
        0.0,
        (x.days + s * y.days) as f64,
        (x.hours + s * y.hours) as f64,
        (x.minutes + s * y.minutes) as f64,
        (x.seconds + s * y.seconds) as f64,
    )
}

/// 1970-01-01 + a signed number of calendar months (chrono's `Months` is
/// unsigned; first-of-month so no day-clamping ever occurs).
///
/// `None` if the result falls outside chrono's proleptic-Gregorian range. This
/// used to end in `div_euclid(12) as i32` — another saturating cast — behind an
/// `unwrap_or_else(epoch_date)`, so an out-of-calendar duration answered *the
/// epoch* rather than failing.
fn add_calendar_months(d: NaiveDate, months: i64) -> Option<NaiveDate> {
    let total = (d.year() as i64)
        .checked_mul(12)?
        .checked_add(d.month0() as i64)?
        .checked_add(months)?;
    let y = i32::try_from(total.div_euclid(12)).ok()?;
    let m = (total.rem_euclid(12) + 1) as u32;
    NaiveDate::from_ymd_opt(y, m, d.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `whole_i64` is the guard that replaced the saturating `as` casts, so its
    /// boundary is the whole fix: `1e19 as i64` was `i64::MAX` and
    /// `INFINITY as i64` was `i64::MAX` too — a saturated *number*, which the
    /// caller then had no way to tell from a real one.
    #[test]
    fn whole_i64_rejects_what_an_as_cast_would_have_saturated() {
        // The exact bound. 2^63 has no i64 counterpart; the largest f64 below it
        // that does is 2^63 - 1024 (f64 cannot represent i64::MAX itself).
        assert_eq!(whole_i64(9_223_372_036_854_775_808.0), None); // 2^63
        assert_eq!(
            whole_i64(-9_223_372_036_854_775_808.0), // -2^63 == i64::MIN, exact
            Some(i64::MIN)
        );
        assert_eq!(
            whole_i64(9_223_372_036_854_774_784.0), // 2^63 - 1024
            Some(9_223_372_036_854_774_784)
        );
        // `i64::MAX as f64` rounds *up* to 2^63, so a bound written that way
        // would have admitted an unrepresentable value. It does not.
        assert_eq!(whole_i64(i64::MAX as f64), None);
        assert_eq!(whole_i64(i64::MIN as f64), Some(i64::MIN));

        // The reported inputs.
        assert_eq!(whole_i64(1e19), None);
        assert_eq!(whole_i64(-1e19), None);
        assert_eq!(whole_i64(f64::INFINITY), None);
        assert_eq!(whole_i64(f64::NEG_INFINITY), None);
        assert_eq!(whole_i64(f64::NAN), None);

        // Ordinary values still truncate toward zero, as the C `(time_t)` did.
        assert_eq!(whole_i64(1.9), Some(1));
        assert_eq!(whole_i64(-1.9), Some(-1));
        assert_eq!(whole_i64(0.0), Some(0));
    }

    /// Every component is user-supplied, so every one of them is checked — not
    /// just `years`, which is where the bug was reported.
    #[test]
    fn every_absurd_component_is_a_typed_error() {
        const NAMES: [&str; 7] = [
            "years", "months", "weeks", "days", "hours", "minutes", "seconds",
        ];
        // One component set to `v`, the rest zero.
        let only = |slot: usize, v: f64| {
            let mut c = [0.0_f64; 7];
            c[slot] = v;
            duration_to_timet(c[0], c[1], c[2], c[3], c[4], c[5], c[6])
        };
        for (slot, name) in NAMES.iter().enumerate() {
            assert_eq!(
                only(slot, f64::INFINITY),
                Err(DurationOutOfRange::NonFinite(name)),
                "component `{name}` = INFINITY",
            );
            assert_eq!(
                only(slot, f64::NEG_INFINITY),
                Err(DurationOutOfRange::NonFinite(name)),
                "component `{name}` = -INFINITY",
            );
            assert_eq!(
                only(slot, f64::NAN),
                Err(DurationOutOfRange::NonFinite(name)),
                "component `{name}` = NaN",
            );
            // Finite but absurd: `years`/`months` fail on their own cast, the
            // rest on the seconds fold — either way, an error, never a value.
            assert!(only(slot, 1e19).is_err(), "component `{name}` = 1e19");
            assert!(only(slot, -1e19).is_err(), "component `{name}` = -1e19");
        }
    }

    /// The reported case, at the level the cast lives.
    ///
    /// Pre-fix this returned `-31_536_000` in release (`i64::MAX * 12` wraps to
    /// `-12` months → epoch minus one year) and panicked in debug.
    #[test]
    fn ten_quintillion_years_is_not_minus_one_year() {
        assert_eq!(
            duration_to_timet(1e19, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            Err(DurationOutOfRange::Component("years"))
        );
        // A year count that survives the cast but overflows the ×12 fold.
        assert_eq!(
            duration_to_timet(9e18, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            Err(DurationOutOfRange::Overflow)
        );
        // …and one that survives the fold but leaves chrono's calendar. This
        // needs no overflow at all to go wrong: pre-fix, `add_calendar_months`
        // ran `div_euclid(12) as i32` into a `from_ymd_opt(…).unwrap_or_else(
        // epoch_date)`, so *every* year count past chrono's 262143 silently
        // returned a duration of **zero** (verified against v0.23.1:
        // `duration({years: 300000})` → `secs=0` → `"P"`). The cap is the
        // duration that lands on chrono's last year (`NaiveDate::MAX` is
        // +262142-12-31) — and a duration counts from 1970, so it is 260172
        // years, not 262142.
        assert!(duration_to_timet(260_172.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0).is_ok());
        for absurd in [260_173.0, 300_000.0, 1e9] {
            assert_eq!(
                duration_to_timet(absurd, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                Err(DurationOutOfRange::Overflow),
                "{absurd} years must not be a silent zero",
            );
        }
        // The seconds fold: representable component, unrepresentable seconds.
        // `base_time` is non-zero here, which is what made the add overflow.
        assert_eq!(
            duration_to_timet(1.0, 0.0, 0.0, 1e18, 0.0, 0.0, 0.0),
            Err(DurationOutOfRange::Overflow)
        );
        assert_eq!(
            duration_to_timet(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 9.3e18),
            Err(DurationOutOfRange::Overflow)
        );
    }

    /// The guard rejects only the unrepresentable: ordinary durations, the
    /// fractional-year/month averages and the round trip are untouched.
    #[test]
    fn ordinary_durations_round_trip_unchanged() {
        // 1970-01-01 + 1y1m1d 1:01:01.
        let secs = duration_to_timet(1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0).unwrap();
        let d = duration_components(secs);
        assert_eq!((d.years, d.months, d.days), (1, 1, 1));
        assert_eq!((d.hours, d.minutes, d.seconds), (1, 1, 1));

        // Fractional years still use the 365.25 average (0.5y = 182.625d → 182d
        // 15h, truncated).
        let half = duration_to_timet(0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0).unwrap();
        assert_eq!(half, (182.625 * SECONDS_IN_DAY as f64) as i64);

        // Weeks fold into days; negative components are ordinary values.
        assert_eq!(
            duration_to_timet(0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0).unwrap(),
            7 * SECONDS_IN_DAY
        );
        assert_eq!(
            duration_to_timet(-1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0).unwrap(),
            date_to_secs(NaiveDate::from_ymd_opt(1969, 1, 1).unwrap())
        );

        // A big-but-representable duration: 100_000 years is inside chrono.
        assert!(duration_to_timet(100_000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0).is_ok());
    }

    /// The string spelling reaches the same fold. A *malformed* string is still
    /// `Ok(None)` → Cypher `null` (FalkorDB parity); an unrepresentable one is a
    /// typed error, not another silent `null`.
    #[test]
    fn string_form_separates_malformed_from_unrepresentable() {
        assert_eq!(duration_from_string("P1Y"), Ok(Some(31_536_000)));
        assert_eq!(duration_from_string("nonsense"), Ok(None));
        assert_eq!(duration_from_string("P1Z"), Ok(None));
        assert_eq!(duration_from_string("P"), Ok(Some(0)));
        assert_eq!(
            duration_from_string("P9999999999999999999Y"),
            Err(DurationOutOfRange::Component("years"))
        );
        assert_eq!(
            duration_from_string("-P9999999999999999999Y"),
            Ok(None), // a leading '-' is not the `P…` grammar
        );
        assert_eq!(
            duration_from_string("P-9999999999999999999Y"),
            Err(DurationOutOfRange::Component("years"))
        );
    }

    /// `duration ± duration` re-encodes through the same fold. Both operands
    /// decode out of a stored `time_t`, so the components are bounded by
    /// chrono's calendar and the extremes of the *i64* domain cannot reach the
    /// cast — assert that rather than assume it.
    #[test]
    fn add_durations_survives_the_time_t_extremes() {
        for a in [i64::MIN, i64::MAX, 0, -1, i64::MIN + 1, i64::MAX - 1] {
            for b in [i64::MIN, i64::MAX, 0, -1] {
                // No panic and no wrap: either a value or a clean error.
                let _ = add_durations(a, b, false);
                let _ = add_durations(a, b, true);
            }
        }
        let year = duration_to_timet(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0).unwrap();
        let month = duration_to_timet(0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0).unwrap();
        let sum = add_durations(year, month, false).unwrap();
        let d = duration_components(sum);
        assert_eq!((d.years, d.months), (1, 1));
    }

    /// The decode side (`duration_components`, `temporal ± duration`, the Bolt
    /// `Duration` struct) takes a bare `i64` and cannot fail — it is total only
    /// because `duration_to_timet` refuses to build a `time_t` chrono cannot
    /// decode. This pins that invariant from both sides.
    ///
    /// Without the range check, `duration({days: 1e14})` built a `time_t` of
    /// 8.64e18 — a fine `i64`, but past chrono's calendar, so `to_ndt` fell back
    /// to the epoch and `duration_components` reported the whole span back as
    /// 1e14 **days**. `localdatetime(…) + duration({days: 1e14})` then evaluated
    /// `1e14 * 86_400`, which overflows: the same debug-panic /
    /// release-wrap-to-a-wrong-answer pair as the reported `years` bug, one
    /// function over. Found by sweeping, not by the report.
    #[test]
    fn no_duration_can_be_built_that_the_decode_side_cannot_take() {
        // The widest durations that exist: chrono's calendar edges.
        let max = date_to_secs(NaiveDate::MAX);
        let min = date_to_secs(NaiveDate::MIN);
        assert_eq!(
            duration_to_timet(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, max as f64),
            Ok(max)
        );
        assert_eq!(
            duration_to_timet(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, min as f64),
            Ok(min)
        );
        // One day past either edge is refused rather than built.
        for past in [
            max as f64 + SECONDS_IN_DAY as f64,
            min as f64 - SECONDS_IN_DAY as f64,
        ] {
            assert_eq!(
                duration_to_timet(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, past),
                Err(DurationOutOfRange::Overflow)
            );
        }
        // The `days` spelling of the same overflow (1e14 days ≈ 2.7e11 years).
        assert_eq!(
            duration_to_timet(0.0, 0.0, 0.0, 1e14, 0.0, 0.0, 0.0),
            Err(DurationOutOfRange::Overflow)
        );

        // Everything constructible decodes to a sane, bounded span and survives
        // every consumer.
        for secs in [min, max, 0, -1, 1, max - 1, min + 1] {
            let d = duration_components(secs);
            assert!(
                d.days.abs() < 32,
                "decode must land inside a month, got {d:?} for {secs}"
            );
            for kind in [TKind::Date, TKind::Time, TKind::DateTime] {
                for temporal in [0, max, min] {
                    // No panic (debug: overflow-checks on) and no wrap.
                    let _ = add_duration(kind, temporal, secs, false);
                    let _ = add_duration(kind, temporal, secs, true);
                }
            }
            // The Bolt encode's `d.years * 12 + d.months` folds too.
            assert!(d
                .years
                .checked_mul(12)
                .and_then(|m| m.checked_add(d.months))
                .is_some());
        }
    }
}
