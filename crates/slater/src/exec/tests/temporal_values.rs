// SPDX-License-Identifier: Apache-2.0
//! `temporal` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Phase 10 — temporal value types (date/localtime/localdatetime/duration) ──
// Vectors ported from FalkorDB `tests/flow/test_temporal.py`. The inline `run`
// harness has no params, so the `$map`/`$str` inputs become literal map/string
// expressions in the query text.

/// `localtime` from a map and from a string, its `.hour/.minute/.second`
/// components, and `toString` (sub-second is dropped → `HH:MM:SS`).
#[test]
fn phase10_localtime_construction_and_components() {
    let (root, res) = run(
        "exec_p10_lt",
        "WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d \
             RETURN toString(d) AS s, d.hour AS h, d.minute AS mi, d.second AS se, typeOf(d) AS t",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'12:31:14'");
    assert!(matches!(r[1], Val::Int(12)));
    assert!(matches!(r[2], Val::Int(31)));
    assert!(matches!(r[3], Val::Int(14)));
    assert_eq!(render(&r[4]), "'Time'");

    // String forms (compact + colon) and the trailing-fraction drop.
    let (root2, res2) = run(
        "exec_p10_lt_str",
        "RETURN toString(localtime('21')) AS a, toString(localtime('2140')) AS b, \
                    toString(localtime('214032')) AS c, toString(localtime('21:40:32.143')) AS e",
    );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'21:00:00'");
    assert_eq!(render(&r[1]), "'21:40:00'");
    assert_eq!(render(&r[2]), "'21:40:32'");
    assert_eq!(render(&r[3]), "'21:40:32'");

    // toString round-trips back to an equal value.
    let (root3, res3) = run(
        "exec_p10_lt_rt",
        "WITH localtime({hour: 12, minute: 31, second: 14}) AS d \
             RETURN localtime(toString(d)) = d AS b",
    );
    assert!(matches!(res3.rows[0][0], Val::Bool(true)));
    for p in [root, root2, root3] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `date` from components (y/m/d, ISO week, quarter) and strings, its many
/// components, and `toString` (`YYYY-MM-DD`).
#[test]
fn phase10_date_construction_and_components() {
    // Component-map and string constructions agree on the rendered date.
    let (root, res) = run(
        "exec_p10_date_build",
        "RETURN toString(date({year:1984})) AS a, \
                    toString(date({year:1984, month:10})) AS b, \
                    toString(date({year:1984, week:10})) AS c, \
                    toString(date({year:1984, month:10, day:11})) AS d, \
                    toString(date({year:1984, week:10, dayOfWeek:3})) AS e, \
                    toString(date({year:1984, quarter:3, dayOfQuarter:45})) AS f, \
                    toString(date({year:1984, quarter:3})) AS g, \
                    toString(date('2015202')) AS h, toString(date('2015-W30-2')) AS i, \
                    toString(date('20150721')) AS j",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'1984-01-01'");
    assert_eq!(render(&r[1]), "'1984-10-01'");
    assert_eq!(render(&r[2]), "'1984-03-05'");
    assert_eq!(render(&r[3]), "'1984-10-11'");
    assert_eq!(render(&r[4]), "'1984-03-07'");
    assert_eq!(render(&r[5]), "'1984-08-14'");
    assert_eq!(render(&r[6]), "'1984-07-01'");
    assert_eq!(render(&r[7]), "'2015-07-21'"); // ordinal day 202
    assert_eq!(render(&r[8]), "'2015-07-21'"); // ISO week 30, Tue
    assert_eq!(render(&r[9]), "'2015-07-21'");

    // Components of date(1984-10-21) — incl. FalkorDB's quirky dayOfQuarter (23).
    let (root2, res2) = run(
        "exec_p10_date_comp",
        "WITH date({year: 1984, month:10, day:21}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.dayOfWeek, \
                    d.dayOfQuarter, d.ordinalDay, typeOf(d)",
    );
    let r = &res2.rows[0];
    let ints: Vec<i64> = (0..8)
        .map(|i| match r[i] {
            Val::Int(v) => v,
            ref o => panic!("col {i}: expected int, got {o:?}"),
        })
        .collect();
    assert_eq!(ints, vec![1984, 4, 10, 42, 21, 0, 23, 295]);
    assert_eq!(render(&r[8]), "'Date'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `localdatetime` from components/strings, its `toString` (`…T…`), the
/// ISO-week construction edge cases, and component access.
#[test]
fn phase10_localdatetime_construction_and_components() {
    let (root, res) = run(
            "exec_p10_ldt",
            "RETURN toString(localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond:645876123})) AS a, \
                    toString(localdatetime({year:1984, month:10, day:11, hour:12})) AS b, \
                    toString(localdatetime({year:1984})) AS c, \
                    toString(localdatetime({year:1918, week:1})) AS d, \
                    toString(localdatetime({year:1918, week:53})) AS e, \
                    toString(localdatetime('2025-02-18T12:34:56')) AS f, \
                    toString(localdatetime('20250218T123456')) AS g",
        );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'1984-10-11T12:31:14'");
    assert_eq!(render(&r[1]), "'1984-10-11T12:00:00'");
    assert_eq!(render(&r[2]), "'1984-01-01T00:00:00'");
    assert_eq!(render(&r[3]), "'1917-12-31T00:00:00'"); // ISO week 1 of 1918
    assert_eq!(render(&r[4]), "'1918-12-30T00:00:00'"); // lenient week 53
    assert_eq!(render(&r[5]), "'2025-02-18T12:34:56'");
    assert_eq!(render(&r[6]), "'2025-02-18T12:34:56'");

    // Components incl. clock parts + round-trip via toString.
    let (root2, res2) = run(
        "exec_p10_ldt_comp",
        "WITH localdatetime({year:1984, month:10, day:21, hour:10, minute:31, second:46}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.ordinalDay, \
                    d.hour, d.minute, d.second, \
                    localdatetime(toString(d)) = d AS rt, typeOf(d) AS t",
    );
    let r = &res2.rows[0];
    let ints: Vec<i64> = (0..9)
        .map(|i| match r[i] {
            Val::Int(v) => v,
            ref o => panic!("col {i}: expected int, got {o:?}"),
        })
        .collect();
    assert_eq!(ints, vec![1984, 4, 10, 42, 21, 295, 10, 31, 46]);
    assert!(matches!(r[9], Val::Bool(true)), "toString round-trip");
    assert_eq!(render(&r[10]), "'Datetime'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `duration` from a map and ISO-8601 string, its components (weeks fold into
/// days), and `toString`.
#[test]
fn phase10_duration_construction_and_components() {
    // Components: weeks fold into days (1 week + 4 days → 11 days, 0 weeks).
    let (root, res) = run(
            "exec_p10_dur_comp",
            "WITH duration({years:2, months:3, weeks:1, days:4, hours:5, minutes:22, seconds:7}) AS d \
             RETURN d.years, d.months, d.weeks, d.days, d.hours, d.minutes, d.seconds, typeOf(d) AS t",
        );
    let r = &res.rows[0];
    // Duration components are doubles (FalkorDB `SI_DoubleVal`) → render as ints.
    let got: Vec<String> = (0..7).map(|i| render(&r[i])).collect();
    assert_eq!(got, vec!["2", "3", "0", "11", "5", "22", "7"]);
    assert_eq!(render(&r[7]), "'Duration'");

    // String form + toString round-trips ('P1M' stays 'P1M').
    let (root2, res2) = run(
            "exec_p10_dur_str",
            "RETURN toString(duration('P1M')) AS a, \
                    toString(duration('P1Y2M3DT4H5M6S')) AS b, \
                    toString(duration({years:2, months:3, days:11, hours:5, minutes:22, seconds:7})) AS c",
        );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'P1M'");
    assert_eq!(render(&r[1]), "'P1Y2M3DT4H5M6S'");
    assert_eq!(render(&r[2]), "'P2Y3M11DT5H22M7S'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Comparison operators over each temporal type (test_temporal.py *_compare).
#[test]
fn phase10_temporal_comparison() {
    let (root, res) = run(
            "exec_p10_cmp",
            "WITH date({year:1980, month:12, day:24}) AS d1, date({year:1984, month:10, day:11}) AS d2, \
                  localtime({hour:10, minute:35}) AS t1, localtime({hour:12, minute:31, second:14}) AS t2, \
                  duration({years:1, months:11}) AS u1, duration({years:1, months:10}) AS u2 \
             RETURN d1 < d2, d1 = d2, t1 < t2, t1 >= t2, u1 > u2, u1 = u2, \
                    d1 = d1, t2 = t2",
        );
    let r = &res.rows[0];
    let b: Vec<bool> = (0..8)
        .map(|i| match r[i] {
            Val::Bool(v) => v,
            ref o => panic!("col {i}: {o:?}"),
        })
        .collect();
    // d1<d2 T, d1=d2 F, t1<t2 T, t1>=t2 F, u1>u2 T, u1=u2 F, d1=d1 T, t2=t2 T
    assert_eq!(b, vec![true, false, true, false, true, false, true, true]);

    // Cross-type comparison (date vs duration) is `null`, not an error.
    let (root2, res2) = run(
        "exec_p10_cmp_x",
        "WITH date({year:2000, month:1, day:1}) AS d, duration({days:1}) AS u \
             RETURN d < u AS lt, d = u AS eq",
    );
    let r = &res2.rows[0];
    assert!(matches!(r[0], Val::Null), "date<duration → null");
    assert!(matches!(r[1], Val::Bool(false)), "date=duration → false");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Temporal ± duration and duration ± duration (test_temporal.py
/// test_duration_add + test_month_end_duration_arithmetic).
#[test]
fn phase10_temporal_arithmetic() {
    let (root, res) = run(
            "exec_p10_arith",
            "WITH duration({years:1, months:1, weeks:1, days:1, hours:1, minutes:32, seconds:10}) AS a, \
                  duration({years:2, months:2, weeks:2, days:2, hours:2, minutes:34, seconds:12}) AS b \
             RETURN toString(a + b) AS sum, toString(b - a) AS diff",
        );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'P3Y3M24DT4H6M22S'"); // 66 min normalises to 4h6m
    assert_eq!(render(&r[1]), "'P1Y1M8DT1H2M2S'");

    let (root2, res2) = run(
            "exec_p10_arith2",
            "RETURN toString(date({year:1984, month:10, day:21}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS d, \
                    toString(duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1}) + date({year:1984, month:10, day:21})) AS d2, \
                    toString(localtime({hour:2, minute:34, second:32}) + duration({years:1, months:1, days:1, hours:1, minutes:35, seconds:35})) AS t, \
                    toString(localtime({hour:10, minute:30, second:10}) - duration({hours:2, minutes:40, seconds:30})) AS t2, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) - duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt2",
        );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'1985-11-22'"); // date + dur (clock parts ignored)
    assert_eq!(render(&r[1]), "'1985-11-22'"); // commutative
    assert_eq!(render(&r[2]), "'04:10:07'"); // time + dur (calendar parts ignored)
    assert_eq!(render(&r[3]), "'07:49:40'"); // time - dur
    assert_eq!(render(&r[4]), "'1985-11-22T06:31:11'");
    assert_eq!(render(&r[5]), "'1983-09-20T04:29:09'");

    // Month-end overflow normalises forward (Jan 31 + 1mo → Mar 02).
    let (root3, res3) = run(
        "exec_p10_arith_me",
        "RETURN toString(date('2024-01-31') + duration('P1M')) AS d, \
                    toString(localdatetime('2024-01-31T00:00:00') + duration('P1M')) AS l",
    );
    let r = &res3.rows[0];
    assert_eq!(render(&r[0]), "'2024-03-02'");
    assert_eq!(render(&r[1]), "'2024-03-02T00:00:00'");
    for p in [root, root2, root3] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Unsupported temporal arithmetic errors (duration − temporal is invalid),
/// and `null`/unknown-component handling.
#[test]
fn phase10_temporal_errors_and_null() {
    for (tag, q) in [
        (
            "exec_p10_e1",
            "RETURN duration({days:1}) - date({year:1984, month:10, day:21})",
        ),
        (
            "exec_p10_e2",
            "RETURN duration({hours:2}) - localtime({hour:10, minute:30})",
        ),
        (
            "exec_p10_e3",
            "RETURN duration({days:1}) - localdatetime({year:1984})",
        ),
    ] {
        let e = run_err(tag, q);
        assert!(e.contains("cannot be subtracted"), "query `{q}` → `{e}`");
    }

    // Unknown component on a temporal is an error (unlike Point/Map → NULL).
    let e = run_err(
        "exec_p10_e_comp",
        "WITH date({year:2000, month:1, day:1}) AS d RETURN d.bogus",
    );
    assert!(e.contains("unknown date component"), "{e}");

    // NULL / bad-string inputs propagate to NULL.
    let (root, res) = run(
        "exec_p10_null",
        "RETURN date(null) AS a, localtime('nonsense') AS b, duration('not-a-duration') AS c",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Null));
    assert!(matches!(r[1], Val::Null));
    assert!(matches!(r[2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}
