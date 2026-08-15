// SPDX-License-Identifier: Apache-2.0
//! `points` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Phase 9 — Val::Point, point()/distance(), coordinate reads ────────────

// point() construction + coordinate property reads (test_point.py
// test_point_coordinates). FalkorDB stores f32; coordinates are asserted to
// 1e-5. An unknown coordinate key yields NULL.
#[test]
fn phase9_point_construction_and_coordinates() {
    let (root, res) = run(
        "exec_p9_coords",
        "WITH point({latitude: 32.070794860, longitude: 34.820751118}) AS p \
             RETURN p.latitude AS lat, p.longitude AS lon, p.v AS missing, typeOf(p) AS t",
    );
    let r = &res.rows[0];
    match r[0] {
        Val::Float(x) => assert!((x - 32.070794860).abs() < 1e-5, "lat {x}"),
        ref o => panic!("expected float latitude, got {o:?}"),
    }
    match r[1] {
        Val::Float(x) => assert!((x - 34.820751118).abs() < 1e-5, "lon {x}"),
        ref o => panic!("expected float longitude, got {o:?}"),
    }
    assert!(matches!(r[2], Val::Null), "unknown key → NULL");
    assert_eq!(render(&r[3]), "'Point'");
    let _ = std::fs::remove_dir_all(&root);
}

// distance() haversine, in metres (test_point.py test_point_distance). The
// FalkorDB suite tolerates 10% error; we assert the same vectors well within it.
#[test]
fn phase9_point_distance() {
    let (root, res) = run(
        "exec_p9_dist",
        "WITH point({latitude:32.070794860, longitude:34.820751118}) AS a, \
                  point({latitude:32.070109656, longitude:34.822351298}) AS b, \
                  point({latitude:30.621734079, longitude:-96.33775507}) AS c \
             RETURN distance(a, a) AS d0, distance(a, b) AS d160, distance(a, c) AS d_far",
    );
    let r = &res.rows[0];
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        o => panic!("expected float, got {o:?}"),
    };
    assert!(f(&r[0]).abs() < 1e-6, "same point → 0, got {}", f(&r[0]));
    let within =
        |got: f64, want: f64| assert!((got - want).abs() <= 0.1 * want, "got {got}, want ~{want}");
    within(f(&r[1]), 160.0);
    within(f(&r[2]), 11_352_120.0);
    let _ = std::fs::remove_dir_all(&root);
}

// Coordinate range validation + bad-key errors (test_point.py test_point_values).
#[test]
fn phase9_point_validation_errors() {
    for (tag, q, needle) in [
        (
            "exec_p9_lat_hi",
            "RETURN point({latitude:90.1, longitude:20}) AS p",
            "latitude should be within",
        ),
        (
            "exec_p9_lat_lo",
            "RETURN point({latitude:-90.1, longitude:20}) AS p",
            "latitude should be within",
        ),
        (
            "exec_p9_lon_hi",
            "RETURN point({latitude:10, longitude:180.1}) AS p",
            "longitude should be within",
        ),
        (
            "exec_p9_lon_lo",
            "RETURN point({latitude:10, longitude:-180.1}) AS p",
            "longitude should be within",
        ),
        (
            "exec_p9_one_key",
            "RETURN point({latitude:10}) AS p",
            "should have 2 elements",
        ),
        (
            "exec_p9_no_lat",
            "RETURN point({x:1, y:2}) AS p",
            "Did not find 'latitude'",
        ),
    ] {
        let e = run_err(tag, q);
        assert!(e.contains(needle), "query `{q}` → `{e}` (want `{needle}`)");
    }
}

// Ordering + equality. FalkorDB orders points by longitude then latitude
// (test_point.py test_nested_point ORDER BY p), and equal points are `=`.
#[test]
fn phase9_point_ordering_and_equality() {
    let (root, res) = run(
        "exec_p9_order",
        "UNWIND [point({latitude:33, longitude:35}), \
                     point({latitude:32, longitude:31}), \
                     point({latitude:32, longitude:32}), \
                     point({latitude:31, longitude:32}), \
                     point({latitude:29, longitude:36})] AS p \
             WITH p ORDER BY p RETURN p.longitude AS lon, p.latitude AS lat",
    );
    let lons: Vec<f64> = res
        .rows
        .iter()
        .map(|r| match r[0] {
            Val::Float(x) => x,
            ref o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(lons, vec![31.0, 32.0, 32.0, 35.0, 36.0]);
    // The lon-32 tie breaks on latitude ascending (31 before 32).
    assert!(matches!(res.rows[1][1], Val::Float(x) if (x - 31.0).abs() < 1e-9));
    assert!(matches!(res.rows[2][1], Val::Float(x) if (x - 32.0).abs() < 1e-9));

    let (root2, eq) = run(
        "exec_p9_eq",
        "WITH point({latitude:32, longitude:34}) AS a, \
                  point({latitude:32, longitude:34}) AS b, \
                  point({latitude:32, longitude:35}) AS c \
             RETURN a = b AS same, a = c AS diff",
    );
    assert!(matches!(eq.rows[0][0], Val::Bool(true)));
    assert!(matches!(eq.rows[0][1], Val::Bool(false)));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

// NULL propagation + toString rendering (%f, 6 decimals — test_nested_point).
#[test]
fn phase9_point_null_and_tostring() {
    let (root, res) = run(
        "exec_p9_null_str",
        "RETURN point(null) AS np, distance(null, point({latitude:1, longitude:2})) AS nd, \
             toString(point({latitude:32, longitude:34})) AS s",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Null));
    assert!(matches!(r[1], Val::Null));
    assert_eq!(
        render(&r[2]),
        "'point({latitude: 32.000000, longitude: 34.000000})'"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_trig_and_angle_functions() {
    let (root, res) = run(
        "exec_p1_trig",
        "RETURN sin(0.0) AS s, cos(0.0) AS c, tan(0.0) AS t, \
             cot(0.7853981633974483) AS cot, asin(1.0) AS asin, acos(1.0) AS acos, \
             atan(1.0) AS atan, atan2(1.0, 1.0) AS atan2, \
             degrees(3.141592653589793) AS deg, radians(180.0) AS rad, \
             haversin(0.0) AS hav",
    );
    let f = |i: usize| match res.rows[0][i] {
        Val::Float(x) => x,
        _ => panic!("expected float at col {i}"),
    };
    let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-9, "{a} != {b}");
    close(f(0), 0.0); // sin 0
    close(f(1), 1.0); // cos 0
    close(f(2), 0.0); // tan 0
    close(f(3), 1.0); // cot(pi/4)
    close(f(4), std::f64::consts::FRAC_PI_2); // asin 1
    close(f(5), 0.0); // acos 1
    close(f(6), std::f64::consts::FRAC_PI_4); // atan 1
    close(f(7), std::f64::consts::FRAC_PI_4); // atan2(1,1)
    close(f(8), 180.0); // degrees(pi)
    close(f(9), std::f64::consts::PI); // radians(180)
    close(f(10), 0.0); // haversin 0
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_left_right_and_isempty_typeof() {
    let (root, res) = run(
        "exec_p1_str",
        "RETURN left('muchacho', 4) AS l, right('muchacho', 4) AS r, \
             left('hi', 9) AS lover, right('hi', 9) AS rover, \
             isEmpty('') AS e1, isEmpty('x') AS e2, isEmpty([]) AS e3, \
             typeOf(1) AS t1, typeOf(1.5) AS t2, typeOf('a') AS t3, \
             typeOf(true) AS t4, typeOf([1]) AS t5, typeOf(null) AS t6",
    );
    let row = &res.rows[0];
    assert!(matches!(&row[0], Val::Str(s) if s == "much"));
    assert!(matches!(&row[1], Val::Str(s) if s == "acho"));
    assert!(matches!(&row[2], Val::Str(s) if s == "hi"));
    assert!(matches!(&row[3], Val::Str(s) if s == "hi"));
    assert!(matches!(row[4], Val::Bool(true)));
    assert!(matches!(row[5], Val::Bool(false)));
    assert!(matches!(row[6], Val::Bool(true)));
    assert!(matches!(&row[7], Val::Str(s) if s == "Integer"));
    assert!(matches!(&row[8], Val::Str(s) if s == "Float"));
    assert!(matches!(&row[9], Val::Str(s) if s == "String"));
    assert!(matches!(&row[10], Val::Str(s) if s == "Boolean"));
    assert!(matches!(&row[11], Val::Str(s) if s == "List"));
    assert!(matches!(&row[12], Val::Str(s) if s == "Null"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_ornull_conversions() {
    let (root, res) = run(
        "exec_p1_ornull",
        "RETURN toIntegerOrNull('7') AS i, toIntegerOrNull('x') AS i2, \
             toFloatOrNull('1.5') AS f, toFloatOrNull('x') AS f2, \
             toBooleanOrNull('true') AS b, toBooleanOrNull('x') AS b2, \
             toStringOrNull(42) AS s, toStringOrNull(null) AS s2",
    );
    let row = &res.rows[0];
    assert!(matches!(row[0], Val::Int(7)));
    assert!(matches!(row[1], Val::Null));
    assert!(matches!(row[2], Val::Float(x) if (x - 1.5).abs() < 1e-9));
    assert!(matches!(row[3], Val::Null));
    assert!(matches!(row[4], Val::Bool(true)));
    assert!(matches!(row[5], Val::Null));
    assert!(matches!(&row[6], Val::Str(s) if s == "42"));
    assert!(matches!(row[7], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 2 — list functions tail / list.* and the to*List family.
#[test]
fn phase2_tail_dedup_sort() {
    let (root, res) = run(
        "exec_p2_list_a",
        "RETURN tail([1,2,3]) AS t, tail([7]) AS t1, tail([]) AS te, \
             list.dedup([1,2,1,3,3,2]) AS d, list.dedup([3,[1,2],3,[1],[1,2]]) AS dn, \
             list.sort([3,1,2]) AS s, list.sort([1,3,2], false) AS sd, \
             list.sort([[4,5,6],[1,2,3]]) AS sl",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[2,3]");
    assert_eq!(render(&r[1]), "[]");
    assert_eq!(render(&r[2]), "[]");
    assert_eq!(render(&r[3]), "[1,2,3]");
    assert_eq!(render(&r[4]), "[3,[1,2],[1]]");
    assert_eq!(render(&r[5]), "[1,2,3]");
    assert_eq!(render(&r[6]), "[3,2,1]");
    assert_eq!(render(&r[7]), "[[1,2,3],[4,5,6]]");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_list_remove() {
    // Vectors ported from FalkorDB tests/flow/test_list.py test09_remove.
    let (root, res) = run(
        "exec_p2_remove",
        "RETURN list.remove([1,2,3], 1, 2) AS a, list.remove([1,2,3,4], 1, 2) AS b, \
             list.remove([1,2,3], 2) AS c, list.remove([1,2,3,4], -1, 1) AS d, \
             list.remove([1,2,3,4], -4, 1) AS e, list.remove([1,2,3,4], -3, 5) AS f, \
             list.remove([1,2,3,4], -5, 5) AS g, list.remove([1,2,3,4], 4, 5) AS h, \
             list.remove([1,2,3], 1, 0) AS i, list.remove(null, 2) AS j",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[1]");
    assert_eq!(render(&r[1]), "[1,4]");
    assert_eq!(render(&r[2]), "[1,2]");
    assert_eq!(render(&r[3]), "[1,2,3]");
    assert_eq!(render(&r[4]), "[2,3,4]");
    assert_eq!(render(&r[5]), "[1]");
    assert_eq!(render(&r[6]), "[1,2,3,4]"); // out-of-bound index → unchanged
    assert_eq!(render(&r[7]), "[1,2,3,4]");
    assert_eq!(render(&r[8]), "[1,2,3]"); // count 0 → unchanged
    assert_eq!(render(&r[9]), "null");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_list_insert_and_insert_elements() {
    // Vectors ported from FalkorDB test_list.py test11_insert / test12.
    let (root, res) = run(
        "exec_p2_insert",
        "RETURN list.insert([1,2,3], 0, 4) AS a, list.insert([1,2,3], 3, 4) AS b, \
             list.insert([1,2,3], -1, 4) AS c, list.insert([1,2,3], -3, 4) AS d, \
             list.insert([], 0, 4) AS e, list.insert(null, 2, 3) AS f, \
             list.insert([1,2,3], 0, 2, false) AS g, \
             list.insertListElements([1,2,3], [4,5,6], 0) AS h, \
             list.insertListElements([1,2,3], [4], -1) AS i, \
             list.insertListElements([1,2,3], [9,3,2,7], 0, false) AS j, \
             list.insertListElements([1,2,3], null, 1) AS k",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[4,1,2,3]");
    assert_eq!(render(&r[1]), "[1,2,3,4]");
    assert_eq!(render(&r[2]), "[1,2,3,4]");
    assert_eq!(render(&r[3]), "[1,4,2,3]");
    assert_eq!(render(&r[4]), "[4]");
    assert_eq!(render(&r[5]), "null");
    assert_eq!(render(&r[6]), "[1,2,3]"); // dups=false + 2 already present → unchanged
    assert_eq!(render(&r[7]), "[4,5,6,1,2,3]");
    assert_eq!(render(&r[8]), "[1,2,3,4]"); // idx -1 with inclusive bounds → append
    assert_eq!(render(&r[9]), "[9,7,1,2,3]"); // dups dropped vs list1
    assert_eq!(render(&r[10]), "[1,2,3]"); // null list2 → unchanged
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_to_type_lists() {
    // Vectors ported from FalkorDB test_list.py test06–09.
    let (root, res) = run(
        "exec_p2_tolists",
        "RETURN toBooleanList(null) AS a, toBooleanList([null, null]) AS b, \
             toBooleanList(['abc', true, 'false', null, ['a','b']]) AS c, \
             toFloatList(['abc', 1.5, 7.0578, null, ['a','b']]) AS d, \
             toIntegerList(['abc', 7, '5', null, ['a','b']]) AS e, \
             toStringList([1, 2.5, 'x', null]) AS f",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "null");
    assert_eq!(render(&r[1]), "[null,null]");
    assert_eq!(render(&r[2]), "[null,true,false,null,null]");
    assert_eq!(render(&r[3]), "[null,1.5,7.0578,null,null]");
    assert_eq!(render(&r[4]), "[null,7,5,null,null]");
    assert_eq!(render(&r[5]), "['1','2.5','x',null]");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_entity_haslabels_and_degree() {
    // Fixture: Alice -KNOWS-> Bob, -WORKS_AT-> Acme, -KNOWS-> Carol;
    //          Bob -KNOWS-> Carol; Carol -WORKS_AT-> Globex.
    let (root, res) = run(
        "exec_p2_entity",
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}), \
                   (k:Company {name: 'Acme'}) \
             RETURN hasLabels(a, ['Person']) AS h1, hasLabels(a, ['Company']) AS h2, \
                    hasLabels(a, ['Person','Foo']) AS h3, hasLabels(k, ['Company']) AS h4, \
                    outdegree(a) AS od, outdegree(a, 'KNOWS') AS odk, \
                    outdegree(a, 'WORKS_AT') AS odw, outdegree(a, ['KNOWS','WORKS_AT']) AS oda, \
                    indegree(a) AS ai, indegree(c) AS ci, indegree(c, 'KNOWS') AS cik, \
                    indegree(c, 'WORKS_AT') AS ciw",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Bool(true)));
    assert!(matches!(r[1], Val::Bool(false)));
    assert!(matches!(r[2], Val::Bool(false)));
    assert!(matches!(r[3], Val::Bool(true)));
    assert!(matches!(r[4], Val::Int(3)));
    assert!(matches!(r[5], Val::Int(2)));
    assert!(matches!(r[6], Val::Int(1)));
    assert!(matches!(r[7], Val::Int(3)));
    assert!(matches!(r[8], Val::Int(0)));
    assert!(matches!(r[9], Val::Int(2)));
    assert!(matches!(r[10], Val::Int(2)));
    assert!(matches!(r[11], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}
