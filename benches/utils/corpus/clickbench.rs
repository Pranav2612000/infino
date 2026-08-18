// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Infino Authors

//! The canonical ClickBench query battery (DataFusion's Apache-2.0 port,
//! see `benches/queries/clickbench/LICENSE-NOTE.md` for provenance).
//!
//! Vendored files stay byte-identical to upstream so they can be diffed
//! directly; the `FROM hits` -> `FROM supertable` rewrite happens here at
//! load time instead, because Infino's `query_sql` always registers the
//! corpus as `supertable` (`src/supertable/query/provider.rs`) and the
//! arbitrary-name registration path is `pub(crate)`, unreachable from
//! this crate.

use std::sync::OnceLock;

use crate::harness::SqlQuery;

/// Canonical ClickBench battery size. Published results are keyed by
/// `q0`..`q42`, so this count is part of the comparison contract.
const CLICKBENCH_QUERY_COUNT: usize = 43;

/// The only table reference in the vendored queries — audited across all
/// 43 files (no joins, aliases, or qualified columns use the token
/// `hits`), and re-checked by this module's own test on every run.
const FROM_HITS: &str = "FROM hits";

/// Rewrite target: the fixed table name Infino's `query_sql` registers
/// the corpus under.
const FROM_SUPERTABLE: &str = "FROM supertable";

/// Stable per-query names, `q<index>` — the key published ClickBench
/// results are indexed by.
const QUERY_NAMES: [&str; CLICKBENCH_QUERY_COUNT] = [
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11", "q12", "q13", "q14",
    "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22", "q23", "q24", "q25", "q26", "q27",
    "q28", "q29", "q30", "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39", "q40",
    "q41", "q42",
];

/// Vendored query text, byte-identical to DataFusion's upstream files.
const RAW_QUERIES: [&str; CLICKBENCH_QUERY_COUNT] = [
    include_str!("../../queries/clickbench/q0.sql"),
    include_str!("../../queries/clickbench/q1.sql"),
    include_str!("../../queries/clickbench/q2.sql"),
    include_str!("../../queries/clickbench/q3.sql"),
    include_str!("../../queries/clickbench/q4.sql"),
    include_str!("../../queries/clickbench/q5.sql"),
    include_str!("../../queries/clickbench/q6.sql"),
    include_str!("../../queries/clickbench/q7.sql"),
    include_str!("../../queries/clickbench/q8.sql"),
    include_str!("../../queries/clickbench/q9.sql"),
    include_str!("../../queries/clickbench/q10.sql"),
    include_str!("../../queries/clickbench/q11.sql"),
    include_str!("../../queries/clickbench/q12.sql"),
    include_str!("../../queries/clickbench/q13.sql"),
    include_str!("../../queries/clickbench/q14.sql"),
    include_str!("../../queries/clickbench/q15.sql"),
    include_str!("../../queries/clickbench/q16.sql"),
    include_str!("../../queries/clickbench/q17.sql"),
    include_str!("../../queries/clickbench/q18.sql"),
    include_str!("../../queries/clickbench/q19.sql"),
    include_str!("../../queries/clickbench/q20.sql"),
    include_str!("../../queries/clickbench/q21.sql"),
    include_str!("../../queries/clickbench/q22.sql"),
    include_str!("../../queries/clickbench/q23.sql"),
    include_str!("../../queries/clickbench/q24.sql"),
    include_str!("../../queries/clickbench/q25.sql"),
    include_str!("../../queries/clickbench/q26.sql"),
    include_str!("../../queries/clickbench/q27.sql"),
    include_str!("../../queries/clickbench/q28.sql"),
    include_str!("../../queries/clickbench/q29.sql"),
    include_str!("../../queries/clickbench/q30.sql"),
    include_str!("../../queries/clickbench/q31.sql"),
    include_str!("../../queries/clickbench/q32.sql"),
    include_str!("../../queries/clickbench/q33.sql"),
    include_str!("../../queries/clickbench/q34.sql"),
    include_str!("../../queries/clickbench/q35.sql"),
    include_str!("../../queries/clickbench/q36.sql"),
    include_str!("../../queries/clickbench/q37.sql"),
    include_str!("../../queries/clickbench/q38.sql"),
    include_str!("../../queries/clickbench/q39.sql"),
    include_str!("../../queries/clickbench/q40.sql"),
    include_str!("../../queries/clickbench/q41.sql"),
    include_str!("../../queries/clickbench/q42.sql"),
];

/// Rewritten query text, held here so `SqlQuery::sql` can hand out
/// `&'static str` borrowed from process-lifetime storage rather than a
/// leaked or unsafely-extended allocation.
static QUERY_TEXT: OnceLock<Vec<String>> = OnceLock::new();
static BATTERY: OnceLock<Vec<SqlQuery>> = OnceLock::new();

/// The canonical ClickBench battery, with `FROM hits` rewritten to
/// `FROM supertable` for Infino's fixed corpus table name.
pub fn queries() -> &'static [SqlQuery] {
    let text = QUERY_TEXT.get_or_init(|| {
        RAW_QUERIES
            .iter()
            .map(|raw| raw.replace(FROM_HITS, FROM_SUPERTABLE))
            .collect()
    });
    BATTERY.get_or_init(|| {
        QUERY_NAMES
            .iter()
            .zip(text.iter())
            .map(|(&name, sql)| SqlQuery {
                name,
                sql: sql.as_str(),
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// All 43 canonical ClickBench queries load, in order, with stable
    /// names — the names published results are keyed by.
    #[test]
    fn loads_the_canonical_clickbench_battery() {
        let qs = queries();
        assert_eq!(qs.len(), CLICKBENCH_QUERY_COUNT);
        assert_eq!(qs[0].name, "q0");
        assert_eq!(qs[CLICKBENCH_QUERY_COUNT - 1].name, "q42");
        assert!(
            qs.iter().all(|q| !q.sql.trim().is_empty()),
            "no query may be empty"
        );
    }

    /// Every name is unique and its index matches `q<i>` — a hand-authored
    /// name array can silently drift out of step with the query array.
    #[test]
    fn query_names_are_unique_and_index_ordered() {
        let qs = queries();
        let mut seen = HashSet::new();
        for (i, q) in qs.iter().enumerate() {
            assert_eq!(q.name, format!("q{i}"));
            assert!(seen.insert(q.name), "duplicate query name: {}", q.name);
        }
    }

    /// The load-time rewrite must be total: no vendored query may still
    /// reference upstream's `hits` table, and every one must resolve
    /// against Infino's `supertable`.
    #[test]
    fn rewrites_every_from_hits_to_from_supertable() {
        for q in queries() {
            assert!(
                !q.sql.contains(FROM_HITS),
                "{} still references `FROM hits`",
                q.name
            );
            assert!(
                q.sql.contains(FROM_SUPERTABLE),
                "{} missing `FROM supertable`",
                q.name
            );
        }
    }
}
