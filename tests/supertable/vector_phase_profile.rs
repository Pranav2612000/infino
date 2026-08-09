// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Infino Authors

//! Warm vector-query phase profile against a prebuilt local table
//! (ignored: diagnostic, not CI). Prints the engine's own per-phase
//! timers so a latency number decomposes into admit / fan-out /
//! shortlist / survivor fetch / rerank / stable-id instead of being
//! argued about. Companion to `bioasq_admit_diag` (same env contract):
//!
//! ```sh
//! INFINO_DIAG_TABLE=/mnt/scratch/vdbb/vdbb10m \
//! INFINO_DIAG_QUERIES=/mnt/scratch/vdbb/cohere_diag_queries.txt \
//! INFINO_DIAG_K=100 \
//! INFINO_TRACE_VECTOR_WARM_PHASES=1 \
//! cargo test --test supertable vector_phase_profile -- --ignored --nocapture
//! ```

#![deny(clippy::unwrap_used)]

use std::{env, fs};

use infino::storage::io_counters;

/// Queries used to warm caches before any timed pass.
const WARMUP_QUERIES: usize = 50;
/// Timed queries (each sampled individually so phase spans attribute to
/// exactly one query).
const TIMED_QUERIES: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic against a prebuilt local table; see module docs"]
async fn vector_phase_profile() {
    let table_dir = env::var("INFINO_DIAG_TABLE").expect("INFINO_DIAG_TABLE");
    let queries_path = env::var("INFINO_DIAG_QUERIES").expect("INFINO_DIAG_QUERIES");
    let k: usize = env::var("INFINO_DIAG_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let queries: Vec<Vec<f32>> = fs::read_to_string(&queries_path)
        .expect("queries file")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split(',').map(|x| x.parse().expect("f32")).collect())
        .collect();
    assert!(
        io_counters::phase_enabled(),
        "set INFINO_TRACE_VECTOR_WARM_PHASES=1 so the engine records phases"
    );

    // Attach the disk cache exactly as the python sweeps and the admit diag
    // do — without it every query re-reads and re-decodes code sections and
    // the profile measures I/O, not serving (first run: 7.5s/query vs the
    // harness's 69ms on this same table).
    let cache_dir = format!("{table_dir}/cache");
    let conn = infino::connect_with(
        &table_dir,
        infino::ConnectOptions::default()
            .with_cache_dir(&cache_dir)
            .with_cache_budget_bytes(21_474_836_480),
    )
    .expect("connect");
    let table = conn.open_table("vdbbench_infino").expect("open table");
    let st = table.local_handle();
    let reader = st.reader().expect("reader");
    if let Some((width, fine, rerank)) = reader.diag_hidden_probe_laws() {
        println!("stamped laws: width={width:?} fine={fine:?} rerank={rerank:?}");
    }

    for q in queries.iter().cycle().take(WARMUP_QUERIES) {
        st.vector_search("emb", q, k, Default::default(), None, None)
            .expect("warmup search");
    }

    let mut walls_us: Vec<u64> = Vec::with_capacity(TIMED_QUERIES);
    let mut phase_sums: Vec<(&'static str, u64)> = Vec::new();
    for q in queries.iter().cycle().take(TIMED_QUERIES) {
        io_counters::phase_reset();
        let t0 = std::time::Instant::now();
        st.vector_search("emb", q, k, Default::default(), None, None)
            .expect("timed search");
        walls_us.push(t0.elapsed().as_micros() as u64);
        for (name, us) in io_counters::phase_take_summed() {
            match phase_sums.iter_mut().find(|(n, _)| *n == name) {
                Some((_, acc)) => *acc += us,
                None => phase_sums.push((name, us)),
            }
        }
    }

    walls_us.sort_unstable();
    let n = walls_us.len();
    let sum: u64 = walls_us.iter().sum();
    println!(
        "k={k} queries={n}: wall avg={:.1}ms p50={:.1}ms p95={:.1}ms",
        sum as f64 / n as f64 / 1000.0,
        walls_us[n / 2] as f64 / 1000.0,
        walls_us[n * 95 / 100] as f64 / 1000.0,
    );
    phase_sums.sort_by_key(|&(_, us)| std::cmp::Reverse(us));
    for (name, us) in &phase_sums {
        println!(
            "  {name:<20} avg {:>8.1}ms  ({:>5.1}% of wall)",
            *us as f64 / n as f64 / 1000.0,
            *us as f64 / sum as f64 * 100.0,
        );
    }
}
