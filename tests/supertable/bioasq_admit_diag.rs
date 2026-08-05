// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Infino Authors

//! #515 stage-split diagnostic (ignored: needs a prebuilt local table).
//!
//! For each real query it runs the default path once (recording the
//! 1-bit admit window and the exact-fine cell ranking through the
//! `admit_trace` probe) and an all-cells pinned run once (whose top-k is
//! the truth at measured recall 1.0), then splits the default path's
//! loss between the two routing stages: truth cells missing from the
//! admit window vs admitted-but-ranked-deep in the exact fine ranking.
//!
//! Run:
//!   INFINO_DIAG_TABLE=/mnt/scratch/vdbb/vdbb-bioasq \
//!   INFINO_DIAG_QUERIES=/mnt/scratch/vdbb/bioasq_diag_queries.txt \
//!   cargo test --test supertable bioasq_admit_diag -- --ignored --nocapture

#![deny(clippy::unwrap_used)]

use std::{collections::HashSet, env, fs};

use arrow_array::{Decimal128Array, RecordBatch};
use infino::{VectorSearchOptions, test_helpers::admit_trace};

/// Search k for both runs.
const K: usize = 10;
/// Pinned width far above any grid size — the pin clamps to populated
/// cells, so this probes everything (the truth run).
const ALL_CELLS: usize = 1_000_000;

fn hit_ids(batches: &[RecordBatch]) -> Vec<i128> {
    let mut ids = Vec::new();
    for batch in batches {
        let idx = batch.schema().index_of("_id").expect("_id projected");
        let column = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("_id decimal column");
        for row in 0..batch.num_rows() {
            ids.push(column.value(row));
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic against a prebuilt local table; see module docs"]
async fn bioasq_admit_stage_split() {
    let table_dir =
        env::var("INFINO_DIAG_TABLE").unwrap_or_else(|_| "/mnt/scratch/vdbb/vdbb-bioasq".into());
    let queries_path = env::var("INFINO_DIAG_QUERIES")
        .unwrap_or_else(|_| "/mnt/scratch/vdbb/bioasq_diag_queries.txt".into());
    let queries: Vec<Vec<f32>> = fs::read_to_string(&queries_path)
        .expect("queries file")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split(',').map(|x| x.parse().expect("f32")).collect())
        .collect();

    // Attach the disk cache exactly as the python sweeps do — without it
    // every all-cells truth run re-reads and re-decodes the code section
    // (the first run of this diag cost 77 minutes that way).
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
    let cell_map = reader
        .diag_hidden_stable_cell_map("emb")
        .await
        .expect("cell map");
    println!("stable-id -> cell map: {} ids", cell_map.len());
    if let Some((width, fine, rerank)) = reader.diag_hidden_probe_laws() {
        println!("stamped laws: width={width:?} fine={fine:?} rerank={rerank:?}");
    }
    assert!(
        cell_map.len() >= 900_000,
        "cell map suspiciously small ({}) — cell-contiguity assumption broken?",
        cell_map.len()
    );

    let nq = queries.len();
    let mut admitted_all = 0usize;
    let mut admitted_partial = 0usize;
    let mut admitted_none = 0usize;
    let mut worst_fine_rank: Vec<usize> = Vec::new();
    let mut default_recall_sum = 0.0f64;
    // Score-ratio evidence for the serve window: for each admitted truth
    // cell, its best exact fine score relative to the fine winner's
    // (ratio - 1 = the slack a serve window needs to reach it).
    let mut truth_slacks: Vec<f32> = Vec::new();
    // Replay: recall-if-served-top-m from the ranking already recorded.
    const DEPTHS: [usize; 7] = [1, 2, 4, 8, 16, 32, 48];
    let mut cov_at_depth = [0.0f64; 7];

    for q in &queries {
        admit_trace::drain();
        let default_hits = st
            .vector_search("emb", q, K, VectorSearchOptions::new(), None, None)
            .expect("default search");
        let (admits, fines) = admit_trace::drain();
        let admit: HashSet<u32> = admits
            .last()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let fine = fines.last().cloned().unwrap_or_default();

        let truth_hits = st
            .vector_search(
                "emb",
                q,
                K,
                VectorSearchOptions::new().with_nprobe(ALL_CELLS),
                None,
                None,
            )
            .expect("truth search");
        admit_trace::drain();

        let truth_ids = hit_ids(&truth_hits);
        let default_ids: HashSet<i128> = hit_ids(&default_hits).into_iter().collect();
        default_recall_sum += truth_ids
            .iter()
            .filter(|id| default_ids.contains(*id))
            .count() as f64
            / K as f64;

        let truth_cells: HashSet<u32> = truth_ids
            .iter()
            .filter_map(|id| cell_map.get(id).copied())
            .collect();
        let n_in = truth_cells.iter().filter(|c| admit.contains(c)).count();
        if truth_cells.is_empty() {
            continue;
        }
        // replay coverage per depth + slack evidence (all queries)
        let winner_score = fine.first().map(|(_, s)| *s).unwrap_or(0.0);
        for (di, depth) in DEPTHS.iter().enumerate() {
            let served: HashSet<u32> = fine.iter().take(*depth).map(|(c, _)| *c).collect();
            let truth_in = truth_ids
                .iter()
                .filter(|id| cell_map.get(id).is_some_and(|c| served.contains(c)))
                .count();
            cov_at_depth[di] += truth_in as f64 / K as f64;
        }
        for c in &truth_cells {
            if let Some((_, s)) = fine.iter().find(|(cell, _)| cell == c)
                && winner_score > 0.0
            {
                truth_slacks.push(s / winner_score - 1.0);
            }
        }
        if n_in == truth_cells.len() {
            admitted_all += 1;
            let mut worst = 0usize;
            for c in &truth_cells {
                let rank = fine
                    .iter()
                    .position(|(cell, _)| cell == c)
                    .unwrap_or(usize::MAX);
                worst = worst.max(rank);
            }
            worst_fine_rank.push(worst);
        } else if n_in > 0 {
            admitted_partial += 1;
        } else {
            admitted_none += 1;
        }
    }

    worst_fine_rank.sort_unstable();
    let pct = |v: &[usize], p: f64| {
        if v.is_empty() {
            0
        } else {
            v[((v.len() as f64 - 1.0) * p) as usize]
        }
    };
    println!("queries: {nq}");
    println!(
        "default recall@10 vs all-cells truth: {:.4}",
        default_recall_sum / nq as f64
    );
    println!(
        "truth cells fully inside the admit window: {admitted_all} \
         ({:.0}%) | partially: {admitted_partial} | none: {admitted_none}",
        admitted_all as f64 * 100.0 / nq as f64
    );
    println!(
        "worst exact-fine rank of truth cells when fully admitted: \
         p50={} p90={} p99={} max={}",
        pct(&worst_fine_rank, 0.5),
        pct(&worst_fine_rank, 0.9),
        pct(&worst_fine_rank, 0.99),
        worst_fine_rank.last().copied().unwrap_or(0)
    );
    for (di, depth) in DEPTHS.iter().enumerate() {
        println!(
            "recall-if-served-fine-top-{depth}: {:.4}",
            cov_at_depth[di] / nq as f64
        );
    }
    truth_slacks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let fpct = |v: &[f32], p: f64| {
        if v.is_empty() {
            0.0
        } else {
            v[((v.len() as f64 - 1.0) * p) as usize]
        }
    };
    println!(
        "truth-cell score slack vs fine winner: p50={:.3} p90={:.3} p99={:.3} max={:.3}",
        fpct(&truth_slacks, 0.5),
        fpct(&truth_slacks, 0.9),
        fpct(&truth_slacks, 0.99),
        truth_slacks.last().copied().unwrap_or(0.0)
    );
}
