// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Infino Authors

//! Merge the FTS indexes of already-built superfiles term by term.
//!
//! Every input's dictionary is in term order, so a k-way merge over the
//! inputs' dictionaries yields the output terms in final order, with no
//! corpus-sized accumulator; peak memory is one term's postings across all
//! inputs. When the remaps keep arrival order, a term's postings joined in
//! input order are already sorted by output doc id. When the merge chooses
//! a doc order of its own, or an input stores one, they are not, and that
//! one term's postings are sorted before they are emitted.

use std::{
    cmp::Reverse, collections::BinaryHeap, io::Error, iter::once, str::from_utf8, sync::Arc, vec,
};

use bytes::Bytes;

use crate::{
    superfile::{
        BuildError, FtsError, SuperfileReader,
        fts::{positions::TermRuns, reader::FtsReader},
        id_space::FtsDocId,
    },
    utils::{
        terms::FstValue,
        trace::{Stopwatch, record},
    },
};

/// Terms each input cursor reads from its dictionary at a time.
pub(crate) const TERMS_PER_CHUNK: usize = 4096;

/// One merge input: a superfile and where its rows land in the output.
pub(crate) struct SortedInput {
    pub(crate) reader: Arc<SuperfileReader>,
    /// Input blob doc id → output doc id; `None` drops the row.
    pub(crate) remap: Vec<Option<FtsDocId>>,
}

/// Merge `column_id` across `inputs` and call `emit(term, postings, runs)`
/// once per term that still has a surviving posting, in term order.
/// `postings` are `(output_doc_id, tf)`, doc ids ascending; `runs` holds
/// each posting's position run, decoded (empty for a non-positional
/// column).
///
/// Under `detailed-tracing`, records where the time went on the enclosing
/// span: `dict_ms`, `read_ms`, `sort_ms` and `emit_ms`, plus the
/// `postings`, `term_inputs`, `sorted_terms`, `sorted_postings` and
/// `run_values` counts.
pub(crate) fn merge_column(
    inputs: &[SortedInput],
    column_id: u32,
    mut emit: impl FnMut(&str, &[(u32, u32)], TermRuns<'_>) -> Result<(), BuildError>,
) -> Result<(), BuildError> {
    let readers = inputs
        .iter()
        .map(|input| input.reader.fts().ok_or(BuildError::BatchReadError))
        .collect::<Result<Vec<_>, _>>()?;
    let mut cursors: Vec<TermChunks> = readers
        .iter()
        .map(|fts| TermChunks::new(fts, column_id))
        .collect::<Result<_, _>>()
        .map_err(read_error)?;

    // Min-heap of each input's current term; ties pop in input order.
    let mut heap = BinaryHeap::new();
    let mut values = Vec::with_capacity(inputs.len());
    for (i, cursor) in cursors.iter_mut().enumerate() {
        let next = cursor.next().map_err(read_error)?;
        values.push(next.as_ref().map(|(_, value)| *value));
        if let Some((term, _)) = next {
            heap.push(Reverse((term, i)));
        }
    }

    let mut contributors: Vec<usize> = Vec::new();
    let mut postings: Vec<(u32, u32)> = Vec::new();
    // Every posting's run values back to back, and where each run starts.
    // A sort reorders the starts and leaves the values where they are.
    let mut runs: Vec<u32> = Vec::new();
    let mut run_starts: Vec<usize> = Vec::new();
    let mut positions_buf: Vec<u32> = Vec::new();
    let mut sorted_postings: Vec<(u32, u32)> = Vec::new();
    let mut sorted_starts: Vec<usize> = Vec::new();
    // Walking the dictionaries and the heap; decoding and remapping the
    // postings; sorting a term whose postings arrive out of order; and
    // encoding and writing the term.
    let mut dict = Stopwatch::default();
    let mut read = Stopwatch::default();
    let mut sort = Stopwatch::default();
    let mut emit_time = Stopwatch::default();
    let (mut n_postings, mut n_term_inputs, mut n_sorted_terms, mut n_sorted_postings) =
        (0u64, 0u64, 0u64, 0u64);
    let mut n_run_values = 0u64;
    loop {
        let started = Stopwatch::start();
        let Some(Reverse((term, first))) = heap.pop() else {
            dict.stop(started);
            break;
        };
        contributors.clear();
        contributors.push(first);
        while heap.peek().is_some_and(|Reverse((t, _))| *t == term) {
            if let Some(Reverse((_, i))) = heap.pop() {
                contributors.push(i);
            }
        }
        dict.stop(started);
        n_term_inputs += contributors.len() as u64;

        postings.clear();
        runs.clear();
        run_starts.clear();
        let mut ascending = true;
        for &i in &contributors {
            let started = Stopwatch::start();
            let value = values[i].ok_or(BuildError::BatchReadError)?;
            let remap = &inputs[i].remap;
            readers[i]
                .for_each_posting_in(
                    column_id,
                    once((term.as_slice(), value)),
                    &mut positions_buf,
                    |_, doc, tf, pos| {
                        if let Some(out_doc) = remap[doc as usize] {
                            let out_doc = out_doc.get();
                            ascending &= postings.last().is_none_or(|&(d, _)| d < out_doc);
                            postings.push((out_doc, tf));
                            run_starts.push(runs.len());
                            push_run_values(&mut runs, pos);
                        }
                        Ok(())
                    },
                )
                .map_err(read_error)?;
            read.stop(started);
            let started = Stopwatch::start();
            let next = cursors[i].next().map_err(read_error)?;
            values[i] = next.as_ref().map(|(_, value)| *value);
            if let Some((next_term, _)) = next {
                heap.push(Reverse((next_term, i)));
            }
            dict.stop(started);
        }
        n_postings += postings.len() as u64;
        n_run_values += runs.len() as u64;

        let term = from_utf8(&term)
            .map_err(|_| BuildError::Io(Error::other("fts sorted merge: non-utf8 term")))?;
        // A term whose every posting was deleted leaves the output.
        if postings.is_empty() {
            continue;
        }
        if ascending {
            let started = Stopwatch::start();
            let term_runs = TermRuns::Values {
                values: &runs,
                starts: &run_starts,
            };
            emit(term, &postings, term_runs)?;
            emit_time.stop(started);
            continue;
        }
        // Sort this term's postings by output doc id, taking each
        // posting's run start with it.
        n_sorted_terms += 1;
        n_sorted_postings += postings.len() as u64;
        let started = Stopwatch::start();
        let mut order: Vec<usize> = (0..postings.len()).collect();
        order.sort_unstable_by_key(|&k| postings[k].0);
        sorted_postings.clear();
        sorted_starts.clear();
        for k in order {
            sorted_postings.push(postings[k]);
            sorted_starts.push(run_starts[k]);
        }
        sort.stop(started);
        let started = Stopwatch::start();
        let term_runs = TermRuns::Values {
            values: &runs,
            starts: &sorted_starts,
        };
        emit(term, &sorted_postings, term_runs)?;
        emit_time.stop(started);
    }
    record("dict_ms", dict.ms());
    record("read_ms", read.ms());
    record("sort_ms", sort.ms());
    record("emit_ms", emit_time.ms());
    record("postings", n_postings);
    record("term_inputs", n_term_inputs);
    record("sorted_terms", n_sorted_terms);
    record("sorted_postings", n_sorted_postings);
    record("run_values", n_run_values);
    Ok(())
}

/// Append one document's positions to `out` as run values: the first
/// position, then the gap to each next one. The same values a LEB128 run
/// holds, without the encoding.
fn push_run_values(out: &mut Vec<u32>, positions: &[u32]) {
    let mut prev = 0u32;
    for (i, &p) in positions.iter().enumerate() {
        debug_assert!(i == 0 || p > prev, "positions must be strictly increasing");
        out.push(p - prev);
        prev = p;
    }
}

/// Walks one input column's dictionary in term order, a chunk at a time.
struct TermChunks<'a> {
    fts: &'a FtsReader,
    /// The input's dictionary, fetched once rather than per chunk.
    fst_bytes: Bytes,
    column_id: u32,
    chunk: vec::IntoIter<(Vec<u8>, FstValue)>,
    /// Last term of the latest chunk; the next chunk starts after it.
    resume: Vec<u8>,
    started: bool,
    done: bool,
}

impl<'a> TermChunks<'a> {
    fn new(fts: &'a FtsReader, column_id: u32) -> Result<Self, FtsError> {
        Ok(Self {
            fts,
            fst_bytes: fts.dict_bytes()?,
            column_id,
            chunk: Vec::new().into_iter(),
            resume: Vec::new(),
            started: false,
            done: false,
        })
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, FstValue)>, FtsError> {
        loop {
            if let Some(entry) = self.chunk.next() {
                return Ok(Some(entry));
            }
            if self.done {
                return Ok(None);
            }
            let terms = self.fts.column_terms_from(
                &self.fst_bytes,
                self.column_id,
                &self.resume,
                TERMS_PER_CHUNK,
            )?;
            self.done = terms.len() < TERMS_PER_CHUNK;
            // A chunk starts at `resume` itself, which was already handed out.
            let skip_first =
                self.started && terms.first().is_some_and(|(term, _)| *term == self.resume);
            if let Some((last, _)) = terms.last() {
                self.resume = last.clone();
            }
            self.started = true;
            self.chunk = terms.into_iter();
            if skip_first {
                self.chunk.next();
            }
        }
    }
}

fn read_error(e: FtsError) -> BuildError {
    BuildError::Io(Error::other(format!("fts sorted merge: {e}")))
}
