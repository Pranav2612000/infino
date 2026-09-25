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
//!
//! The walk over the dictionaries and the writes stay in term order on one
//! thread. Between them, each batch of terms is read, sorted and encoded in
//! parallel: a term's work depends on nothing outside the term.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    io::Error,
    iter::once,
    marker::PhantomData,
    mem,
    ops::Range,
    str::from_utf8,
    sync::{Arc, Mutex, PoisonError},
    vec,
};

use bytes::Bytes;
use rayon::prelude::*;

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

/// Most terms in one parallel batch. Tiny under test, so the merge tests
/// cross a batch boundary at every kind of term they build.
const BATCH_TERMS: usize = if cfg!(test) { 3 } else { 1 << 16 };

/// Most posting bytes in one batch, as the dictionary's length hints
/// count them, so a batch of very common terms stays bounded in memory. A
/// term larger than this is a batch of its own.
const BATCH_POSTING_BYTES: u64 = if cfg!(test) { 256 } else { 64 << 20 };

/// Terms one parallel task takes, so it takes a worker's scratch once per
/// few hundred terms rather than once per term.
const TASK_TERMS: usize = if cfg!(test) { 2 } else { 256 };

/// One merge input: a superfile and where its rows land in the output.
pub(crate) struct SortedInput {
    pub(crate) reader: Arc<SuperfileReader>,
    /// Input blob doc id → output doc id; `None` drops the row.
    pub(crate) remap: Vec<Option<FtsDocId>>,
}

/// Merge `column_id` across `inputs`: for every term that still has a
/// surviving posting, `encode(state, postings, runs)` builds its output
/// and `write(term, output)` places it. `postings` are
/// `(output_doc_id, tf)`, doc ids ascending; `runs` holds each posting's
/// position run, decoded (empty for a non-positional column).
///
/// `encode` runs on many threads at once, each call with a worker state
/// from `states`, grown with `new_state` as needed; `write` runs on this
/// thread, once per term, in term order. The states are handed back in
/// `states`, so the caller can fold what they gathered.
///
/// Under `detailed-tracing`, records where the time went on the enclosing
/// span: `dict_ms` and `write_ms` on this thread and `parallel_ms` for the
/// batches' wall time; `read_ms`, `sort_ms` and `encode_ms` summed across
/// threads; plus the `postings`, `term_inputs`, `sorted_terms`,
/// `sorted_postings` and `run_values` counts.
pub(crate) fn merge_column<S: Send, T: Send>(
    inputs: &[SortedInput],
    column_id: u32,
    states: &mut Vec<S>,
    new_state: impl Fn() -> S + Sync,
    encode: impl Fn(&mut S, &[(u32, u32)], TermRuns<'_>) -> Result<T, BuildError> + Sync,
    mut write: impl FnMut(&str, T) -> Result<(), BuildError>,
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

    let work = BatchWork {
        readers: &readers,
        inputs,
        column_id,
        workers: Mutex::new(
            mem::take(states)
                .into_iter()
                .map(|s| (TermWork::default(), s))
                .collect(),
        ),
        new_state: &new_state,
        encode: &encode,
        output: PhantomData,
    };
    let mut dict = Stopwatch::default();
    let mut parallel = Stopwatch::default();
    let mut write_time = Stopwatch::default();
    let mut n_term_inputs = 0u64;
    let mut batch = Batch::default();
    loop {
        let started = Stopwatch::start();
        let Some(Reverse((term, first))) = heap.pop() else {
            dict.stop(started);
            break;
        };
        let term_start = batch.term_bytes.len();
        batch.term_bytes.extend_from_slice(&term);
        let inputs_start = batch.inputs.len();
        let mut next_input = Some(first);
        while let Some(i) = next_input {
            let value = values[i].ok_or(BuildError::BatchReadError)?;
            batch.inputs.push((i, value));
            if let FstValue::Pfor {
                postings_length_hint: Some(len),
                ..
            } = value
            {
                batch.posting_bytes += u64::from(len);
            }
            let next = cursors[i].next().map_err(read_error)?;
            values[i] = next.as_ref().map(|(_, value)| *value);
            if let Some((next_term, _)) = next {
                heap.push(Reverse((next_term, i)));
            }
            next_input = match heap.peek() {
                Some(Reverse((t, _))) if *t == term => heap.pop().map(|Reverse((_, i))| i),
                _ => None,
            };
        }
        n_term_inputs += (batch.inputs.len() - inputs_start) as u64;
        batch.terms.push(PendingTerm {
            term: term_start..batch.term_bytes.len(),
            inputs: inputs_start..batch.inputs.len(),
        });
        dict.stop(started);
        if batch.terms.len() >= BATCH_TERMS || batch.posting_bytes >= BATCH_POSTING_BYTES {
            work.run(&batch, &mut parallel, &mut write_time, &mut write)?;
            batch.clear();
        }
    }
    work.run(&batch, &mut parallel, &mut write_time, &mut write)?;

    let workers = work
        .workers
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    let mut totals = TermWork::default();
    for (w, _) in &workers {
        totals.absorb(w);
    }
    *states = workers.into_iter().map(|(_, s)| s).collect();
    record("dict_ms", dict.ms());
    record("parallel_ms", parallel.ms());
    record("write_ms", write_time.ms());
    record("read_ms", totals.read.ms());
    record("sort_ms", totals.sort.ms());
    record("encode_ms", totals.encode.ms());
    record("postings", totals.n_postings);
    record("term_inputs", n_term_inputs);
    record("sorted_terms", totals.n_sorted_terms);
    record("sorted_postings", totals.n_sorted_postings);
    record("run_values", totals.n_run_values);
    Ok(())
}

/// One term waiting in a batch: where its bytes and its inputs'
/// dictionary values sit in the batch's shared buffers.
struct PendingTerm {
    term: Range<usize>,
    inputs: Range<usize>,
}

/// Terms walked off the dictionaries, waiting to be merged together.
#[derive(Default)]
struct Batch {
    terms: Vec<PendingTerm>,
    term_bytes: Vec<u8>,
    /// `(input, that input's dictionary value)` for every term's inputs.
    inputs: Vec<(usize, FstValue)>,
    posting_bytes: u64,
}

impl Batch {
    fn clear(&mut self) {
        self.terms.clear();
        self.term_bytes.clear();
        self.inputs.clear();
        self.posting_bytes = 0;
    }
}

/// One worker's buffers for merging a term, and what it has measured.
#[derive(Default)]
struct TermWork {
    postings: Vec<(u32, u32)>,
    /// Every posting's run values back to back, and where each run starts.
    /// A sort reorders the starts and leaves the values where they are.
    runs: Vec<u32>,
    run_starts: Vec<usize>,
    positions_buf: Vec<u32>,
    sorted_postings: Vec<(u32, u32)>,
    sorted_starts: Vec<usize>,
    /// Decoding and remapping; sorting a term whose postings arrive out of
    /// order; and encoding.
    read: Stopwatch,
    sort: Stopwatch,
    encode: Stopwatch,
    n_postings: u64,
    n_sorted_terms: u64,
    n_sorted_postings: u64,
    n_run_values: u64,
}

impl TermWork {
    fn absorb(&mut self, other: &TermWork) {
        self.read.add(&other.read);
        self.sort.add(&other.sort);
        self.encode.add(&other.encode);
        self.n_postings += other.n_postings;
        self.n_sorted_terms += other.n_sorted_terms;
        self.n_sorted_postings += other.n_sorted_postings;
        self.n_run_values += other.n_run_values;
    }
}

/// What every batch of one column's merge shares.
struct BatchWork<'a, S, T, N, E> {
    readers: &'a [&'a FtsReader],
    inputs: &'a [SortedInput],
    column_id: u32,
    /// Idle workers; a task takes one and puts it back.
    workers: Mutex<Vec<(TermWork, S)>>,
    new_state: &'a N,
    encode: &'a E,
    /// What `encode` returns.
    output: PhantomData<fn() -> T>,
}

impl<S, T, N, E> BatchWork<'_, S, T, N, E>
where
    S: Send,
    T: Send,
    N: Fn() -> S + Sync,
    E: Fn(&mut S, &[(u32, u32)], TermRuns<'_>) -> Result<T, BuildError> + Sync,
{
    /// Merge a batch's terms in parallel, then write them in term order.
    fn run(
        &self,
        batch: &Batch,
        parallel: &mut Stopwatch,
        write_time: &mut Stopwatch,
        write: &mut impl FnMut(&str, T) -> Result<(), BuildError>,
    ) -> Result<(), BuildError> {
        let started = Stopwatch::start();
        let merged = batch
            .terms
            .par_chunks(TASK_TERMS)
            .map(|chunk| self.merge_chunk(batch, chunk))
            .collect::<Result<Vec<_>, _>>()?;
        parallel.stop(started);

        let started = Stopwatch::start();
        for (pending, output) in batch.terms.iter().zip(merged.into_iter().flatten()) {
            let term = from_utf8(&batch.term_bytes[pending.term.clone()])
                .map_err(|_| BuildError::Io(Error::other("fts sorted merge: non-utf8 term")))?;
            // A term whose every posting was deleted leaves the output.
            if let Some(output) = output {
                write(term, output)?;
            }
        }
        write_time.stop(started);
        Ok(())
    }

    /// Merge some consecutive terms of a batch on one worker.
    fn merge_chunk(
        &self,
        batch: &Batch,
        chunk: &[PendingTerm],
    ) -> Result<Vec<Option<T>>, BuildError> {
        let taken = self
            .workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let (mut work, mut state) =
            taken.unwrap_or_else(|| (TermWork::default(), (self.new_state)()));
        let merged = chunk
            .iter()
            .map(|pending| {
                let term = &batch.term_bytes[pending.term.clone()];
                let inputs = &batch.inputs[pending.inputs.clone()];
                self.merge_term(term, inputs, &mut work, &mut state)
            })
            .collect();
        self.workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((work, state));
        merged
    }

    /// Read one term's postings from its inputs, remapped to output doc
    /// ids, sort them if they arrive out of order, and encode them. `None`
    /// when every posting was deleted.
    fn merge_term(
        &self,
        term: &[u8],
        term_inputs: &[(usize, FstValue)],
        work: &mut TermWork,
        state: &mut S,
    ) -> Result<Option<T>, BuildError> {
        let started = Stopwatch::start();
        let TermWork {
            postings,
            runs,
            run_starts,
            positions_buf,
            ..
        } = work;
        postings.clear();
        runs.clear();
        run_starts.clear();
        let mut ascending = true;
        for &(i, value) in term_inputs {
            let remap = &self.inputs[i].remap;
            self.readers[i]
                .for_each_posting_in(
                    self.column_id,
                    once((term, value)),
                    positions_buf,
                    |_, doc, tf, pos| {
                        if let Some(out_doc) = remap[doc as usize] {
                            let out_doc = out_doc.get();
                            ascending &= postings.last().is_none_or(|&(d, _)| d < out_doc);
                            postings.push((out_doc, tf));
                            run_starts.push(runs.len());
                            push_run_values(runs, pos);
                        }
                        Ok(())
                    },
                )
                .map_err(read_error)?;
        }
        work.read.stop(started);
        if work.postings.is_empty() {
            return Ok(None);
        }
        work.n_postings += work.postings.len() as u64;
        work.n_run_values += work.runs.len() as u64;

        let (pairs, starts) = match ascending {
            true => (&work.postings, &work.run_starts),
            false => {
                // Sort this term's postings by output doc id, taking each
                // posting's run start with it.
                work.n_sorted_terms += 1;
                work.n_sorted_postings += work.postings.len() as u64;
                let started = Stopwatch::start();
                let mut order: Vec<usize> = (0..work.postings.len()).collect();
                order.sort_unstable_by_key(|&k| work.postings[k].0);
                work.sorted_postings.clear();
                work.sorted_starts.clear();
                for k in order {
                    work.sorted_postings.push(work.postings[k]);
                    work.sorted_starts.push(work.run_starts[k]);
                }
                work.sort.stop(started);
                (&work.sorted_postings, &work.sorted_starts)
            }
        };
        let started = Stopwatch::start();
        let runs = TermRuns::Values {
            values: &work.runs,
            starts,
        };
        let output = (self.encode)(state, pairs, runs)?;
        work.encode.stop(started);
        Ok(Some(output))
    }
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
