"""Unit-classification tests for `bench_summary.py`.

Run: `python3 .github/scripts/test_bench_summary.py`

The merge gate compares raw metric deltas against a NANOSECOND threshold, so
whether a header is time-valued decides whether that metric can block a
merge. That used to be inferred from the absence of two substrings, which
swept byte-valued columns into the latency gate: a 494 MiB -> 788 MiB
file-backed RSS move blocked a PR as "294 ms past a 5 ms gate". These cases
pin the real header set to its real units, so a newly added column cannot
land in the latency gate unnoticed.
"""

import importlib.util
import os
import sys

spec = importlib.util.spec_from_file_location(
    "bench_summary",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_summary.py"),
)
bench_summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench_summary)

# Every header the bench emits that reaches the gate, with whether it is
# genuinely nanosecond-valued (and therefore gate-eligible).
CASES = [
    # Byte-valued — footprints, on-disk sizes, transfer volumes.
    ("Peak file", False),
    ("Peak anon", False),
    ("Peak RSS", False),
    ("Median RSS", False),
    ("P90 RSS", False),
    ("Stored", False),
    ("Payload", False),
    ("Downloaded", False),
    ("Uploaded", False),
    ("Cold open GET/bytes", False),
    ("Hidden data GET/bytes", False),
    ("Manifest GET/bytes", False),
    ("User data GET/bytes", False),
    # Count- and ratio-valued.
    ("Warm GET/query", False),
    ("Requests", False),
    ("Rows", False),
    ("Hits (u/h)", False),
    ("OS threads", False),
    ("Recall", False),
    # Higher-is-better rates.
    ("Throughput", False),
    ("Bandwidth", False),
    # Genuine nanosecond metrics — these MUST stay gate-eligible.
    ("Time", True),
    ("Wall", True),
    ("warm p50", True),
    ("warm p90", True),
    ("warm p99", True),
    ("+fetch p50", True),
    ("cold 1st query (median)", True),
    ("cold open (median)", True),
    ("Delta commit", True),
    ("Drain", True),
    ("Optimize", True),
    ("Ingest", True),
]

BYTES_PER_MIB = 1048576


def main():
    failures = []

    for header, want_latency in CASES:
        got = bench_summary.is_latency(header)
        if got != want_latency:
            failures.append(
                f"is_latency({header!r}) = {got}, want {want_latency}"
            )

    # The exact values that blocked a merge: file-backed RSS in bytes, which
    # cleared both gate thresholds when read as nanoseconds.
    old = 494.23 * BYTES_PER_MIB
    new = 787.78 * BYTES_PER_MIB
    delta, pct = new - old, (new - old) / old * 100.0
    if not (delta > bench_summary.GATE_ABS_NS and pct > bench_summary.GATE_REL_PCT):
        failures.append(
            "the regression case no longer clears both gate thresholds; "
            "pick values that would block if misclassified"
        )
    if bench_summary.is_latency("Peak file"):
        failures.append("'Peak file' is gate-eligible: a byte metric can block a merge")

    rendered = bench_summary.human("Peak file", new)
    if "ms" in rendered or "MiB" not in rendered:
        failures.append(f"human('Peak file', 788 MiB) = {rendered!r}, want MiB")

    # Latency formatting and eligibility must be untouched.
    if bench_summary.human("warm p50", 195_150_000.0) != "195.15 ms":
        failures.append(
            f"human('warm p50') = {bench_summary.human('warm p50', 195_150_000.0)!r}"
        )
    if bench_summary.human("Wall", 24_400_000_000.0) != "24.40 s":
        failures.append(f"human('Wall') = {bench_summary.human('Wall', 24_400_000_000.0)!r}")

    if failures:
        print("FAILED:")
        for f in failures:
            print(f"  {f}")
        return 1
    print(f"ok — {len(CASES)} header classifications, gate and formatter checks pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
