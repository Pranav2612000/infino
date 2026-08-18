# Provenance

The 43 `q0.sql`..`q42.sql` files in this directory are vendored
byte-identical from Apache DataFusion's ClickBench query battery:
`benchmarks/queries/clickbench/queries/` in
[apache/datafusion](https://github.com/apache/datafusion), licensed
Apache-2.0.

We use DataFusion's port rather than the upstream ClickHouse SQL
(`https://github.com/ClickHouse/ClickBench`) because the upstream
queries are written in ClickHouse's SQL dialect and don't run
unmodified against a standard SQL engine such as DataFusion or Infino.

No dataset bytes are vendored here — only query text. Infino's bench
loader (`benches/utils/corpus/clickbench.rs`) rewrites `FROM hits` to
`FROM supertable` at load time; the files on disk are left untouched
so they can be diffed against upstream directly.
