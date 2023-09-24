create table if not exists quote_1min_agg
( ticker      LowCardinality(String)
, ts          DATETIME64(3, 'EST') CODEC (DoubleDelta, ZSTD(1))
, log_ret     Float64              CODEC (Gorilla, ZSTD(1))
) engine = MergeTree
order by (ticker, ts)