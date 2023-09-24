create table if not exists ticker_count
( ticker LowCardinality(String)
, total UInt64 CODEC (T64, ZSTD(1))
) engine = MergeTree
order by ticker