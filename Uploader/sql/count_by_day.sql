create table if not exists ticker_count_by_day
( ticker LowCardinality(String)
, ts DATETIME64(3, 'EST') CODEC  (DoubleDelta, ZSTD(1))
, total UInt64 CODEC (T64, ZSTD(1))
) engine = MergeTree
  order by (ticker, ts)