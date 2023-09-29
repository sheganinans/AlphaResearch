create table if not exists features
( unit LowCardinality(String)
, interval UInt32 CODEC  (T64, ZSTD(1))
, min_avg_total UInt32 CODEC (T64, ZSTD(1))
, ts DATETIME64(3, 'EST') CODEC (DoubleDelta, ZSTD(1))
, features Array(Float32)
) engine = MergeTree
  order by (unit, interval, min_avg_total, ts)