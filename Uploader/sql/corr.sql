create table if not exists corr
( a      LowCardinality(String)
, b      LowCardinality(String)
, ts     DATETIME64(3, 'EST') CODEC (DoubleDelta, ZSTD(1))
, corr   Float64              CODEC (Gorilla, ZSTD(1))
) engine = MergeTree
  order by (a, b, ts)