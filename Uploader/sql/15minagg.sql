create table thetadata_stock_trade_quotes_15minagg
( ticker LowCardinality(String)
, ts Datetime64(3, 'EST') CODEC (DoubleDelta, ZSTD(1))
, open Float32            CODEC (Gorilla, ZSTD(1))
, high Float32            CODEC (Gorilla, ZSTD(1))
, low Float32             CODEC (Gorilla, ZSTD(1))
, close Float32           CODEC (Gorilla, ZSTD(1))
, volume Float32          CODEC (Gorilla, ZSTD(1))
, liquidity Float32       CODEC (Gorilla, ZSTD(1))
, vwap Float32            CODEC (Gorilla, ZSTD(1))
, count UInt32            CODEC (T64, ZSTD(1))
, diff Float32            CODEC (Gorilla, ZSTD(1))
, log_ret Float32         CODEC (Gorilla, ZSTD(1))
, ts_diff_sum Float32     CODEC (Gorilla, ZSTD(1))
, diffiquity Float32      CODEC (Gorilla, ZSTD(1))
, twap Float32            CODEC (Gorilla, ZSTD(1))
, condition_map Map(String, UInt32)
)
engine = AggregatingMergeTree
partition by toYYYYMMDD(ts)
order by (ticker, ts);

create materialized view thetadata_stock_trade_quotes_15minagg_mv 
to thetadata_stock_trade_quotes_15minagg as
with 
  toStartOfInterval(time_of_trade, interval 15 minute) as ts
, if(neighbor(ts, -1) = '1969-12-31 19:00:00', ts, neighbor(ts, -1)) as prev
, if(prev == ts, 1, date_diff('s', prev, ts)) as time_diff
select 
  ticker
, ts
, count() as count

, first_value(price) as open
, last_value(price) as close

, sum(size) as volume
, avg(size) as avg_size
, max(size) as max_size
, stddevPopStable(size) as std_dev_size
, skewPop(size) as skew_size
, kurtPop(size) as kurt_size
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(size) as quantiles_size
, entropy(size) as entropy_size

, avg(bid_size) as avg_bid_size
, max(bid_size) as max_bid_size
, stddevPopStable(bid_size) as std_dev_bid_size
, skewPop(bid_size) as skew_bid_size
, kurtPop(bid_size) as kurt_bid_size
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(bid_size) as quantiles_bid_size
, entropy(bid_size) as entropy_bid_size

, avg(ask_size) as avg_ask_size
, max(ask_size) as max_ask_size
, stddevPopStable(ask_size) as std_dev_ask_size
, skewPop(ask_size) as skew_ask_size
, kurtPop(ask_size) as kurt_ask_size
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(ask_size) as quantiles_ask_size
, entropy(ask_size) as entropy_ask_size

, avg(price) as avg_price
, max(price) as max_price
, min(price) as min_price
, stddevPopStable(price) as std_dev_price
, skewPop(price) as skew_price
, kurtPop(price) as kurt_price
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(price) as quantiles_price
, entropy(price) as entropy_price

, avg(ask - bid) as avg_spread
, max(ask - bid) as max_spread
, min(ask - bid) as min_spread
, stddevPopStable(ask - bid) as std_dev_spread
, skewPop(ask - bid) as skew_spread
, kurtPop(ask - bid) as kurt_spread
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(ask - bid) as quantiles_spread
, entropy(ask - bid) as entropy_spread

, avg(ask_size - bid_size) as avg_size_diff
, max(ask_size - bid_size) as max_size_diff
, min(ask_size - bid_size) as min_size_diff
, stddevPopStable(ask_size - bid_size) as std_dev_size_diff
, skewPop(ask_size - bid_size) as skew_size_diff
, kurtPop(ask_size - bid_size) as kurt_size_diff
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(ask_size - bid_size) as quantiles_size_diff
, entropy(ask_size - bid_size) as entropy_size_diff

, covarPopStable(price, bid) as covar_price_bid
, covarPopStable(price, ask) as covar_price_ask
, covarPopStable(bid, ask) as covar_bid_ask

, sum(price * size) as liquidity
, liquidity / volume as vwap
, close - open as diff
, log(close/open) as log_ret

, sum(time_diff) as ts_diff_sum
, sum(price * time_diff) as diffiquity
, diffiquity / ts_diff_sum as twap

, sumMap(map(condition, 1)) as condition_map
, sumMap(map(bid_exchange, 1)) as bid_exchange_map
, sumMap(map(ask_exchange, 1)) as ask_exchange_map

from thetadata_stock_trade_quotes
group by ticker, ts
order by ticker, ts
limit 20;