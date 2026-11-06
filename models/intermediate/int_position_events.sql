-- Every deposit, withdraw and collect for positions in int_lp_positions,
-- valued in USDC at the pool price right before the event.
--
-- Decisions:
-- 1. Price at an event = price after the most recent earlier swap.
--    Carried forward with a running count over swaps (gaps and islands).
-- 2. Events before the first swap in the range have no price and are dropped.
-- 3. value_usdc = WETH amount * price + USDC amount. Deposits are money in,
--    collects are money out (principal plus fees). Withdraws only move tokens
--    into "owed" inside the position, so they carry liquidity but no cash.
with positions as (
    select position_id from {{ ref('int_lp_positions') }}
),

events as (
    select position_id, tx_hash, block_number, log_index, block_timestamp,
           'deposit' as event_type, liquidity as liquidity_delta, amount0_raw, amount1_raw
    from {{ ref('stg_position_increases') }}
    union all
    select position_id, tx_hash, block_number, log_index, block_timestamp,
           'withdraw', -liquidity, amount0_raw, amount1_raw
    from {{ ref('stg_position_decreases') }}
    union all
    select position_id, tx_hash, block_number, log_index, block_timestamp,
           'collect', 0, amount0_raw, amount1_raw
    from {{ ref('stg_position_collects') }}
),

our_events as (
    select e.*
    from events e
    join positions p using (position_id)
),

timeline as (
    select event_order, price_usdc_per_weth, tick,
           null::bigint as position_id, null::text as tx_hash, null::bigint as block_number,
           null::integer as log_index, null::timestamptz as block_timestamp, null::text as event_type,
           null::numeric as liquidity_delta, null::numeric as amount0_raw, null::numeric as amount1_raw
    from {{ ref('int_pool_prices') }}
    union all
    select block_number * 1000000 + log_index, null, null,
           position_id, tx_hash, block_number, log_index, block_timestamp, event_type,
           liquidity_delta, amount0_raw, amount1_raw
    from our_events
),

grouped as (
    select *,
           count(price_usdc_per_weth) over (order by event_order) as price_group
    from timeline
),

priced as (
    select *,
           max(price_usdc_per_weth) over (partition by price_group) as last_price,
           max(tick) over (partition by price_group)                as last_tick
    from grouped
)

select
    position_id,
    event_type,
    event_order,
    tx_hash,
    block_number,
    log_index,
    block_timestamp,
    liquidity_delta,
    amount0_raw / power(10, {{ var('token0_decimals') }})  as amount_weth,
    amount1_raw / power(10, {{ var('token1_decimals') }})  as amount_usdc,
    last_price                                             as price_usdc_per_weth,
    last_tick                                              as pool_tick,
    (amount0_raw / power(10, {{ var('token0_decimals') }}))::double precision * last_price
        + (amount1_raw / power(10, {{ var('token1_decimals') }}))::double precision as value_usdc
from priced
where position_id is not null
  and price_group > 0
