-- Pool Swap events. Cast and rename only.
select
    tx_hash,
    log_index,
    block_number,
    block_timestamp,
    (args ->> 'sender')::text           as sender_address,
    (args ->> 'recipient')::text        as recipient_address,
    (args ->> 'amount0')::numeric       as amount0_raw,
    (args ->> 'amount1')::numeric       as amount1_raw,
    (args ->> 'sqrtPriceX96')::numeric  as sqrt_price_x96,
    (args ->> 'liquidity')::numeric     as pool_liquidity,
    (args ->> 'tick')::integer          as tick
from {{ source('base_uniswap', 'uniswap_events') }}
where contract_address = '{{ var("pool_address") }}'
  and event_name = 'Swap'
