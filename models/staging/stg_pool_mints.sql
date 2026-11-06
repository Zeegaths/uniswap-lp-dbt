-- Pool Mint events: liquidity added to a tick range. Cast and rename only.
select
    tx_hash,
    log_index,
    block_number,
    block_timestamp,
    (args ->> 'sender')::text      as sender_address,
    (args ->> 'owner')::text       as owner_address,
    (args ->> 'tickLower')::integer as tick_lower,
    (args ->> 'tickUpper')::integer as tick_upper,
    (args ->> 'amount')::numeric   as liquidity,
    (args ->> 'amount0')::numeric  as amount0_raw,
    (args ->> 'amount1')::numeric  as amount1_raw
from {{ source('base_uniswap', 'uniswap_events') }}
where contract_address = '{{ var("pool_address") }}'
  and event_name = 'Mint'
