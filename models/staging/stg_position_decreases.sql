-- Position manager DecreaseLiquidity events: liquidity removed from an NFT position. Cast and rename only.
select
    tx_hash,
    log_index,
    block_number,
    block_timestamp,
    (args ->> 'tokenId')::bigint   as position_id,
    (args ->> 'liquidity')::numeric as liquidity,
    (args ->> 'amount0')::numeric  as amount0_raw,
    (args ->> 'amount1')::numeric  as amount1_raw
from {{ source('base_uniswap', 'uniswap_events') }}
where contract_address = '{{ var("position_manager_address") }}'
  and event_name = 'DecreaseLiquidity'
