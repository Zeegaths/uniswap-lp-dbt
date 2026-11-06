-- Position NFT Transfer events. Cast and rename only.
select
    tx_hash,
    log_index,
    block_number,
    block_timestamp,
    (args ->> 'tokenId')::bigint as position_id,
    (args ->> 'from')::text      as from_address,
    (args ->> 'to')::text        as to_address
from {{ source('base_uniswap', 'uniswap_events') }}
where contract_address = '{{ var("position_manager_address") }}'
  and event_name = 'Transfer'
