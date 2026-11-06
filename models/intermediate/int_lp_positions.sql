-- One row per NFT position opened in this pool during the extracted range.
--
-- Decisions:
-- 1. A position belongs to the pool when its first IncreaseLiquidity sits in the
--    same transaction as a pool Mint sent by the position manager with the same
--    liquidity and token amounts. That Mint gives the tick range.
-- 2. The LP is the address that received the NFT when it was minted
--    (Transfer from the zero address). Later NFT transfers are ignored.
-- 3. Positions minted before the range have no NFT mint here, so they drop out.
with pool_mints as (
    select *
    from {{ ref('stg_pool_mints') }}
    where sender_address = '{{ var("position_manager_address") }}'
),

matched as (
    select
        i.position_id,
        i.tx_hash,
        i.block_number,
        i.log_index,
        i.block_timestamp,
        m.tick_lower,
        m.tick_upper,
        row_number() over (
            partition by i.position_id
            order by i.block_number, i.log_index
        ) as rn
    from {{ ref('stg_position_increases') }} i
    join pool_mints m
      on m.tx_hash = i.tx_hash
     and m.liquidity = i.liquidity
     and m.amount0_raw = i.amount0_raw
     and m.amount1_raw = i.amount1_raw
     and m.log_index < i.log_index
),

nft_mints as (
    select position_id, tx_hash, to_address as owner_address
    from {{ ref('stg_position_transfers') }}
    where from_address = '0x0000000000000000000000000000000000000000'
)

select
    m.position_id,
    n.owner_address,
    m.tick_lower,
    m.tick_upper,
    power(1.0001::double precision, m.tick_lower)
        * power(10, {{ var('token0_decimals') }} - {{ var('token1_decimals') }}) as price_lower_usdc,
    power(1.0001::double precision, m.tick_upper)
        * power(10, {{ var('token0_decimals') }} - {{ var('token1_decimals') }}) as price_upper_usdc,
    m.block_timestamp                          as opened_at,
    m.block_number * 1000000 + m.log_index     as opened_event_order
from matched m
join nft_mints n
  on n.position_id = m.position_id
 and n.tx_hash = m.tx_hash
where m.rn = 1
