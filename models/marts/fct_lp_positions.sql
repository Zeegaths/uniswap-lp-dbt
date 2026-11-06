{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per Uniswap V3 NFT liquidity position opened in the
-- WETH/USDC 0.05% pool on Base during the extracted block range.
select
    position_id,
    owner_address,
    tick_lower,
    tick_upper,
    price_lower_usdc,
    price_upper_usdc,
    range_width_usdc,
    entry_price_usdc,
    entry_location,
    opened_at,
    last_activity_at,
    active_seconds,
    event_count,
    deposited_usdc,
    collected_usdc,
    realized_pnl_usdc,
    position_status,
    outcome
from {{ ref('int_position_pnl') }}
