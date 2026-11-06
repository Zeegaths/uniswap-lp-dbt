-- One row per position with its realized PnL and derived categories.
--
-- Decisions:
-- 1. deposited_usdc = deposits valued at the price when they happened.
-- 2. collected_usdc = collects valued at the price when they happened.
--    Collects include both principal and earned fees.
-- 3. realized_pnl_usdc = collected_usdc - deposited_usdc. This follows the
--    capital-in vs capital-out method of Urusov et al. (2026), arXiv:2604.22069.
-- 4. A position is closed when all added liquidity was removed and a collect
--    came after the last withdraw. Anything else is open.
-- 5. Outcome is win / loss / breakeven for closed positions, open otherwise.
-- 6. entry_location compares the pool tick at the first deposit to the range.
with events as (
    select * from {{ ref('int_position_events') }}
),

agg as (
    select
        position_id,
        sum(case when event_type = 'deposit'  then liquidity_delta else 0 end)  as liquidity_added,
        -sum(case when event_type = 'withdraw' then liquidity_delta else 0 end) as liquidity_removed,
        sum(case when event_type = 'deposit' then value_usdc else 0 end)        as deposited_usdc,
        sum(case when event_type = 'collect' then value_usdc else 0 end)        as collected_usdc,
        coalesce(max(case when event_type = 'withdraw' then event_order end), 0) as last_withdraw_order,
        coalesce(max(case when event_type = 'collect'  then event_order end), 0) as last_collect_order,
        max(block_timestamp)                                                    as last_activity_at,
        count(*)                                                                as event_count
    from events
    group by position_id
),

entry as (
    select distinct on (position_id)
        position_id,
        price_usdc_per_weth as entry_price_usdc,
        pool_tick           as entry_tick
    from events
    where event_type = 'deposit'
    order by position_id, event_order
),

base as (
    select
        p.position_id,
        p.owner_address,
        p.tick_lower,
        p.tick_upper,
        p.price_lower_usdc,
        p.price_upper_usdc,
        p.price_upper_usdc - p.price_lower_usdc as range_width_usdc,
        p.opened_at,
        a.last_activity_at,
        extract(epoch from a.last_activity_at - p.opened_at)::bigint as active_seconds,
        e.entry_price_usdc,
        e.entry_tick,
        case
            when e.entry_tick <  p.tick_lower then 'price_below_range'
            when e.entry_tick >= p.tick_upper then 'price_above_range'
            else 'price_in_range'
        end as entry_location,
        a.liquidity_added,
        a.liquidity_removed,
        a.deposited_usdc,
        a.collected_usdc,
        a.collected_usdc - a.deposited_usdc as realized_pnl_usdc,
        case
            when a.liquidity_removed >= a.liquidity_added
             and a.last_collect_order > a.last_withdraw_order
            then 'closed'
            else 'open'
        end as position_status,
        a.event_count
    from {{ ref('int_lp_positions') }} p
    join agg a   using (position_id)
    join entry e using (position_id)
)

select
    *,
    case
        when position_status = 'open'   then 'open'
        when realized_pnl_usdc > 0.01   then 'win'
        when realized_pnl_usdc < -0.01  then 'loss'
        else 'breakeven'
    end as outcome
from base
