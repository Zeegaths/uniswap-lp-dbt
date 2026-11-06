-- Pool price after every swap, in USDC per WETH.
-- sqrtPriceX96 = sqrt(token1 / token0) * 2^96, in raw units.
-- price = (sqrtPriceX96 / 2^96)^2 * 10^(decimals0 - decimals1)
select
    block_number * 1000000 + log_index                         as event_order,
    block_number,
    log_index,
    block_timestamp,
    tick,
    power(sqrt_price_x96::double precision / 2 ^ 96, 2)
        * power(10, {{ var('token0_decimals') }} - {{ var('token1_decimals') }}) as price_usdc_per_weth
from {{ ref('stg_swaps') }}
