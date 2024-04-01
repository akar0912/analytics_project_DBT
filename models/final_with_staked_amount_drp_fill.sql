select staking_date, asset_hold, SUM(staked_amount) as total_staked_amount, SUM(reward_amount) as total_expected_reward
from {{ ref('staked_amount_drp_fill') }}
group by staking_date, asset_hold
order by asset_hold, staking_date