-- This model gives the total_staked_amount and total_expected_reward for each asset on each consecutive day as mentioned in 7.1
select staking_date, asset_hold, SUM(staked_amount) as total_staked_amount, SUM(reward_amount) as total_expected_reward
from {{ ref('staked_amount_drp_reset') }}
group by staking_date, asset_hold
order by asset_hold, staking_date
-- we are doing SUM of (staked_amount and Expected daily rewards) and Group by on asste_hold as it has multiple contract_iDs