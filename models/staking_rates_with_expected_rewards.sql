-- rewards_merged joins the staking_rates_consecutive_day with staking_Rewards
with rewards_merged as (
	select c.*, r.amount as reward_amount from {{ ref('staking_rates_consecutive_day') }} as c
left join {{ ref('staking_Rewards') }} as r
on c.staking_date = r.date and c.contract_id = r.reward and r.type like 'expected' and not r.deleted
)
-- backfill and forward fill
select staking_date, end_date, contract_id, asset_reward, asset_hold, staked_amount, daily_reward_per_unit,
	case 
       when reward_amount is null then
           case
                -- back-fill
               when rev_group = 0 then 0 
           else 
                -- forward-fill
               case
                    -- forward-fill reward zero if staking_date is more than end_date of contract
                   when end_date < staking_date then 0
                    -- otherwise forward-fill reward equal to last non-null value
                   else max(reward_amount) over (partition by contract_id, rev_group)
               end
           end 
       else reward_amount 
end as reward_amount
from (
	SELECT *,
    	COUNT(reward_amount) OVER (PARTITION BY contract_id ORDER BY staking_date) AS rev_group
	from rewards_merged
) as inners
order by staking_date