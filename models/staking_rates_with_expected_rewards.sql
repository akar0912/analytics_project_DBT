with rewards_merged as (
	select c.*, r.amount as reward_amount from {{ ref('staking_rates_consecutive_day') }} as c
left join public."staking_Rewards" as r
on c.staking_date = r.date and c.contract_id = r.reward and r.type like 'expected' and not r.deleted
)
-- backfill and forward fill
select staking_date, end_date, contract_id, asset_reward, asset_hold, staked_amount, daily_reward_per_unit,
	case 
       when reward_amount is null then
           case
               when rev_group = 0 then 0 
           else 
               case
                   when end_date < staking_date then 0
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