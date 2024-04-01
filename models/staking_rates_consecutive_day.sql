With consecutive_day_each_contract as (
select distinct contract_id, date
from {{ ref('staking_rates_last_per_day') }}
cross join {{ ref('calender') }}
),
staking_rate_consecutive_day_with_null as (
select c.date, l.start_date, l.end_date, c.contract_id, l.asset_reward, l.asset_hold, l.staked_amount, l.staking_date, l.daily_reward_per_unit 
from consecutive_day_each_contract as c
left join {{ ref('staking_rates_last_per_day') }} as l
on c.date = l.staking_date and c.contract_id = l.contract_id
),
staking_rate_filled as (
select date as staking_date, 
case
    when grp = 0 then first_value(start_date) OVER (PARTITION BY contract_id ORDER BY case when start_date is not null then 0 else 1 end ASC, staking_date)
    else Max(start_date) over (partition by contract_id, grp)
end as start_date,
end_date,
contract_id,
case
    when grp = 0 then first_value(asset_reward) OVER (PARTITION BY contract_id ORDER BY case when asset_reward is not null then 0 else 1 end ASC, staking_date)
    else Max(asset_reward) over (partition by contract_id, grp) 
end as asset_reward,
case
    when grp = 0 then first_value(asset_hold) OVER (PARTITION BY contract_id ORDER BY case when asset_hold is not null then 0 else 1 end ASC, staking_date)
    else Max(asset_hold) over (partition by contract_id, grp) 
end as asset_hold,
case
    when grp = 0 then 0
    else Max(staked_amount) over (partition by contract_id, grp)
end as staked_amount,
case
    when grp = 0 then 0
    else Max(daily_reward_per_unit) over (partition by contract_id, grp)
end as daily_reward_per_unit	
from (
    select *,
    COUNT(staked_amount) OVER (partition by contract_id ORDER BY date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    from staking_rate_consecutive_day_with_null
) as ranked
)

select staking_date, start_date, 
case
    when grp = 0 then first_value(end_date) OVER (PARTITION BY contract_id ORDER BY case when end_date is not null then 0 else 1 end ASC, staking_date)
    else Max(end_date) over (partition by contract_id, grp)
end as end_date,
contract_id, asset_reward, asset_hold, staked_amount, daily_reward_per_unit 
from(
    select *,
    COUNT(end_date) OVER (partition by contract_id ORDER BY staking_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    from staking_rate_filled
) as ranked 
