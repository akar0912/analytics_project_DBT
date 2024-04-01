/*
consecutive_day_each_contract creates a combination of contract_id, date for each consecutive day
2023-07-01 1
2023-07-02 1
...
curr-date 1
2023-07-01 2
2023-07-01 2
...
curr-date 2
...
*/
With consecutive_day_each_contract as (
select distinct contract_id, date
from {{ ref('staking_rates_last_per_day') }}
cross join {{ ref('calender') }}
),
/*
staking_rate_consecutive_day_with_null left join with consecutive_day_each_contract to get staking entry for each consecutive day
*/
staking_rate_consecutive_day_with_null as (
select c.date, l.start_date, l.end_date, c.contract_id, l.asset_reward, l.asset_hold, l.staked_amount, l.staking_date, l.daily_reward_per_unit 
from consecutive_day_each_contract as c
left join {{ ref('staking_rates_last_per_day') }} as l
on c.date = l.staking_date and c.contract_id = l.contract_id
),
/*
staking_rate_filled back-fill and forward-fill the staking_rate_consecutive_day_with_null table
*/
staking_rate_filled as (
select date as staking_date, 
case
    -- back-fill start_date with first non-null entry
    when grp = 0 then first_value(start_date) OVER (PARTITION BY contract_id ORDER BY case when start_date is not null then 0 else 1 end ASC, staking_date)
    -- forward-fill start_date with last non-null entry
    else Max(start_date) over (partition by contract_id, grp)
end as start_date,
end_date,
contract_id,
case
    -- back-fill asset_reward with first non-null entry
    when grp = 0 then first_value(asset_reward) OVER (PARTITION BY contract_id ORDER BY case when asset_reward is not null then 0 else 1 end ASC, staking_date)
    -- forward-fill asset_reward with last non-null entry
    else Max(asset_reward) over (partition by contract_id, grp) 
end as asset_reward,
case
    -- back-fill asset_hold with first non-null entry
    when grp = 0 then first_value(asset_hold) OVER (PARTITION BY contract_id ORDER BY case when asset_hold is not null then 0 else 1 end ASC, staking_date)
    -- forward-fill asset_hold with last non-null entry
    else Max(asset_hold) over (partition by contract_id, grp) 
end as asset_hold,
staked_amount,
daily_reward_per_unit
from (
    select *,
    COUNT(staked_amount) OVER (partition by contract_id ORDER BY date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    from staking_rate_consecutive_day_with_null
) as ranked
)

select staking_date, start_date, 
case
    -- back-fill end_date with first non-null entry
    when grp = 0 then first_value(end_date) OVER (PARTITION BY contract_id ORDER BY case when end_date is not null then 0 else 1 end ASC, staking_date)
    -- forward-fill end_date with last non-null entry
    else Max(end_date) over (partition by contract_id, grp)
end as end_date,
contract_id, asset_reward, asset_hold, staked_amount, daily_reward_per_unit 
from(
    select *,
    COUNT(end_date) OVER (partition by contract_id ORDER BY staking_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    from staking_rate_filled
) as ranked 
