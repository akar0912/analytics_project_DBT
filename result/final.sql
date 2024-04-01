With calender As (
SELECT date_trunc('day', generate_series)::date AS date
    FROM generate_series(
        '2023-07-01'::timestamp,
        CURRENT_DATE,
        '1 day'::interval
    )
),
-- add a column staking_date that reperesent date stc
staked_with_date as (
	SELECT xx, "assetReward", "assetHold", "stakedAmount", "dailyRewardPerUnit", "end" as end_date,
	    to_timestamp(cast(source_ts_ms/1000 as bigint)) as stc, 
	    to_timestamp(cast(source_ts_ms/1000 as bigint))::date as staking_date
FROM public."staking_Rates" as c
),

-- fetch the last staked entry in a singel day for each contract
last_staked_per_day as (
select xx, "assetReward", "assetHold", "stakedAmount", staking_date, "dailyRewardPerUnit", end_date from (
	SELECT *,
		ROW_NUMBER() over (
			PARTITION BY xx, staking_date
			ORDER BY stc desc
		) as rn
	FROM staked_with_date 
	) as inners
where inners.rn = 1
order by stc 
),
consecutive_day_contract_combination as (
select distinct xx, date
from last_staked_per_day
cross join calender
order by xx, date
),
consecutive_day_staked_asset as (
	select c.date, c.xx, l."assetReward", l."assetHold", l."stakedAmount", l."dailyRewardPerUnit", end_date from 
	consecutive_day_contract_combination as c
	left join last_staked_per_day as l
	on c.date = l.staking_date and c.xx = l.xx
),

--select * from consecutive_day_staked_asset as c
--where c.xx = 130

ranked as (
	select *,
	COUNT("stakedAmount") OVER (partition by xx ORDER BY date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
	from consecutive_day_staked_asset
),
filled_consecutive_day_contract as  (
select date, xx, end_date,
Max("stakedAmount") over (partition by xx, grp) as "stakedAmount",
Max("assetHold") over (partition by xx, grp) as "assetHold", 
Max("assetReward") over (partition by xx, grp) as "assetReward",
Max("dailyRewardPerUnit") over (partition by xx, grp) as "dailyRewardPerUnit"	
from ranked
), 
rewards_merged as (
	select c.*, r.amount as reward_amount from filled_consecutive_day_contract as c
left join public."staking_Rewards" as r
on c.date = r.date and c.xx = r.reward and r.type like 'expected'
order by c.xx, date
),
-- backfill and forward fill
rewards_merged_filled as (
	select date, xx, "assetReward", "assetHold", "stakedAmount", "dailyRewardPerUnit",
 case 
	when rev_group = 0 then 0 
	else 
	max(reward_amount) over (partition by xx, rev_group)
 end as reward_amount 
	from (
SELECT *,
     COUNT(reward_amount) OVER (PARTITION BY xx ORDER BY date) AS rev_group
from rewards_merged
	) as inners
),
final_with_stake_null as (
select date, xx, "stakedAmount", reward_amount,
	case when "assetReward" is null then
	first_value("assetReward") OVER (PARTITION BY xx ORDER BY case when "assetReward" is not null then 0 else 1 end ASC, date)
	else "assetReward"
	end,
	case when "assetHold" is null then
	first_value("assetHold") OVER (PARTITION BY xx ORDER BY case when "assetHold" is not null then 0 else 1 end ASC, date)
	else "assetHold"
	end,
	case when "dailyRewardPerUnit" is null then
	first_value("dailyRewardPerUnit") OVER (PARTITION BY xx ORDER BY case when "dailyRewardPerUnit" is not null then 0 else 1 end ASC, date)
	else "dailyRewardPerUnit"
	end
from rewards_merged_filled
	
),
/*
select * from last_staked_per_day
where xx = 920
--where "stakedAmount" is null and "dailyRewardPerUnit" = 0
order by staking_date
*/

	final_result as (
select date, xx, 
	case when "stakedAmount" is null then
		case when reward_amount = 0 or "dailyRewardPerUnit" = 0 then 0 
		else reward_amount / "dailyRewardPerUnit"
		end
	else "stakedAmount"
	end,
	reward_amount,
	"assetReward", "assetHold", "dailyRewardPerUnit"
from final_with_stake_null
),
asset_result_per_day as (
	select date, "assetHold", SUM("stakedAmount") as total_staked_amount, SUM(reward_amount) as total_expected_reward
	from final_result
--where "assetHold" like 'MATIC'
	group by date, "assetHold"
	order by "assetHold", date
)

select * from asset_result_per_day
where "assetHold" like 'MATIC'