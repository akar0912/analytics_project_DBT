/*
	start -> start_date
	end -> end_date
	...
	add a column staking_date that reperesent date stc
*/
SELECT 
	"start" as start_date, "end" as end_date, xx as contract_id, "assetReward" as asset_reward, "assetHold" as asset_hold, "stakedAmount" as staked_amount, "dailyRewardPerUnit" as daily_reward_per_unit,
	to_timestamp(cast(source_ts_ms/1000 as bigint)) as staking_time,
	to_timestamp(cast(source_ts_ms/1000 as bigint))::date as staking_date
FROM {{ ref('staking_Rates') }}
where not __deleted 
order by staking_date