/*
	This changes the columns as below

	start -> start_date
	end -> end_date
	xx -> contract_id

	// to avoid the camelCase issue with postgres
	assetReward -> asset_reward
	assetHold -> asset_hold
	stakedAmount -> staked_amount
	dailyRewardPerUnit -> daily_reward_per_unit

	source_ts_ms -> staking_time / staking_date
	...
*/
SELECT 
	"start" as start_date, "end" as end_date, xx as contract_id, "assetReward" as asset_reward, "assetHold" as asset_hold, "stakedAmount" as staked_amount, "dailyRewardPerUnit" as daily_reward_per_unit,
	to_timestamp(cast(source_ts_ms/1000 as bigint)) as staking_time,
	to_timestamp(cast(source_ts_ms/1000 as bigint))::date as staking_date
FROM {{ ref('staking_Rates') }}
where not __deleted 
order by staking_time