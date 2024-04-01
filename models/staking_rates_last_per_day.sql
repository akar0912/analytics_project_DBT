-- fetch the last staked entry in a single day for each contract
select start_date, end_date, contract_id, staking_time, staking_date, asset_reward, asset_hold, staked_amount, daily_reward_per_unit from (
	SELECT *,
		ROW_NUMBER() over (
			PARTITION BY contract_id, staking_date
			ORDER BY staking_time desc -- to put the last entry at row number 1
		) as row_number
	FROM {{ ref('staking_rates_cleaned') }} 
	) as inners
where inners.row_number = 1 -- fetching only the last staking entry for each day
order by staking_date