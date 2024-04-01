-- fetch the last staked entry in a singel day for each contract
select start_date, end_date, contract_id, staking_time, staking_date, asset_reward, asset_hold, staked_amount, daily_reward_per_unit from (
	SELECT *,
		ROW_NUMBER() over (
			PARTITION BY contract_id, staking_date
			ORDER BY staking_time desc
		) as row_number
	FROM {{ ref('staking_rates_cleaned') }} 
	) as inners
where inners.row_number = 1
order by staking_date