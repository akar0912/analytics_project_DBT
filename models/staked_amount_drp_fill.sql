-- this models back-fill and forward-fill staked_amount and daily_reward_per_unit as mentioned in 6.2
With staking_rate_drp_filled as (
    select staking_date, end_date, contract_id, asset_reward, asset_hold, 
    staked_amount,
    case
        -- back-fill with the first non-null value
        when grp = 0 then first_value(daily_reward_per_unit) OVER (PARTITION BY contract_id ORDER BY case when daily_reward_per_unit is not null then 0 else 1 end ASC, staking_date)
        -- forward-fill with the last non-null value
        else Max(daily_reward_per_unit) over (partition by contract_id, grp)
    end as daily_reward_per_unit,
    reward_amount
    from (
        select *,
        COUNT(staked_amount) OVER (partition by contract_id ORDER BY staking_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
        from {{ ref('staking_rates_with_expected_rewards') }}
    ) as ranked
)

select staking_date, end_date, contract_id, asset_reward, asset_hold,
case
    when grp = 0 then 
        case 
            -- back-fill with zero to handle division by zero error
            when reward_amount = 0 or daily_reward_per_unit = 0 then 0 
            -- back-fill using the below calculation
            else reward_amount / daily_reward_per_unit
        end
    -- forward-fill with the last non-null value
    else Max(staked_amount) over (partition by contract_id, grp)
end as staked_amount,
daily_reward_per_unit,
reward_amount
from (
   select *,
        COUNT(staked_amount) OVER (partition by contract_id ORDER BY staking_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
        from staking_rate_drp_filled
) as ranked
