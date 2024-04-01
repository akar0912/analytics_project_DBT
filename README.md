# Analytics Engineering Assessment

Author = Alisha Kar

- [Analytics Engineering Assessment](#analytics-engineering-assessment)
  - [Introduction](#introduction)
  - [Installation](#installation)
  - [Setup](#setup)
    - [Seed file import](#seed-file-import)
  - [Findings from data](#findings-from-data)
  - [Approach](#approach)
## Introduction
This project contains the solution for the [assessement](./assessment_README.md).
I have used the DBT itself as the framework to frame the model for the asked assessement as this provides me a good opportunity to learn about DBT for the first time to be used for data transformation.

## Installation
Below are the details for various setups used while implementing the solutions
1. Machine = Macbook Pro (Apple M2)
2. Postgres 14.11
    ```
    brew install postgresql
    brew services start postgresql
    ```
3. Python virutal env = 
4. DBT  
    ```
    python -m pip install dbt-core
    python -m pip install dbt-postgres
    ```
    ```
    ❯ dbt --version
    Core:
    - installed: 1.7.10
    - latest:    1.7.11 - Update available!

    Your version of dbt-core is out of date!
    You can find instructions for upgrading here:
    https://docs.getdbt.com/docs/installation

    Plugins:
    - postgres: 1.7.10 - Update available!

    At least one plugin is out of date or incompatible with dbt-core.
    You can find instructions for upgrading here:
    https://docs.getdbt.com/docs/installation
    ```

## Setup

### Seed file import
There were two files `staking_Rates.csv` and `staking_Rewards.csv` that need to be imported for the required modeling. These files were imported into the database using the `dbt seed` command.  
However, the following adjustments were made to make the import successfull
1. column name modification
Since, camelCase is not friendly with the postgres, staking_Rewards was not getting imported as it had a column called `rewardXx`. This column was changed to `reward` in the CSV file
2. type cast
Since, `source_ts_ms` is a huge integer value, `dbt seed` was throwing error saying `out of range integer`. Hence, this was typecased to `BIGINT` in the [properties.yml](./seeds/properties.yml).

## Findings from data
`staking_Rates.csv` contains the information regarding various staking entries for a particular contract (`assetHold` and `assetReward`). Each entry correspond to asset being staked, and relevant asset being awarded.  
`staking_Rewards.csv` contains the reward information starting from `2023-07-01` for each of the contract specified in `staking_Rewards.csv` 
There is a clear pattern which can be found in the `amount` column for `staking_Rewards.csv` and (`stakedAmount` and `dailyRewardPerUnit`) for `staking_Rates.csv`  
`amount = stakedAmount  * dailyRewardPerUnit`
If we look at the following images for the contract 130, for staking happened on `2023-07-03`, reward amount is shown in the `staking_Rewards.csv` table on `2023-07-04` as `35757973 * 0.00023287671232876712 = 8327.199192`
* Staking Rates
![staking_Rates](<images/Screenshot 2024-04-01 at 8.46.11 PM.png>)
* Staking Rewards
![Staking_Rewards](<images/Screenshot 2024-04-01 at 8.49.44 PM.png>)

Also, the amount in the `staking_Rewards.csv` remains same for the next consecutive days until there is another staking happening in the future. Hence, I assume that if staking happened on T day and another staking happened on T+3 day, then staking amount will remain same for T+1, T+2 as reward remain same for consecutive days until next staking.

* End for expected reward in `staking_Rewards.csv`  
Also, another observation is that, if end_date of contract is nil (for instance, for the contract 130) in the `staking_Rates.csv`, then reward amount is shown till `2023-11-09`. 
However, if end_date is mentioned (for instance, for contract 920), then reward amount is shown only till that end_date

## Approach
Models are built using the DRY principles so that they can be used again for future model transformations. Following is the dependency graph for the written models generated using `dbt docs serve`. 
![alt text](<images/Screenshot 2024-04-01 at 10.28.11 PM.png>)
Note that, two end models are generated at last based on two different approach with the assumptions mentioned below. 

Details for the various models are described below.

1. [calender.sql](models/calender.sql)  
Since, we need to show the staked amount and expected reward from 2023-07-01 till current date, I have generated a calender series in this model.

2. [staking_rates_cleaned.sql](models/staking_rates_cleaned.sql)  
This model cleanup the staking_Rewards table by adding date, time for the staking event and taking only non-deleted entries.

3. [staking_rates_last_per_day.sql](models/staking_rates_last_per_day.sql)
Since, there are multiple staking entries for the same day, the amount shown in the `staking_Rewards` table is only considering the most recent staking entry for that day
If we look in the picture below, there are multiple staking entries on `2023-07-01` for contract 639.
![text](<images/Screenshot 2024-04-01 at 9.12.25 PM.png>)
However, amount shown as reward amount contains `10.8533195511` in `2023-07-02` which is the multiplication for `drp` and `stakedAmount` for the most-recent entry in `2023-07-01`
![alt text](<images/Screenshot 2024-04-01 at 9.17.47 PM.png>)
Hence, this model takes the most-recent staking entry for a particular day as this is being used to calculate the expected reward amount.

4. [staking_rates_consecutive_day.sql](models/staking_rates_consecutive_day.sql)
This model does two things.
   1. create entry for staking_rates for all the asset contract starting from `2023-07-01` till current date by doing cross join with the [calender.sql](models/calender.sql)
   2. This also forward fill the null entries for the days with the last entry in the staking_Rates table as staking amount is assumed to be same for the next day until new staking happens as discussed in [Findings](#findings-from-data)  
   Note that, forward filling for the daily_reward_per_unit and staked_amount is done as part of the 6.

5. [staking_rates_with_expected_rewards.sql](models/staking_rates_with_expected_rewards.sql)
This model merge the expected reward with the above mentioned model in 4. to get both the staked amount and expected reward for each asset contract for the consecutive days.   
Since, expected rewards are present in `staking_Rewards` only till `2023-11-09` or before dependening on the end_date. This forward-fill the reward amount till current date as follows
    * if end-date is not present -> forward-fill as the last value.
    * if end-date is present -> forward-fill to zero.

6. back-filling and forward-filling of daily_reward_per_unit and staked_amount
If we look at the example below for the contract 130, we can see that  reward amount is shown for the `2023-07-01` and `2023-07-02`, However, staking event happens only on `2023-07-03`. We can back-fill the staking amount for the `2023-07-01` and `2023-07-02` in one of the following ways  
   6.1 Assume them to be zero
   This avoid the equation between drp, stakedAmount and reward by assuming the values to be zero. This is done as part of [staked_amount_drp_reset.sql](models/staked_amount_drp_reset.sql)  
   6.2 calculate staked_amount using reward_amount
   This approach assumes the drp to be the same for those days, and using the equation of drp, stakedAmount and reward, it back-fills the null entries for staked amount using the below formula
   staked_amount = reward / drp
   This is done as part of [staked_amount_drp_fill.sql](models/staked_amount_drp_fill.sql) 
7. All of the above models are created for each contract between `assetHold` and `assetReward` for each consecutive day. This model group the staked amount, and expected reward using date and `assetHold` to show the total_staked_amount and total_expected_reward for each asset for all consecutive days.  
   Since, we have back-fill the staked_amount using the two different assumptions as mentioned in the 6., the final result is achieved in two ways by taking 6.1 and 6.2  

    7.1 [final_with_staked_amount_drp_reset.sql](models/final_with_staked_amount_drp_reset.sql)  
    ![alt text](<images/Screenshot 2024-04-01 at 9.50.00 PM.png>)
    7.2 [final_with_staked_amount_drp_fill.sql](models/final_with_staked_amount_drp_fill.sql)
    ![alt text](<images/Screenshot 2024-04-01 at 9.49.20 PM.png>)
