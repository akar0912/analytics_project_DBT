# Analytics Engineering Assessment

Author = Alisha Kar

- [Analytics Engineering Assessment](#analytics-engineering-assessment)
  - [Introduction](#introduction)
  - [Installation](#installation)
  - [Setup](#setup)
    - [Seed file import](#seed-file-import)
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

## Approach