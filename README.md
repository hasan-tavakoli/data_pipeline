

## Project Description

This project implements a data processing pipeline to transform raw data from various sources (GameTransaction, Player, Game, GameCategory, GameProvider, and CurrencyExchange) into a destination data model consisting of fact and dimension tables (fact_bet, dim_game, and dim_player). 
**This project leverages Google BigQuery as the data warehouse platform and dbt (data build tool) for building and managing the data processing pipelines.**

## Data Flow Diagram

![Model Design](https://github.com/hasan-tavakoli/data_pipeline/blob/main/image/image.png?raw=true)

## Layer Description

**This project utilizes Google BigQuery as the data warehouse and dbt as the primary tool for data transformation and modeling.**

### Bronze Layer

The Bronze layer contains the raw data ingested from the following sources using dbt sources. This layer represents the raw, unprocessed data.

* **GameTransaction.csv:** Defined as `source('bronze', 'raw_game_transaction')`. Raw transaction data for bets, including transaction type (WAGER, RESULT), amounts (cash and bonus), currency, and timestamps.
* **Player.csv:** Defined as `source('bronze', 'raw_player')`. Raw player data, including player ID, gender, country, and latest update timestamp (`latestUpdate`) for tracking player country changes.
* **Game.csv:** Defined as `source('bronze', 'raw_game')`. Raw data describing games, including game ID and name.
* **GameCategory.csv:** Defined as `source('bronze', 'raw_game_category')`. Raw data linking game IDs to game categories.
* **GameProvider.csv:** Defined as `source('bronze', 'raw_game_provider')`. Raw data describing game providers, including provider ID and name.
* **CurrencyExchange.csv:** Defined as `source('bronze', 'raw_currency_exchange')`. Raw data for currency exchange rates, including date, currency code, and base exchange rate to EUR.

### Silver Layer

The Silver layer contains transformed and curated data, derived from the Bronze layer. This layer includes staging models and the core dimension and fact tables.  Data in this layer has been cleaned, transformed, and prepared for use in the Gold layer (or for direct querying).

* **src\_game\_transaction:** Staging model for GameTransaction data, performing initial data type conversions and cleaning.
* **src\_currency\_exchange:** Staging model for CurrencyExchange data, preparing exchange rate information.
* **src\_player:** Staging model for Player data, cleaning and transforming player information, particularly for date conversions.
* **src\_game:** Staging model for Game data, selecting and renaming relevant columns.
* **src\_game\_category:** Staging model for GameCategory data, selecting and renaming columns.
* **src\_game\_provider:** Staging model for GameProvider data, selecting and renaming relevant columns.
* **dim\_game:** Dimension table containing game information (game\_id, game\_name, game\_category, Provider\_name). This table is derived from the staging models `src_game`, `src_game_category`, and `src_game_provider`.
* **dim\_player:** Dimension table containing player information, with only the latest update for each player (player\_id, gender, country, latestUpdate). This table is derived from the staging model `src_player`.
* **fact\_bet:** Fact table containing bet transaction data aggregated by date, player\_id, country, and game\_id, including cash turnover, bonus turnover, cash winnings, bonus winnings, and calculated metrics (Turnover, Winnings, Cash result, Bonus result, Gross result). This table is derived from the staging models `src_game_transaction`, `src_currency_exchange`, and `src_player`.

#### Data Cleaning and Transformation

##### src\_game\_transaction

This model performs the following cleaning and transformation steps on the raw game transaction data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `Date` to `transaction_date`, `txCurrency` to `transaction_currency`, `gameID` to `game_id`, `txType` to `transaction_type`, `BetId` to `bet_id`, `PlayerId` to `player_id`).
* **Data type conversion:**
    * Converts `realAmount` and `bonusAmount` to `FLOAT64`.
    * **Handling commas:** Replaces commas (`,`) with periods (`.`) in `realAmount` and `bonusAmount` before casting to `FLOAT64` to handle decimal formatting differences. This observation was made during initial data exploration, where it was noticed that some amount fields used commas as decimal separators.

##### src\_game

This model performs the following cleaning and transformation steps on the raw game data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `ID` to `game_id`, `Game Name` to `game_name`).

##### src\_player

This model performs the following cleaning and transformation steps on the raw player data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `playerID` to `player_id`, `country` to `player_country`).
* **Data type conversion:**
    * Converts `BirthDate` to a `birth_date` using `DATE_ADD` and `INTERVAL DAY`. This assumes `BirthDate` is an integer representing the number of days since a base date (likely 1899-12-30, a common convention in some systems).
* **Boolean conversion:**
    * Converts binary fields (`VIP`, `KYC`, `wantsNewsletter`) to Boolean values (`TRUE` or `FALSE`) using `CASE WHEN`.

##### src\_game\_provider

This model performs the following cleaning and transformation steps on the raw game provider data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `ID` to `game_provider_id`, `Game Provider Name` to `game_provider_name`).

##### src\_game\_category

This model performs the following cleaning and transformation steps on the raw game category data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `Game ID` to `game_id`, `Game Category` to `game_category`).

##### src\_currency\_exchange

This model performs the following cleaning and transformation steps on the raw currency exchange data using **BigQuery SQL**:

* **Renaming columns:** Renames columns for consistency and clarity (e.g., `date` to `exchange_date`, `currency` to `exchange_currency`, `baseRateEuro` to `base_rate_euro`).

### Gold Layer

The Gold layer (not currently implemented in this project) would typically contain aggregated and summarized data optimized for reporting and analysis. This layer would be built upon the Silver layer, providing pre-aggregated or highly refined data.

## Model Descriptions

### fact\_bet (Materialization: Incremental Table)

The `fact_bet` model aggregates bet transaction data from the `GameTransaction` table, converting amounts to Euro using exchange rates from the `CurrencyExchange` table. It also joins with the `Player` table to track player country at the time of the transaction. This model is materialized as an incremental table in BigQuery, improving performance for large datasets.

* **Purpose:**
    * To create a fact table that stores aggregated bet transaction data.
    * To convert transaction amounts to Euro using currency exchange rates.
    * To track player country at the time of each transaction.
    * To calculate key metrics such as turnover, winnings, and results (cash, bonus, and gross).
* **Logic:**
    * Uses a series of Common Table Expressions (CTEs):
        * `game_transaction_currency`:
            * Joins `src_currency_exchange` and `src_game_transaction` on `exchange_currency` and `transaction_date`.
            * Calculates `cash_turnover`, `bonus_turnover`, `cash_winnings`, and `bonus_winnings` by multiplying the respective amounts with the `base_rate_euro` from the exchange rates.
            * Uses `COALESCE` to handle potential null values in amounts.
            * Filters transactions based on `transaction_type` ('WAGER' or 'RESULT') for turnover and winnings calculations.
        * `player_country_history`:
            * Selects `player_id`, `player_country`, `latest_update` from the `src_player` model.
            * Uses the `LEAD()` window function to get the `next_update` date for each player country record, which helps define the validity period of a player's country.
        * `matched_player_country`:
            * Joins `game_transaction_currency` and `player_country_history` on `player_id`.
            * Filters player country records based on the transaction date to get the correct country at the time of the transaction.
        * `final`:
            * Aggregates data from `matched_player_country` by `transaction_date`, `player_id`, `player_country`, and `game_id`.
            * Calculates `cash_turnover`, `bonus_turnover`, `cash_winnings`, `bonus_winnings` using `SUM()`.
            * Calculates `turnover`, `winnings`, `cash_result`, `bonus_result`, and `gross_result` using the aggregated turnover and winnings amounts.
    * The final SELECT statement:
        * Selects all the calculated fields from the CTE `final`.
        * Renames some columns for consistency.
        * Implements incremental logic:
            * Uses `is_incremental()` macro to determine if the model is running incrementally.
            * Uses `var("start_date", False)` and `var("end_date", False)` to filter data based on a date range, if provided.
            * If no date range is provided, it filters data to include only the transaction dates greater than the maximum transaction date in the existing table.
* **Materialization:**
    * `incremental`: The table is built incrementally, processing only new or updated data in each dbt run.
    * `on_schema_change='fail'`: If there are schema changes, dbt will throw an error.
* **Calculated Metrics:**
    * **Cash turnover:** Calculated by summing the `realAmount` and `bonusAmount` for "WAGER" transactions, converted to Euro.
    * **Bonus turnover:** Calculated by summing the `bonusAmount` for "WAGER" transactions, converted to Euro.
    * **Cash winnings:** Calculated by summing the `realAmount` for "RESULT" transactions, converted to Euro.
    * **Bonus winnings:** Calculated by summing the `bonusAmount` for "RESULT" transactions, converted to Euro.
    * **Turnover:** Sum of `Cash turnover` and `Bonus turnover`.
    * **Winnings:** Sum of `Cash winnings` and `Bonus winnings`.
    * **Cash result:** Calculated as `Cash turnover` - `Cash winnings`.
    * **Bonus result:** Calculated as `Bonus turnover` - `Bonus winnings`.
    * **Gross result:** Calculated as `Turnover` - `Winnings`.
    * **Player Country:** Determined by joining with `dim_player` on `player_id` and filtering for the `latestUpdate` that is less than or equal to the transaction date.

### dim\_game (Materialization: Table)

The `dim_game` model provides a dimension table for games, containing information such as game ID, name, category, and provider. This model is materialized as a table in BigQuery.

* **Purpose:**
    * To create a clean and consistent table containing game-related information for analysis and reporting.
    * To join information from different source tables (`src_game`, `src_game_category`, `src_game_provider`) to create a comprehensive view of game data.
* **Logic:**
    * Selects `game_id` and `game_name` from the `src_game` model.
    * Joins with the `src_game_category` model on `game_id` to get the `game_category`.
    * Joins with the `src_game_provider` model on `game_provider_id` to get the `Provider_name`.
    * Renames columns for clarity.
* **Materialization:**
    * `table`: The table is created by a full refresh on each dbt run.
* **Logging:**
    * Logs the start and end of the model execution using a custom dbt macro (`log_message`).

### dim\_player (Materialization: Table)

The `dim_player` model provides a dimension table for players, storing only the latest country information for each player based on the `latestUpdate` field in the raw `Player` data. This model is materialized as a table in BigQuery.

* **Purpose:**
    * To create a dimension table of players.
    * To track player country changes over time and store only the most up-to-date country for each player.
* **Logic:**
    * Uses a Common Table Expression (CTE) `ranked_players` to:
        * Select `player_id`, `gender`, `country`, and `latest_update` from the `src_player` model.
        * Use the `ROW_NUMBER()` window function partitioned by `player_id` and ordered by `latest_update` in descending order to assign a rank (`rn`) to each player country record. This ranking helps to identify the latest country record for each player.
    * The final SELECT statement filters the `ranked_players` CTE to keep only the rows where `rn` is 1, which corresponds to the latest country for each player.
* **Materialization:**
    * `table`: The table is created by a full refresh on each dbt run.
* **Latest Country:** Determined by selecting the row with the maximum `latestUpdate` for each `player_id`.
* **Logging:**
    * Logs the start and end of the model execution using a custom dbt macro (`log_message`).

### Tracking Player Country Changes

Player country changes are tracked in the `dim_player` model. The `latestUpdate` field in the raw `Player` data is used to determine the most recent country for each player. The `fact_bet` model joins with the `dim_player` data to capture the player's country at the time of each transaction.

## How to Run

  **Prerequisites:**
    * Install dbt: `pip install dbt-bigquery` (or relevant adapter)  (e.g., `pip install dbt-bigquery`)

## Tests

Data quality and model logic are validated using dbt tests. Tests are defined in `.yml` files associated with models and sources.

### Test Types

### Example Tests

* **fact\_bet:**
    * `not_null` tests on `date`, `player_id`, `game_id`.
    * `positive_value` tests on `Cash turnover`, `Bonus turnover`, `Cash winnings`, `Bonus winnings`.
    * `relationships` tests to validate foreign key relationships with `dim_game` and `dim_player`.
* **dim\_player:**
    * `not_null` tests on `player_id`, `latestUpdate`.
    * `unique` test on `player_id`.
* **dim_game:**
    * `not_null` tests on `game_id`.
    * `unique` test on `game_id`.
### Running Tests

* Run all tests: `dbt test`

## Logging

Logging is implemented using a custom dbt macro (`log_message`).

### Logging Levels

* `info`: General information about model execution.
* `warn`: Warnings indicating potential issues.
* `error`: Errors encountered during model execution.
