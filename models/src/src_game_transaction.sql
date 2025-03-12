WITH raw_game_transaction AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_game_transaction') }}
)
SELECT
    Date AS transaction_date,
    CAST(REPLACE(realAmount, ',', '.') AS FLOAT64) AS real_amount,  
    CAST(REPLACE(bonusAmount, ',', '.') AS FLOAT64) AS bonus_amount,
    channelUID AS channel_uid,
    txCurrency AS transaction_currency,
    gameID AS game_id,
    txType AS transaction_type,
    BetId AS bet_id,
    PlayerId AS player_id
FROM
    raw_game_transaction