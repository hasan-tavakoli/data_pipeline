{{ config(materialized='table')}}

WITH ranked_players AS (
    SELECT
        playerID AS player_id,
        gender AS gender,
        country AS country,
        latestUpdate AS latest_update,
        ROW_NUMBER() OVER (PARTITION BY playerID ORDER BY latestUpdate DESC) AS rn
    FROM
        {{ source('bronze', 'raw_player') }}
)
SELECT
    player_id,
    gender,
    country,
    latest_update
FROM
    ranked_players
WHERE
    rn = 1