WITH raw_game_category AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_game_category') }}
)
SELECT
    `Game ID` AS game_id,
    `Game Category` AS game_category
FROM
    raw_game_category