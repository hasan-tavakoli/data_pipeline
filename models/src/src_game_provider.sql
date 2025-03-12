WITH raw_game_provider AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_game_provider') }}
)
SELECT
    ID AS game_provider_id,
    `Game Provider Name` AS game_provider_name
FROM
    raw_game_provider