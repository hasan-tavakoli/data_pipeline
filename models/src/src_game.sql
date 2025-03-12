WITH raw_game AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_game') }}
)
SELECT
    ID AS game_id,
    `Game Name` AS game_name,
    GameProviderId AS game_provider_id
FROM
    raw_game
