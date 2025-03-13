{{ config(materialized='table')}}

{{ log_message('Starting execution of dim_player model.', level='info') }}
WITH ranked_players AS (
    SELECT
        player_id,
        player_gender AS gender,
        player_country AS country,
        latest_update,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY latest_update DESC) AS rn
    FROM
        {{ ref('src_player') }}
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


{{ log_message('Finished execution of dim_player model.', level='info') }}