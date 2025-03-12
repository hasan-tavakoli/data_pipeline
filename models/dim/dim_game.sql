{{ config(
    materialized='table'
) }}

SELECT
    sg.game_id,
    sg.game_name,
    sgc.game_category,
    sgp.game_provider_name AS Provider_name
FROM
    {{ ref('src_game') }} AS sg
LEFT JOIN
    {{ ref('src_game_category') }} AS sgc ON sg.game_id = sgc.game_id
LEFT JOIN
    {{ ref('src_game_provider') }} AS sgp ON sg.game_provider_id = sgp.game_provider_id