{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}
{{ log_message('Starting execution of fact_bet model.', level='info') }}
WITH game_transaction_currency AS (
    SELECT
        t.transaction_date,
        t.player_id,
        t.game_id,
        CASE WHEN t.transaction_type = 'WAGER' THEN  COALESCE((t.real_amount * c.base_rate_euro),0) ELSE 0 END AS cash_turnover,
        CASE WHEN t.transaction_type = 'WAGER' THEN  COALESCE((t.bonus_amount * c.base_rate_euro),0) ELSE 0 END AS bonus_turnover,
        CASE WHEN t.transaction_type = 'RESULT' THEN  COALESCE((t.real_amount * c.base_rate_euro),0) ELSE 0 END AS cash_winnings,
        CASE WHEN t.transaction_type = 'RESULT' THEN  COALESCE((t.bonus_amount * c.base_rate_euro),0) ELSE 0 END AS bonus_winnings
    FROM {{ ref('src_currency_exchange') }} AS c
    JOIN {{ ref('src_game_transaction') }} AS t
        ON c.exchange_currency = t.transaction_currency
        AND t.transaction_date = c.exchange_date
),
player_country_history AS (
    SELECT
        player_id,
        player_country,
        latest_update,
        LEAD(latest_update) OVER (PARTITION BY player_id ORDER BY latest_update) AS next_update
    FROM {{ ref('src_player') }}
),
matched_player_country AS (
    SELECT
        t.transaction_date,
        t.player_id,
        t.game_id,
        p.player_country,
        cash_turnover,
        bonus_turnover,
        cash_winnings,
        bonus_winnings
    FROM game_transaction_currency AS t
    LEFT JOIN player_country_history AS p
        ON t.player_id = p.player_id
        AND t.transaction_date >= p.latest_update
        AND (p.next_update IS NULL OR t.transaction_date < p.next_update)
),
final AS (
    SELECT
        transaction_date,
        player_id,
        player_country,
        game_id,
        SUM(cash_turnover) AS cash_turnover,
        SUM(bonus_turnover) AS bonus_turnover,
        SUM(cash_winnings) AS cash_winnings,
        SUM(bonus_winnings) AS bonus_winnings,
        SUM(cash_turnover + bonus_turnover) AS turnover,
        SUM(cash_winnings + bonus_winnings) AS winnings,
        SUM(cash_turnover - cash_winnings) AS cash_result,
        SUM(bonus_turnover - bonus_winnings) AS bonus_result,
        SUM((cash_turnover + bonus_turnover) - (cash_winnings + bonus_winnings)) AS gross_result
    FROM matched_player_country
    GROUP BY transaction_date, player_id, player_country, game_id
)
SELECT
   transaction_date as date, 
    player_id,
    player_country as country,
    game_id,
    cash_turnover as `Cash turnover`,
    bonus_turnover as `Bonus turnover`,
    cash_winnings as `Cash winnings`,
    bonus_winnings as `Bonus winnings`,
    turnover as Turnover,
    winnings as Winnings,
    cash_result as `Cash result`,
    bonus_result as `Bonus result`,
    gross_result as `Gross result`
FROM final
{% if is_incremental() %}
    {% if var("start_date", False) and var("end_date", False) %}
        {{ log_message('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', level='info') }}
        WHERE transaction_date >= '{{ var("start_date") }}'
        AND transaction_date < '{{ var("end_date") }}'
    {% else %}
        {{ log_message('Loading ' ~ this ~ ' incrementally (all missing dates)', level='info')}}
        WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
{% endif %}

{{ log_message('Finished execution of fact_bet model.', level='info') }}