WITH raw_currency_exchange AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_currency_exchange') }}
)
SELECT
    date AS exchange_date,
    currency AS exchange_currency,
    baseRateEuro AS base_rate_euro
FROM
    raw_currency_exchange