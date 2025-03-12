WITH raw_player AS (
    SELECT
        *
    FROM
        {{ source('bronze', 'raw_player') }}
)
SELECT
    playerID AS player_id,
    country AS player_country,
    DATE_ADD('1899-12-30', INTERVAL BirthDate DAY) AS birth_date, 
    gender AS player_gender,
    playerState AS player_state,
    CASE WHEN VIP = 1 THEN TRUE ELSE FALSE END AS is_vip,
    CASE WHEN KYC = 1 THEN TRUE ELSE FALSE END AS is_kyc,
    CASE WHEN wantsNewsletter = 1 THEN TRUE ELSE FALSE END AS wants_newsletter,
    latestUpdate AS latest_update
FROM
    raw_player