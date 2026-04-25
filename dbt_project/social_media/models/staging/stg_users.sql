/*
  stg_users.sql — Silver layer: clean user profiles

  What this model does:
  1. Standardizes text fields (trim, lowercase email)
  2. Casts types explicitly
  3. Adds derived columns (account_age_days, is_influencer)
  4. Filters out truly invalid rows (null user_id)
  5. Adds dbt metadata columns

  In dbt, {{ source('bronze', 'users') }} is how you reference
  a source defined in sources.yml — dbt tracks this as lineage.
*/

WITH

raw AS (
    SELECT * FROM {{ source('bronze', 'users') }}
),

cleaned AS (
    SELECT
        -- identifiers
        user_id,
        TRIM(username)                          AS username,
        TRIM(display_name)                      AS display_name,
        LOWER(TRIM(email))                      AS email,

        -- profile
        TRIM(bio)                               AS bio,
        TRIM(location)                          AS location,
        website,
        profile_image_url,

        -- account metadata
        LOWER(account_type)                     AS account_type,
        LOWER(account_status)                   AS account_status,
        CAST(is_verified AS BOOLEAN)            AS is_verified,
        LOWER(language)                         AS language,

        -- engagement stats
        CAST(follower_count AS INTEGER)         AS follower_count,
        CAST(following_count AS INTEGER)        AS following_count,
        CAST(post_count AS INTEGER)             AS post_count,

        -- derived: follower tiers (useful for segmentation)
        CASE
            WHEN follower_count >= 1000000 THEN 'mega'
            WHEN follower_count >= 100000  THEN 'macro'
            WHEN follower_count >= 10000   THEN 'micro'
            WHEN follower_count >= 1000    THEN 'nano'
            ELSE 'regular'
        END                                     AS influencer_tier,

        -- derived: is this account an influencer?
        follower_count >= 10000                 AS is_influencer,

        -- derived: follower to following ratio
        CASE
            WHEN following_count > 0
            THEN ROUND(
                CAST(follower_count AS DOUBLE) / following_count, 2
            )
            ELSE NULL
        END                                     AS follower_ratio,

        -- timestamps
        CAST(joined_at AS TIMESTAMP)            AS joined_at,
        CAST(last_active_at AS TIMESTAMP)       AS last_active_at,

        -- derived: how many days since they joined
        DATEDIFF(
            'day',
            CAST(joined_at AS TIMESTAMP),
            CURRENT_TIMESTAMP
        )                                       AS account_age_days,

        -- pipeline metadata
        CAST(ingested_at AS TIMESTAMP)          AS ingested_at,
        source_system,
        CURRENT_TIMESTAMP                       AS transformed_at

    FROM raw
    WHERE user_id IS NOT NULL
      AND username IS NOT NULL
)

SELECT * FROM cleaned