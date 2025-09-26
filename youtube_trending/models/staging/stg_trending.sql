WITH raw as (
    SELECT * FROM {{ source('raw', 'trending_ca_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_de_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_fr_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_gb_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_in_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_jp_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_kr_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_mx_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_ru_raw')}}
    UNION ALL
    SELECT * FROM {{ source('raw', 'trending_us_raw')}}
)

SELECT
    video_id,
    title,
    channel_title,
    CAST(publish_time AS timestamp) AS publish_time,
    TO_DATE(REPLACE(trending_date, '.', '-'), 'YY-DD-MM') AS trending_date,
    category_id,
    category_name,
    CAST(views AS int) AS views,
    CAST(likes AS int) AS likes,
    CAST(dislikes AS int) AS dislikes,
    CAST(comment_count AS int) AS comment_count,
    country,
    CASE UPPER(country)
        WHEN 'CA' THEN 'Canada'
        WHEN 'DE' THEN 'Germany'
        WHEN 'FR' THEN 'France'
        WHEN 'GB' THEN 'United Kingdom'
        WHEN 'IN' THEN 'India'
        WHEN 'JP' THEN 'Japan'
        WHEN 'KR' THEN 'South Korea'
        WHEN 'MX' THEN 'Mexico'
        WHEN 'RU' THEN 'Russia'
        WHEN 'US' THEN 'United States'
        ELSE 'Unknown'
    END AS country_name
FROM raw