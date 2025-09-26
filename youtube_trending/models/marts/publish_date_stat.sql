SELECT
    publish_time,
    COUNT(DISTINCT video_id) AS total_videos,
    SUM(views) AS total_views,
    SUM(likes) AS total_likes,
    SUM(dislikes) AS total_dislikes,
    SUM(comment_count) AS total_comments,
    ROUND(AVG(views), 0) AS avg_views_per_video,
    ROUND(AVG(likes), 0) AS avg_likes_per_video,
    ROUND(AVG(dislikes), 0) AS avg_dislikes_per_video,
    ROUND(AVG(comment_count), 0) AS avg_comments_per_video
FROM {{ ref('stg_trending') }}
GROUP BY publish_time