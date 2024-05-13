/* step 1. get JSON of the last event and all events performed per session */
WITH session_last_event_all_events_json AS (
    SELECT session_id,
           event_timestamp,
           /* take the start date of the session as the session date */
           MIN(TO_DATE(event_timestamp)) OVER(PARTITION BY session_id) AS session_date,
           /* get JSON of last event */
           LAST_VALUE(PARSE_JSON(event_details)) OVER(
               PARTITION BY session_id
               ORDER BY event_timestamp
           ) AS last_event_json,
           /* get JSON of all events */
           PARSE_JSON(event_details) AS event_json
    FROM events.website_activity
),
/* step 2. get the most viewed recipe each day
   (a) get sessions in which the last event was viewing a recipe
   (b) for the (last) event in the sessions from 2. (a), get (i) recipe_id and
   (ii) get the number of times the recipe was viewed daily
   (c) get the recipe_id with the most daily views using QUALIFY */
most_viewed_recipe_daily AS (
    SELECT session_date,
           /* step 2. (b) (i) get recipe_id */
           last_event_json:recipe_id AS recipe_id
    FROM session_last_event_all_events_json
    /* step 2. (a) get sessions where the last event was to view a recipe */
    WHERE last_event_json:event = 'view_recipe'
    /* step 2. (b) get events in which the recipe was viewed */
    AND event_json:event = 'view_recipe'
    GROUP BY ALL
    /* step 2. (c) get the recipe_id with the most daily views */
    QUALIFY RANK() OVER(PARTITION BY session_date ORDER BY COUNT(*) DESC) = 1
),
/* step 3. summarize each session by getting the following stats
   (a) indicate if it ended with a user viewing a recpie
   (b) count number of searches performed during the session
   (c) get session length */
session_summary AS (
    SELECT session_date,
           session_id,
           /* step 3. (a) indicate if session ended with viewing recipe */
           (
               CASE
                   WHEN last_event_json:event = 'view_recipe'
                   THEN TRUE
                   ELSE FALSE
               END
           ) AS has_viewed_recipe,
           /* step 3. (b) count number of searches performed per session */
           SUM(
               CASE
                   WHEN event_json:event = 'search'
                   THEN 1
                   ELSE 0
               END
           ) AS num_searches,
           /* step 3. (c) get session length */
           TIMESTAMPDIFF(
               'second', MIN(event_timestamp), MAX(event_timestamp)
           ) AS session_length
    FROM session_last_event_all_events_json
    GROUP BY ALL
),
/* step 4. aggregate stats per day to get the following
   (a) number of sessions
   (b) average session length in seconds
   (c) average number of searches before viewing a recipe */
daily_session_summary AS (
    SELECT session_date,
           /* step 4. (a) number of daily sessions */
           COUNT(session_id) AS num_sessions,
           /* step 4. (b) average daily session length */
           AVG(session_length) AS avg_session_length_seconds,
           /* step 4. (c) average number of searches before viewing recipe */
           AVG(
               CASE
                   WHEN has_viewed_recipe = TRUE
                   THEN num_searches
                   ELSE NULL
               END
           ) AS avg_num_searches_before_viewing_recipe
    FROM session_summary
    GROUP BY ALL
),
/* step 5. combine daily session summary and most daily viewed recipe(s)
   (a) perform LEFT JOIN between recipe(s) with the most daily views and
   daily session summary
   (b) concatenate most-viewed recipes into comma-delimited string, using
   GROUP BY+LISTAGG
   (c) sort result by session date in chronological order for readability */
daily_report_summary_sessions_recipe AS (
    SELECT * EXCLUDE(most_viewed_recipe),
           /* clean the most_viewed_recipe ID(s) per day */
           REPLACE(most_viewed_recipe, '"', '') AS most_viewed_recipe
    FROM (
        SELECT ds.session_date,
               ds.num_sessions,
               ds.avg_session_length_seconds,
               ds.avg_num_searches_before_viewing_recipe,
               /* step 5. (b) concatenate ties beteween most-viewed recipes into
                  single row */
               LISTAGG(dv.recipe_id, ', ') AS most_viewed_recipe
        FROM daily_session_summary AS ds
        /* step 5. (a) combine session summary and most-viewed receipe each day */
        LEFT JOIN most_viewed_recipe_daily AS dv USING (session_date)
        GROUP BY ALL
        /* step 5. (c) sort result in chronological order for readability */
        ORDER BY ds.session_date
    )
)
SELECT *
FROM daily_report_summary_sessions_recipe
