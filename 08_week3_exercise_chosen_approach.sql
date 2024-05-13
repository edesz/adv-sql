/* 1. perform the following operations on the events level data
  - use PARSE_JSON to convert (a) last event and (b) all events in the event_details column into the VARIANT datatype that holds JSON information whose attributes be accessed later
  - extract session date from the event_timestamp column
  - exclude columns that are not used in downstream CTEs of the overall query
    - user_id
    - event_id
2. get the most viewed recipe_id every day
  - (a) get all sessions in which the last event was viewing a recipe
  - (b) for all matching sessions from (a), use GROUP BY get the
    - (i) recipe_id
    - (ii) number of events in which a recipe was viewed per day
      - this is the number of daily views per recipe, which will be used to rank the recipes
  - (c) get the recipe_id with the most daily views
    - use a RANK() window function to assign ranks to the `recipe_id`s based on their number of daily views
    - use QUALIFY to get the recipe_ids for which RANK() OVER(...) = 1
3. Summarize each session by getting the following stats per session per date
  - (a) indicate if the session ended with a view of a recipe (i.e. if the last event in a session was to view a recipe)
  - (b) count the number of events in which a search performed during the session
    - this will be used to answer question 3
  - (c) calculate the session length in seconds using TIMESTAMPDIFF('second', ...)
4. From the output of step 3., get the following stats per date
  - (a) number of unique sessions from 3. (a)
  - (b) average session length from 3. (c)
  - (c) average number of searches performed before viewing recipe
    - for sessions in which a recipe was viewed from 3. (a), this is the average of the number of daily views during those sessions which was found in 3. (b)
5. Combine daily session summary from step 4. and most daily viewed recipe(s) from step 2.
  - (a) perform a LEFT JOIN using the session date column since both steps 2. and 4. are aggregated by date
  - (b) in the daily_session_summary CTE, session date was extracted as the date on which the session started. Some sessions can be spread out across two days.
    Such sessions should only have been counted on the first day of the session and not on the second day. Also, session length was recorded on the first date
    since session_date was recorded on the date on which the session started, so the session length should been excluded from the second date. In summary, such
    multi-day sessions should not have been (i) counted on both days and (ii) used to calculate session length on the second day. However, such logic has
    not been implemented earlier in the query. So, these rows must now be filtered out using WHERE session_length > 0.
  - (c) the output of step 2. contains some dates on which multiple recipes were tied as the most viewed recipe since they had the same number of views. For
    these rows, concatenate the recipe_ids into a comma-separated string on a single row using GROUP BY+LISTAGG. Based on SQL order of operations, the JOIN
    is executed before the GROUP BY. For this reason, a LEFT JOIN is needed so that no rows are lost if multiple recipes were tied for the most daily views.
  - (d) for readability, sort the result in chronological order using the session date column */

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
   (b) handle multi-day sessions using WHERE avg_session_length_seconds > 0
   (c) concatenate most-viewed recipes into comma-delimited string, using
   GROUP BY+LISTAGG
   (d) sort result by session date in chronological order for readability */
daily_report_summary_sessions_recipe AS (
    SELECT * EXCLUDE(most_viewed_recipe),
           /* clean the most_viewed_recipe ID(s) per day */
           REPLACE(most_viewed_recipe, '"', '') AS most_viewed_recipe
    FROM (
        SELECT ds.session_date,
               ds.num_sessions,
               ds.avg_session_length_seconds,
               ds.avg_num_searches_before_viewing_recipe,
               /* step 5. (c) concatenate ties beteween most-viewed recipes into
                  single row */
               LISTAGG(dv.recipe_id, ', ') AS most_viewed_recipe
        FROM daily_session_summary AS ds
        /* step 5. (a) combine session summary and most-viewed receipe each day */
        LEFT JOIN most_viewed_recipe_daily AS dv USING (session_date)
        /* step 5. (b) exclude sessions which have been counted on previous day and so
           have a session length of zero */
        WHERE avg_session_length_seconds > 0
        GROUP BY ALL
        /* step 5. (d) sort result in chronological order for readability */
        ORDER BY ds.session_date
    )
)
SELECT *
FROM daily_report_summary_sessions_recipe
