## Summary from the Snowflake Query Profile

1. The WithReference operator appears twice in the query execution plan. This is expected since the first CTE is referenced by the most_viewed_recipe_daily and session_summary CTEs.
2. The Sort operator is expensive and only appears once. This is expected since ORDER BY was only applied on the final result which was aggregated daily.
3. A Filter is applied after a TableScan once. In this scenario, the TableScan performs the filtering so the Filter step has no impact. Also, a WHERE clause is not used in the main CTE. For this reason, the input and output row counts of the filter are the same after the Filter operator.
4. From a high-level view of the query execution plan on slide 2
   - the two types of metrics to be calculated (three session-based metrics and one metric for recipe views) are separated. This is expected since this separation was necessary because a window function function was used to find the most viewed daily recipe
     - session_summary and daily_session_summary calculate the three daily session-based metrics. They do not need to rank recipes based on views, so they do not need to apply the RANK() window function
     - most_viewed_recipe_daily is the only CTE that is concerned about ranking recipes based on daily views so it uses the window function
5. From the zoomed verson of the execution plan on slide 3, the two separate daily aggregations are LEFT JOINed. The number of rows after the JOIN is larger than before the JOIN since multiple recipes are tied for most views on some days. After concatenating ties into the same row using LISTAGG, the number of rows matches those in the CTE on the LHS of the LEFT JOIN and there is one row per date.
6. The query profile verifies findings from data exploration
   - in most_viewed_recipe_daily, 55 rows are produced which matches 55 recipe views found in the website_activity table
   - in session_summary, 178 rows are produced which the total unique sessions found in the table
7. In the first CTE, the WindowFunction node is the most expensive node of the entire query. This is expected since
   - two window functions were used and they were both applied to all events in the table. It was necessary to read all events from the table in order to calculate session length for the second report metric (average session length)
   - the window function contains a nested PARSE_JSON, which is also applied to each row since we need to extract the attributes of the event_details column for all events in the table so they could be accessed in CTEs
8. There are a lot of processing operations being performed such as aggregations, window functions and JOINs. So, from the Profile Overview, it is not surprising that processing time dominates the overall time required to execute the query.
