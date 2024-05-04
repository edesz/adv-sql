/* The refactored query is written by performing the following changes
   1. Extract the sub-query to count customer food preferences into its own CTE named customer_food_preferences
   2. Extract the Chicago store's GPS co-ordinates into its own CTE named chicago_store_gps_coordinates
   3. Extract the Gary, Indiana store's GPS co-ordinates into its own CTE named chicago_store_gps_coordinates
   4. Get impacted customers (customer ID and name, location, GPS co-ordinates and food preferences) in its own CTE named customers_impacted. The first JOIN gives the customer ID, name and location and this must be an INNER JOIN since only customers whose city/state is present in the database are eligible to place a purchase. The second JOIN is the LEFT JOIN, which gives the (eligible) customer city's GPS co-ordinates. The third JOIN gives each customer's food preferences and this must be an INNER JOIN to only get customers who have food preferences. All the fields needed to filter eligible customers by their location, using the WHERE clause, are available after these three JOINs have been performed. So, the WHERE clause can also be included in this CTE.
   5. Append the distance between each customer and each available store (Chicago, IL and Gary, IN) in its own CTE named customer_store_distances. Use the two CROSS JOINs to create customer-store combinations and calculate the customer-store distance in miles
*/

WITH customer_food_preferences AS (
    SELECT customer_id,
           COUNT(*) as food_pref_count
    FROM customers.customer_survey
    WHERE is_active = TRUE
    GROUP BY 1
),
chicago_store_gps_coordinates AS (
    SELECT geo_location
    FROM vk_data.resources.us_cities 
    WHERE city_name = 'CHICAGO'
    AND state_abbr = 'IL'
),
gary_store_gps_coordinates AS (
    SELECT geo_location
    FROM vk_data.resources.us_cities 
    WHERE city_name = 'GARY'
    AND state_abbr = 'IN'
),
customers_impacted AS (
    SELECT ca.customer_id,
           cd.first_name,
           cd.last_name,
           ca.customer_city,
           ca.customer_state,
           cf.food_pref_count,
           uc.geo_location
    FROM customers.customer_address AS ca
    INNER JOIN customers.customer_data AS cd USING (customer_id)
    LEFT JOIN resources.us_cities AS uc
        ON TRIM(LOWER(ca.customer_city)) = TRIM(LOWER(uc.city_name))
        AND TRIM(UPPER(ca.customer_state)) = TRIM(UPPER(uc.state_abbr))
    INNER JOIN customer_food_preferences AS cf USING (customer_id)
    WHERE(
        (
            TRIM(uc.city_name) ILIKE '%concord%'
            or TRIM(uc.city_name) ILIKE '%georgetown%'
            or TRIM(uc.city_name) ILIKE '%ashland%'
        )
        AND ca.customer_state = 'KY'
    ) OR (
        ca.customer_state = 'CA'
        AND (
            TRIM(uc.city_name) ILIKE '%oakland%'
            OR TRIM(uc.city_name) ILIKE '%pleasant hill%'
        )
    ) OR (
        ca.customer_state = 'TX'
        AND (
            (TRIM(uc.city_name) ILIKE '%arlington%')
            OR TRIM(uc.city_name) ILIKE '%brownsville%'
        )
    )
),
customer_store_distances AS (
    SELECT 
        caf.first_name || ' ' || caf.last_name AS customer_name,
        caf.customer_city,
        caf.customer_state,
        caf.food_pref_count,
        (
            ST_DISTANCE(caf.geo_location, chic.geo_location) / 1609
        )::int AS chicago_distance_miles,
        (
            ST_DISTANCE(caf.geo_location, gary.geo_location) / 1609
        )::int AS gary_distance_miles
    FROM customers_impacted AS caf
    CROSS JOIN chicago_store_gps_coordinates AS chic
    CROSS JOIN gary_store_gps_coordinates AS gary
)
SELECT *
FROM customer_store_distances;
