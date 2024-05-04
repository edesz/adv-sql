/* The reworked query is written by performing the following changes
   1. From the supplier_geography CTE, extract (a)suppliers as its own CTE and (b) ucity as its own CTE and rename it to cities
   2. From the main query, extract the following nested sub-query (which calculates all inter-supplier distances) as its own CTE and rename it to supplier_distances
   3. From the main query, extract the following sub-query to filter inter-supplier distances by distance, to get the closest inter-supplier distance, into its own CTE named supplier_distances_closest
   4. Split the final selection into two parts: (a) capture the first JOIN as a subquery which gets a supplier and its closest (or neighbouring) option and (b) add supplier metadata (supplier_id and supplier_name) from the suppliers.supplier_info table. This improves readibility since parts (a) and (b) have a different purpose. Part (a) will be kept as a sub-query instead of extracting it into a CTE.
*/

WITH suppliers AS (
    SELECT supplier_id,
            supplier_name,
            supplier_city || ', ' || supplier_state AS supplier_location,
            TRIM(UPPER(supplier_city)) AS supplier_city,
            TRIM(UPPER(supplier_state)) AS supplier_state
    FROM suppliers.supplier_info
),
cities AS (
    SELECT city_name,
            state_abbr,
            MIN(city_id) AS city_id
    FROM resources.us_cities
    GROUP BY ALL
),
supplier_geography as (
    SELECT suppliers.supplier_id,
            /* added schema prefix (suppliers.) for supplier_name
            since it is also provided for the other selected columns */
            suppliers.supplier_name,
            suppliers.supplier_location,
            city_details.geo_location
    FROM suppliers
    INNER JOIN (
        SELECT city.city_id,
                TRIM(UPPER(city.city_name)) AS city_name,
                TRIM(UPPER(city.state_abbr)) AS state_abbr,
                city.lat,
                city.long,
                city.geo_location
        FROM resources.us_cities AS city
        INNER JOIN cities AS ucity ON city.city_id = ucity.city_id
    ) AS city_details
    ON suppliers.supplier_city = city_details.city_name
    AND suppliers.supplier_state = city_details.state_abbr
),
supplier_distances AS (
        SELECT sg1.supplier_id AS supplier_main,
                sg2.supplier_id AS supplier_backup,
                sg1.supplier_location AS location_main,
                sg2.supplier_location AS location_backup,
                st_distance(sg1.geo_location, sg2.geo_location) as distance_measure
        FROM supplier_geography AS sg1
        INNER JOIN supplier_geography AS sg2
),
main_supplier_distance_to_closest_neighbour AS (
    SELECT supplier_main,
            MIN(distance_measure) AS closest_distance
    FROM supplier_distances
    WHERE distance_measure > 0
    GROUP BY supplier_main
),
query_output AS (
    SELECT s.supplier_id,
            s.supplier_name,
            main_to_backup.location_main,
            main_to_backup.location_backup,
            main_to_backup.travel_miles
    FROM (
        /* get supplier, its closest neighboring supplier and distance
        between them */
        SELECT cc.location_main,
                cs.supplier_main AS supplier_main,
                cc.location_backup,
                ROUND(cc.distance_measure / 1609) AS travel_miles
        FROM main_supplier_distance_to_closest_neighbour AS cs
        INNER JOIN supplier_distances AS cc
        ON cs.closest_distance = cc.distance_measure
        AND cs.supplier_main = cc.supplier_main
    ) AS main_to_backup
    INNER JOIN suppliers AS s ON main_to_backup.supplier_main = s.supplier_id
)
SELECT *
FROM query_output
ORDER BY supplier_name;
