/* The reworked query is written by performing the following changes
   1. Extract the first nested sub-query with the alias r (on the LHS of the LEFT JOIN, which gets ingredients per recipe) as its own CTE named recipe_ingredients. Since the WHERE clause only applies to the recipe_name column, which is found in this same sub-query, the WHERE clause can be included in this CTE.
   2. Extract the second nested sub-query with the alias i (on the RHS of the LEFT JOIN, which gets nutrition info per ingredient) as its own CTE named ingredient_nutrition.
   3. Include the LEFT JOIN as its own CTE named recipe_nutrition_per_ingredient. This gives nutrition information per ingredient in each recipe. The recipe_id and ingredient columns are included in the output of the LEFT JOIN are not used later CTEs so they are excluded.
   4. The output of the LEFT JOIN contains all the columns needed to assemble the final required output (total nutrition information per recipe). So, the final INNER JOIN and min(id) as first_record are not required. These are both excluded.
   5. Extract the final calculation of total nutrition per recipe into its own CTE.
*/

WITH recipe_ingredients AS (
    SELECT recipe_id,
           recipe_name,
           flat_ingredients.index,
           TRIM(UPPER(REPLACE(flat_ingredients.value, '"', ''))) AS ingredient
    FROM chefs.recipe, table(flatten(ingredients)) AS flat_ingredients
    WHERE recipe_name IN (
        'birthday cookie',
        'a perfect sugar cookie',
        'honey oatmeal raisin cookies',
        'frosted lemon cookies',
        'snickerdoodles cinnamon cookies'
    )
),
ingredient_nutrition AS (
    SELECT TRIM(
               UPPER(
                   REPLACE(
                       SUBSTRING(
                           ingredient_name, 1, charindex(',', ingredient_name)
                       ),
                       ',',
                       ''
                   )
               )
           ) AS ingredient_name,
           MAX(calories) AS calories,
           MAX(total_fat) AS total_fat
    FROM resources.nutrition
    GROUP BY 1
),
recipe_nutrition_per_ingredient AS (
    SELECT r.recipe_name,
           i.calories,
           i.total_fat
    FROM recipe_ingredients as r
    LEFT JOIN ingredient_nutrition AS i ON r.ingredient = i.ingredient_name
),
recipe_total_nutrition AS (
    SELECT recipe_name,
           SUM(calories) AS total_calories,
           SUM(CAST(REPLACE(total_fat, 'g', '') AS int)) AS total_fat
    FROM recipe_nutrition_per_ingredient
    GROUP BY 1
    ORDER BY 1
)
SELECT *
FROM recipe_total_nutrition;
