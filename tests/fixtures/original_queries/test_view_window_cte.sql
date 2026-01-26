CREATE TEMPORARY VIEW first_view AS (
    SELECT
        a,
        b,
        c
    FROM source_table
);

CREATE TEMPORARY VIEW second_view AS
WITH first_view_cte AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY a ORDER BY b DESC
        ) AS row_num
    FROM first_view
)
SELECT * FROM first_view_cte
WHERE c = 1;

INSERT OVERWRITE output_table
SELECT
    a, 
    b, 
    c, 
    row_num 
FROM second_view;
