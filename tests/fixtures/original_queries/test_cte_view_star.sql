CREATE TEMPORARY VIEW first_view AS
SELECT
    v1,
    v2,
    v3
FROM source_db.source_table;
CREATE TEMPORARY VIEW second_view AS WITH cte AS (
    SELECT
        *,
        row_number() OVER (ORDER BY v1) AS row_num
    FROM first_view
)

SELECT * FROM cte
WHERE row_num = 1;

INSERT INTO target_db.target_table
SELECT
    v1,
    v2,
    v3
FROM second_view;
