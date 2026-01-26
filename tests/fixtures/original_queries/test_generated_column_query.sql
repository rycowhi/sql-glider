INSERT OVERWRITE TABLE db.output_table_1
SELECT DISTINCT
    a.id,
    a.update_date,
    trim(
        concat(
            coalesce(a.address_one, ""),
            " ",
            coalesce(a.address_two, ""),
            " ",
            coalesce(a.address_three, "")
        )
    ) AS full_address
FROM db.input_a AS a

UNION

SELECT DISTINCT
    b.id,
    b.update_date,
    trim(
        concat(
            coalesce(b.address_part_a, ""),
            " ",
            coalesce(b.address_part_b, "")
        )
    ) AS full_address
FROM db.input_b AS b;

INSERT OVERWRITE TABLE db.output_table_2
SELECT
    o.id,
    o.full_address AS address
FROM db.output_table_1 AS o;
