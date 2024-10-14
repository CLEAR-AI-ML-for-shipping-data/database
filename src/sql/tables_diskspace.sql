SELECT
    table_name,
    pg_size_pretty(table_size) AS table_size,
    pg_size_pretty(indexes_size) AS indexes_size,
    pg_size_pretty(total_size) AS total_size
FROM (
    SELECT
        table_name,
        pg_table_size(table_name) AS table_size,
        pg_indexes_size(table_name) AS indexes_size,
        pg_total_relation_size(table_name) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes
WHERE table_name LIKE '%public"%';


SELECT
    pretty_sizes.table_name,
    pg_size_pretty(pretty_sizes.table_size) AS table_size,
    pg_size_pretty(pretty_sizes.indexes_size) AS indexes_size,
    pg_size_pretty(pretty_sizes.total_size) AS total_size,
    stats.n_live_tup AS row_count
FROM (
    SELECT
        table_name,
        pg_table_size(table_name) AS table_size,
        pg_indexes_size(table_name) AS indexes_size,
        pg_total_relation_size(table_name) AS total_size
    FROM (
        SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name, table_schema
        FROM information_schema.tables
    ) AS all_tables
    ORDER BY total_size DESC
) AS pretty_sizes
JOIN pg_stat_user_tables AS stats ON pretty_sizes.table_name = ('"' || stats.schemaname || '"."' || stats.relname || '"')
WHERE pretty_sizes.table_name LIKE '%public%"';
