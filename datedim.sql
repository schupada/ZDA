DO $$
DECLARE
    table_record RECORD;
    column_exists BOOLEAN;
BEGIN
    FOR table_record IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE 'SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = ''public''
                    AND table_name = $1
                    AND column_name = ''last_update''
                )' INTO column_exists USING table_record.table_name;

        IF column_exists THEN
            EXECUTE '
                INSERT INTO date_dim (date, year, quarter, month, day, day_of_week, day_name, is_weekend)
                SELECT
                    DATE(last_update) AS date,
                    EXTRACT(YEAR FROM last_update) AS year,
                    EXTRACT(QUARTER FROM last_update) AS quarter,
                    EXTRACT(MONTH FROM last_update) AS month,
                    EXTRACT(DAY FROM last_update) AS day,
                    (EXTRACT(DOW FROM last_update) + 6) % 7 AS day_of_week,
                    TO_CHAR(last_update, ''Day'') AS day_name,
                    CASE WHEN EXTRACT(ISODOW FROM last_update) IN (6,7) THEN true ELSE false END AS is_weekend
                FROM ' || table_record.table_name;
        END IF;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred while processing table: %, SQLSTATE: %, SQLERRM: %', table_record.table_name, SQLSTATE, SQLERRM;
END $$;