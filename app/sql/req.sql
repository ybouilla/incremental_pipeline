PRAGMA recursive_triggers = ON;


WITH RECURSIVE

-- 1. Clean changes (keep last per license_id + date)
changed_clean AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY license_id, date
                   ORDER BY id DESC
               ) rn
        FROM license_changes
    )
    WHERE rn = 1
),

-- 2. Initial states
initial_state AS (
    SELECT
        id AS license_id,
        date(creation_date) AS date,
        renewable,
        price,
        type,
        -1 AS id
    FROM initial_licenses
),

-- 3. Remove initial rows if change exists same day
filtered_initial AS (
    SELECT *
    FROM initial_state i
    WHERE NOT EXISTS (
        SELECT id
        FROM changed_clean c
        WHERE c.license_id = i.license_id
          AND date(c.date) = i.date
    )
),

-- 4. Unified states
states AS (
    SELECT license_id, date(date) AS date, renewable, price, type, id FROM changed_clean
    UNION ALL
    SELECT license_id, date, renewable, price, type, id FROM filtered_initial
),

-- 5. Deduplicate (keep last per license/date)
states_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY license_id, date
                   ORDER BY id DESC
               ) rn
        FROM states
    )
    WHERE rn = 1
),

-- 6. Calendar
date_bounds AS (
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM states_dedup
),

calendar(date) AS (
    SELECT min_date FROM date_bounds
    UNION ALL
    SELECT date(date, '+1 day')
    FROM calendar, date_bounds
    WHERE date < max_date
),

-- 7. Licenses with creation date
licenses AS (
    SELECT id AS license_id, date(creation_date) AS creation_date
    FROM initial_licenses
),

-- 8. Full grid (date × license) AFTER creation
grid AS (
    SELECT
        c.date,
        l.license_id
    FROM calendar c
    JOIN licenses l
      ON c.date >= l.creation_date
),

-- 9. Forward-fill using latest known state
joined AS (
    SELECT
        g.license_id,
        g.date,
        s.renewable,
        s.type,
        s.price,
        ROW_NUMBER() OVER (
            PARTITION BY g.license_id, g.date
            ORDER BY s.date DESC, s.id DESC
        ) rn
    FROM grid g
    LEFT JOIN states_dedup s
        ON s.license_id = g.license_id
       AND s.date <= g.date
),

daily_states AS (
    SELECT
        license_id,
        date,
        renewable,
        type,
        price
    FROM joined
    WHERE rn = 1
      AND renewable IS NOT NULL
),

-- 10. Counts (active / inactive)
counts AS (
    SELECT
        date,
        type,
        SUM(CASE WHEN renewable = 1 THEN 1 ELSE 0 END) AS active_license_count,
        SUM(CASE WHEN renewable = 0 THEN 1 ELSE 0 END) AS inactive_license_count
    FROM daily_states
    GROUP BY date, type
),

-- 11. Price
price AS (
    SELECT
        date,
        type,
        SUM(CASE WHEN renewable = 1 THEN price ELSE 0 END) AS active_license_price
    FROM daily_states
    GROUP BY date, type
),

-- 12. Build full date × type grid
types AS (
    SELECT DISTINCT type FROM daily_states
),

date_type_grid AS (
    SELECT c.date, t.type
    FROM calendar c
    CROSS JOIN types t
),

-- 13. Combine
final AS (
    SELECT
        g.date,
        g.type,
        COALESCE(c.active_license_count, 0) AS active_license_count,
        COALESCE(c.inactive_license_count, 0) AS inactive_license_count,
        COALESCE(p.active_license_price, 0) AS active_license_price
    FROM date_type_grid g
    LEFT JOIN counts c
        ON g.date = c.date AND g.type = c.type
    LEFT JOIN price p
        ON g.date = p.date AND g.type = p.type
),

-- 14. Diffs
final_with_diff AS (
    SELECT
        date,
        type,
        active_license_count,
        active_license_price,
        inactive_license_count,

        COALESCE(
            active_license_count
            - LAG(active_license_count) OVER (PARTITION BY type ORDER BY date),
            0
        ) AS daily_active_diff,

        COALESCE(
            active_license_price
            - LAG(active_license_price) OVER (PARTITION BY type ORDER BY date),
            0
        ) AS daily_price_diff,

        COALESCE(
            inactive_license_count
            - LAG(inactive_license_count) OVER (PARTITION BY type ORDER BY date),
            0
        ) AS daily_inactive_diff

    FROM final
)

-- 15. Final output
SELECT
    date,
    type,
    active_license_count,
    active_license_price,
    inactive_license_count,
    daily_active_diff,
    daily_price_diff,
    daily_inactive_diff
FROM final_with_diff
ORDER BY date, type;
