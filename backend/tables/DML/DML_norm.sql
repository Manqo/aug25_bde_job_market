-- Companies 
INSERT INTO norm.companies (id, description, name, publication_date, size)
select distinct company_id, description, company_name, publication_date::timestamp, size
FROM raw.companies;

-- Industries
-- Alle Industry-Namen extrahieren
INSERT INTO norm.industries(name)
SELECT DISTINCT
    TRIM(BOTH ' ' FROM value) AS name
FROM (
    SELECT unnest(
        string_to_array(
            regexp_replace(industries, 'name:\s*', '', 'g'),
            ','
        )
    ) AS value
    FROM raw.companies
) t
WHERE value IS NOT NULL
  AND value <> ''
ON CONFLICT (name) DO NOTHING;



--companies_industries
INSERT INTO norm.companies_industries (company_id, industry_id)
SELECT DISTINCT 
    c.id AS company_id,
    i.id AS industry_id
FROM raw.companies rc
JOIN norm.companies c 
    ON c.name = rc.company_name
CROSS JOIN LATERAL (
    SELECT 
        TRIM(BOTH ' ' FROM value) AS industry_name
    FROM unnest(
        string_to_array(
            regexp_replace(rc.industries, 'name:\s*', '', 'g'),
            ','
        )
    ) AS t(value)
) AS sub
JOIN norm.industries i 
    ON i.name = sub.industry_name
WHERE sub.industry_name IS NOT NULL 
  AND sub.industry_name <> '';


--locations
WITH combined AS (
  SELECT locations FROM raw.companies
  UNION ALL
  SELECT locations FROM raw.jobs
  UNION ALL
  SELECT locations FROM raw.salaries
),
parsed AS (
  SELECT
    -- 🎯 city extrahieren
    CASE
      WHEN locations ILIKE '%city:%'
        THEN NULLIF(TRIM(REGEXP_REPLACE(locations, '.*city:\s*([^,]+).*', '\1')), 'None')
      ELSE NULL
    END AS city,

    -- 🎯 subdivision_code oder state extrahieren
    CASE
      WHEN locations ILIKE '%subdivision_code:%'
        THEN NULLIF(TRIM(REGEXP_REPLACE(locations, '.*subdivision_code:\s*([^,]+).*', '\1')), 'None')
      WHEN locations ILIKE '%state:%'
        THEN NULLIF(TRIM(REGEXP_REPLACE(locations, '.*state:\s*([^,]+).*', '\1')), 'None')
      ELSE NULL
    END AS subdivision_code,

    -- 🎯 country_code oder country extrahieren
    CASE
      WHEN locations ILIKE '%country_code:%'
        THEN NULLIF(TRIM(REGEXP_REPLACE(locations, '.*country_code:\s*([^,]+).*', '\1')), 'None')
      WHEN locations ILIKE '%country:%'
        THEN NULLIF(TRIM(REGEXP_REPLACE(locations, '.*country:\s*([^,]+).*', '\1')), 'None')
      ELSE NULL
    END AS country_code
  FROM combined
)
INSERT INTO norm.locations (city, subdivision_code, country_code)
SELECT DISTINCT
  city,
  subdivision_code,
  country_code
FROM parsed
WHERE city IS NOT NULL
  AND city <> ''
ON CONFLICT DO NOTHING;




--jobs
INSERT INTO norm.jobs (id, company_id, name, level, publication_date)
SELECT DISTINCT
    j.job_id,
    j.company_id,
    j.job_name,
    j.level,
    CAST(j.publication_date AS TIMESTAMP)
FROM raw.jobs j
JOIN norm.companies c ON c.id = j.company_id;

--categories
INSERT INTO norm.categories (name)
SELECT DISTINCT
    TRIM(BOTH ' ' FROM value) AS name
FROM (
    SELECT unnest(
        string_to_array(
            regexp_replace(categories, 'name:\s*', '', 'g'),
            ','
        )
    ) AS value
    FROM raw.jobs
) t
WHERE value IS NOT NULL
  AND value <> ''
ON CONFLICT (name) DO NOTHING;


-- Jobs_categories
INSERT INTO norm.jobs_categories (job_id, category_id)
SELECT DISTINCT 
    j.id AS job_id,
    c.id AS category_id
FROM raw.jobs rj
JOIN norm.jobs j 
    ON j.name = rj.job_name
CROSS JOIN LATERAL (
    SELECT 
        TRIM(BOTH ' ' FROM value) AS category_name
    FROM unnest(
        string_to_array(
            regexp_replace(rj.categories, 'name:\s*', '', 'g'),
            ','
        )
    ) AS t(value)
) AS sub
JOIN norm.categories c 
    ON c.name = sub.category_name
WHERE sub.category_name IS NOT NULL
  AND sub.category_name <> '';


--company_location
INSERT INTO norm.companies_locations (company_id, location_id)
SELECT DISTINCT 
    c.id AS company_id,
    l.id AS location_id
FROM raw.companies rc
JOIN norm.companies c 
    ON c.name = rc.company_name
CROSS JOIN LATERAL (
    SELECT
        -- 🎯 city extrahieren
        CASE
            WHEN rc.locations ILIKE '%city:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*city:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS city,

        -- 🎯 subdivision_code oder state extrahieren
        CASE
            WHEN rc.locations ILIKE '%subdivision_code:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*subdivision_code:\s*([^,]+).*', '\1')), 'None')
            WHEN rc.locations ILIKE '%state:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*state:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS subdivision_code,

        -- 🎯 country_code oder country extrahieren
        CASE
            WHEN rc.locations ILIKE '%country_code:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*country_code:\s*([^,]+).*', '\1')), 'None')
            WHEN rc.locations ILIKE '%country:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*country:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS country_code
    FROM unnest(
        regexp_split_to_array(
            regexp_replace(rc.locations, '(\[|\]|\{|\})', '', 'g'),
            '},|],'
        )
    ) AS t(loc_text)
) AS parsed
JOIN norm.locations l 
  ON l.city = parsed.city
 AND (
        l.subdivision_code = parsed.subdivision_code
        OR (l.subdivision_code IS NULL AND parsed.subdivision_code IS NULL)
     )
 AND (
        l.country_code = parsed.country_code
        OR (l.country_code IS NULL AND parsed.country_code IS NULL)
     )
WHERE parsed.city IS NOT NULL
  AND parsed.city <> ''
ON CONFLICT DO NOTHING;


--jobs_location
INSERT INTO norm.jobs_locations (job_id, location_id)
SELECT DISTINCT 
    j.id AS job_id,
    l.id AS location_id
FROM raw.jobs rj
JOIN norm.jobs j 
    ON j.name = rj.job_name
CROSS JOIN LATERAL (
    SELECT
        -- 🎯 city extrahieren
        CASE
            WHEN rj.locations ILIKE '%city:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*city:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS city,

        -- 🎯 subdivision_code oder state extrahieren
        CASE
            WHEN rj.locations ILIKE '%subdivision_code:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*subdivision_code:\s*([^,]+).*', '\1')), 'None')
            WHEN rj.locations ILIKE '%state:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*state:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS subdivision_code,

        -- 🎯 country_code oder country extrahieren
        CASE
            WHEN rj.locations ILIKE '%country_code:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*country_code:\s*([^,]+).*', '\1')), 'None')
            WHEN rj.locations ILIKE '%country:%'
                THEN NULLIF(TRIM(REGEXP_REPLACE(loc_text, '.*country:\s*([^,]+).*', '\1')), 'None')
            ELSE NULL
        END AS country_code
    FROM unnest(
        regexp_split_to_array(
            regexp_replace(rj.locations, '(\[|\]|\{|\})', '', 'g'),
            '},|],'
        )
    ) AS t(loc_text)
) AS parsed
JOIN norm.locations l 
    ON l.city = parsed.city
   AND (
        l.subdivision_code = parsed.subdivision_code
        OR (l.subdivision_code IS NULL AND parsed.subdivision_code IS NULL)
       )
   AND (
        l.country_code = parsed.country_code
        OR (l.country_code IS NULL AND parsed.country_code IS NULL)
       )
WHERE parsed.city IS NOT NULL
  AND parsed.city <> ''
ON CONFLICT DO NOTHING;


--salaries


INSERT INTO norm.salaries (
    company_id,
    location_id,
    title,
    salary_min,
    salary_max
)
SELECT DISTINCT
    c.id AS company_id,
    l.id AS location_id,
    TRIM(s.adz_job_name) AS title,
    s.salary_min,
    s.salary_max
FROM raw.salaries s
LEFT JOIN norm.companies c
   ON LOWER(TRIM(c.name)) = LOWER(TRIM(s.company_name))
CROSS JOIN LATERAL (
    SELECT 
        -- Werte direkt extrahieren (da Format bekannt)
        NULLIF(TRIM(REGEXP_REPLACE(s.locations, '.*city:\s*([^,]+).*', '\1')), 'None') AS city,
        NULLIF(TRIM(REGEXP_REPLACE(s.locations, '.*subdivision_code:\s*([^,]+).*', '\1')), 'None') AS subdivision_code,
        NULLIF(TRIM(REGEXP_REPLACE(s.locations, '.*country_code:\s*([^,]+).*', '\1')), 'None') AS country_code
) AS parsed
LEFT JOIN norm.locations l 
    ON l.city = parsed.city
   AND (
        l.subdivision_code = parsed.subdivision_code
        OR (l.subdivision_code IS NULL AND parsed.subdivision_code IS NULL)
       )
   AND (
        l.country_code = parsed.country_code
        OR (l.country_code IS NULL AND parsed.country_code IS NULL)
       )
WHERE (s.salary_min IS NOT NULL OR s.salary_max IS NOT NULL)
  AND parsed.city IS NOT NULL
  AND parsed.city <> ''
ON CONFLICT DO NOTHING;

