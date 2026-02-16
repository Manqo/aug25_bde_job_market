-- job_categories
SELECT DISTINCT "name" as category
FROM star.dim_categories
WHERE "name" IS NOT NULL
ORDER BY "name";

-- salary_range
SELECT MIN(salary_min) AS min_salary,
       MAX(salary_max) AS max_salary
FROM star.fact_job_postings;

-- job_locations
SELECT DISTINCT country, state, city
FROM star.dim_locations
WHERE city IS NOT NULL
ORDER BY country, state, city;

-- job_entry_level
SELECT DISTINCT level
FROM star.dim_levels
WHERE level IS NOT NULL
ORDER BY level;

-- company_size
SELECT DISTINCT size
FROM star.dim_companies
WHERE size IS NOT NULL
ORDER BY size;
