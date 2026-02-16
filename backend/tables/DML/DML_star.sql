--Dimension Companies
INSERT INTO star.dim_companies (company_id, name, description, size, industry)
SELECT DISTINCT
    c.id AS company_id,
    c.name,
    c.description,
    c.size,
    i.name AS industry
FROM norm.companies c
LEFT JOIN norm.companies_industries ci ON ci.company_id = c.id
LEFT JOIN norm.industries i ON i.id = ci.industry_id
WHERE c.name IS NOT NULL;



--Dimension Jobs
INSERT INTO star.dim_jobs (job_id, name, level, category)
SELECT DISTINCT
    j.id AS job_id,
    j.name,
    j.level,
    cat.name AS category
FROM norm.jobs j
LEFT JOIN norm.jobs_categories jc ON jc.job_id = j.id
LEFT JOIN norm.categories cat ON cat.id = jc.category_id
WHERE j.name IS NOT NULL;



--Dimension Location
INSERT INTO star.dim_locations (city, state, country)
SELECT DISTINCT
    city,
    subdivision_code AS state,
    country_code AS country
FROM norm.locations
WHERE city IS NOT NULL;


--Dimension Date
INSERT INTO star.dim_date (full_date, day, month, year)
SELECT DISTINCT
    d.publication_date::date AS full_date,
    EXTRACT(DAY FROM d.publication_date)::INT AS day,
    EXTRACT(MONTH FROM d.publication_date)::INT AS month,
    EXTRACT(YEAR FROM d.publication_date)::INT AS year
FROM (
    SELECT publication_date FROM norm.jobs
    UNION
    SELECT publication_date FROM norm.companies
) AS d
WHERE d.publication_date IS NOT NULL;




--fact Job Posting
INSERT INTO star.fact_job_postings (
    job_key,
    company_key,
    location_key,
    date_key,
    salary_min,
    salary_max
)
SELECT DISTINCT
    dj.job_key,
    dc.company_key,
    dl.location_key,
    dd.date_key,
    s.salary_min,
    s.salary_max
FROM norm.jobs j
JOIN star.dim_jobs dj 
    ON dj.job_id = j.id
JOIN norm.companies c 
    ON c.id = j.company_id
JOIN star.dim_companies dc 
    ON dc.company_id = c.id
LEFT JOIN norm.jobs_locations jl 
    ON jl.job_id = j.id
LEFT JOIN norm.locations l 
    ON l.id = jl.location_id
LEFT JOIN star.dim_locations dl 
    ON LOWER(TRIM(dl.city)) = LOWER(TRIM(l.city))
   AND LOWER(TRIM(dl.state)) = LOWER(TRIM(l.subdivision_code))
   AND LOWER(TRIM(dl.country)) = LOWER(TRIM(l.country_code))
LEFT JOIN star.dim_date dd 
    ON dd.full_date = DATE(j.publication_date)
LEFT JOIN norm.salaries s 
    ON s.company_id = j.company_id
   AND s.location_id = l.id
WHERE s.salary_min IS NOT NULL OR s.salary_max IS NOT NULL;






