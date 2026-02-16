
--Dimension Companies
CREATE TABLE star.dim_companies (
    company_key BIGSERIAL PRIMARY KEY,
    company_id INT,
    name TEXT,
    description TEXT,
    size TEXT,
    industry TEXT
);

--Dimension Jobs
CREATE TABLE star.dim_jobs (
    job_key BIGSERIAL PRIMARY KEY,
    job_id INT,
    name TEXT,
    level TEXT,
    category TEXT
);

--Dimension Locations
CREATE TABLE star.dim_locations (
    location_key BIGSERIAL PRIMARY KEY,
    city TEXT,
    state TEXT,
    country TEXT
);

--Dimension Date
CREATE TABLE star.dim_date (
    date_key BIGSERIAL PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT
);


--Dimension Fact Job Posting
CREATE TABLE star.fact_job_postings (
    fact_id BIGSERIAL PRIMARY KEY,
    job_key INT REFERENCES star.dim_jobs(job_key),
    company_key INT REFERENCES star.dim_companies(company_key),
    location_key INT REFERENCES star.dim_locations(location_key),
    date_key INT REFERENCES star.dim_date(date_key),
    salary_min FLOAT,
    salary_max FLOAT
);
