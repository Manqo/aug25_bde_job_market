
-- ========== Dimensionstabellen ==========

CREATE TABLE norm.industries (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE norm.categories (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE norm.locations (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    subdivision_code TEXT,
    country_code TEXT
);



-- ========== Haupttabellen ==========

CREATE TABLE norm.companies (
    id BIGSERIAL PRIMARY KEY,
    description TEXT,
    name TEXT,
    publication_date TIMESTAMP,
    size TEXT
);



CREATE TABLE norm.jobs (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT REFERENCES norm.companies(id),
    name TEXT,
    level TEXT,
    publication_date TIMESTAMP
);

CREATE TABLE norm.salaries (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT REFERENCES norm.companies(id),
    location_id BIGINT REFERENCES norm.locations(id),
    title TEXT,
    salary_min FLOAT,
    salary_max FLOAT
);


-- ========== Verknüpfungstabellen ==========

CREATE TABLE norm.companies_industries (
    company_id BIGINT REFERENCES norm.companies(id),
    industry_id BIGINT REFERENCES norm.industries(id),
    PRIMARY KEY (company_id, industry_id)
);

CREATE TABLE norm.companies_locations (
    company_id BIGINT REFERENCES norm.companies(id),
    location_id BIGINT REFERENCES norm.locations(id),
    PRIMARY KEY (company_id, location_id)
);

CREATE TABLE norm.jobs_locations (
    job_id BIGINT REFERENCES norm.jobs(id),
    location_id BIGINT REFERENCES norm.locations(id),
    PRIMARY KEY (job_id, location_id)
);

CREATE TABLE norm.jobs_categories (
    job_id BIGINT REFERENCES norm.jobs(id),
    category_id BIGINT REFERENCES norm.categories(id),
    PRIMARY KEY (job_id, category_id)
);
