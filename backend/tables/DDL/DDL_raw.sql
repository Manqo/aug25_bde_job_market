create table raw.companies(
	company_id INT, 
	company_name TEXT,
	description TEXT,
	publication_date timestamp ,
	size TEXT,
	locations TEXT,
	industries TEXT
)


CREATE TABLE raw.jobs(
    job_id INT,
    company_id INT,
    job_name TEXT,
    level TEXT,
    publication_date TIMESTAMP,
    locations TEXT,
    categories TEXT
);


CREATE TABLE raw.salaries (
    adz_job_id BIGINT,
    company_name TEXT,
    adz_job_name TEXT,
    adz_category TEXT ,
    publication_date TIMESTAMP,
    locations TEXT,
    salary_min float,
    salary_max float,
    salary_is_predicted int
);