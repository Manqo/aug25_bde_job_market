from pathlib import Path

_QUERIES_FILE = Path(__file__).parent / "queries.sql"


def load_query(name: str) -> str:
    """
    Liest eine benannte Abfrage aus queries.sql.
    Format:
        -- name
        SELECT ...
    """
    content = _QUERIES_FILE.read_text(encoding="utf-8")

    # In Abschnitte splitten, die mit "-- " beginnen
    parts = content.split("-- ")
    for part in parts:
        lines = [ln for ln in part.splitlines() if ln.strip() != ""]
        if not lines:
            continue
        header = lines[0].strip()
        sql = "\n".join(lines[1:]).strip()
        if header == name:
            return sql

    raise ValueError(f"Query '{name}' not found in {str(_QUERIES_FILE)}")


def build_filters_and_params(
    country=None,
    subdivision=None,
    city=None,
    entry_level=None,
    company_size=None,
    start_date=None,
    end_date=None,
    job_category=None,
    job_title=None,
):
    """
    Builds SQL filter clauses and parameter values based on the provided inputs.
    Returns a list of SQL filter fragments ('AND ...') and a params dict for binding.
    Case-insensitive matching is used for string filters.
    """

    filters = []
    params = {}

    if country:
        filters.append("AND LOWER(country) = LOWER(:country)")
        params["country"] = country
    if subdivision:
        filters.append("AND LOWER(subdivision) = LOWER(:subdivision)")
        params["subdivision"] = subdivision
    if city:
        filters.append("AND LOWER(city) = LOWER(:city)")
        params["city"] = city
    if entry_level:
        filters.append("AND LOWER(entry_level) = LOWER(:entry_level)")
        params["entry_level"] = entry_level
    if company_size:
        filters.append("AND LOWER(company_size) = LOWER(:company_size)")
        params["company_size"] = company_size
    if start_date:
        filters.append("AND date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        filters.append("AND date <= :end_date")
        params["end_date"] = end_date
    if job_category:
        filters.append("AND LOWER(job_category) = LOWER(:job_category)")
        params["job_category"] = job_category
    if job_title:
        filters.append("AND job_title ILIKE :job_title")
        params["job_title"] = f"%{job_title}%"
    return filters, params


def build_query_job_count(
    dimension,
    start_date=None,
    end_date=None,
    country=None,
    subdivision=None,
    city=None,
    entry_level=None,
    company_size=None,
    job_category=None,
    job_title=None,
):
    """
    Build a sql query string for the job count by the specified dimension
    and returns the string and the corresponding parameters.
    """

    # build sql query
    sql = f"""
            SELECT
                {dimension},
                COUNT(DISTINCT job_id) AS job_count
            FROM star.v_job_postings
            WHERE 1=1
        """

    filters, params = build_filters_and_params(
        country=country,
        subdivision=subdivision,
        city=city,
        entry_level=entry_level,
        company_size=company_size,
        start_date=start_date,
        end_date=end_date,
        job_category=job_category,
        job_title=job_title,
    )

    if filters:
        sql += "\n" + "\n".join(filters)

    sql += f"""
            GROUP BY {dimension}
            ORDER BY job_count DESC
        """

    return sql, params


def build_query_salary_stats(
    dimension,
    start_date=None,
    end_date=None,
    country=None,
    subdivision=None,
    city=None,
    entry_level=None,
    company_size=None,
    job_category=None,
    job_title=None,
):
    """
    Build a sql query string for salary stats by the specified dimension
    and returns the string and the corresponding parameters.
    """

    # build sql query
    sql = f"""
            SELECT
                {dimension},
                round(avg((salary_min + salary_max) / 2)::numeric, 0) AS avg_salary,
                round(min(salary_min)::numeric, 0) AS min_salary,
                round(max(salary_max)::numeric, 0) AS max_salary,
                count(job_id) AS job_x_location_count
            FROM star.v_job_salaries
            WHERE 1=1
        """

    filters, params = build_filters_and_params(
        country=country,
        subdivision=subdivision,
        city=city,
        entry_level=entry_level,
        company_size=company_size,
        start_date=start_date,
        end_date=end_date,
        job_category=job_category,
        job_title=job_title,
    )

    if filters:
        sql += "\n" + "\n".join(filters)

    sql += f"""
            GROUP BY {dimension}
            ORDER BY avg_salary DESC
        """

    return sql, params
