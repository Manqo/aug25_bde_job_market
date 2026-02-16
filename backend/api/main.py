import logging
import threading

import pandas as pd
import uvicorn
from api.db import get_engine
from api.sql_loader import build_query_job_count, build_query_salary_stats, load_query
from config.config import ETL_TOKEN
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query
from sqlalchemy import text

# ---- FastAPI App ----
app = FastAPI(
    title="Job Market Insights API",
    version="1.0.0",
)


# --- Global Lock for ETL Pipeline ---
etl_lock = threading.Lock()


# --- Basic ---
@app.get("/")
def welcome():
    return {"message": "Welcome to the Job Market Insights API"}


@app.get("/status")
def status():
    return {"status": "ok"}


# --- Filter ---
@app.get("/job_categories")
def job_categories():
    """Get all distinct job categories"""
    query = load_query("job_categories")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        return {"job_categories": [r[0] for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/salary_range")
def salary_range():
    """Get min and max salary range"""
    query = load_query("salary_range")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
        return {"min_salary": result[0], "max_salary": result[1]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job_locations")
def job_locations():
    """Get distinct job locations (country, state, city)"""
    query = load_query("job_locations")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        locations = [{"country": r[0], "state": r[1], "city": r[2]} for r in rows]
        return {"locations": locations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job_entry_level")
def job_entry_level():
    """Get distinct job entry levels"""
    query = load_query("job_entry_level")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        return {"entry_levels": [r[0] for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/company_size")
def company_size():
    query = load_query("company_size")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        return {"company_sizes": [r[0] for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Statistics ---


@app.get("/stats/job_count", name="Get number of jobs by dimension")
def get_stats_job_count(
    dimension: str = Query(
        ...,
        description=f"Group results by given dimension. Allowed dimensions: company_name, company_industry, company_size, country, subdivision, city, job_category, entry_level",
    ),
    start_date: str = Query(
        None,
        description="Filters results by publication date after start date. Format: YYYY-MM-DD",
    ),
    end_date: str = Query(
        None,
        description="Filters results by publication date before end date. Format: YYYY-MM-DD",
    ),
    country: str = Query(
        None,
        description="Filters results by country. Format: ISO 3166 ALPHA-2, i.e. US, DE, ...",
    ),
    subdivision: str = Query(
        None,
        description="Filters results by subdivision. Format: ISO 3166-2, i.e. US-NY, US-TX, US-CA, ...",
    ),
    city: str = Query(None, description="Filters results by the name of the city"),
    entry_level: str = Query(
        None,
        description="Filters results by entry level. Allowed values: Senior Level, Mid Level, Entry Level, Internship",
    ),
    company_size: str = Query(
        None,
        description="Filters results by company size. Allowed values: Small Size, Medium Size, Large Size",
    ),
    job_category: str = Query(
        None,
        description="Filters results by job_category. Allowed values: Computer and IT, Data and Analytics, Software Engineering",
    ),
    job_title: str = Query(
        None,
        description="Filters results by job title using a case-insensitive substring match",
    ),
):
    """
    Returns the number of jobs by the specified dimension
    """

    # validate parameters
    allowed_dimensions = {
        "company_name",
        "company_industry",
        "company_size",
        "country",
        "subdivision",
        "city",
        "job_category",
        "entry_level",
    }
    if dimension not in allowed_dimensions:
        raise HTTPException(
            400,
            f"Invalid dimension: {dimension}. Allowed dimension: {allowed_dimensions}",
        )

    if start_date and end_date and start_date > end_date:
        raise HTTPException(400, "start_date cannot be after end_date")

    try:
        # calculation of the result
        engine = get_engine()
        query, params = build_query_job_count(
            dimension=dimension,
            start_date=start_date,
            end_date=end_date,
            country=country,
            subdivision=subdivision,
            city=city,
            entry_level=entry_level,
            company_size=company_size,
            job_category=job_category,
            job_title=job_title,
        )

        # execute sql query
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # specify applied filters für response
        applied_filters = {
            "country": country,
            "subdivision": subdivision,
            "city": city,
            "entry_level": entry_level,
            "company_size": company_size,
            "start_date": start_date,
            "end_date": end_date,
            "job_category": job_category,
            "job_title": job_title,
        }
        applied_filters = {k: v for k, v in applied_filters.items() if v is not None}
        return {
            "meta": {
                "dimension": dimension,
                "metric": "job_count",
                "aggregation": "count_distinct",
                "filters": applied_filters,
                "row_count": len(df),
            },
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/stats/salary_stats",
    name="Get avg, min and max of salaries by specified dimension",
)
def get_stats_salary(
    dimension: str = Query(
        ...,
        description=f"allowed dimensions: company_name, company_size, country, subdivision, city",
    ),
    start_date: str = Query(
        None,
        description="Filters results by publication date after start date. Format: YYYY-MM-DD",
    ),
    end_date: str = Query(
        None,
        description="Filters results by publication date before end date. Format: YYYY-MM-DD",
    ),
    country: str = Query(
        None,
        description="Filters results by country. Format: ISO 3166 ALPHA-2, i.e. US, DE, ...",
    ),
    subdivision: str = Query(
        None,
        description="Filters results by subdivision. Format: ISO 3166-2, i.e. US-NY, US-TX, US-CA, ...",
    ),
    city: str = Query(None, description="Filters results by the name of the city"),
    entry_level: str = Query(
        None,
        description="Filters results by entry level. Allowed values: Senior Level, Mid Level, Entry Level, Internship",
    ),
    company_size: str = Query(
        None,
        description="Filters results by company size. Allowed values: Small Size, Medium Size, Large Size",
    ),
    job_category: str = Query(
        None,
        description="Filters results by job_category. Allowed values: Computer and IT, Data and Analytics, Software Engineering",
    ),
    job_title: str = Query(
        None,
        description="Filters results by job title using a case-insensitive substring match",
    ),
):
    """
    Returns the avg, min and max of salaries by specified dimension.
    """

    # validate parameters
    allowed_dimensions = {
        "company_name",
        "company_size",
        "country",
        "subdivision",
        "city",
    }
    if dimension not in allowed_dimensions:
        raise HTTPException(
            400,
            f"Invalid dimension: {dimension}. Allowed dimension: {allowed_dimensions}",
        )

    if start_date and end_date and start_date > end_date:
        raise HTTPException(400, "start_date cannot be after end_date")

    try:
        # calculation of the result
        engine = get_engine()
        query, params = build_query_salary_stats(
            dimension=dimension,
            start_date=start_date,
            end_date=end_date,
            country=country,
            subdivision=subdivision,
            city=city,
            entry_level=entry_level,
            company_size=company_size,
            job_category=job_category,
            job_title=job_title,
        )

        # execute sql query
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # specify applied filters für response
        applied_filters = {
            "country": country,
            "subdivision": subdivision,
            "city": city,
            "entry_level": entry_level,
            "company_size": company_size,
            "start_date": start_date,
            "end_date": end_date,
            "job_category": job_category,
            "job_title": job_title,
        }
        applied_filters = {k: v for k, v in applied_filters.items() if v is not None}
        return {
            "meta": {
                "dimension": dimension,
                "metric": "salary metrics",
                "filters": applied_filters,
                "row_count": len(df),
            },
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/etl/run")
def run_etl(background_tasks: BackgroundTasks, x_token: str = Header(default=None)):
    """
    Triggers the ETL pipeline asynchronously.
    """

    # --- API Security Token check ---
    expected_token = ETL_TOKEN
    if expected_token:
        if x_token != expected_token:
            raise HTTPException(status_code=403, detail="Forbidden")

    # --- LOCK CHECK ---
    if etl_lock.locked():
        raise HTTPException(status_code=409, detail="ETL already running")

    acquired = etl_lock.acquire(blocking=False)
    if not acquired:
        raise HTTPException(status_code=409, detail="ETL already running")

    # --- schedule ETL ---
    background_tasks.add_task(run_etl_pipeline)

    return {"status": "accepted", "message": "ETL pipeline started"}


def run_etl_pipeline():
    logger = logging.getLogger("ETL")
    logger.info("ETL pipeline started")

    try:
        try:
            from etl.extract import pipeline as extract_pipeline
            from etl.load import load_pipeline
            from etl.transform import pipeline_transform
        except Exception as e:
            logger.exception("Importing ETL modules failed: %s", e)
            return

        try:
            extract_pipeline.run_all()
            logger.info("Extract finished")
        except Exception as e:
            logger.exception("Extract pipeline failed: %s", e)
            return

        try:
            pipeline_transform.main()
            logger.info("Transform finished")
        except Exception as e:
            logger.exception("Transform pipeline failed: %s", e)
            return

        try:
            load_pipeline.main()
            logger.info("Load finished")
        except Exception as e:
            logger.exception("Load pipeline failed: %s", e)
            return

        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.exception("ETL pipeline fatal error: %s", e)

    finally:
        # ALWAYS release lock
        if etl_lock.locked():
            etl_lock.release()
            logger.info("ETL lock released")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
