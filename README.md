# Data Engineering Projct for Job Market analysis

This project implements a containerized data platform for collecting, transforming, and analyzing job and salary data from external APIs.  
It consists of a batch ETL pipeline, a backend API, a frontend dashboard, and a dedicated orchestration layer.

---

## 🧱 Architecture Overview

The system is composed of the following components:

- **Orchestrator Container**
  - Schedules and triggers ETL runs using APScheduler
  - Prevents overlapping executions via locking

- **Backend Container**
  - FastAPI application acting as API gateway
  - Hosts the ETL process
  - Exposes endpoints to:
    - Run the ETL pipeline
    - Retrieve statistics
    - Retrieve metadata

- **Frontend Container**
  - Streamlit dashboard
  - Visualizes job market and salary analytics

---

## 🔄 ETL Process

The ETL pipeline is a **batch-driven process** and represents the core of the system.

### Extract
- Fetch job and company data from **The Muse API**
- Fetch salary data from **Adzuna API**
- Store raw responses as JSON files

### Transform
- Flatten semi-structured JSON data
- Clean and normalize fields (especially locations)
- Deduplicate records
- Enrich data with categories, entry levels, and geo information
- Align datasets to enable cross-source merging

### Load
- Export transformed data as CSV files
- Load data into Supabase schemas:
  - **Raw**
  - **Normalized**
  - **Star Schema (fact & dimension tables)**

---

## 🗄️ Data Model

The platform follows a layered data modeling approach:

- **Raw** – structured raw data loaded from CSV files  
- **Normalized** – cleaned and transformed data stored in a fully normalized (3NF) data model
- **Star Schema** – analytics-ready fact and dimension tables optimized for reporting

This design improves data quality, maintainability, and analytical performance.

---

## 🚀 Scheduling & Orchestration

- ETL runs are triggered on a schedule using **APScheduler**
- The orchestrator runs in its own container
- A locking mechanism ensures no overlapping ETL executions
- ETL runs are triggered via a **POST request** to the backend API

---

## 📊 Frontend Dashboard

- Built with **Streamlit**
- Displays aggregated job and salary insights
- Uses backend API endpoints for data access

---

## 🧰 Technology Stack

- **Python**
- **FastAPI**
- **APScheduler**
- **Streamlit**
- **Docker**
- **Supabase (PostgreSQL)**
- External APIs:
  - The Muse
  - Adzuna

---

## ▶️ Getting Started

### Prerequisites
- Docker
- Docker Compose
- A valid `.env` file with all required environment variables

### Setup & Run

1. Navigate to the deployment directory:
    ```bash
    cd deployment
    ```
2. Ensure the .env file is present in the deployment folder.
3. Start all services using Docker Compose:
    ```bash
    docker-compose up -d
    ```

This will start the following containers:

- Orchestrator
- Backend (FastAPI + ETL)
- Frontend (Streamlit)

Once the services are running, the Streamlit dashboard is available at:

http://localhost:8501

---

## 🔐 Environment Configuration

The project relies on environment variables provided via a `.env` file.  
This file is required and **not included** in the repository.

It contains:
- API keys for external Adzuna API
- Database credentials (Supabase)
- Scheduling configuration (cron expressions)
- ETL Token for Post request on /etl/run endpoint

Make sure the `.env` file is correctly configured before starting the project.
