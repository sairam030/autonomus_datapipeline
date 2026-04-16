<p align="center">
  <h1 align="center">🚀 Autonomous Data Pipeline</h1>
  <p align="center">
    <strong>AI-Powered, Configuration-Driven Data Pipeline with Medallion Architecture</strong>
  </p>
  <p align="center">
    <em>Upload → Detect Schema → Bronze → Silver → Gold → PostgreSQL — all from a visual UI.</em>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI"/>
    <img src="https://img.shields.io/badge/React-61DAFB?style=for-the-badge&logo=react&logoColor=black" alt="React"/>
    <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"/>
    <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/>
    <img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white" alt="MinIO"/>
    <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
    <img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka"/>
    <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
    <img src="https://img.shields.io/badge/Gemini_AI-8E75B2?style=for-the-badge&logo=googlegemini&logoColor=white" alt="Gemini AI"/>
  </p>
</p>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Demo Video](#demo-video)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Service Ports](#service-ports)
- [Usage Workflow](#usage-workflow)
- [API Endpoints](#api-endpoints)
- [Environment Variables](#environment-variables)

---

## Overview

**Autonomous Data Pipeline** is a full-stack, AI-assisted data engineering platform that automates the entire data pipeline lifecycle — from raw data ingestion to enriched, query-ready tables — using the **Medallion Architecture** (Bronze → Silver → Gold).

Users interact through a **React-based dark-themed UI** to upload data files, auto-detect schemas, configure AI-powered transformations via natural language chat (powered by **Google Gemini**), and orchestrate everything through auto-generated **Airflow DAGs**.

---

## Architecture Diagram
![Architecture Diagram](pic/design.png)

---

## 🎬 Demo & Screenshots

📹 **[Watch Demo Video](screenshots/automation.mp4)** — Full walkthrough of the pipeline (upload → Bronze → Silver → Gold → PostgreSQL)

📸 **[View All Screenshots](screenshots/)** — UI screenshots covering all major steps:


### Data Flow

```
  📁 Upload Files (CSV/JSON/Parquet) ─────┐
  🌐 REST API Sources ───────────────────┤
  📡 Apache Kafka Streams ───────────────┘
        │
        ▼
  🔍 Auto Schema Detection (pandas + heuristic inference)
        │
        ▼
  ✅ Confirm Schema → Trigger Bronze Ingestion
        │
        ▼
  🥉 BRONZE LAYER (MinIO: s3a://bronze/<project>/v<N>/...)
  │     Raw data stored as Parquet, partitioned by ingestion date
        │
        ▼
  🤖 AI Chat (Gemini) → Generate PySpark transform() code
  │     Dry-run → Confirm → Version control
        │
        ▼
  🥈 SILVER LAYER (MinIO: s3a://silver/<project>/...)
  │     Cleaned, validated, transformed data
        │
        ▼
  🤖 AI Chat (Gemini) → Generate Gold enrichment code
  │     Joins, aggregations, API enrichment
        │
        ▼
  🥇 GOLD LAYER (MinIO: s3a://gold/<project>/...)
  │     Business-ready, enriched datasets
        │
        ▼
  🐘 Push to PostgreSQL (append / replace / fail modes)
        │
        ▼
  ⚙️  Auto-generate Airflow DAGs (Bronze → Silver → Gold → Master)
```

---

## Tech Stack

### Backend
| Technology | Version | Purpose |
|---|---|---|
| **Python** | 3.11 | Core language |
| **FastAPI** | 0.109.0 | REST API framework |
| **SQLAlchemy** | Latest | ORM for PostgreSQL |
| **Pydantic** | v2 | Settings & request/response validation |
| **Uvicorn** | 0.27.0 | ASGI server |

### Frontend
| Technology | Version | Purpose |
|---|---|---|
| **React** | 18.2 | UI framework |
| **TypeScript** | 4.9 | Type-safe JavaScript |
| **Material UI (MUI)** | 5.15 | Component library (dark theme) |
| **React Router** | 6.21 | Client-side routing |
| **TanStack React Query** | 5.17 | Server state management |
| **Monaco Editor** | 4.7 | In-browser code editor |
| **Axios** | 1.6 | HTTP client |

### Data Processing
| Technology | Version | Purpose |
|---|---|---|
| **Apache Spark** | 3.5.1 | Distributed data processing |
| **PySpark** | 3.5.1 | Python API for Spark |
| **Pandas** | 2.2.0 | Schema detection & small data |
| **PyArrow** | Latest | Parquet read/write |

### Orchestration
| Technology | Version | Purpose |
|---|---|---|
| **Apache Airflow** | 2.9.0 | Workflow orchestration |
| **LocalExecutor** | — | Single-node task execution |

### Storage
| Technology | Version | Purpose |
|---|---|---|
| **PostgreSQL** | 15 (Alpine) | Metadata store, Gold layer export |
| **MinIO** | Latest | S3-compatible object storage (Bronze/Silver/Gold) |
| **Redis** | 7 (Alpine) | Caching layer |

### AI / LLM
| Technology | Model | Purpose |
|---|---|---|
| **Google Gemini API** | gemini-2.5-flash | AI-powered PySpark code generation for Silver & Gold transformations |

### Streaming
| Technology | Version | Purpose |
|---|---|---|
| **Apache Kafka** | 7.6.0 (CP) | Real-time data streaming |
| **Zookeeper** | 7.6.0 (CP) | Kafka cluster coordination |
| **kafka-python-ng** | 2.2.3 | Python Kafka consumer/producer |

### Infrastructure
| Technology | Purpose |
|---|---|
| **Docker & Docker Compose** | Containerized deployment (13 services) |
| **Hadoop AWS (3.3.4)** | S3A filesystem connector for Spark |
| **PostgreSQL JDBC** | Spark ↔ PostgreSQL connector |

---

## Features

- ✅ **Auto Schema Detection** — Upload CSV/JSON/Parquet files and auto-detect column types, nullability, and sample values with confidence scoring
- ✅ **Medallion Architecture** — Bronze (raw) → Silver (cleaned) → Gold (enriched) data layers stored in MinIO
- ✅ **AI-Powered Transformations** — Chat with Google Gemini to generate PySpark transformation code using natural language
- ✅ **Conversational AI Workflow** — Iterative code generation with clarifying questions, dry-runs, and version control
- ✅ **Visual Code Editor** — Edit generated PySpark code directly in the browser using Monaco Editor
- ✅ **Dry-Run Validation** — Test transformations on sample data before committing
- ✅ **Transformation Versioning** — Track, rollback, and reorder transformation steps
- ✅ **Auto DAG Generation** — Generate Airflow DAGs (Bronze, Silver, Gold, Master) from templates
- ✅ **PostgreSQL Export** — Push Gold-layer data to PostgreSQL tables (append/replace/fail modes)
- ✅ **Full Audit Trail** — Every generated code file is timestamped and saved to `generated_queries/`
- ✅ **Multi-Source Support** — CSV, JSON, Parquet, REST API, Kafka, and Database sources
- ✅ **Kafka Streaming** — Real-time data ingestion from Kafka topics with configurable consumer groups
- ✅ **Push to PostgreSQL** — Gold-layer data exported to PostgreSQL tables for BI/dashboards
- ✅ **Spark Cluster** — Dedicated Spark Master + Worker for distributed processing
- ✅ **Dark-Themed UI** — Modern React + MUI dark theme with project-based navigation
- ✅ **Modular Service Layer** — Clean separation of routers (HTTP) from services (business logic)

---

## Project Structure

```
autonomous_pipeline/
├── backend/                        # FastAPI backend application
│   └── app/
│       ├── main.py                 # Application entry point & CORS config
│       ├── config.py               # Pydantic settings (env-based config)
│       ├── database.py             # SQLAlchemy session management
│       ├── models/
│       │   └── models.py           # SQLAlchemy ORM models (20 tables)
│       ├── routers/                # Thin HTTP layer — request handling only
│       │   ├── pipelines.py        # Project CRUD + Kafka connectivity
│       │   ├── schemas.py          # Schema detection & Bronze ingestion
│       │   ├── bronze.py           # Bronze ingestion status & callbacks
│       │   ├── silver.py           # Silver transformation endpoints
│       │   ├── gold.py             # Gold transformation + PG push endpoints
│       │   ├── upload.py           # File upload management
│       │   └── dags.py             # Airflow DAG generation & management
│       ├── schemas/                # Pydantic request/response schemas
│       └── services/               # Business logic layer
│           ├── ai_service.py       # Gemini AI integration
│           ├── sandbox.py          # Safe exec() for user/AI code (sandboxed)
│           ├── spark_utils.py      # Shared Spark session builders & MinIO I/O
│           ├── silver_service.py   # Silver upload execution engine
│           ├── gold_service.py     # Gold upload + push-to-Postgres engine
│           ├── schema_detection.py # Type inference & schema analysis
│           ├── dag_generator.py    # Template-based DAG rendering (6 templates)
│           ├── minio_service.py    # MinIO client utilities
│           └── code_saver.py       # Generated code persistence
├── frontend/                       # React TypeScript frontend
│   └── src/
│       ├── App.tsx                 # Routes & dark theme config
│       ├── components/
│       │   └── Layout.tsx          # Sidebar + AppBar layout
│       ├── pages/
│       │   ├── ProjectsPage.tsx        # Project listing
│       │   ├── CreateProjectPage.tsx   # New project wizard
│       │   ├── ProjectDetailPage.tsx   # Project dashboard + push-to-PG
│       │   ├── CreateTaskPage.tsx      # DAG task creation
│       │   ├── SchemaPreviewPage.tsx   # Schema review & confirm
│       │   ├── SilverEnrichmentPage.tsx # AI chat + code editor
│       │   └── GoldEnrichmentPage.tsx  # AI chat + code editor
│       └── services/
│           └── api.ts              # Axios API client
├── dags/                           # Auto-generated Airflow DAGs (gitignored)
├── data/                           # Sample / reference data files
├── db/
│   └── init.sql                    # PostgreSQL schema (20 tables)
├── engine/                         # Execution engine modules
├── generated_queries/              # Audit trail of all generated code
├── logs/                           # Airflow task logs (gitignored)
├── scripts/
│   ├── spark_master_entrypoint.sh
│   └── kafka_producer.py           # Test Kafka producer script
├── docker-compose.yml              # Full stack orchestration (13 services)
├── Dockerfile                      # Custom Airflow + Spark + Python image
├── .env.example                    # Template for environment variables
└── README.md
```

---

## Getting Started

### Prerequisites

| Requirement | Minimum Version |
|---|---|
| **Docker** | 24.0+ |
| **Docker Compose** | v2.20+ |
| **Google Gemini API Key** | Required for AI features |
| **RAM** | 8 GB+ recommended |
| **Disk** | 10 GB+ free space |

### 1. Clone the Repository

```bash
git clone https://github.com/sairam030/autonomus_datapipeline.git
cd autonomous_pipeline
```

### 2. Set Environment Variables

Copy the example env file and fill in your Gemini API key:

```bash
cp .env.example .env
# Edit .env and set your GEMINI_API_KEY
```

The `.env.example` includes all configurable ports, credentials, and the Gemini API key placeholder.

### 3. Build and Start All Services

```bash
# Build the custom Docker image and start all 13 services
docker compose up -d --build
```

> ⏳ **First run takes ~5–10 minutes** to build the image, download Spark/Hadoop JARs, initialize the database, create Kafka topics, and install frontend dependencies.

### 4. Verify Services Are Running

```bash
docker compose ps
```

You should see all containers in a `healthy` or `running` state:

```
NAME                  STATUS
ap-postgres           healthy
ap-minio              healthy
ap-redis              healthy
ap-zookeeper          healthy
ap-kafka              healthy
ap-spark-master       healthy
ap-spark-worker-1     running
ap-backend            running
ap-airflow-webserver  healthy
ap-airflow-scheduler  healthy
ap-frontend           running
```

### 5. Access the Applications

| Service | URL | Credentials |
|---|---|---|
| **Frontend UI** | [http://localhost:3001](http://localhost:3001) | — |
| **Backend API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | — |
| **Airflow UI** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9011](http://localhost:9011) | `minioadmin` / `minioadmin` |
| **Spark Master UI** | [http://localhost:8090](http://localhost:8090) | — |

### 6. Stop All Services

```bash
docker compose down
```

To also remove persistent volumes (database, MinIO data, etc.):

```bash
docker compose down -v
```

### 7. Run Basic Smoke Tests

Backend health endpoint tests:

```bash
docker compose run --rm --entrypoint /bin/sh backend -lc "cd /opt/pipeline && python -m unittest discover -s backend/tests -p 'test_*.py'"
```

Frontend tests (CI mode):

```bash
docker compose run --rm frontend npm run test:ci
```

Frontend production build:

```bash
docker compose run --rm frontend npm run build
```

### 8. Build and Push Images to Your Docker Registry

If you want to avoid local rebuilds in the future and run from your own registry:

```bash
# Login once
docker login

# Build + push project images
./scripts/docker_build_and_push.sh <dockerhub_user> <tag>
```

This pushes:
- `<dockerhub_user>/autonomous-pipeline:<tag>`
- `<dockerhub_user>/autonomous-spark:<tag>`

Then create `.env.images` in project root:

```bash
PIPELINE_IMAGE=<dockerhub_user>/autonomous-pipeline:<tag>
SPARK_IMAGE=<dockerhub_user>/autonomous-spark:<tag>
```

Pull and run without building:

```bash
./scripts/docker_pull_and_up.sh
```

---

## Service Ports

| Service | Host Port | Container Port | Notes |
|---|---|---|---|
| PostgreSQL | `5433` | `5432` | Metadata & Gold export |
| MinIO API | `9010` | `9000` | S3-compatible API |
| MinIO Console | `9011` | `9001` | Web UI |
| Redis | `6380` | `6379` | Cache |
| Spark Master | `7078` | `7077` | Cluster manager |
| Spark Master UI | `8090` | `8080` | Web UI |
| Spark Worker UI | `8091` | `8081` | Web UI |
| FastAPI Backend | `8000` | `8000` | REST API |
| Airflow UI | `8085` | `8080` | Web UI |
| Kafka (External) | `9093` | `9093` | External listener |
| Kafka (Internal) | — | `9092` | Container-to-container |
| Zookeeper | `2181` | `2181` | Kafka coordination |
| React Frontend | `3001` | `3000` | Dev server |

---

## Usage Workflow

### Step 1: Create a Project
Open the **Frontend UI** at `http://localhost:3001`, click **"New Project"**, provide a name, description, and source type (CSV, JSON, etc.).

### Step 2: Upload Data Files
Navigate to the project and upload your data files (CSV, JSON, or Parquet).

### Step 3: Detect & Confirm Schema
Click **"Detect Schema"** to auto-detect column names, types, and statistics. Review the schema preview and click **"Confirm"** to trigger Bronze ingestion.

### Step 4: Silver Transformations (AI-Powered)
Navigate to **Silver Enrichment** and chat with the AI:
> *"Filter out rows where status is cancelled and add a column for flight duration in minutes"*

The AI generates PySpark code. **Dry-run** it on sample data, iterate, then **confirm** the version.

### Step 5: Gold Transformations (AI-Powered)
Navigate to **Gold Enrichment** for aggregations, joins, and business-level transformations.

### Step 6: Push to PostgreSQL
Export Gold-layer data to a PostgreSQL table with append/replace/fail modes.

### Step 7: Generate Airflow DAGs
Auto-generate Bronze, Silver, Gold, and Master DAGs. Enable them in the Airflow UI for scheduled execution.

---

## API Endpoints

### Pipelines
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/pipelines/` | Create a new pipeline |
| `GET` | `/api/pipelines/` | List all pipelines |
| `GET` | `/api/pipelines/{id}` | Get pipeline details |
| `PUT` | `/api/pipelines/{id}` | Update pipeline |
| `DELETE` | `/api/pipelines/{id}` | Delete pipeline |

### Schema & Bronze
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/schemas/detect` | Auto-detect schema |
| `POST` | `/api/schemas/confirm` | Confirm schema & ingest |
| `GET` | `/api/bronze/{id}/preview` | Preview Bronze data |

### Silver Transformations
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/silver/{id}/transformations` | Create transformation |
| `POST` | `/api/silver/{id}/transformations/{tid}/chat` | AI chat |
| `POST` | `/api/silver/{id}/transformations/{tid}/dry-run` | Dry-run code |
| `POST` | `/api/silver/{id}/transformations/{tid}/confirm` | Confirm version |
| `POST` | `/api/silver/{id}/upload-to-silver` | Execute pipeline |

### Gold Transformations
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/gold/{id}/transformations` | Create transformation |
| `POST` | `/api/gold/{id}/transformations/{tid}/chat` | AI chat |
| `POST` | `/api/gold/{id}/refresh-schema` | Refresh schema from Silver |
| `POST` | `/api/gold/{id}/upload-to-gold` | Execute pipeline |
| `POST` | `/api/gold/{id}/push-to-postgres` | Export to PostgreSQL |
| `GET` | `/api/gold/{id}/preview` | Preview Gold data |
| `GET` | `/api/gold/{id}/postgres-pushes` | Push history |

### Kafka & Data Sources
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/pipelines/kafka/test-connection` | Test Kafka connectivity |
| `POST` | `/api/pipelines/kafka/topics` | List available Kafka topics |

### DAGs
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/dags/{id}/generate` | Generate all DAGs |
| `GET` | `/api/dags/{id}` | List generated DAGs |
| `DELETE` | `/api/dags/{id}/{dag_id}` | Delete a DAG file |

> 📖 Full interactive API docs available at **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GEMINI_API_KEY` | — | Google Gemini API key (required for AI features) |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Gemini model to use |
| `DATABASE_URL` | `postgresql+psycopg2://pipeline:pipeline123@postgres:5432/autonomous_pipeline` | PostgreSQL connection |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO endpoint |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |
| `SPARK_MASTER_URL` | `local[*]` | Spark master URL |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |
| `BACKEND_URL` | `http://backend:8000` | Backend URL (used by Airflow DAGs) |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host for Gold push |
| `POSTGRES_PORT` | `5432` | PostgreSQL port for Gold push |
| `POSTGRES_USER` | `pipeline` | PostgreSQL user for Gold push |
| `POSTGRES_PASSWORD` | `pipeline123` | PostgreSQL password for Gold push |
| `POSTGRES_DB` | `autonomous_pipeline` | PostgreSQL database for Gold push |
| `AIRFLOW_UID` | `1000` | Airflow user ID |

---

## License

This project is for educational and development purposes.

---

