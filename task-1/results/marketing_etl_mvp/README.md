# Marketing ETL MVP

Proof of Concept (POC) demonstration of Apache Airflow-based batch processing system with Clean Architecture principles.

## Features Demonstrated

This POC demonstrates all required features:

1. **Reading from data sources** - PostgreSQL database and CSV file system
2. **Data analysis and conditional branching** - Pipeline branches based on pending orders count
3. **Email notifications** - Both success and failure notifications configured
4. **Retry policies** - Configurable retries with exponential backoff

## Architecture

### Clean Architecture Layers

```
src/
├── domain/              # Business logic (entities, interfaces)
│   ├── entities/       # Business entities (Customer, Order, Delivery)
│   └── interfaces/     # Abstract interfaces (DataSource, Notification)
├── application/         # Use cases (business logic orchestration)
│   └── use_cases/      # ProcessCustomerDataUseCase
├── infrastructure/      # External integrations
│   ├── config/         # Settings and configuration
│   ├── repositories/   # Data access (PostgreSQL, CSV)
│   └── services/       # External services (Email notifications)
└── container.py        # Dependency Injection container
```

### Technology Stack

- **Orchestration**: Apache Airflow 2.8.1
- **Language**: Python 3.11+ with AsyncIO
- **DI Container**: dependency-injector
- **Configuration**: pydantic-settings
- **Logging**: structlog
- **Database**: PostgreSQL 13
- **Message Broker**: Redis
- **Executor**: CeleryExecutor
- **Linting**: Ruff

## Prerequisites

- Docker and Docker Compose
- 4GB+ RAM available for Docker
- Ports available: 8080 (Airflow), 8025 (MailHog UI), 5432 (PostgreSQL)

## Quick Start

### 1. Setup Environment

```bash
# Clone or navigate to the project directory
cd task-1/results/marketing_etl_mvp

# Create .env file from template
cp .env.example .env

# On Linux, set your user ID to avoid permission issues
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Build and Start Services

```bash
# Build the custom Airflow image
docker-compose build

# Start all services
docker-compose up -d

# Wait for services to initialize (approximately 2-3 minutes)
# Check the status
docker-compose ps
```

### 3. Access Airflow Web UI

Open your browser and navigate to: http://localhost:8080

- **Username**: `airflow`
- **Password**: `airflow`

### 4. Run the DAG

1. In the Airflow UI, find the DAG named `marketing_etl_pipeline`
2. Toggle the DAG to "ON" (unpause it)
3. Click the "Play" button to trigger a manual run
4. Click on the DAG name to view the Graph/Grid view
5. Watch the tasks execute in real-time

### 5. View Email Notifications

Open MailHog UI: http://localhost:8025

You'll see email notifications sent during the pipeline execution.

## DAG Overview

### Pipeline Flow

```
read_data_sources
       |
  analyze_data
       |
  branch_decision
       |
   +---+---+
   |       |
process   skip
pending   pending
   |       |
   +---+---+
       |
generate_report
       |
send_notification
       |
    +--+--+
    |     |
success failure
 email   email
```

### Tasks Description

1. **read_data_sources**
   - Reads customer data from PostgreSQL (stub)
   - Reads order data from PostgreSQL (stub)
   - Reads delivery data from CSV file (stub)
   - **Retry**: 3 attempts with 2-minute delay

2. **analyze_data**
   - Analyzes the fetched data
   - Calculates statistics (pending orders, active customers)
   - **Retry**: 2 attempts

3. **branch_decision**
   - Conditional branching based on analysis
   - If pending orders > 0: process_pending_orders
   - If pending orders = 0: skip_pending_processing

4. **process_pending_orders** / **skip_pending_processing**
   - Executes business logic via DI container and use cases
   - Only one branch executes based on condition

5. **generate_report**
   - Generates final report with statistics
   - Trigger rule: `none_failed` (runs even if branch was skipped)

6. **send_notification**
   - Sends programmatic notification via EmailNotificationService
   - Logs notification details

7. **send_success_email** / **send_failure_email**
   - EmailOperator-based notifications
   - Success: `none_failed` trigger
   - Failure: `one_failed` trigger

## Retry Configuration

Default retry settings (applied to all tasks unless overridden):

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
```

Custom retry per task:
- `read_data_sources`: 3 retries
- `analyze_data`: 2 retries

## Email Notification Configuration

### SMTP Settings

The project uses MailHog (local SMTP server) for testing:

```yaml
AIRFLOW__SMTP__SMTP_HOST: mailhog
AIRFLOW__SMTP__SMTP_PORT: 1025
AIRFLOW__SMTP__SMTP_MAIL_FROM: airflow@example.com
```

### Notification Types

1. **Success notifications**: Sent when pipeline completes successfully
2. **Failure notifications**: Sent when any task fails
3. **Retry notifications**: Disabled (can be enabled with `email_on_retry: True`)

## Development

### Run Linting

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run ruff linter
ruff check src/ dags/

# Run ruff formatter
ruff format src/ dags/
```

### View Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# View Airflow task logs in the UI
# Navigate to DAG → Task → Logs
```

### Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Project Structure

```
marketing_etl_mvp/
├── dags/
│   └── marketing_etl_dag.py          # Main Airflow DAG
├── src/
│   ├── domain/
│   │   ├── entities/                 # Customer, Order, Delivery
│   │   └── interfaces/               # DataSource, Notification interfaces
│   ├── application/
│   │   └── use_cases/                # ProcessCustomerDataUseCase
│   ├── infrastructure/
│   │   ├── config/                   # Settings
│   │   ├── repositories/             # PostgreSQL, CSV repositories
│   │   └── services/                 # Email, CombinedDataSource
│   └── container.py                  # DI Container
├── tests/                            # Unit tests (future)
├── data/                             # CSV data files
├── logs/                             # Airflow logs
├── Dockerfile                        # Custom Airflow image
├── docker-compose.yml                # Service orchestration
├── init-db.sql                       # Database initialization
├── pyproject.toml                    # Python dependencies
├── .ruff.toml                        # Ruff linter config
└── README.md                         # This file
```

## Key Design Decisions

### 1. Clean Architecture

- **Domain layer** is independent of frameworks and external systems
- **Application layer** orchestrates business logic via use cases
- **Infrastructure layer** implements interfaces and handles external dependencies
- **Dependency Inversion**: High-level modules don't depend on low-level modules

### 2. Dependency Injection

Using `dependency-injector` for:
- Loose coupling between components
- Easy testing (can mock dependencies)
- Centralized configuration

### 3. Async Python

- All repositories and services use `async/await`
- Better performance for I/O-bound operations
- Demonstrates modern Python patterns

### 4. Stubs with Logging

- Real database queries and file I/O are stubbed
- Extensive structured logging shows what *would* happen
- Easy to replace stubs with real implementations

## Troubleshooting

### Permission Issues (Linux)

If you encounter permission issues:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down -v
docker-compose up -d
```

### Services Not Starting

Check service health:

```bash
docker-compose ps
docker-compose logs postgres
docker-compose logs redis
```

### DAG Not Appearing

1. Check DAG file syntax:
   ```bash
   docker-compose exec airflow-scheduler airflow dags list
   ```

2. Check for import errors:
   ```bash
   docker-compose logs airflow-scheduler | grep ERROR
   ```

### Email Not Sending

1. Check MailHog is running: http://localhost:8025
2. Verify SMTP settings in docker-compose.yml
3. Check task logs in Airflow UI

## Screenshots/Screencast

To demonstrate the POC, capture:

1. **Airflow Web UI** - DAG graph view showing all tasks
2. **DAG Run** - Tasks executing with colors (green=success, yellow=running)
3. **Task Logs** - Show structured logs from tasks
4. **Branching** - Demonstrate both branches (with/without pending orders)
5. **Email Notifications** - MailHog UI showing received emails
6. **Retry Behavior** - Simulate task failure to show retry

## Future Enhancements

- Replace stubs with real database queries (asyncpg)
- Implement actual CSV file reading (aiofiles)
- Add Kafka integration for event streaming
- Implement BigQuery/Redshift data loading
- Add comprehensive unit tests
- Implement data validation with Pydantic
- Add metrics and monitoring (Prometheus/Grafana)
- Implement Spark jobs for large-scale processing

## License

MIT
