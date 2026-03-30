"""
Marketing ETL DAG demonstrating:
- Reading from data sources (database, file system)
- Data analysis and conditional branching
- Email notifications on success/failure
- Retry policies
"""

import asyncio
from datetime import datetime, timedelta

import structlog
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()

# Default arguments for all tasks
default_args = {
    "owner": "marketing-team",
    "depends_on_past": False,
    "email": ["marketing@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="marketing_etl_pipeline",
    default_args=default_args,
    description="Marketing ETL pipeline with Clean Architecture",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2026, 3, 19),
    catchup=False,
    tags=["marketing", "etl", "poc"],
)
def marketing_etl_pipeline():
    """Main Marketing ETL Pipeline."""

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def read_data_sources():
        """
        Read data from multiple sources (PostgreSQL, CSV files).
        Demonstrates reading from data sources.
        """
        from src.container import Container

        logger.info("Starting data reading task")

        # Initialize DI container
        container = Container()

        # Get repositories
        postgres_repo = container.postgres_repository()
        csv_repo = container.csv_repository()

        # Read data (using stubs)
        customers = asyncio.run(postgres_repo.get_customers(limit=100))
        orders = asyncio.run(postgres_repo.get_orders(limit=100))
        deliveries = asyncio.run(csv_repo.get_deliveries())

        logger.info(
            "Data read successfully",
            customers_count=len(customers),
            orders_count=len(orders),
            deliveries_count=len(deliveries),
        )

        return {
            "customers_count": len(customers),
            "orders_count": len(orders),
            "deliveries_count": len(deliveries),
            "active_customers": len([c for c in customers if c.is_active]),
            "pending_orders": len([o for o in orders if o.status == "pending"]),
        }

    @task(retries=2)
    def analyze_data(data_stats: dict):
        """
        Analyze the data and prepare for branching decision.
        """
        logger.info("Analyzing data", stats=data_stats)

        analysis_result = {
            "stats": data_stats,
            "has_pending_orders": data_stats["pending_orders"] > 0,
            "customer_ratio": data_stats["active_customers"] / max(data_stats["customers_count"], 1),
        }

        logger.info("Analysis completed", result=analysis_result)
        return analysis_result

    def decide_branch(**context):
        """
        Branching logic based on data analysis.
        Demonstrates conditional branching in the pipeline.
        """
        ti = context["ti"]
        analysis = ti.xcom_pull(task_ids="analyze_data")

        logger.info("Making branching decision", analysis=analysis)

        # Branch based on pending orders
        if analysis["has_pending_orders"]:
            logger.info("Branch: Processing pending orders path")
            return "process_pending_orders"
        else:
            logger.info("Branch: No pending orders path")
            return "skip_pending_processing"

    @task
    def process_pending_orders():
        """
        Process pending orders (executed when there are pending orders).
        """
        from src.container import Container

        logger.info("Processing pending orders")

        container = Container()
        use_case = container.process_customer_data_use_case()

        # Execute the use case
        result = asyncio.run(use_case.execute())

        logger.info("Pending orders processed", result=result)
        return result

    @task
    def skip_pending_processing():
        """
        Skip pending orders processing (executed when no pending orders).
        """
        logger.info("No pending orders to process, skipping")
        return {"status": "skipped", "reason": "no_pending_orders"}

    @task(trigger_rule="none_failed")
    def generate_report(**context):
        """
        Generate final report regardless of which branch was taken.
        Uses trigger_rule='none_failed' to execute even if some tasks were skipped.
        """
        ti = context["ti"]

        data_stats = ti.xcom_pull(task_ids="read_data_sources")
        analysis = ti.xcom_pull(task_ids="analyze_data")

        logger.info("Generating final report", data_stats=data_stats, analysis=analysis)

        report = {
            "timestamp": datetime.now().isoformat(),
            "total_customers": data_stats["customers_count"],
            "active_customers": data_stats["active_customers"],
            "total_orders": data_stats["orders_count"],
            "pending_orders": data_stats["pending_orders"],
            "customer_activity_ratio": f"{analysis['customer_ratio']:.2%}",
        }

        logger.info("Report generated", report=report)
        return report

    @task(trigger_rule="none_failed")
    def send_notification(**context):
        """
        Send notification about pipeline completion.
        Demonstrates programmatic notification.
        """
        from src.container import Container

        ti = context["ti"]
        report = ti.xcom_pull(task_ids="generate_report")

        logger.info("Sending success notification", report=report)

        container = Container()
        notification_service = container.email_notification_service()

        message = f"Marketing ETL pipeline completed successfully. Report: {report}"
        asyncio.run(notification_service.send_success_notification(message))

        return {"notification_sent": True}

    # Additional email notification using EmailOperator
    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="marketing@example.com",
        subject="Marketing ETL Pipeline - Success",
        html_content="""
        <h3>Marketing ETL Pipeline Completed Successfully</h3>
        <p>Execution Date: {{ ds }}</p>
        <p>All tasks completed without errors.</p>
        <p>Check the logs for detailed report.</p>
        """,
        trigger_rule="none_failed",
    )

    send_failure_email = EmailOperator(
        task_id="send_failure_email",
        to="marketing@example.com",
        subject="Marketing ETL Pipeline - FAILURE",
        html_content="""
        <h3>Marketing ETL Pipeline FAILED</h3>
        <p>Execution Date: {{ ds }}</p>
        <p>Please check the logs for error details.</p>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Run ID: {{ run_id }}</p>
        """,
        trigger_rule="one_failed",
    )

    # Define task dependencies
    data_stats = read_data_sources()
    analysis = analyze_data(data_stats)

    # Branching operator
    branch_decision = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=decide_branch,
    )

    # Branch tasks
    pending_orders_task = process_pending_orders()
    skip_task = skip_pending_processing()

    # Report and notification tasks
    report_task = generate_report()
    notification_task = send_notification()

    # Task flow
    analysis >> branch_decision
    branch_decision >> [pending_orders_task, skip_task]
    [pending_orders_task, skip_task] >> report_task
    report_task >> notification_task
    notification_task >> [send_success_email, send_failure_email]


# Instantiate the DAG
marketing_dag = marketing_etl_pipeline()
