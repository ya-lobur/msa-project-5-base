# Implementing task-1

- Implement what is asked in the `task-1/README.md`.

## For the first part of the task with "Обосновать выбор технологического решения"

- use python stack and Airflow as primary technologies
- create C4 diagrams (level 1 and level 2) to show the consept, put it into `task-1/results/diagrams/puml`
- fill `task-1/results/README.md`

When you are done, stop and let me review the diagrams and the README.

## For the second part of the task with "Продемонстрировать POC (Proof of Concept) на примере простого проекта"

- I prepared a dedicated directory for this task in `task-1/results/marketing_etl_mvp`
- Build a simple project with Airflow and async Python stack.
- Prepare docker and docker-compose files
- setup ruff linter
- prefer using clean architecture
- You are allowed to use stubs with logs instead of making real requests or db queries
- Create and use the app di container having all the necessary dependencies/resources
- You cannot mock/stub Airflow, you need to set it up and run via docker-compose (so the app and Airflow can
  communicate)
