# Shipments ETL - Kubernetes CronJob

Система ETL на базе Kubernetes CronJob для ежедневной выгрузки данных о перевозках в CSV-файлы для аналитики.

## Описание проекта

Онлайн-платформа грузоперевозок выгружает данные в специализированное хранилище для аналитиков каждый день в 20:00. На
основе выгруженных данных обновляются дашборды дневной отчётности.

### Архитектура

Проект реализован с использованием **Clean Architecture**:

- **Domain Layer** (`src/domain/`) - бизнес-сущности и интерфейсы репозиториев
- **Application Layer** (`src/application/`) - бизнес-логика и use cases (сервис экспорта)
- **Infrastructure Layer** (`src/infrastructure/`) - реализации для БД (asyncpg) и хранилища (CSV)
- **Presentation Layer** (`src/presentation/`) - CLI интерфейс

### Технологический стек

- **Python 3.12+** с асинхронным стеком
- **asyncpg** - асинхронный драйвер PostgreSQL
- **aiofiles** - асинхронная работа с файлами
- **pydantic-settings** - управление конфигурацией
- **uv** - менеджер пакетов
- **PostgreSQL 16** - база данных
- **Kubernetes** - оркестрация и планирование задач
- **Docker** - контейнеризация

### Таблицы базы данных

- `clients` - заказчики (~20,000 записей)
- `drivers` - водители (~2,000 записей)
- `vehicles` - транспорт (~1,000 записей)
- `shipments` - перевозки (~100,000 записей) - **экспортируется в CSV**
- `shipment_events` - события по перевозкам (~700,000 записей)

## Предварительные требования

- [Minikube](https://minikube.sigs.k8s.io/docs/start/) v1.30+
- [kubectl](https://kubernetes.io/docs/tasks/tools/) v1.27+
- [Docker](https://docs.docker.com/get-docker/) 20.10+
- Минимум 4GB RAM для Minikube

## Быстрый старт

### 1. Запуск Minikube

```shell
# Запустить Minikube с достаточными ресурсами
minikube start --cpus=2 --memory=4096

# Проверить статус
minikube status
```

### 2. Сборка Docker-образа

```shell
# Перейти в директорию проекта
cd task-3/results/k8s_cron_job_simple_etl

# Собрать образ в Minikube (использует Docker daemon Minikube)
eval $(minikube docker-env)
docker build -t shipments-etl:latest .

# Проверить, что образ создан
docker images | grep shipments-etl
```

### 3. Развёртывание в Kubernetes

```shell
# Создать namespace и базовые ресурсы
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/secret.yaml

# Развернуть PostgreSQL базу данных
kubectl apply -f deploy/postgres-deployment.yaml
kubectl apply -f deploy/postgres-service.yaml

# Дождаться готовности PostgreSQL
kubectl wait --for=condition=ready pod -l app=postgres -n shipments-etl --timeout=120s

# Создать PVC для хранения CSV файлов
kubectl apply -f deploy/pvc.yaml

# Развернуть CronJob
kubectl apply -f deploy/cronjob.yaml
```

### 4. Тестовый запуск

Для проверки работы без ожидания расписания используйте manual Job:

```shell
# Запустить задачу вручную
kubectl apply -f deploy/job.yaml

# Следить за статусом
kubectl get jobs -n shipments-etl -w

# Посмотреть логи
kubectl logs -n shipments-etl -l component=etl-job --tail=100 -f
```

Далее если надо получить доступ к файлу можно создать debug-pod:

```shell
# запускаем
kubectl apply -f deploy/debug_pod.yaml

# ждем запуска
kubectl get pod debug-etl-data -n shipments-etl -w

# заходим в под
kubectl exec -it -n shipments-etl debug-etl-data -- sh

# В самом поде
ls -lah /data
head -n 20 /data/shipments_20260326_180043.csv

# Скопировать на локалку
kubectl cp shipments-etl/debug-etl-data:/data/shipments_20260326_180043.csv ./shipments_20260326_180043.csv
```

## Управление и мониторинг

### Просмотр статуса CronJob

```shell
# Список CronJob
kubectl get cronjobs -n shipments-etl

# Детальная информация
kubectl describe cronjob shipments-etl-cronjob -n shipments-etl

# История запусков
kubectl get jobs -n shipments-etl
```

### Просмотр логов

```shell
# Логи последнего выполнения
kubectl logs -n shipments-etl -l component=etl-job --tail=100

# Логи конкретного Pod
kubectl logs -n shipments-etl <pod-name>

# Следить за логами в реальном времени
kubectl logs -n shipments-etl -l component=etl-job -f
```

### Как пробосить порт

```shell
# Получаем под нужный
kubectl get pods -n shipments-etl -l app=postgres

# Пробрасываем
kubectl port-forward -n shipments-etl pod/postgres-6fdddb4c6-w7zq8 5432:5432
```

### Доступ к CSV файлам

```shell
# Получить Pod с PVC
kubectl get pods -n shipments-etl -l component=etl-job

# Посмотреть список CSV файлов
kubectl exec -n shipments-etl <pod-name> -- ls -lh /data

# Скопировать CSV файл локально
kubectl cp shipments-etl/<pod-name>:/data/shipments_20260325_200000.csv ./shipments.csv
```

### Доступ к PostgreSQL

```shell
# Подключиться к PostgreSQL
kubectl exec -it -n shipments-etl <postgres-pod-name> -- psql -U postgres -d shipments_db

# Проверить количество записей
kubectl exec -it -n shipments-etl <postgres-pod-name> -- \
  psql -U postgres -d shipments_db -c "SELECT COUNT(*) FROM shipments;"
```

## Конфигурация

### Расписание CronJob

По умолчанию: `0 20 * * *` (каждый день в 20:00 UTC)

Для тестирования можно изменить на `*/5 * * * *` (каждые 5 минут):

```shell
kubectl edit cronjob shipments-etl-cronjob -n shipments-etl
```

### Переменные окружения

Настройки в `deploy/configmap.yaml`:

- `DB_HOST` - хост базы данных (default: `postgres-service`)
- `DB_PORT` - порт PostgreSQL (default: `5432`)
- `DB_NAME` - имя базы данных (default: `shipments_db`)
- `BATCH_SIZE` - размер батча для обработки (default: `1000`)
- `OUTPUT_DIR` - директория для CSV (default: `/data`)
- `LOG_LEVEL` - уровень логирования (default: `INFO`)

Секретные данные в `deploy/secret.yaml`:

- `DB_USER` - пользователь БД (default: `postgres`)
- `DB_PASSWORD` - пароль БД (default: `postgres`)

**⚠️ Внимание:** В production используйте внешние системы управления секретами (Sealed Secrets, HashiCorp Vault, AWS
Secrets Manager).

## Разработка и отладка

### Локальный запуск

```shell
# Установить зависимости с помощью uv
uv sync

# Настроить переменные окружения
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=shipments_db
export DB_USER=postgres
export DB_PASSWORD=postgres
export BATCH_SIZE=1000
export OUTPUT_DIR=./data
export LOG_LEVEL=INFO

# Запустить приложение
python main.py
```

### Запуск PostgreSQL локально

```shell
docker run --name postgres-local -d \
  -e POSTGRES_DB=shipments_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16-alpine

# Инициализировать базу данных
docker exec -i postgres-local psql -U postgres -d shipments_db < init.sql
```

### Пересборка образа

```shell
eval $(minikube docker-env)
docker build -t shipments-etl:latest .

# Удалить старые Job/Pods для использования нового образа
kubectl delete job -n shipments-etl --all
```

## Структура проекта

```
.
├── src/
│   ├── domain/              # Бизнес-сущности и интерфейсы
│   │   ├── entities.py      # Shipment entity
│   │   └── repositories.py  # Repository interfaces
│   ├── application/         # Бизнес-логика
│   │   └── services.py      # ExportService
│   ├── infrastructure/      # Реализации
│   │   ├── database.py      # PostgreSQL repository
│   │   └── storage.py       # CSV storage writer
│   └── presentation/        # CLI интерфейс
│       └── cli.py           # Entry point
├── config/
│   └── settings.py          # Конфигурация приложения
├── deploy/                  # Kubernetes манифесты
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── postgres-deployment.yaml
│   ├── postgres-service.yaml
│   ├── pvc.yaml
│   ├── cronjob.yaml
│   └── job.yaml
├── Dockerfile              # Multi-stage сборка с uv
├── pyproject.toml          # Зависимости проекта
├── main.py                 # Entry point
├── init.sql                # SQL скрипт инициализации
└── README.md               # Документация
```

## Устранение неполадок

### Pod не запускается

```shell
# Проверить события
kubectl describe pod -n shipments-etl <pod-name>

# Проверить образ
kubectl get pod -n shipments-etl <pod-name> -o yaml | grep image
```

### Ошибка подключения к БД

```shell
# Проверить, что PostgreSQL запущен
kubectl get pods -n shipments-etl -l app=postgres

# Проверить логи PostgreSQL
kubectl logs -n shipments-etl -l app=postgres

# Проверить Service
kubectl get svc -n shipments-etl postgres-service
```

### CronJob не создаёт Job

```shell
# Проверить статус CronJob
kubectl get cronjob -n shipments-etl

# Проверить события
kubectl describe cronjob shipments-etl-cronjob -n shipments-etl

# Проверить расписание (может быть временная зона)
kubectl get cronjob shipments-etl-cronjob -n shipments-etl -o yaml | grep schedule
```

### Недостаточно ресурсов

```shell
# Проверить ресурсы Minikube
kubectl top nodes
kubectl top pods -n shipments-etl

# Увеличить ресурсы Minikube
minikube stop
minikube start --cpus=4 --memory=8192
```

## Очистка ресурсов

```shell
# Удалить все ресурсы в namespace
kubectl delete namespace shipments-etl

# Остановить Minikube
minikube stop

# Удалить Minikube кластер (опционально)
minikube delete
```

## Демонстрация работы

Для демонстрации работы системы:

1. Выполните команды из раздела "Быстрый старт"
2. Запустите manual Job: `kubectl apply -f deploy/job.yaml`
3. Следите за выполнением: `kubectl logs -n shipments-etl -l component=etl-job -f`
4. Проверьте созданный CSV файл:
   ```shell
   POD=$(kubectl get pod -n shipments-etl -l component=etl-job -o jsonpath='{.items[0].metadata.name}')
   kubectl exec -n shipments-etl $POD -- ls -lh /data
   ```

## Производственные рекомендации

1. **Secrets Management**: Используйте внешние системы (Vault, AWS Secrets Manager)
2. **Monitoring**: Настройте Prometheus + Grafana для мониторинга
3. **Alerting**: Настройте алерты на failed jobs
4. **Backup**: Регулярно делайте бэкапы PostgreSQL
5. **Resource Limits**: Настройте правильные лимиты CPU/Memory
6. **Storage**: Используйте S3/MinIO вместо PVC для CSV файлов
7. **Logging**: Интегрируйте с ELK/Loki для централизованных логов
8. **Data Retention**: Настройте политику удаления старых CSV файлов


