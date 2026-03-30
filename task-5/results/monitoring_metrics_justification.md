# Обоснование выбора метрик для мониторинга, логирования и оповещения

**Дата:** 2026-03-26
**Автор:** Iaroslav L

---

## Введение

Данный документ описывает архитектурное решение для внедрения централизованной системы мониторинга, логирования и
оповещения в TradeWare Warehouse System. Решение направлено на устранение существующих проблем с отсутствием
observability, обеспечение надежной работы под пиковыми нагрузками и своевременное обнаружение проблем в production.

## Текущие проблемы

1. **Отсутствие централизованного мониторинга** — невозможно отследить метрики производительности системы в реальном
   времени
2. **Разрозненные логи** — логи пишутся только в файлы, сложно корреляировать события между компонентами
3. **Нет проактивных оповещений** — проблемы обнаруживаются только после жалоб пользователей
4. **Невозможность root cause analysis** — сложно понять причину деградации производительности в пиковые периоды
5. **Отсутствие бизнес-метрик** — нет видимости по количеству обработанных отчетов, ошибкам валидации

## Архитектурное решение

### Выбранный стек технологий

| Компонент                 | Технология                                      | Назначение                                      |
|---------------------------|-------------------------------------------------|-------------------------------------------------|
| Метрики                   | **Prometheus**                                  | Сбор и хранение временных рядов метрик          |
| Визуализация              | **Grafana**                                     | Дашборды для визуализации метрик и алертинг     |
| Логирование               | **ELK Stack** (Elasticsearch, Logstash, Kibana) | Централизованное хранение и поиск по логам      |
| Оповещения                | **Alertmanager**                                | Управление алертами, маршрутизация, группировка |
| Инструментация            | **Micrometer** (Spring Boot Actuator)           | Экспорт метрик из приложений                    |
| Логирование               | **Logback** + **Logstash Encoder**              | Структурированные JSON логи                     |
| Трассировка (опционально) | **OpenTelemetry** + **Jaeger**                  | Distributed tracing для отладки                 |

### Обоснование выбора

#### Почему Prometheus + Grafana:

- ✅ Де-факто стандарт для мониторинга микросервисов
- ✅ Pull-based модель: Prometheus сам опрашивает targets
- ✅ Мощный язык запросов PromQL для анализа метрик
- ✅ Интеграция с Spring Boot Actuator из коробки
- ✅ Service discovery для динамического обнаружения сервисов в Kubernetes
- ✅ Grafana предоставляет богатые возможности визуализации

#### Почему ELK Stack:

- ✅ Elasticsearch — масштабируемое хранилище для поиска по логам
- ✅ Logstash — гибкая обработка и трансформация логов
- ✅ Kibana — мощный UI для поиска, фильтрации и визуализации логов
- ✅ Поддержка структурированных JSON логов
- ✅ Full-text search по логам
- ✅ Интеграция с Grafana для единого интерфейса

#### Почему Alertmanager:

- ✅ Интеграция с Prometheus
- ✅ Дедупликация и группировка алертов
- ✅ Маршрутизация по severity и командам
- ✅ Интеграции с Slack, PagerDuty, email

---

## 1. Метрики приложений (Application Metrics)

### 1.1 Метрики Spring Batch Jobs

| Метрика                          | Название в Prometheus                                          | Тип       | Описание                                            | Обоснование                                                                                          |
|----------------------------------|----------------------------------------------------------------|-----------|-----------------------------------------------------|------------------------------------------------------------------------------------------------------|
| **Job Execution Time**           | `spring_batch_job_duration_seconds`                            | Histogram | Время выполнения batch job от запуска до завершения | Критично для SLA: обработка 2000 строк за 30 сек. Позволяет обнаружить деградацию производительности |
| **Job Success Rate**             | `spring_batch_job_status_total` (label: status=SUCCESS/FAILED) | Counter   | Количество успешных/упавших job                     | Показатель надежности системы. Alert при падении success rate ниже 95%                               |
| **Records Processed per Second** | `spring_batch_records_processed_rate`                          | Gauge     | Скорость обработки записей (records/sec)            | Индикатор производительности. Alert при падении ниже порога                                          |
| **Records Read**                 | `spring_batch_item_read_count`                                 | Counter   | Количество прочитанных записей из CSV               | Для отслеживания прогресса обработки                                                                 |
| **Records Written**              | `spring_batch_item_write_count`                                | Counter   | Количество записанных записей в БД                  | Для валидации: read count должен примерно равняться write count (минус skip)                         |
| **Records Skipped**              | `spring_batch_item_skip_count`                                 | Counter   | Количество пропущенных невалидных записей           | Индикатор качества данных. Alert при резком росте                                                    |
| **Active Jobs Count**            | `spring_batch_active_jobs`                                     | Gauge     | Количество одновременно выполняющихся job           | Для мониторинга текущей нагрузки. Max = 150 (по требованиям)                                         |
| **Job Queue Size**               | `spring_batch_job_queue_size`                                  | Gauge     | Количество job в очереди на обработку               | Alert при росте очереди — признак перегрузки                                                         |
| **Chunk Processing Time**        | `spring_batch_chunk_duration_seconds`                          | Histogram | Время обработки одного chunk (500 записей)          | Для оптимизации chunk size и выявления узких мест                                                    |
| **Retry Count**                  | `spring_batch_retry_count`                                     | Counter   | Количество повторных попыток при ошибках            | Индикатор транзиентных проблем (сеть, БД)                                                            |
| **Job Start/End Events**         | `spring_batch_job_events_total` (label: event=START/END)       | Counter   | События старта и завершения job                     | Для аудита и трассировки                                                                             |

**Alert Rules:**

```yaml
# Превышение времени обработки
- alert: BatchJobTooSlow
  expr: histogram_quantile(0.95, spring_batch_job_duration_seconds) > 30
  for: 5m
  annotations:
    summary: "Batch job processing is too slow (95th percentile > 30s)"

# Высокий процент ошибок
- alert: BatchJobHighFailureRate
  expr: rate(spring_batch_job_status_total{status="FAILED"}[5m]) / rate(spring_batch_job_status_total[5m]) > 0.05
  for: 5m
  annotations:
    summary: "Batch job failure rate > 5%"

# Много skip записей
- alert: BatchJobHighSkipRate
  expr: rate(spring_batch_item_skip_count[5m]) > 100
  for: 5m
  annotations:
    summary: "High number of skipped records (possible data quality issue)"
```

---

### 1.2 Метрики Backend API (WildFly)

| Метрика                     | Название                                           | Тип       | Описание                                              | Обоснование                                             |
|-----------------------------|----------------------------------------------------|-----------|-------------------------------------------------------|---------------------------------------------------------|
| **HTTP Request Rate**       | `http_server_requests_total`                       | Counter   | Количество HTTP запросов (label: method, status, uri) | Мониторинг нагрузки на API                              |
| **HTTP Request Duration**   | `http_server_requests_seconds`                     | Histogram | Время обработки HTTP запросов                         | SLA на latency API endpoints                            |
| **HTTP Error Rate**         | `http_server_requests_total{status=~"5.."}`        | Counter   | Количество 5xx ошибок                                 | Alert при росте ошибок сервера                          |
| **Active HTTP Connections** | `http_server_active_connections`                   | Gauge     | Количество активных соединений                        | Мониторинг текущей нагрузки                             |
| **File Upload Size**        | `file_upload_size_bytes`                           | Histogram | Размер загружаемых CSV файлов                         | Анализ распределения размеров файлов                    |
| **File Validation Errors**  | `file_validation_errors_total` (label: error_type) | Counter   | Количество ошибок валидации файлов                    | Индикатор проблем с качеством данных на уровне загрузки |

---

### 1.3 Метрики базы данных

| Метрика                     | Название                                                | Тип       | Описание                                       | Обоснование                                         |
|-----------------------------|---------------------------------------------------------|-----------|------------------------------------------------|-----------------------------------------------------|
| **DB Connection Pool Size** | `hikaricp_connections_active`                           | Gauge     | Количество активных соединений в пуле          | Alert при исчерпании пула                           |
| **DB Connection Wait Time** | `hikaricp_connections_acquire_seconds`                  | Histogram | Время ожидания соединения из пула              | Индикатор перегрузки БД                             |
| **Query Execution Time**    | `db_query_duration_seconds` (label: query_type)         | Histogram | Время выполнения SQL запросов                  | Оптимизация медленных запросов                      |
| **DB Transaction Rate**     | `db_transactions_total` (label: status=COMMIT/ROLLBACK) | Counter   | Количество транзакций                          | Мониторинг транзакционной нагрузки                  |
| **Deadlock Count**          | `db_deadlocks_total`                                    | Counter   | Количество deadlock при параллельной обработке | Critical alert — требует немедленного вмешательства |

---

## 2. Инфраструктурные метрики (Infrastructure Metrics)

### 2.1 JVM Metrics

| Метрика               | Название                             | Тип       | Описание                            | Обоснование                            |
|-----------------------|--------------------------------------|-----------|-------------------------------------|----------------------------------------|
| **Heap Memory Usage** | `jvm_memory_used_bytes{area="heap"}` | Gauge     | Используемая heap память            | Alert при >85% использования           |
| **Heap Memory Max**   | `jvm_memory_max_bytes{area="heap"}`  | Gauge     | Максимальная heap память            | Для расчета процента использования     |
| **GC Pause Time**     | `jvm_gc_pause_seconds`               | Histogram | Время пауз на Garbage Collection    | Alert при длительных GC паузах (>1sec) |
| **GC Count**          | `jvm_gc_pause_total`                 | Counter   | Количество GC событий               | Мониторинг частоты GC                  |
| **Thread Count**      | `jvm_threads_live`                   | Gauge     | Количество активных потоков         | Мониторинг thread pool                 |
| **CPU Usage**         | `process_cpu_usage`                  | Gauge     | Процент использования CPU процессом | Alert при >80%                         |

**Alert Rules:**

```yaml
- alert: HighMemoryUsage
  expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85
  for: 5m
  annotations:
    summary: "JVM heap memory usage > 85%"

- alert: LongGCPauses
  expr: histogram_quantile(0.99, jvm_gc_pause_seconds) > 1
  for: 5m
  annotations:
    summary: "GC pauses > 1 second (p99)"
```

---

### 2.2 System Metrics (Node Exporter)

| Метрика             | Название                                                                | Тип     | Описание                                 | Обоснование                     |
|---------------------|-------------------------------------------------------------------------|---------|------------------------------------------|---------------------------------|
| **CPU Usage**       | `node_cpu_seconds_total`                                                | Counter | CPU time по режимам (user, system, idle) | Мониторинг загрузки CPU         |
| **Memory Usage**    | `node_memory_MemAvailable_bytes`                                        | Gauge   | Доступная оперативная память             | Alert при <10% свободной памяти |
| **Disk I/O**        | `node_disk_io_time_seconds_total`                                       | Counter | Время, потраченное на I/O операции       | Индикатор disk bottleneck       |
| **Disk Space**      | `node_filesystem_avail_bytes`                                           | Gauge   | Свободное место на диске                 | Alert при <20% свободного места |
| **Network Traffic** | `node_network_receive_bytes_total`, `node_network_transmit_bytes_total` | Counter | Входящий и исходящий сетевой трафик      | Мониторинг сетевой нагрузки     |

---

## 3. Бизнес-метрики (Business Metrics)

| Метрика                              | Название                                                | Тип     | Описание                                 | Обоснование                                     |
|--------------------------------------|---------------------------------------------------------|---------|------------------------------------------|-------------------------------------------------|
| **Files Processed Daily**            | `business_files_processed_total`                        | Counter | Количество обработанных файлов за сутки  | KPI для бизнеса: цель 150 файлов в пиковые часы |
| **Average File Size**                | `business_file_size_avg_bytes`                          | Gauge   | Средний размер обрабатываемого файла     | Анализ трендов                                  |
| **Records Processed Daily**          | `business_records_processed_total`                      | Counter | Количество обработанных записей за сутки | KPI: цель 1.2М записей в пиковые дни            |
| **Processing Success Rate**          | `business_processing_success_rate`                      | Gauge   | Процент успешно обработанных файлов      | SLA метрика: target >95%                        |
| **Average Processing Time per File** | `business_avg_processing_time_seconds`                  | Gauge   | Среднее время обработки одного файла     | SLA: <30 сек для 2000 строк                     |
| **User Upload Errors**               | `business_user_upload_errors_total` (label: error_type) | Counter | Ошибки загрузки от пользователей         | Для улучшения UX и обучения пользователей       |

**Пример бизнес-дашборда в Grafana:**

- Количество обработанных файлов по часам (столбчатая диаграмма)
- Success rate тренд (линейный график)
- Топ-5 типов ошибок валидации (pie chart)
- Среднее время обработки vs SLA (gauge)

---

## 4. Логирование (Logging)

### 4.1 Структура логов

Все логи должны быть в формате **JSON** для удобного парсинга в ELK.

**Обязательные поля:**

```json
{
  "timestamp": "2026-03-28T10:15:30.123Z",
  "level": "INFO",
  "logger": "com.tradeware.batch.service.JobLauncher",
  "message": "Batch job started",
  "thread": "batch-job-1",
  "trace_id": "a1b2c3d4e5f6",
  "span_id": "12345678",
  "service": "spring-batch-service",
  "environment": "production",
  "job_id": "inventory-update-job-20260328-101530",
  "user_id": "employee-12345",
  "file_name": "warehouse_report_2026-03-28.csv"
}
```

**Контекстные поля:**

- `trace_id` — для корреляции логов между сервисами (distributed tracing)
- `span_id` — для детализации trace
- `job_id` — уникальный идентификатор batch job
- `user_id` — кто инициировал операцию
- `file_name` — какой файл обрабатывается

---

### 4.2 Уровни логирования

| Уровень   | Когда использовать                      | Примеры событий                                                                                       | Обоснование                                            |
|-----------|-----------------------------------------|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| **ERROR** | Ошибки, требующие немедленного внимания | - Job упал с exception<br>- Database connection failed<br>- File not found в GCS                      | Alert в Alertmanager, требует реакции on-call инженера |
| **WARN**  | Потенциальные проблемы, не критичные    | - Retry attempt на транзиентную ошибку<br>- Высокое время обработки chunk<br>- Skip невалидной записи | Мониторинг трендов, может потребоваться вмешательство  |
| **INFO**  | Важные бизнес-события                   | - Job started/completed<br>- File uploaded<br>- X records processed<br>- Notification sent            | Аудит и понимание flow системы                         |
| **DEBUG** | Детальная информация для отладки        | - Query execution details<br>- Chunk processing steps<br>- Cache hits/misses                          | Включается только при troubleshooting                  |
| **TRACE** | Очень детальная информация              | - Method входы/выходы<br>- Variable values                                                            | Только для dev окружения                               |

**Production настройки:**

- `spring-batch-service`: INFO (ERROR/WARN алерты)
- `backend-api`: INFO
- `root`: WARN

---

### 4.3 Ключевые типы логов

#### Логи batch job lifecycle:

```json
// Job start
{
  "level": "INFO",
  "message": "Batch job started",
  "job_id": "job-12345",
  "job_name": "inventoryUpdateJob",
  "file_name": "report.csv",
  "file_size_bytes": 204800,
  "estimated_records": 2000
}

// Job completion
{
  "level": "INFO",
  "message": "Batch job completed successfully",
  "job_id": "job-12345",
  "duration_seconds": 28.5,
  "records_read": 2000,
  "records_written": 1998,
  "records_skipped": 2,
  "status": "COMPLETED"
}

// Job failure
{
  "level": "ERROR",
  "message": "Batch job failed",
  "job_id": "job-12345",
  "duration_seconds": 15.2,
  "error_type": "DatabaseConnectionException",
  "error_message": "Connection timeout after 30s",
  "stack_trace": "...",
  "retry_count": 3,
  "status": "FAILED"
}
```

#### Логи валидации:

```json
{
  "level": "WARN",
  "message": "Record validation failed",
  "job_id": "job-12345",
  "record_line_number": 157,
  "validation_errors": [
    {
      "field": "quantity",
      "error": "Must be positive integer"
    },
    {
      "field": "warehouse_id",
      "error": "Unknown warehouse ID"
    }
  ]
}
```

#### Логи производительности:

```json
{
  "level": "WARN",
  "message": "Chunk processing slow",
  "job_id": "job-12345",
  "chunk_number": 4,
  "chunk_size": 500,
  "processing_time_seconds": 12.5,
  "threshold_seconds": 6.0
}
```

#### Логи database operations:

```json
{
  "level": "DEBUG",
  "message": "Batch insert executed",
  "job_id": "job-12345",
  "table": "inventory",
  "records_inserted": 500,
  "query_duration_ms": 345
}
```

---

### 4.4 Log aggregation flow

```
Spring Batch Service (Logback)
    → JSON logs to stdout
    → Filebeat (log shipper)
    → Logstash (parsing, filtering, enrichment)
    → Elasticsearch (storage, indexing)
    → Kibana (search, visualization)
```

**Elasticsearch индексы:**

- `logs-spring-batch-*` — логи Spring Batch сервиса
- `logs-backend-api-*` — логи Backend API
- `logs-frontend-*` — логи Frontend (если есть)

**Retention policy:**

- Hot tier: последние 7 дней (быстрый SSD)
- Warm tier: 8-30 дней (медленнее, дешевле)
- Cold tier: 31-90 дней (архив)
- Удаление: старше 90 дней

---

## 5. Distributed Tracing (опционально)

### Зачем нужен tracing:

В микросервисной архитектуре один запрос пользователя проходит через несколько сервисов:

```
User → Web App → Backend API → Spring Batch Service → PostgreSQL
```

Tracing позволяет:

- Визуализировать полный путь запроса
- Понять, где происходит задержка (latency breakdown)
- Корреляция логов по `trace_id`

### Технология: OpenTelemetry + Jaeger

**Интеграция:**

1. OpenTelemetry Java Agent в каждом сервисе
2. Автоматическая инструментация HTTP, JDBC, Spring
3. Экспорт traces в Jaeger Collector
4. Визуализация в Jaeger UI

**Пример trace:**

```
Trace ID: a1b2c3d4e5f6
├─ Span 1: HTTP POST /api/upload [Backend API] (150ms)
│  ├─ Span 2: Save file to GCS (80ms)
│  └─ Span 3: Trigger batch job (20ms)
│     └─ Span 4: Job execution [Spring Batch] (28000ms)
│        ├─ Span 5: Read CSV from GCS (500ms)
│        ├─ Span 6: Process records (25000ms)
│        │  ├─ Span 7: Query reference DB (2000ms)
│        │  └─ Span 8: Batch insert to inventory DB (23000ms)  ← Bottleneck!
│        └─ Span 9: Send notification (100ms)
```

Видно, что узкое место — batch insert в inventory DB.

**Sampling:**

- Production: 10% запросов (снижение overhead)
- Staging: 100% запросов
- Всегда trace для requests с ошибками

---

## 6. Алерты (Alerting)

### 6.1 Критические алерты (P1 - immediate action)

| Alert                 | Условие                                                    | Действие                    |
|-----------------------|------------------------------------------------------------|-----------------------------|
| **ServiceDown**       | `up{job="spring-batch"} == 0`                              | PagerDuty → on-call инженер |
| **HighErrorRate**     | `rate(http_server_requests_total{status=~"5.."}[5m]) > 10` | PagerDuty                   |
| **DatabaseDown**      | `db_up == 0`                                               | PagerDuty                   |
| **OutOfMemory**       | `jvm_memory_used_bytes/jvm_memory_max_bytes > 0.95`        | PagerDuty                   |
| **DiskSpaceCritical** | `node_filesystem_avail_bytes < 10%`                        | PagerDuty                   |

**Уведомления:** PagerDuty (phone call + SMS)

---

### 6.2 Важные алерты (P2 - action within 1 hour)

| Alert               | Условие                                       | Действие      |
|---------------------|-----------------------------------------------|---------------|
| **BatchJobTooSlow** | `p95(spring_batch_job_duration_seconds) > 30` | Slack channel |
| **HighMemoryUsage** | `heap usage > 85%`                            | Slack         |
| **HighCPUUsage**    | `cpu > 80% for 5m`                            | Slack         |
| **HighSkipRate**    | `rate(spring_batch_item_skip_count) > 100`    | Slack         |
| **LongGCPauses**    | `p99(jvm_gc_pause_seconds) > 1`               | Slack         |

**Уведомления:** Slack #alerts-production

---

### 6.3 Предупреждающие алерты (P3 - investigate)

| Alert                    | Условие                                   | Действие |
|--------------------------|-------------------------------------------|----------|
| **HighDiskUsage**        | `disk usage > 80%`                        | Email    |
| **SlowQueries**          | `p95(db_query_duration_seconds) > 2`      | Email    |
| **HighRetryRate**        | `rate(spring_batch_retry_count) > 50`     | Email    |
| **FileValidationErrors** | `rate(file_validation_errors_total) > 10` | Email    |

**Уведомления:** Email to dev team

---

### 6.4 Alerting маршрутизация

**Alertmanager configuration:**

```yaml
route:
  group_by: [ 'alertname', 'severity' ]
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'team-slack'

  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
      continue: true

    - match:
        severity: warning
      receiver: 'team-slack'

    - match:
        severity: info
      receiver: 'team-email'

receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: '<pagerduty_key>'

  - name: 'team-slack'
    slack_configs:
      - api_url: '<slack_webhook>'
        channel: '#alerts-production'

  - name: 'team-email'
    email_configs:
      - to: 'dev-team@tradeware.com'
```

---

## 7. Дашборды Grafana

### 7.1 Operational Dashboard (для DevOps)

**Панели:**

1. **Service Health**
    - Up/Down статус всех сервисов (зеленые/красные индикаторы)
    - Latency per service (линейный график)

2. **Batch Jobs**
    - Active jobs count (gauge)
    - Job queue size (gauge)
    - Job success rate (линейный график, target 95%)
    - Jobs processed per hour (столбчатая диаграмма)

3. **System Resources**
    - CPU usage per service (stacked area chart)
    - Memory usage per service (stacked area chart)
    - Disk I/O (линейный график)
    - Network traffic (линейный график)

4. **Database**
    - Connection pool usage (линейный график)
    - Query duration p50/p95/p99 (линейный график)
    - Transaction rate (линейный график)

---

### 7.2 Business Dashboard (для менеджмента)

**Панели:**

1. **Processing Volume**
    - Files processed today (big number stat)
    - Records processed today (big number stat)
    - Current processing rate (files/hour)

2. **Performance SLA**
    - Average processing time vs SLA (gauge: <30s)
    - Success rate vs SLA (gauge: >95%)
    - Uptime (gauge: >99.9%)

3. **Error Analysis**
    - Top 5 validation errors (pie chart)
    - Failed jobs trend (линейный график)

4. **Trends**
    - Files processed per day (last 30 days)
    - Average file size trend
    - Peak load hours (heat map)

---

### 7.3 Spring Batch Detailed Dashboard

**Панели:**

1. Job execution duration (histogram)
2. Records processed per second (линейный график)
3. Skip/Retry count per job (table)
4. Chunk processing time distribution (heat map)
5. Job status breakdown (pie chart: SUCCESS/FAILED/RUNNING)

---

## 8. Мониторинг инфраструктуры в Kubernetes

Если система развернута в Kubernetes:

**Дополнительные метрики:**

- `kube_pod_status_phase` — статус pods
- `kube_pod_container_resource_limits_cpu_cores` — CPU limits
- `kube_pod_container_resource_requests_memory_bytes` — memory requests
- `kube_deployment_status_replicas` — количество реплик

**Alert:**

```yaml
- alert: PodCrashLooping
  expr: rate(kube_pod_container_status_restarts_total[15m]) > 0
  annotations:
    summary: "Pod {{ $labels.pod }} is crash looping"
```

---

## 9. Стратегия внедрения

### Фаза 1: Foundation (1-2 недели)

- ✅ Развертывание Prometheus + Grafana
- ✅ Развертывание ELK Stack
- ✅ Настройка Node Exporter для system metrics
- ✅ Базовые дашборды

### Фаза 2: Application Instrumentation (2-3 недели)

- ✅ Интеграция Spring Boot Actuator + Micrometer
- ✅ Экспорт метрик из Spring Batch
- ✅ Структурированные JSON логи
- ✅ Filebeat/Logstash конфигурация

### Фаза 3: Alerting (1 неделя)

- ✅ Настройка Alertmanager
- ✅ Определение alert rules
- ✅ Интеграции с PagerDuty, Slack, Email

### Фаза 4: Advanced Observability (2-3 недели)

- ✅ OpenTelemetry трассировка
- ✅ Детальные бизнес-дашборды
- ✅ Log retention policies
- ✅ Runbooks для alerts

### Фаза 5: Optimization (ongoing)

- ✅ Тюнинг alert thresholds на основе baseline
- ✅ Добавление новых метрик по запросам
- ✅ Performance optimization на основе данных

---

## 10. Ожидаемые результаты

### Количественные:

- **MTTD (Mean Time To Detect)**: с ~30 минут до <2 минут
- **MTTR (Mean Time To Resolve)**: с ~2 часов до <30 минут
- **Visibility**: 100% покрытие метриками и логами
- **Alerting**: <5% false positive rate

### Качественные:

- Проактивное обнаружение проблем до жалоб пользователей
- Быстрый root cause analysis при инцидентах
- Capacity planning на основе реальных метрик
- Улучшение SLA и user experience

---

## Заключение

Предложенное решение по мониторингу, логированию и оповещению на базе Prometheus, Grafana, ELK и Alertmanager обеспечит
TradeWare необходимой observability для:

- Гарантированного выполнения SLA (30 сек на 2000 строк, >95% success rate)
- Обработки пиковых нагрузок (100-150 параллельных загрузок)
- Быстрого обнаружения и устранения проблем
- Принятия data-driven решений по оптимизации

Решение масштабируемо, интегрируется с существующим стеком и подготовлено для дальнейшей миграции в микросервисную
архитектуру.
