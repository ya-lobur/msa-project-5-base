# Команды для демонстрации работы системы

## Полный цикл демонстрации

### 1. Подготовка среды

```bash
# Запустить Minikube
minikube start --cpus=2 --memory=4096

# Проверить статус
minikube status
kubectl cluster-info
```

### 2. Сборка и развёртывание

```bash
# Перейти в директорию проекта
cd task-3/results/k8s_cron_job_simple_etl

# Использовать Docker daemon Minikube
eval $(minikube docker-env)

# Собрать Docker образ
docker build -t shipments-etl:latest .

# Проверить образ
docker images | grep shipments-etl
```

### 3. Развёртывание в Kubernetes

```bash
# Применить все манифесты
kubectl apply -f deploy/namespace.yaml
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/postgres-deployment.yaml
kubectl apply -f deploy/postgres-service.yaml
kubectl apply -f deploy/pvc.yaml
kubectl apply -f deploy/cronjob.yaml

# Или одной командой:
kubectl apply -f deploy/

# Проверить созданные ресурсы
kubectl get all -n shipments-etl
```

### 4. Ожидание готовности PostgreSQL

```bash
# Дождаться готовности PostgreSQL (2-3 минуты)
kubectl wait --for=condition=ready pod -l app=postgres -n shipments-etl --timeout=180s

# Проверить статус PostgreSQL
kubectl get pods -n shipments-etl -l app=postgres

# Проверить логи инициализации
kubectl logs -n shipments-etl -l app=postgres --tail=50
```

### 5. Проверка базы данных

```bash
# Получить имя Pod PostgreSQL
POSTGRES_POD=$(kubectl get pod -n shipments-etl -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Проверить количество записей в таблицах
kubectl exec -n shipments-etl $POSTGRES_POD -- psql -U postgres -d shipments_db -c "
SELECT
    'clients' as table_name, COUNT(*) as count FROM clients
UNION ALL
SELECT 'drivers', COUNT(*) FROM drivers
UNION ALL
SELECT 'vehicles', COUNT(*) FROM vehicles
UNION ALL
SELECT 'shipments', COUNT(*) FROM shipments
UNION ALL
SELECT 'shipment_events', COUNT(*) FROM shipment_events;
"

# Посмотреть примеры данных
kubectl exec -n shipments-etl $POSTGRES_POD -- psql -U postgres -d shipments_db -c "SELECT * FROM shipments LIMIT 5;"
```

### 6. Запуск ручного тестового задания

```bash
# Запустить Job вручную для тестирования
kubectl apply -f deploy/job.yaml

# Проверить статус Job
kubectl get jobs -n shipments-etl

# Следить за статусом Pod
kubectl get pods -n shipments-etl -l component=etl-job -w
```

### 7. Просмотр логов выполнения

```bash
# Получить имя Pod ETL задания
ETL_POD=$(kubectl get pod -n shipments-etl -l component=etl-job -o jsonpath='{.items[0].metadata.name}')

# Посмотреть логи в реальном времени
kubectl logs -n shipments-etl $ETL_POD -f

# Или по label
kubectl logs -n shipments-etl -l component=etl-job --tail=100 -f
```

### 8. Проверка результатов (CSV файлы)

```bash
# Подождать пока Pod перейдёт в состояние Completed
kubectl wait --for=condition=Ready pod -l component=etl-job -n shipments-etl --timeout=300s || true

# Проверить CSV файлы
kubectl exec -n shipments-etl $ETL_POD -- ls -lh /data

# Посмотреть содержимое CSV (первые 10 строк)
kubectl exec -n shipments-etl $ETL_POD -- head -10 /data/shipments_*.csv

# Подсчитать количество строк в CSV
kubectl exec -n shipments-etl $ETL_POD -- sh -c "wc -l /data/shipments_*.csv"

# Скопировать CSV файл локально
CSV_FILE=$(kubectl exec -n shipments-etl $ETL_POD -- ls /data | grep shipments)
kubectl cp shipments-etl/$ETL_POD:/data/$CSV_FILE ./shipments_export.csv
echo "CSV файл скопирован в ./shipments_export.csv"
```

### 9. Проверка CronJob

```bash
# Посмотреть статус CronJob
kubectl get cronjobs -n shipments-etl

# Детальная информация о CronJob
kubectl describe cronjob shipments-etl-cronjob -n shipments-etl

# Проверить расписание
kubectl get cronjob shipments-etl-cronjob -n shipments-etl -o yaml | grep schedule
```

### 10. Тестирование с частым расписанием (опционально)

```bash
# Применить CronJob с запуском каждые 5 минут
kubectl apply -f deploy/cronjob-test.yaml

# Дождаться автоматического запуска (до 5 минут)
# Следить за созданием новых Jobs
kubectl get jobs -n shipments-etl -w

# Проверить накопление CSV файлов
kubectl exec -n shipments-etl $ETL_POD -- ls -lh /data
```

## Команды для скриншотов/видео

### Обзор ресурсов

```bash
# 1. Все ресурсы в namespace
kubectl get all -n shipments-etl

# 2. CronJob
kubectl get cronjobs -n shipments-etl -o wide

# 3. Jobs
kubectl get jobs -n shipments-etl

# 4. Pods
kubectl get pods -n shipments-etl -o wide

# 5. ConfigMaps и Secrets
kubectl get configmaps,secrets -n shipments-etl

# 6. PVC
kubectl get pvc -n shipments-etl
```

### Детальная информация

```bash
# Описание CronJob
kubectl describe cronjob shipments-etl-cronjob -n shipments-etl

# Описание Job
kubectl describe job -n shipments-etl -l component=etl-job

# Логи ETL задания
kubectl logs -n shipments-etl -l component=etl-job --tail=100
```

### Проверка данных

```bash
# Количество записей в БД
kubectl exec -n shipments-etl $POSTGRES_POD -- psql -U postgres -d shipments_db -c "SELECT COUNT(*) FROM shipments;"

# CSV файлы
kubectl exec -n shipments-etl $ETL_POD -- ls -lh /data

# Содержимое CSV
kubectl exec -n shipments-etl $ETL_POD -- head -20 /data/shipments_*.csv
```

## Очистка

```bash
# Удалить namespace (все ресурсы)
kubectl delete namespace shipments-etl

# Остановить Minikube
minikube stop

# Удалить Minikube кластер (опционально)
minikube delete
```

## Полезные команды для отладки

```bash
# Проверить события в namespace
kubectl get events -n shipments-etl --sort-by='.lastTimestamp'

# Проверить ресурсы
kubectl top pods -n shipments-etl
kubectl top nodes

# Войти в Pod для отладки
kubectl exec -it -n shipments-etl $ETL_POD -- /bin/sh

# Проверить переменные окружения в Pod
kubectl exec -n shipments-etl $ETL_POD -- env | grep DB_

# Форсировать создание Job из CronJob
kubectl create job --from=cronjob/shipments-etl-cronjob manual-run-1 -n shipments-etl
```

## Ожидаемые результаты

После выполнения всех команд вы должны увидеть:

1. ✅ CronJob создан с расписанием `0 20 * * *`
2. ✅ Job успешно выполнен (статус `Completed`)
3. ✅ В логах видно:
   - Подключение к БД
   - Обработка записей батчами
   - Успешное создание CSV файла
   - Количество экспортированных записей (~5000)
4. ✅ CSV файл создан в `/data` с именем `shipments_YYYYMMDD_HHMMSS.csv`
5. ✅ CSV содержит все поля из таблицы shipments
6. ✅ PostgreSQL содержит тестовые данные в 5 таблицах

## Типичные проблемы и решения

### Pod не запускается

```bash
kubectl describe pod -n shipments-etl <pod-name>
kubectl logs -n shipments-etl <pod-name>
```

### Образ не найден

```bash
# Убедитесь, что используется Docker daemon Minikube
eval $(minikube docker-env)
docker images | grep shipments-etl

# Пересобрать образ
docker build -t shipments-etl:latest .
```

### PostgreSQL не готов

```bash
# Проверить статус
kubectl get pods -n shipments-etl -l app=postgres

# Проверить логи
kubectl logs -n shipments-etl -l app=postgres

# Подождать дольше
kubectl wait --for=condition=ready pod -l app=postgres -n shipments-etl --timeout=300s
```

### CronJob не создаёт Job

```bash
# Проверить расписание и следующий запуск
kubectl get cronjob shipments-etl-cronjob -n shipments-etl

# Для тестирования использовать cronjob-test.yaml (каждые 5 минут)
kubectl apply -f deploy/cronjob-test.yaml
```
