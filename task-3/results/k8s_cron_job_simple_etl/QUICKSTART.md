# Быстрый старт для демонстрации

## Одной командой - полное развёртывание

```bash
# 1. Запустить Minikube
minikube start --cpus=2 --memory=4096

# 2. Перейти в директорию проекта
cd task-3/results/k8s_cron_job_simple_etl

# 3. Собрать Docker образ
eval $(minikube docker-env)
docker build -t shipments-etl:latest .

# 4. Развернуть всё
kubectl apply -f deploy/

# 5. Дождаться готовности PostgreSQL (2-3 минуты)
kubectl wait --for=condition=ready pod -l app=postgres -n shipments-etl --timeout=180s

# 6. Запустить тестовое задание
kubectl apply -f deploy/job.yaml

# 7. Следить за выполнением
kubectl logs -n shipments-etl -l component=etl-job --tail=100 -f
```

## Проверка результата

```bash
# Получить имя Pod
POD=$(kubectl get pod -n shipments-etl -l component=etl-job -o jsonpath='{.items[0].metadata.name}')

# Посмотреть CSV файлы
kubectl exec -n shipments-etl $POD -- ls -lh /data

# Посмотреть первые строки CSV
kubectl exec -n shipments-etl $POD -- head -20 /data/shipments_*.csv
```

## Для скриншотов/видео

```bash
# 1. Список CronJob
kubectl get cronjobs -n shipments-etl

# 2. Список Jobs
kubectl get jobs -n shipments-etl

# 3. Список Pods
kubectl get pods -n shipments-etl

# 4. Логи выполнения
kubectl logs -n shipments-etl $POD

# 5. Количество записей в БД
kubectl exec -n shipments-etl $(kubectl get pod -n shipments-etl -l app=postgres -o jsonpath='{.items[0].metadata.name}') -- \
  psql -U postgres -d shipments_db -c "SELECT COUNT(*) FROM shipments;"

# 6. CSV файлы
kubectl exec -n shipments-etl $POD -- ls -lh /data
```

## Очистка

```bash
kubectl delete namespace shipments-etl
minikube stop
```

## Расписание CronJob

- **Production**: `0 20 * * *` (каждый день в 20:00 UTC)
- **Для тестирования**: Измените на `*/5 * * * *` в `deploy/cronjob.yaml`

## Архитектура

- ✅ Clean Architecture (Domain → Application → Infrastructure → Presentation)
- ✅ Async Python stack (asyncpg, aiofiles)
- ✅ PostgreSQL с тестовыми данными (~5,000 shipments)
- ✅ Kubernetes CronJob для автоматического запуска
- ✅ PersistentVolume для хранения CSV файлов
- ✅ ConfigMap и Secret для конфигурации
- ✅ Multi-stage Dockerfile с uv
