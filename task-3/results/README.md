# Результаты

## Исходники

- [приложуха](k8s_cron_job_simple_etl)
- [конфиги](k8s_cron_job_simple_etl/deploy)

## Proof of Work

1. После применения конфигов и того как крон-джоба пару раз отработала на дашборде видно:
    - Саму джобу ![dashboard_cronjob.png](img/dashboard_cronjob.png)
    - Отработавшие поды ![dashboard_succeeded_pods.png](img/dashboard_succeeded_pods.png)
2. Логи из подов ![pod_logs.png](img/pod_logs.png)
3. Логи из kubectl

```text
❯ kubectl get pods -n shipments-etl                                                                          
NAME                                   READY   STATUS      RESTARTS   AGE
postgres-6fdddb4c6-w7zq8               1/1     Running     0          60m
shipments-etl-cronjob-29575835-774vp   0/1     Completed   0          14m
shipments-etl-cronjob-29575840-cbdl2   0/1     Completed   0          9m1s
shipments-etl-cronjob-29575845-nppcn   0/1     Completed   0          4m1s
```

3. Файл с выгрузкой я сохраняю в хранилище в кубе, при желании его можно найти и
   достать:
    - ![csv_files_in_volume.png](img/csv_files_in_volume.png)
    - ![cp_csv.png](img/cp_csv.png)
4. Файл корректно открывается ![csv_open.png](img/csv_open.png)
    - Данные соответствуют тем, что весть в бд (из них и формируется выгрузка) ![db_data.png](img/db_data.png)