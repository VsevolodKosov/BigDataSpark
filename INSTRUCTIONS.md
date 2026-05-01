# Инструкция по запуску Spark-джобов

## Предварительно

Скачать JDBC-драйверы в папку `jars/` (так как Maven Central недоступен из Docker-контейнера — драйвер скачивается на хосте и передаётся через `--jars`):
(Уже находится в репо)

```bash
mkdir jars
curl -L -o jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

Запустить контейнеры:

```bash
docker-compose up -d
```

## Шаг 1 — Загрузка CSV в PostgreSQL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/postgresql-42.7.3.jar \
  --driver-class-path /opt/spark/extra-jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/00_load_csv.py
```

## Шаг 2 — Построение схемы «Звезда» в PostgreSQL

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/postgresql-42.7.3.jar \
  --driver-class-path /opt/spark/extra-jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/01_star_schema.py
```

## Шаг 3 — Создание отчётов в ClickHouse

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/postgresql-42.7.3.jar \
  --driver-class-path /opt/spark/extra-jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/02_clickhouse.py
```
