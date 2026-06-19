# Streaming IoT Analytics with Kafka, PostgreSQL and Apache Flink

Учебный проект по потоковой обработке данных в реальном времени.

---

## Описание проекта

Python-генератор каждую секунду публикует IoT-события (температура и влажность) в Apache Kafka. Apache Flink читает этот поток, обогащает каждое событие названием типа устройства из справочника PostgreSQL и за каждую минуту вычисляет агрегаты по каждому типу устройства: среднюю температуру, медиану влажности и количество событий в окне. Результат пишется обратно в Kafka.

---

## Архитектура

```
┌─────────────┐   событие/сек    ┌───────────────────┐
│  generator  │ ───────────────► │  Kafka             │
│  (Python)   │                  │  topic: iot_events │
└─────────────┘                  └────────┬──────────┘
                                          │
                                          ▼
                                 ┌─────────────────────┐
                                 │   Apache Flink       │
                                 │   PyFlink DataStream │
                                 │                      │
                                 │  1. parse JSON       │
                                 │  2. enrich ──────────┼──► PostgreSQL
                                 │  3. window 1 min     │    device_types
                                 │  4. avg_temperature  │
                                 │     median_humidity  │
                                 │     events_count     │
                                 └──────────┬──────────┘
                                            │
                                            ▼
                                 ┌───────────────────────┐
                                 │  Kafka                 │
                                 │  topic: iot_aggregated │
                                 └───────────────────────┘
```

---

## Входное событие (topic: `iot_events`)

Генератор публикует JSON-сообщение раз в секунду:

```json
{
  "device_type_id": 1,
  "event_time":     "2026-04-20T12:30:05.123Z",
  "temperature":    23.5,
  "humidity":       61.2
}
```

| Поле             | Тип    | Описание                              |
|------------------|--------|---------------------------------------|
| `device_type_id` | int    | Тип устройства, случайно от 1 до 5    |
| `event_time`     | string | Время события, UTC, ISO-8601          |
| `temperature`    | float  | Температура, °C, диапазон 15–35       |
| `humidity`       | float  | Влажность, %, диапазон 30–90          |

---

## Справочник PostgreSQL (`device_types`)

Хранит расшифровку типов устройств. Flink загружает эту таблицу один раз при старте job и кладёт в память.

```sql
CREATE TABLE device_types (
    id          INTEGER PRIMARY KEY,
    type_name   VARCHAR(100) NOT NULL,
    description VARCHAR(255),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Данные в таблице:

| id | type_name               | description                                        |
|----|-------------------------|----------------------------------------------------|
| 1  | `temperature_sensor`    | Measures ambient air temperature in degrees Celsius |
| 2  | `humidity_sensor`       | Measures relative humidity as a percentage          |
| 3  | `pressure_sensor`       | Measures atmospheric pressure in hPa                |
| 4  | `co2_sensor`            | Measures CO2 concentration in the air in ppm        |
| 5  | `motion_sensor`         | Detects presence and movement in a monitored area   |

---

## Выходное событие (topic: `iot_aggregated`)

Flink публикует агрегат раз в минуту для каждого типа устройства:

```json
{
  "window_start":    "2026-04-20T12:30:00Z",
  "window_end":      "2026-04-20T12:31:00Z",
  "device_type_id":  1,
  "device_type_name": "temperature_sensor",
  "avg_temperature": 23.74,
  "median_humidity": 58.2,
  "events_count":    60
}
```

| Поле               | Описание                                       |
|--------------------|------------------------------------------------|
| `window_start`     | Начало минутного окна (UTC)                    |
| `window_end`       | Конец минутного окна (UTC)                     |
| `device_type_id`   | Тип устройства                                 |
| `device_type_name` | Название из справочника PostgreSQL             |
| `avg_temperature`  | Средняя температура за минуту                  |
| `median_humidity`  | Медиана влажности за минуту                    |
| `events_count`     | Количество событий, попавших в окно            |

---

## Структура проекта

```
big_data/
├── docker-compose.yml          # все сервисы в одной Docker-сети
│
├── postgres/
│   ├── ddl.sql                 # CREATE TABLE device_types
│   └── dml.sql                 # INSERT 5 типов устройств
│
├── generator/
│   ├── Dockerfile
│   ├── producer.py             # публикует 1 событие/сек в Kafka
│   └── requirements.txt        # confluent-kafka
│
├── flink/
│   ├── Dockerfile              # flink:1.18 + Python + Kafka JARs
│   ├── iot_job.py              # PyFlink DataStream job
│   └── requirements.txt        # apache-flink, psycopg2-binary
│
└── kafka/
    └── create_topics.sh        # создаёт iot_events и iot_aggregated
```

---

## Команды запуска

### 1. Запустить все сервисы

```bash
docker compose up -d --build
```

Подождать ~60 секунд, пока все сервисы пройдут healthcheck. Следить за статусом:

```bash
docker compose ps
```

Все сервисы должны показывать `healthy` или `running`.

### 2. Проверить PostgreSQL

```bash
# Проверить доступность
docker exec postgres pg_isready -U iot_user -d iot_db

# Посмотреть справочник устройств
docker exec -it postgres psql -U iot_user -d iot_db \
  -c "SELECT * FROM device_types;"
```

Ожидаемый результат:
```
 id |      type_name      |          description
----+---------------------+--------------------------------
  1 | temperature_sensor  | Measures ambient air temperature
  2 | humidity_sensor     | Measures relative humidity
  ...
```

### 3. Проверить Kafka topics

```bash
# Список топиков
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

# Убедиться, что iot_events и iot_aggregated существуют
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe --topic iot_events
```

### 4. Проверить, что генератор работает

Генератор стартует автоматически вместе с `docker compose up`. Проверить логи:

```bash
docker logs -f generatorb
```

Ожидаемый вывод (одна строка в секунду):
```
[event] {'device_type_id': 3, 'event_time': '2026-04-20T12:30:05.123Z', 'temperature': 27.4, 'humidity': 55.1}
[OK] iot_events [1] offset=42 key=3
```

Посмотреть сырые события в Kafka:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic iot_events \
  --from-beginning \
  --max-messages 5
```

### 5. Запустить Flink job

```bash
docker exec -it flink-jobmanager flink run -py /job/iot_job.py
```

Все переменные окружения (`KAFKA_*`, `POSTGRES_*`) уже заданы в `docker-compose.yml` для сервиса `flink-jobmanager` — явно передавать их не нужно.

> **Важно:** команда `python /job/iot_job.py` тоже технически запускает скрипт, но выполняет job вне Flink-кластера: она не регистрируется в JobManager и не отображается в Web UI. Для полноценного запуска всегда используйте `flink run -py`.

Flink Web UI доступен по адресу [http://localhost:8081](http://localhost:8081).
После запуска job появится в разделе **Running Jobs**.

### 6. Читать результаты из `iot_aggregated`

Первые агрегаты появятся через ~65 секунд после запуска job (1 минута окна + до 5 секунд watermark lag).

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic iot_aggregated \
  --from-beginning
```

Ожидаемый результат:
```json
{"window_start": "2026-04-20T12:30:00Z", "window_end": "2026-04-20T12:31:00Z",
 "device_type_id": 1, "device_type_name": "temperature_sensor",
 "avg_temperature": 24.31, "median_humidity": 57.8, "events_count": 12}
```

---

## Сервисы и порты

| Сервис             | Адрес                        |
|--------------------|------------------------------|
| Flink Web UI       | http://localhost:8081        |
| Kafka (с хоста)    | localhost:9093               |
| Kafka (в Docker)   | kafka:9092                   |
| PostgreSQL (с хоста)| localhost:5432               |

## Доступы PostgreSQL

| Параметр | Значение   |
|----------|------------|
| host     | localhost  |
| port     | 5432       |
| database | iot_db     |
| user     | iot_user   |
| password | iot_pass   |
