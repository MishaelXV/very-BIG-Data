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
docker logs -f generator
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

## Теория: Event Time

В потоковой обработке есть два понятия времени:

- **Processing time** — время, когда Flink обработал событие (время на сервере).
- **Event time** — время, когда событие произошло в реальности (поле `event_time` в JSON).

В этом проекте используется **event time**. Это важно, потому что:
- события могут приходить в Kafka с задержкой или не по порядку;
- агрегаты должны отражать реальный временной отрезок (например, с 12:30 до 12:31), а не время обработки.

Flink извлекает `event_time` из каждого события и использует его для распределения по окнам.

---

## Теория: Watermark

**Watermark** — это сигнал, который говорит Flink: _«все события с event_time < W уже пришли»_.

Зачем нужен watermark:
- события из Kafka могут приходить не строго по порядку;
- Flink не может ждать вечно — нужно решить, когда закрыть окно и посчитать агрегат;
- watermark позволяет подождать немного запоздавших событий и затем закрыть окно.

В этом проекте используется стратегия `BoundedOutOfOrderness(5 seconds)`: Flink допускает, что события могут опоздать не более чем на 5 секунд. Окно [12:30, 12:31) закроется, когда watermark достигнет 12:31:05.

```
Event time:  ──────────────────────────────────────────►
                 12:30:00            12:31:00
                    │                    │
                    │←── window 1 min ───│
                    │                    │
                                         │← watermark 12:31:05 → окно закрыто
```

---

## Теория: Зачем нужно окно 1 минута

Без окна Flink обрабатывал бы каждое событие по отдельности и немедленно. Чтобы посчитать агрегат (среднее, медиана) за период, нужно сначала накопить события за этот период — это и есть **tumbling window**.

**Tumbling (прокатывающееся) окно** не пересекается: каждое событие попадает ровно в одно окно.

```
│← окно 1 [12:30–12:31) →│← окно 2 [12:31–12:32) →│← окно 3 ...
│  ~60 событий            │  ~60 событий            │
│  → 1 агрегат            │  → 1 агрегат            │
```

---

## Теория: Как считаются агрегаты

### avg_temperature (средняя температура)

Простое среднее арифметическое:

```
avg = (t1 + t2 + ... + tN) / N
```

Пример: [23.5, 24.1, 22.8] → (23.5 + 24.1 + 22.8) / 3 = **23.47**

### median_humidity (медиана влажности)

Медиана не чувствительна к выбросам, поэтому лучше отражает «типичное» значение.

Алгоритм:
1. Отсортировать все значения влажности за минуту.
2. Если количество значений **нечётное** — взять центральный элемент.
3. Если количество значений **чётное** — взять среднее двух центральных.

```
Пример (нечётное, N=5):
  sorted: [42.1, 55.3, 61.2, 67.8, 80.0]
  медиана = 61.2  (индекс 2)

Пример (чётное, N=4):
  sorted: [42.1, 55.3, 67.8, 80.0]
  медиана = (55.3 + 67.8) / 2 = 61.55
```

---

## Возможные проблемы и Troubleshooting

### Flink job не видит агрегаты в `iot_aggregated`

**Причина:** первое окно закрывается только через ~65 секунд после старта job.
**Решение:** подождать 1–2 минуты после запуска job.

---

### Flink job падает с ошибкой подключения к PostgreSQL

```
psycopg2.OperationalError: could not connect to server
```

**Причина:** переменные окружения `POSTGRES_*` недоступны внутри контейнера (например, контейнер пересоздан вручную, минуя `docker compose`).

**Решение:** убедиться, что контейнер запущен через `docker compose up` (env vars прописаны в `docker-compose.yml`), и проверить их наличие:

```bash
docker exec flink-jobmanager env | grep POSTGRES
```

Ожидаемый вывод:
```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=iot_db
POSTGRES_USER=iot_user
POSTGRES_PASSWORD=iot_pass
```

Если переменных нет — пересоздать контейнер:

```bash
docker compose up -d --force-recreate flink-jobmanager
```

---

### Kafka topic не существует

```
ERROR: Topic 'iot_events' not found
```

**Причина:** контейнер `kafka-init` не завершился успешно.
**Решение:** посмотреть логи и перезапустить:

```bash
docker logs kafka-init
docker compose restart kafka-init
```

---

### Генератор не подключается к Kafka

```
ERROR delivery failed: KafkaException
```

**Причина:** Kafka ещё не готова (healthcheck не прошёл).
**Решение:** генератор перезапускается автоматически (`restart: unless-stopped`). Подождать 30–60 секунд.

---

### Окна не закрываются, агрегаты не появляются

**Причина:** watermark не продвигается — это происходит, если события перестают поступать (генератор остановлен).
**Решение:** убедиться, что генератор работает:

```bash
docker logs -f generator
```

---

### Flink Web UI недоступен (http://localhost:8081)

**Причина:** jobmanager ещё запускается.
**Решение:** подождать 30–40 секунд после `docker compose up`, затем обновить страницу.

---

## Что показать преподавателю при защите

### 1. Работающий генератор
```bash
docker logs -f generator
# Показать строки: [event] {...} и [OK] iot_events [0] offset=...
```

### 2. Данные в Kafka topic iot_events
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic iot_events --max-messages 3
```

### 3. Справочник в PostgreSQL
```bash
docker exec -it postgres psql -U iot_user -d iot_db \
  -c "SELECT * FROM device_types;"
```

### 4. Запущенный Flink job в Web UI
Открыть [http://localhost:8081](http://localhost:8081) → вкладка **Running Jobs** → показать граф задачи (DAG).

### 5. Агрегаты в iot_aggregated
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic iot_aggregated --from-beginning
```
Показать, что для каждого `device_type_id` раз в минуту появляется агрегат с `avg_temperature`, `median_humidity` и `events_count`.

### 6. Ответить на вопросы
- Чем event time отличается от processing time?
- Зачем нужен watermark и как он влияет на закрытие окна?
- Почему справочник из PostgreSQL загружается один раз в `main()` и передаётся в map-функцию через замыкание (`make_parse_and_enrich`), а не читается из БД внутри каждого вызова map?
- Как изменится поведение job, если увеличить `BoundedOutOfOrderness` до 30 секунд?

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
