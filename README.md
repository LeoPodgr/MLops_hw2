# Kafka → ClickHouse: транзакции по штатам

Учебный проект для переноса транзакций из CSV в Kafka, автоматической загрузки в ClickHouse и поиска категории самой крупной одиночной транзакции в каждом штате. В проекте есть базовая схема для сравнения и оптимизированная схема с предагрегацией.

> **Про данные:** `data/sample_transactions.csv` — маленький синтетический smoke-test, он нужен, чтобы проверить запуск без доступа к Kaggle. Это **не** результат на полном учебном датасете. Перед сдачей положите настоящий CSV в `data/` и заново экспортируйте результат в `results/category_max_by_state.csv`.

## Архитектура

```text
CSV
 │ scripts/publish_csv_to_kafka.py
 ▼
Kafka topic: transactions (JSONEachRow)
 ▼
ClickHouse Kafka Engine table
 ▼ materialized view
transactions_raw  ──materialized view──▶ transactions_optimized
  (baseline)                             (LowCardinality, Decimal,
                                         partitions, ORDER BY)
                                               │ materialized view
                                               ▼
                                      state_category_max_agg
                                      (AggregatingMergeTree)
                                               │
                                               ▼
                                results/category_max_by_state.csv
```

## Что относится к требованиям

1. `scripts/publish_csv_to_kafka.py` — отдельный CSV producer для Kafka.
2. `clickhouse/init/01_baseline_tables.sql`, `clickhouse/init/03_materialized_views.sql` — Kafka Engine, базовая MergeTree-таблица и потоковая загрузка; `clickhouse/queries/01_category_max_by_state.sql` — запрос для базового варианта.
3. `scripts/export_results.py` — выгрузка результата запроса в CSV; образец структуры результата лежит в `results/category_max_by_state.sample.csv`.
4. `clickhouse/init/02_optimized_tables.sql`, `clickhouse/ddl/optimized.sql` и `clickhouse/queries/02_category_max_by_state_optimized.sql` — оптимизированная таблица, агрегирующая таблица и запрос по агрегатным состояниям.

## Требования

- Docker Desktop или Docker Engine с Compose V2;
- настоящий CSV, скачанный со страницы датасета;
- интернет при первом запуске для загрузки Docker images.

## Быстрый запуск со smoke-test

Из корня репозитория выполните:

```bash
docker compose up -d --build
docker compose ps
```

Дождитесь, пока сервис `clickhouse` станет healthy. Таблицы создаются SQL-скриптами при первом старте ClickHouse на пустом volume.

Отправьте тестовые строки в Kafka:

```bash
docker compose run --rm csv-producer --csv /app/data/sample_transactions.csv
```

Проверьте, что ClickHouse получил строки:

```bash
docker compose exec clickhouse clickhouse-client --database transactions_db \
  --query "SELECT count() AS rows FROM transactions_raw"

docker compose exec clickhouse clickhouse-client --database transactions_db \
  --query "SELECT count() AS rows FROM transactions_optimized"
```

Consumer читает Kafka асинхронно, поэтому сразу после завершения producer счётчик может обновиться не мгновенно. Повторите запрос через несколько секунд, если видите `0`.

Выполните оптимизированный SQL напрямую:

```bash
docker compose exec -T clickhouse clickhouse-client --database transactions_db \
  < clickhouse/queries/02_category_max_by_state_optimized.sql
```

Ожидаемый результат для синтетического примера — три штата: CA / travel / 399.99, NY / shopping / 120.00 и TX / travel / 450.00. Для реального CSV значения будут другими.

## Загрузка настоящего Kaggle CSV

1. Скачайте файл данных на странице задания Kaggle. Убедитесь, что в CSV есть колонки даты/времени транзакции, штата, категории и суммы.
2. Поместите файл в каталог `data/`, например `data/FraudTrain.csv`. Файлы данных игнорируются Git через `.gitignore`, чтобы случайно не закоммитить крупный датасет.
3. Запустите загрузку. Для первых проверок можно ограничиться 1000 строками:

```bash
docker compose run --rm csv-producer \
  --csv /app/data/FraudTrain.csv --limit 1000
```

Затем отправьте весь файл без `--limit`:

```bash
docker compose run --rm csv-producer --csv /app/data/FraudTrain.csv
```

Producer понимает распространённые варианты колонок: дата (`trans_date_trans_time`, `transaction_datetime`, `transaction_time`, `transaction_date`, `timestamp`), штат (`state`, `cardholder_state`), категория (`category`, `merchant_category`), сумма (`amt`, `amount`, `transaction_amount`). Исходная строка целиком сохраняется в `raw_payload` базовой таблицы. Если в вашем файле заголовки отличаются, добавьте их варианты в `scripts/publish_csv_to_kafka.py` в соответствующий список alias.

**Важно:** повторный запуск producer отправляет строки заново и добавляет дубликаты. Для чистого повторного прогона используйте команду сброса из раздела «Сброс проекта» и затем заново загрузите CSV.

## Запросы и экспорт CSV

Для результата на базовой таблице:

```bash
docker compose exec -T clickhouse clickhouse-client --database transactions_db \
  < clickhouse/queries/01_category_max_by_state.sql
```

Для оптимизированной таблицы с предагрегацией:

```bash
docker compose exec -T clickhouse clickhouse-client --database transactions_db \
  < clickhouse/queries/02_category_max_by_state_optimized.sql
```

Чтобы создать итоговый файл `results/category_max_by_state.csv`, установите зависимости в локальное Python-окружение и выполните экспорт:

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/export_results.py --mode optimized --output results/category_max_by_state.csv
```

Для выгрузки результата по базовой таблице укажите `--mode raw`. В CSV будут колонки `state`, `category`, `max_amount`.

Запрос выбирает **категорию отдельной транзакции с максимальной суммой внутри штата**, а не категорию с максимальной суммой всех транзакций, сложенных по категории. При равных максимальных суммах ClickHouse `argMax` может вернуть любую из категорий, участвующих в равенстве.

## Как показать оптимизацию

Базовая версия использует `String`, `Float64`, не имеет партиционирования и полезного ключа сортировки; полный исходный ряд сохранён в `raw_payload`. Оптимизированная версия:

- заменяет часто повторяющиеся `state` и `category` на `LowCardinality(String)`;
- хранит денежную сумму как `Decimal(18, 2)`;
- партиционирует детальную таблицу по месяцу;
- сортирует по `(state, amount, event_time)`;
- строит `AggregatingMergeTree` с `maxState` / `argMaxState`, поэтому отчёт читает агрегаты по штатам, а не пересчитывает все исходные строки.

Чтобы измерить время запросов в ClickHouse, запустите команды на одном и том же объёме данных:

```bash
docker compose exec -T clickhouse clickhouse-client --time --database transactions_db \
  < clickhouse/queries/01_category_max_by_state.sql

docker compose exec -T clickhouse clickhouse-client --time --database transactions_db \
  < clickhouse/queries/02_category_max_by_state_optimized.sql
```

На маленьком smoke-test время измерения не показательно. Для отчёта о производительности используйте весь датасет, сравнивайте несколько запусков и записывайте время и количество строк. Агрегирующая таблица потребляет дополнительные ресурсы при вставке, но ускоряет именно повторяющийся отчёт.

## Полезные проверки

```bash
# Убедиться, что topic создан
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Сверить число строк в базовой и оптимизированной таблицах
docker compose exec clickhouse clickhouse-client --database transactions_db \
  --query "SELECT 'raw' AS table, count() AS rows FROM transactions_raw UNION ALL SELECT 'optimized', count() FROM transactions_optimized"

# Размеры таблиц в ClickHouse
docker compose exec clickhouse clickhouse-client --query \
  "SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disk_size, sum(rows) AS rows FROM system.parts WHERE database='transactions_db' AND active GROUP BY table ORDER BY table"

# Логи ClickHouse
docker compose logs --tail=100 clickhouse
```

## Сброс проекта

Чтобы удалить данные Kafka/ClickHouse, consumer offsets и заново выполнить init SQL:

```bash
docker compose down -v
docker compose up -d --build
```

Команда `down -v` удаляет volume ClickHouse. Локальные CSV в `data/` не удаляются. Без удаления volume SQL-скрипты из `clickhouse/init/` повторно не выполняются.

## Структура репозитория

```text
clickhouse-kafka-homework/
├── clickhouse/
│   ├── ddl/                     # DDL для объяснения сравнения baseline/optimized
│   ├── init/                    # SQL, автоматически исполняемый при первом старте
│   └── queries/                 # два самостоятельных аналитических запроса
├── data/
│   └── sample_transactions.csv  # синтетический smoke-test
├── results/
│   └── category_max_by_state.sample.csv
├── scripts/
│   ├── publish_csv_to_kafka.py  # пункт 1
│   └── export_results.py        # генерация результата CSV
├── Dockerfile.producer
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Примечание по точности

Фактический итоговый CSV по полному Kaggle-файлу нельзя заполнить без самого файла. В репозитории зафиксирован только синтетический пример для smoke-test; после загрузки оригинального датасета экспортируйте результат командой выше и сохраните получившийся `results/category_max_by_state.csv` в Git.
