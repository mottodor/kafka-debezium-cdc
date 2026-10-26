# Спецификация: PostgreSQL CDC с Debezium и Kafka

**Статус:** APPROVED
**Исходный материал:** `docs/idea.md` — задаёт назначение, учебные цели, исходный scope и исключения. Исходник не изменяется.
**Skill:** `specify`, версия из текущего сеанса; отдельный номер версии в предоставленном `SKILL.md` не указан.
**Назначение документа:** единая проверяемая спецификация проекта. Решения, ещё не согласованные пользователем, явно отмечаются как открытые вопросы или предложения.

## 1. Цель и ожидаемый результат

Независимый локальный учебный проект должен демонстрировать полный путь Change Data Capture:

`PostgreSQL WAL → Debezium PostgreSQL Connector → Kafka → Python consumer → SQLite sink`.

Пользователь должен получить наблюдаемую лабораторию, в которой можно:

- объяснить роль logical replication, publication и replication slot;
- наблюдать lifecycle Debezium connector, initial snapshot и streaming;
- исследовать Debezium envelope для create, update, delete, tombstone и snapshot/read;
- исследовать Kafka topics, partitions, offsets и consumer group;
- проверить commit consumer offset только после durable sink commit;
- проверить идемпотентность, повторную доставку и восстановление после restart;
- проверить защиту current-state projection от устаревших событий;
- наблюдать изменение схемы и выполнить управляемое восстановление;
- различать Kafka lag, возраст CDC-события и удержание WAL replication slot;
- документировать границы гарантий и не заявлять exactly-once без end-to-end доказательства.

### Распределение работы

**Решено пользователем:** после утверждения спецификации агент реализует весь проект. Пользователь запускает лабораторию, проверяет критерии приёмки и изучает CDC-поведение.

Это разрешение действует только на следующем этапе. На этапе `specify` код продукта, окружение и инфраструктура не создаются.

**Делегировано пользователем:** агент выбирает точные версии PostgreSQL, Kafka/Kafka Connect, Debezium и Python в рамках следующих ограничений:

- только документированно совместимые стабильные версии;
- все версии и container image tags фиксируются; `latest` запрещён;
- совместимость подтверждается по первичной документации;
- major-обновление после фиксации версий требует изменения спецификации.

Документация Debezium Reference 3.6 и выполненный 2026-10-26 технический probe подтвердили совместимость выбранного local/test stack с PostgreSQL 16, `pgoutput`, Debezium PostgreSQL Connector `3.6.3.Final` и single-node Kafka KRaft.

**Принято агентом в делегированной области и подтверждено probe:**

- PostgreSQL — `16.13`, `pgoutput`; образ `postgres:16.13-bookworm@sha256:472efd9a66f2b2f1a5aeb18b28de74332e6ef88c2b93a1a5d812fb6db67a5f60`;
- Kafka KRaft — Kafka `4.3.0` из образа `quay.io/debezium/kafka:3.6.3.Final@sha256:c8af47ccb718e5f17cba0493142079f38ed18df3615200f5ae6a6aa21c49c658`;
- Kafka Connect runtime — Kafka Connect `4.3.0`, Debezium PostgreSQL Connector `3.6.3.Final`, образ `quay.io/debezium/connect:3.6.3.Final@sha256:47f266af35d478b646db2e2e4603d21e56861151ab946e879b38edd5b533b6a6`;
- Python — `3.12.12`, образ `python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c`;
- Python Kafka client — `confluent-kafka==2.16.0`, включающий `librdkafka 2.16.0`; импорт и наличие CPython 3.12 wheel проверены в выбранном Python-образе.

Теги приведены для читаемости, но воспроизводимый контракт использует полную ссылку с digest. `latest` и запуск только по изменяемому tag запрещены. Изменение любого digest, major/minor runtime version или структуры ordering metadata требует повторной проверки совместимости и при необходимости возвращает спецификацию в `NEEDS_PROBE`.

**Дополнительно делегировано пользователем:** агент самостоятельно принимает и фиксирует оставшиеся технические решения, если они не меняют согласованное пользовательское поведение и критерии:

- точные SQL-типы, nullable/unique/check constraints;
- naming conventions для connector, slot, publication, topics и consumer group;
- Python Kafka client и внутреннюю структуру consumer;
- физическую схему SQLite и формат хранения raw key/value/headers;
- точные seed/acceptance fixtures и рассчитанные из правил ожидания;
- точные команды диагностики и recovery.

Вне делегирования остаются изменения scope, delete semantics, ordering rule, durable-commit boundary, recovery generation policy, retention-порогов и ожидаемых результатов критериев.

## 2. Scope

### 2.1. Среда запуска

**Решено пользователем:** основная среда local lab — Docker Engine с Docker Compose v2.

- PostgreSQL, single-node Kafka KRaft и Kafka Connect/Debezium запускаются контейнерами.
- Python consumer также должен иметь контейнерный режим, входящий в основной воспроизводимый сценарий.
- Допустим дополнительный документированный запуск consumer на host для разработки, но он не заменяет контейнерный acceptance path.
- Основной контракт должен работать на Linux, macOS и WSL2 при наличии совместимых Docker Engine/Compose; платформенные различия должны быть явно документированы, если обнаружатся.

**Решено пользователем — ресурсный бюджет:**

- до 8 vCPU;
- до 16 GB RAM;
- 25 GB свободного диска — общий лимит для container images и всех runtime-данных, включая PostgreSQL, Kafka, WAL и SQLite.

Конфигурация retention, JVM heap, volumes и процедуры очистки должны удерживать весь lab в этом общем лимите.

### 2.2. Входит в обязательную базовую версию

- PostgreSQL logical replication.
- Least-privilege replication account.
- Publication и replication slot.
- Single-node Kafka в режиме KRaft для local lab.
- Kafka Connect и Debezium PostgreSQL Connector.
- Отдельные table topics для `customers`, `orders`, `payments`.
- Python consumer.
- SQLite sink с raw event archive и current-state projection.
- Initial snapshot и последующий streaming.
- Create, update, delete, tombstone и snapshot/read events.
- Consumer restart и replay.
- Duplicate delivery.
- Out-of-order protection для current-state projection.
- Lag inspection.
- Schema change exercise.
- Bounded recovery procedures.
- Наблюдение риска slot retention и роста WAL.

**Решено пользователем:** базовая версия считается завершённой только при выполнении всего перечисленного scope. Отдельный урезанный MVP для одной таблицы не заменяет Definition of Done. Реализация может делиться на внутренние этапы, но они не уменьшают итоговый объём приёмки.

### 2.3. Исключено

- Airflow.
- dbt.
- ClickHouse.
- Superset.
- Spark.
- Schema Registry в базовой версии.
- Multi-broker production cluster.
- Cross-region replication.
- Гарантии глобального порядка между topics.
- Обработка всех OLTP-таблиц.
- Join- и агрегированные downstream-проекции.
- Автоматическая эволюция схемы SQLite.
- Поддержка PostgreSQL `TRUNCATE` через CDC.
- Update первичного ключа `id` после создания строки.
- Заявление exactly-once без отдельного end-to-end доказательства.

## 3. Заданные ограничения и принятые решения

### 3.1. Source-модель

**Решено пользователем:** используется минимальная ecommerce-модель:

- `customers(id, email, full_name, status, created_at, updated_at)`;
- `orders(id, customer_id, status, total_amount, created_at, updated_at)`;
- `payments(id, order_id, status, amount, provider_ref, created_at, updated_at)`.

Правила:

- первичные ключи `id` — целочисленные и неизменяемые после создания;
- изменение идентификатора моделируется явным delete старой строки и create новой;
- update primary key исключён из CDC-контракта, чтобы Kafka ordering key оставался стабильным;
- `orders.customer_id` ссылается на `customers.id`;
- `payments.order_id` ссылается на `orders.id`;
- monetary-поля используют PostgreSQL `NUMERIC`, а не floating point;
- внешние ключи используют `ON DELETE RESTRICT` / `NO ACTION`;
- customer нельзя удалить при наличии связанных orders;
- order нельзя удалить при наличии связанных payments;
- полное удаление агрегата выполняется явными операциями в порядке `payments → orders → customer`, чтобы каждое удаление было отдельно наблюдаемо через CDC.

**Принято агентом в делегированной области:**

- все `id` — `BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`;
- неизменность `id` дополнительно обеспечивается trigger, отклоняющим update ключа;
- `email`, `full_name`, status-поля, foreign keys, amounts и `provider_ref` — `NOT NULL`;
- `customers.email` и `payments.provider_ref` — `UNIQUE`;
- `orders.total_amount` и `payments.amount` — `NUMERIC(12,2)` с проверкой неотрицательного/положительного значения соответственно;
- временные поля — `TIMESTAMPTZ NOT NULL`, с `created_at` по умолчанию `CURRENT_TIMESTAMP`; `updated_at` изменяется явным update/trigger и отражается через CDC;
- status-поля ограничиваются небольшими `CHECK`-наборами, используемыми fixtures;
- foreign keys используют явно заданное `ON DELETE RESTRICT`.

### 3.2. Kafka topics, partitions и ordering key

**Решено пользователем:** каждый table topic содержит три partitions. Kafka message key строится Debezium из первичного ключа строки `id`.

Контракт:

- события одного ключа должны попадать в одну partition при неизменной конфигурации partitioner и количестве partitions;
- порядок предполагается только внутри одной partition;
- порядок между разными ключами, partitions и topics не предполагается;
- количество partitions и partitioner во время лаборатории не изменяются;
- lag измеряется отдельно по каждой partition.

### 3.3. Serialization

**Решено пользователем и подтверждено probe:** Kafka Connect использует JSON Converter со встроенными schemas для key и value. Точные worker properties: `key.converter.schemas.enable=true` и `value.converter.schemas.enable=true`; для выбранного Debezium container image им соответствуют environment variables `KEY_CONVERTER_SCHEMAS_ENABLE=true` и `VALUE_CONVERTER_SCHEMAS_ENABLE=true`.

- Key и ненулевой value содержат `schema` и `payload`.
- Consumer разбирает Debezium envelope из `payload`.
- Tombstone содержит обычный key и `value = null`.
- Изменение встроенной schema должно быть наблюдаемо в raw event.

### 3.4. Поддерживаемые операции

Поддерживаются:

- snapshot/read;
- create;
- update;
- delete;
- tombstone.

**Решено пользователем:** `TRUNCATE` исключён из контракта. Для наблюдаемой массовой очистки используются явные `DELETE`. Consumer не обязан корректно обрабатывать truncate event.

### 3.5. Naming и CDC generation

**Принято агентом в делегированной области:** первая генерация имеет идентификатор `g001`.

- database: `cdc_lab`;
- source schema: `public`;
- connector: `postgres-cdc-g001`;
- replication slot: `postgres_cdc_g001`;
- publication: `postgres_cdc_g001_pub`;
- topic prefix: `cdc.g001`;
- table topics: `cdc.g001.public.customers`, `cdc.g001.public.orders`, `cdc.g001.public.payments`;
- consumer group: `sqlite-projector-g001`;
- sink file: `/data/cdc-g001.sqlite3`.

Новая изолированная генерация увеличивает числовой suffix (`g002`, `g003`, …) во всех generation-scoped именах. Имена старой генерации не переиспользуются.

### 3.6. Least-privilege PostgreSQL contract

**Принято агентом в делегированной области:** publication создаётся bootstrap-администратором заранее и включает только три source-таблицы. Connector не получает право автоматически создавать или расширять publication.

Connector role:

- `LOGIN` и `REPLICATION`;
- `CONNECT` к `cdc_lab`;
- `USAGE` на schema `public`;
- `SELECT` на `customers`, `orders`, `payments` для snapshot;
- без `SUPERUSER`, `CREATEDB`, `CREATEROLE`, database-level `CREATE` и DML-прав на source-таблицы.

Connector configuration:

- `plugin.name=pgoutput`;
- `publication.autocreate.mode=disabled`;
- publication и table include list заданы явно;
- connector создаёт уникальный logical slot с имеющимся `REPLICATION` privilege;
- `slot.drop.on.stop=false`, чтобы обычный restart продолжал существующий stream.

Основание: Debezium PostgreSQL Connector Reference 3.6, Context7 `/websites/debezium_io_reference_3_6`. Документация рекомендует заранее создавать publication; автоматическое создание потребовало бы дополнительных database `CREATE`/ownership privileges.

## 4. Sink и граница durable commit

### 4.1. Единый SQLite sink

**Решено пользователем:** один SQLite-файл содержит:

- raw event archive;
- current-state projection;
- служебные данные идемпотентности;
- per-key ordering watermarks, включая watermarks удалённых ключей;
- служебный учёт повторных доставок.

Raw event и изменение проекции выполняются одной SQLite-транзакцией. Kafka consumer offset коммитится только после успешного SQLite commit.

Если SQLite commit завершился, а Kafka offset commit — нет, record может быть доставлен повторно. Такая доставка не должна повторно изменить логическое состояние sink.

### 4.2. Python consumer и offset commit

**Принято агентом в делегированной области:**

- используется `confluent-kafka`;
- `enable.auto.commit=false` и автоматическое сохранение позиции отключено;
- основной path обрабатывает records последовательно в одном consumer process, сохраняя Kafka partition ordering;
- после успешного SQLite commit выполняется синхронный commit конкретного message;
- committed offset равен `message.offset + 1`, то есть позиции следующего record;
- ошибка Kafka commit после успешного SQLite commit приводит к остановке/retry и допустимой повторной доставке, но не к откату durable sink;
- ошибки отдельных partitions в результате synchronous commit проверяются явно;
- Kafka transactions/EOS API не используется, поэтому проект заявляет at-least-once delivery с idempotent sink, а не exactly-once.

Техническое основание: Confluent Kafka Python API, Context7 `/websites/confluent_io_platform_current_clients_confluent-kafka-python`.

### 4.3. Физическая схема SQLite

**Принято агентом в делегированной области:**

- `raw_events` имеет primary key `(topic, partition, offset)` и хранит key/value как `BLOB`, Kafka timestamp, извлечённые Debezium metadata, processing status и delivery count;
- ordered/repeated headers хранятся в дочерней `raw_headers` с порядковым номером и `BLOB` value;
- `ingestion_errors` хранит Kafka coordinate, исходные bytes, классификацию ошибки, первое/последнее наблюдение и число попыток;
- `customers_current`, `orders_current`, `payments_current` содержат доменные поля без SQLite foreign keys;
- `NUMERIC(12,2)` переносится в SQLite как каноническая decimal-строка, чтобы не вносить binary floating-point округление;
- `entity_watermarks` хранит generation, table/key, `phase_rank`, `source_lsn`, `transaction_total_order`, `transaction_data_collection_order` и флаг последнего delete независимо от наличия current row;
- `schema_migrations` хранит версию SQLite schema;
- raw archive и projection изменяются в одной транзакции `BEGIN IMMEDIATE … COMMIT`.

### 4.4. Raw event archive

Для каждого уникального Kafka record сохраняются:

- исходные key и value в полученном consumer представлении без нормализации вместо оригинала;
- headers;
- topic;
- partition;
- offset;
- Kafka timestamp;
- извлечённые Debezium operation, source table, snapshot marker, LSN, `source.sequence`, transaction ID, `transaction.total_order`, `transaction.data_collection_order` и source timestamp;
- результат обработки проекции: `applied`, `stale` или `tombstone`.

Уникальность raw record задаётся координатой `(topic, partition, offset)`.

Повторная доставка уже сохранённой координаты:

- не создаёт новую строку raw archive;
- не изменяет current-state projection;
- может увеличивать отдельный служебный счётчик попыток доставки.

Два похожих Debezium events с разными Kafka offsets автоматически не считаются одним событием.

### 4.5. Current-state projection

SQLite содержит три независимые зеркальные таблицы для `customers`, `orders` и `payments`.

- Событие одной таблицы применяется без ожидания связанных событий из других topics.
- Временные «висячие» ссылки допустимы.
- SQLite не должен блокировать применение событий внешними ключами между projection-таблицами.
- После обработки всех относящихся событий итоговые строки должны совпадать с актуальным состоянием PostgreSQL в рамках поддерживаемых операций.

## 5. Контракты обработки событий

### 5.1. Snapshot и streaming

**Решено пользователем:**

- при первом запуске connector без сохранённых offsets выполняется initial snapshot;
- snapshot/read events сохраняются в raw archive и заполняют current-state projection;
- после snapshot connector автоматически переходит в streaming;
- обычный restart с сохранёнными connector offsets не повторяет snapshot;
- запоздалый snapshot event не может заменить более новое streaming-состояние;
- принудительный повторный snapshot относится к отдельной recovery procedure.

### 5.2. Delete и tombstone

**Решено пользователем:**

- delete event физически удаляет строку из current-state projection;
- delete event сохраняется в raw archive;
- следующий tombstone также сохраняется в raw archive;
- tombstone не изменяет current-state projection;
- история удалённой строки доступна через raw archive;
- после физического удаления служебный watermark ключа сохраняется, чтобы старое событие не воскресило строку.

### 5.3. Out-of-order

**Решено пользователем:** действует правило latest source position wins.

**Подтверждено техническим probe:** для одного table/key внутри одной CDC-generation используется нормализованный строгий token

`(phase_rank, source.lsn, transaction.total_order, transaction.data_collection_order)`.

Правила построения и сравнения:

- `phase_rank = 0` только для initial snapshot event с `op=r` и нормализованным snapshot marker из `true`, `first`, `first_in_data_collection`, `last_in_data_collection`, `last`;
- `phase_rank = 1` только для streaming event с `op` из `c`, `u`, `d` и snapshot marker `false`;
- для initial snapshot `source.lsn` обязан быть ненулевым, а отсутствующие transaction-order fields нормализуются в `0, 0`;
- для streaming `source.lsn`, `transaction.total_order` и `transaction.data_collection_order` обязательны и должны быть корректными целыми значениями; connector запускается с `provide.transaction.metadata=true`;
- tokens сравниваются лексикографически; больший token означает более новое source-состояние;
- `source.txId`, `transaction.id`, `source.sequence`, Kafka timestamp, Debezium processing timestamp и wall-clock time сохраняются для диагностики, но не участвуют в ordering comparator;
- сначала проверяется duplicate coordinate `(topic, partition, offset)`; повтор этой coordinate не проходит повторное ordering-применение;
- равные tokens у разных Kafka coordinates одного table/key считаются неоднозначной коллизией и вызывают fail-stop без продвижения offset;
- tombstone не имеет Debezium envelope, не сравнивается с watermark и не меняет projection;
- incremental/ad hoc snapshot marker `incremental` в активной generation не поддерживается и вызывает fail-stop; повторный snapshot выполняется только как новая изолированная CDC-generation.

Для одного table/key:

- каждый уникальный Kafka record архивируется;
- событие с token больше сохранённого per-key watermark может изменить current state;
- событие с token меньше watermark получает статус `stale` и не меняет current state;
- delete обновляет watermark до физического удаления projection-строки;
- старый create/update после delete не может восстановить удалённую строку.

Probe на закреплённом stack показал общий snapshot LSN для snapshot rows, отсутствие transaction metadata у `op=r`, возрастающие LSN для нескольких изменений одного ключа внутри одной транзакции, `total_order=1,2,…` внутри транзакции и больший LSN для последующих транзакций. Отдельный `phase_rank` фиксирует lifecycle initial snapshot → streaming, а transaction-order fields дают строгий tie-breaker внутри streaming. Полные результаты и границы вывода приведены в разделе 11.

Источники: Debezium PostgreSQL Connector Reference 3.6, Context7 library `/websites/debezium_io_reference_3_6`, и выполненный probe на закреплённых image digests.

### 5.4. Duplicate delivery

Повтор того же `(topic, partition, offset)` считается повторной доставкой, а не новым CDC-событием.

Ожидаемый результат:

- в raw archive остаётся одна строка;
- projection не изменяется повторно;
- служебный счётчик попыток может увеличиться;
- после успешной повторной обработки Kafka offset коммитится.

### 5.5. Schema change

**Решено пользователем:** упражнение добавляет в PostgreSQL nullable-колонку `customers.phone TEXT`.

До применения соответствующей SQLite-миграции:

- consumer обнаруживает неподдерживаемое поле;
- поле не игнорируется и не добавляется автоматически;
- SQLite-транзакция откатывается;
- Kafka offset не коммитится;
- обработка останавливается с понятной диагностикой.

После ручной миграции SQLite:

- consumer перезапускается;
- тот же Kafka record доставляется и обрабатывается повторно;
- `phone` попадает в current-state projection;
- существующие строки получают `phone = NULL`.

### 5.6. Malformed, неизвестные и несопоставимые события

**Решено пользователем:** применяется fail-stop без продвижения Kafka offset.

К таким событиям относятся как минимум:

- невалидный JSON или структура, не соответствующая ожидаемым `schema`/`payload`;
- отсутствующий или некорректный primary-key payload;
- неизвестный `op`;
- неожиданный topic/table;
- отсутствующие или несопоставимые ordering components там, где требуется сравнение;
- тип значения, несовместимый с projection schema.

Поведение:

- current-state projection не изменяется;
- Kafka offset не коммитится;
- исходные bytes и диагностические данные сохраняются/upsert в `ingestion_errors` по Kafka coordinate;
- consumer завершается с ненулевым статусом и понятным сообщением;
- повторные запуски увеличивают attempt count, но не создают неограниченные копии ошибки;
- после исправления конфигурации, схемы или consumer тот же Kafka record обрабатывается повторно;
- молчаливый skip, DLQ-and-commit и продвижение offset запрещены базовым контрактом.

## 6. Lag и эксплуатационные наблюдения

Должны отдельно отображаться:

1. **Kafka consumer-group lag** по каждой partition: разница между log-end offset и committed group offset.
2. **Возраст CDC-события при durable commit:** время SQLite commit минус Debezium source timestamp. Метрика зависит от синхронизации часов и не используется для упорядочивания.
3. **Удерживаемый replication slot WAL:** разница между текущей PostgreSQL WAL position и `confirmed_flush_lsn`, выраженная в байтах. Это риск хранения WAL, а не Kafka lag.

Метрики не объединяются в одно число.

### 6.1. Bounded retention и защита диска

**Решено пользователем:**

- table topics используют `cleanup.policy=delete`;
- целевой time retention — 24 часа;
- целевой size retention — 256 MiB на partition;
- при трёх table topics и трёх partitions номинальный объём table-topic logs составляет около 2.25 GiB до учёта segment granularity и служебного overhead;
- time- и size-retention действуют независимо: segment становится кандидатом на удаление при достижении любой границы;
- удаление в Kafka выполняется целыми log segments, поэтому 256 MiB — целевой retention limit, а не мгновенный жёсткий предел фактического размера;
- PostgreSQL retained-WAL warning — 512 MiB;
- `max_slot_wal_keep_size` — 1 GiB;
- состояние slot контролируется также через `wal_status` и, если поле доступно в выбранной версии PostgreSQL, `safe_wal_size`;
- потеря требуемого WAL и состояние slot `lost` означают невозможность обычного продолжения и запускают процедуру новой CDC-генерации;
- SQLite raw archive автоматически не очищается;
- при размере SQLite около 1 GiB consumer должен завершиться контролируемо до продолжения нагрузочных упражнений; оператор архивирует старую генерацию или создаёт новую;
- новые нагрузочные упражнения не запускаются, если на Docker storage доступно менее 5 GiB.

Техническое основание:

- Apache Kafka удаляет данные целыми segments; при одновременном time- и size-retention достаточно достижения любой границы. Источник: Apache Kafka official repository documentation, Context7 `/apache/kafka`.
- PostgreSQL `max_slot_wal_keep_size` ограничивает удержание WAL на checkpoint; при превышении требуемый WAL может быть удалён, а slot может стать `lost`. Источник: PostgreSQL 16 documentation, Context7 `/websites/postgresql_16`; применимость полей будет повторно проверена для окончательно выбранной версии.

## 7. Recovery

### 7.1. Обычный consumer restart и replay

Обычное продолжение или replay допустимы, только если требуемые offsets не меньше текущих log-start offsets соответствующих Kafka partitions.

При сохранённых Kafka records и consumer-group offsets consumer продолжает чтение. Если crash произошёл после SQLite commit, но до Kafka offset commit, повторная доставка обрабатывается идемпотентно.

Если требуемый offset уже удалён retention-политикой, восстановление из этого consumer group не считается возможным; используется rebuild из ещё доступного полного диапазона Kafka либо новая CDC-генерация.

### 7.2. Потеря connector offsets или replication slot

**Решено пользователем:** создаётся новая изолированная CDC-генерация.

- Старый connector останавливается.
- Старый SQLite sink сохраняется read-only для диагностики.
- Создаются новый connector identity, новый replication slot, новый topic namespace и новый SQLite sink.
- Новая генерация выполняет snapshot и переходит в streaming.
- Старые и новые offsets, raw events и watermarks не смешиваются.
- После проверки новая генерация объявляется активной.

Preconditions перехода к новой генерации включают как минимум потерю/непригодность connector offsets, отсутствие требуемых Kafka records, `wal_status = lost` либо подтверждённое отсутствие требуемого WAL.

**Принято агентом в делегированной области — процедура:**

1. Остановить старый consumer и connector; запретить запись в старый SQLite.
2. Сохранить копию старого SQLite и manifest старых connector/slot/topic/group identifiers.
3. Увеличить generation ID и создать новые publication/slot/topic/group/sink names без переиспользования старых.
4. Запустить новый connector с initial snapshot и новый consumer с пустым SQLite.
5. Дождаться завершения snapshot и Kafka group lag `0` по всем table partitions.
6. Сравнить source и projection по количеству и значениям строк всех трёх таблиц; убедиться, что `ingestion_errors` пуст.
7. Повторить проверки connector RUNNING и lag `0` дважды с интервалом не менее 5 секунд.
8. Только после этого отметить новую generation активной; старая остаётся read-only.

Если новый запуск не проходит проверки, активной остаётся старая генерация либо система остаётся в явно обозначенном состоянии восстановления; смешивание данных запрещено.

### 7.3. Rebuild из Kafka

Rebuild без нового Debezium snapshot допустим только тогда, когда Kafka log-start offsets по всем table partitions не превышают offsets, необходимые для полного восстановления выбранной generation.

Процедура использует новый consumer group и новый пустой SQLite, начинает чтение с earliest доступных offsets, доводит lag до нуля и применяет те же проверки source/projection и `ingestion_errors`, что и новая CDC-generation. Существующий sink in-place не очищается и не переиспользуется.

## 8. Стратегия проверок

### 8.1. Duplicate delivery

Интеграционный сценарий использует контролируемую fault injection point после SQLite commit, но до Kafka offset commit.

После restart проверяются:

- повторная доставка той же Kafka coordinate;
- одна raw-запись для этой coordinate;
- отсутствие повторного изменения projection;
- увеличение счётчика попыток;
- успешный последующий offset commit.

### 8.2. Out-of-order

Используются детерминированные Debezium-envelope fixtures, переданные обработчику в обратном source-порядке.

Проверяются:

- архивирование обоих уникальных records;
- применение более нового события;
- статус `stale` для старого события;
- отсутствие отката projection;
- отсутствие восстановления строки старым событием после более нового delete.

Этот тест доказывает устойчивость sink к входу вне source-порядка. Он не выдаётся за доказательство того, что штатный Debezium обычно нарушает per-key Kafka ordering.

## 9. Критерии приёмки

### 9.1. Детерминированные acceptance fixtures

**Принято агентом в делегированной области:** seed использует явные identity values через контролируемый bootstrap.

Начальное состояние перед snapshot:

- customers: `(1, alice@example.test, Alice Adams, active)`, `(2, bob@example.test, Bob Brown, active)`;
- orders: `(101, customer=1, pending, 120.50)`, `(102, customer=2, paid, 49.99)`;
- payments: `(1001, order=102, captured, 49.99, provider_ref=pay-1001)`.

Streaming-сценарий:

1. Создать customer `3` — `Carol Clark`, `carol@example.test`, `active`.
2. Обновить customer `1`: `full_name=Alice Cooper`, `status=inactive`.
3. Создать order `103` для customer `3` на `15.00` со status `pending`.
4. Создать payment `1002` для order `103` на `15.00`, `provider_ref=pay-1002`, status `authorized`.
5. Обновить order `103` до `paid`, payment `1002` до `captured`.
6. Удалить payment `1002`, затем order `103`, затем customer `3`.
7. В schema-change exercise добавить `customers.phone`, затем установить customer `1` значение `+15550000001`.

После полного сценария current-state должен содержать customers `1` и `2`, orders `101` и `102`, payment `1001`; customer `1` имеет `Alice Cooper`, `inactive`, `phone=+15550000001`, а старый customer `2` имеет `phone=NULL`.

Out-of-order fixtures используют отдельные synthetic Kafka coordinates и один table/key с явно заданными сравнимыми tokens `(phase_rank, source.lsn, transaction.total_order, transaction.data_collection_order)`; они не публикуются как реальные source changes.

ID критериев сохраняются при дальнейших уточнениях.

### AC-001 — Initial snapshot

- **Условия:** source содержит заранее созданные строки во всех трёх таблицах; connector offsets отсутствуют.
- **Действие:** запускается connector и consumer.
- **Ожидание:** snapshot/read events присутствуют в raw archive; current-state projection содержит соответствующие source-строки; connector переходит в streaming.
- **Проверка:** запросы к PostgreSQL, Kafka и SQLite плюс состояние connector.
- **Основание:** `docs/idea.md`, решение пользователя о snapshot lifecycle.

### AC-002 — Streaming create/update

- **Условия:** snapshot завершён.
- **Действие:** создаются и обновляются строки `customers`, `orders`, `payments`.
- **Ожидание:** уникальные events архивируются; projection отражает последние source-значения.
- **Проверка:** сопоставление source rows, Kafka records и SQLite rows.
- **Основание:** `docs/idea.md`.

### AC-003 — Delete и tombstone

- **Условия:** projection содержит выбранную строку.
- **Действие:** строка удаляется в PostgreSQL.
- **Ожидание:** delete и tombstone архивируются; projection-строка отсутствует; tombstone не вызывает дополнительного изменения projection; watermark удалённого ключа сохранён.
- **Проверка:** Kafka records и запросы к SQLite.
- **Основание:** решение пользователя о delete semantics.

### AC-004 — Duplicate после crash window

- **Условия:** включена контролируемая остановка после SQLite commit и до Kafka offset commit.
- **Действие:** consumer обрабатывает record, завершается и запускается снова.
- **Ожидание:** повторно полученная coordinate не создаёт вторую raw-строку и не меняет projection; счётчик попыток увеличивается; offset впоследствии коммитится.
- **Проверка:** уникальность raw archive, состояние projection, счётчик и committed offset.
- **Основание:** решение пользователя о duplicate contract.

### AC-005 — Out-of-order update

- **Условия:** два fixture-events одного table/key имеют доказуемо разные source positions.
- **Действие:** более новое событие обрабатывается раньше старого.
- **Ожидание:** новое применяется; старое архивируется со статусом `stale`; projection не откатывается.
- **Проверка:** raw statuses, watermark и projection.
- **Основание:** решение пользователя latest source position wins.

### AC-006 — Старое событие после delete

- **Условия:** для ключа обработан delete с более новой source position.
- **Действие:** поступает старый create/update того же ключа.
- **Ожидание:** событие архивируется как `stale`; строка не восстанавливается.
- **Проверка:** watermark и отсутствие строки в projection.
- **Основание:** решение пользователя latest source position wins и hard delete.

### AC-007 — Schema change stop/migrate/replay

- **Условия:** source получает `customers.phone`, SQLite ещё не мигрирован.
- **Действие:** изменяется customer с заполненным `phone`.
- **Ожидание до миграции:** транзакция откатывается, offset не коммитится, consumer выдаёт понятную ошибку.
- **Действие после этого:** применяется SQLite-миграция и consumer перезапускается.
- **Ожидание после миграции:** тот же record успешно обрабатывается, `phone` появляется в projection, старые строки имеют `NULL`.
- **Проверка:** committed offset и запросы к SQLite до/после миграции.
- **Основание:** решение пользователя о schema change exercise.

### AC-008 — Independent topics

- **Условия:** связанные customer/order/payment events доступны в разных topics/partitions.
- **Действие:** consumer получает дочернюю запись до родительской.
- **Ожидание:** дочерняя запись применяется без блокировки; после обработки всех событий итоговое состояние совпадает с source.
- **Проверка:** промежуточные и итоговые SQLite queries.
- **Основание:** решение пользователя о независимых проекциях.

### AC-009 — Lag inspection

- **Условия:** consumer временно остановлен или замедлен.
- **Действие:** в source создаются новые изменения, затем выполняется инспекция.
- **Ожидание:** отдельно доступны consumer-group lag по partitions, event age и retained WAL bytes; значения не смешиваются.
- **Проверка:** Kafka group offsets, timestamps events/sink и PostgreSQL replication slot query.
- **Основание:** `docs/idea.md`, решение пользователя о трёх показателях.

### AC-010 — Новая CDC-генерация

- **Условия:** старые connector offsets или replication slot объявлены непригодными.
- **Действие:** выполняется bounded recovery procedure.
- **Ожидание:** старая генерация остаётся read-only; новая использует отдельные connector/slot/topics/SQLite, выполняет snapshot и выходит в streaming без смешивания поколений.
- **Проверка:** идентификаторы ресурсов, содержимое sinks и состояние connector.
- **Основание:** решение пользователя о recovery generation.

### AC-011 — Retention и disk bounds

- **Условия:** настроены согласованные Kafka, WAL и SQLite limits.
- **Действие:** создаётся контролируемый backlog или увеличивается объём raw archive.
- **Ожидание:** отображается retained WAL; при 512 MiB выдаётся предупреждение; Kafka удаляет только eligible segments в рамках time/size policy; при SQLite около 1 GiB consumer останавливается контролируемо; нагрузочный сценарий не стартует при свободном Docker storage менее 5 GiB.
- **Проверка:** Kafka topic configs/log sizes, `pg_replication_slots`, размер SQLite и preflight disk check.
- **Основание:** решение пользователя о bounded retention.

### AC-012 — Malformed event fail-stop

- **Условия:** consumer получает record с известной Kafka coordinate и невалидным envelope, key, op, table или ordering metadata.
- **Действие:** выполняется попытка обработки.
- **Ожидание:** projection не меняется; offset не коммитится; `ingestion_errors` содержит исходные bytes и диагностику; consumer завершается с ошибкой; после исправления тот же record доступен для повторной обработки.
- **Проверка:** SQLite queries, process exit status и committed consumer-group offset.
- **Основание:** решение пользователя о fail-stop contract.

### AC-013 — Least-privilege connector account

- **Условия:** publication создана bootstrap-администратором, connector использует выделенную role.
- **Действие:** выполняются initial snapshot и streaming; отдельно проверяются grants и отрицательная DML-операция от имени connector role.
- **Ожидание:** snapshot/streaming работают; role имеет LOGIN/REPLICATION, CONNECT/USAGE/SELECT, но не имеет SUPERUSER, database CREATE или DML; попытка изменить source-таблицу отклоняется.
- **Проверка:** PostgreSQL catalog/grant queries, connector status и ожидаемо неуспешная DML-команда.
- **Основание:** `docs/idea.md`, delegated least-privilege contract.

### AC-014 — Connector restart без нового snapshot

- **Условия:** initial snapshot завершён, connector offsets и logical slot сохранены.
- **Действие:** Kafka Connect/connector перезапускается, затем source получает новое изменение.
- **Ожидание:** повторный набор snapshot/read events не появляется; connector продолжает streaming с сохранённой позиции; новое изменение попадает в sink.
- **Проверка:** connector state, raw event operations и projection.
- **Основание:** `docs/idea.md`, решение пользователя о snapshot lifecycle.

## 10. Первый законченный срез

**Решено пользователем:** первый принимаемый законченный срез — вся базовая версия из раздела 2.2.

Внутренние этапы реализации допустимы, но промежуточный путь только для `customers` или без recovery/schema-change/out-of-order не считается завершением проекта.

## 11. Результат технического probe и остаточные допущения

### 11.1. Выполненная проверка

Probe выполнен 2026-10-26 на Linux `amd64`, Docker Engine `29.8.0`, Docker Compose `v5.5.1`, с закреплёнными образами из раздела 1.

Конфигурация connector включала:

- `plugin.name=pgoutput`;
- `snapshot.mode=initial`;
- `provide.transaction.metadata=true`;
- `publication.autocreate.mode=disabled`;
- JSON Converter со schemas для key/value;
- три partitions для table topic.

Сценарий:

1. До старта connector созданы две строки `customers`.
2. Initial snapshot создал два `op=r` event.
3. В одной PostgreSQL-транзакции ключ `id=1` обновлён дважды, затем обновлён `id=2`.
4. В отдельных транзакциях `id=1` ещё раз обновлён и удалён.
5. Из Kafka topic прочитаны полные values, partitions и offsets, включая tombstone.

Наблюдения:

- оба snapshot event имели общий `source.lsn=26317104`, `transaction=null`; markers были `first` и `last`;
- два последовательных update одного ключа в одной транзакции имели LSN `26317200` и `26317528`, общий `source.txId=740`, а `transaction.total_order`/`data_collection_order` — `1/1` и `2/2`;
- третье событие той же транзакции имело LSN `26317632` и order `3/3`;
- update следующей транзакции имел LSN `26317792`, delete — `26317944`; после delete получен отдельный tombstone с `value=null`;
- Kafka key сохранил один partition для одного primary key; Kafka offsets внутри partition возрастали в порядке доставки;
- PostgreSQL сообщил version `16.13`, Kafka broker и Connect runtime — `4.3.0`, connector task — `3.6.3.Final`.

Результат probe подтверждает token contract раздела 5.3 для initial snapshot и последующего streaming на закреплённом stack. Probe не обобщается на incremental snapshots, смену generation или иные версии; эти случаи соответственно запрещены базовым контрактом либо требуют нового probe.

### 11.2. Проверка Python client

В закреплённом `python:3.12.12-slim-bookworm` успешно установлен и импортирован `confluent-kafka==2.16.0`; runtime сообщил `confluent-kafka 2.16.0` и `librdkafka 2.16.0`. Exact version включается в lock-файл реализации без диапазона версий.

### 11.3. Остаточные допущения, не блокирующие approval

- Digest в image reference является immutable registry digest; tag используется только как человекочитаемая часть ссылки.
- Сравнение ordering tokens выполняется только внутри одной generation и одного table/key.
- Любое отсутствующее, некорректное или не соответствующее закреплённой схеме ordering metadata считается incomparable event и обрабатывается по fail-stop contract.
- Обновление stack или включение incremental snapshot возвращает документ в `NEEDS_PROBE` до повторной проверки.

## 12. Передача состояния

### Согласовано

- Полный исходный scope входит в базовую версию.
- Агент реализует проект только после явного утверждения спецификации.
- SQLite — единый durable sink.
- Hard delete, raw archive, duplicate и out-of-order semantics.
- Initial snapshot lifecycle.
- Управляемая schema migration через stop/migrate/replay.
- Независимые projection-таблицы.
- JSON со встроенной schema.
- Три partitions и primary-key ordering key.
- `TRUNCATE` исключён.
- Три отдельные lag/retention метрики.
- Потеря connector state восстанавливается новой CDC-генерацией.
- Source foreign keys запрещают каскадное удаление; связанные записи удаляются явно от `payments` к `customers`.
- Первичный ключ `id` неизменяем; update primary key исключён из CDC-контракта.
- Выбор точных совместимых версий делегирован агенту; запрещены плавающие `latest` и несогласованные major-обновления.
- Основной local-lab path использует Docker Engine и Docker Compose v2; consumer входит в контейнерный сценарий.
- Общий ресурсный бюджет: 8 vCPU, 16 GB RAM и 25 GB диска для образов и всех runtime-данных.
- Зафиксированы bounded retention: Kafka 24 часа/256 MiB на partition, WAL warning 512 MiB и slot limit 1 GiB, SQLite около 1 GiB, preflight minimum 5 GiB free.
- Оставшиеся технические решения, не меняющие поведение и критерии, делегированы агенту в явно заданных границах.
- Malformed/unknown/incomparable events используют fail-stop: диагностика сохраняется, projection и committed offset не продвигаются.

### Текущая точка остановки

Все пользовательские behavioral contracts согласованы. Ordering-token probe выполнен, token contract и exact version/digest metadata обновлены, блокирующих технических неизвестных не осталось. Пользователь явно утвердил документ; статус — `APPROVED`.

### Следующий шаг

Спецификация является утверждённым контрактом реализации. Агент может переходить к реализации полного проекта в согласованном scope.
