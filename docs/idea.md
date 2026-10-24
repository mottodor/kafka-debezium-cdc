# PostgreSQL CDC с Debezium и Kafka

## Назначение

Независимый учебный проект для изучения Change Data Capture из PostgreSQL через WAL, Debezium и Kafka. Основная ценность проекта — понимание replication slot, connector lifecycle, event envelope, offsets, create/update/delete semantics, повторной доставки и восстановления consumer.

В базовой версии sink должен быть простым и прозрачным. Полный Iceberg/dbt/ClickHouse pipeline намеренно исключён, чтобы не скрывать CDC-механику за сложностью downstream-платформы.

## Приобретаемые компетенции

- объяснять PostgreSQL logical replication;
- создавать least-privilege replication account;
- понимать publication и replication slot;
- настраивать Debezium connector;
- читать Debezium create/update/delete/tombstone events;
- различать snapshot и streaming phases;
- понимать Kafka topic, partition, offset и consumer group;
- вычислять lag;
- коммитить consumer offset только после durable sink commit;
- обеспечивать idempotent sink;
- восстанавливаться после consumer restart;
- обрабатывать duplicate delivery;
- выбирать ordering key;
- документировать delete semantics;
- понимать риски slot retention и роста WAL;
- не заявлять exactly-once без end-to-end доказательства.

## Границы проекта

### Входит в scope

- PostgreSQL logical replication;
- Kafka в single-node KRaft для local lab;
- Kafka Connect + Debezium PostgreSQL connector;
- topics для customers, orders и payments;
- Python consumer;
- raw event archive;
- current-state projection;
- restart/replay;
- lag inspection;
- schema change exercise;
- duplicate и out-of-order tests;
- bounded recovery procedures.

### Не входит в scope

- Airflow;
- dbt;
- ClickHouse;
- Superset;
- Spark;
- Schema Registry в базовой версии;
- multi-broker production cluster;
- cross-region replication;
- гарантии глобального порядка между topics;
- обработка всех OLTP-таблиц.

