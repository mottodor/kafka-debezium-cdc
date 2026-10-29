CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum_sha256 TEXT NOT NULL CHECK (length(checksum_sha256) = 64),
    applied_at TEXT NOT NULL
) STRICT;

CREATE TABLE sink_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation TEXT NOT NULL UNIQUE CHECK (length(generation) > 0),
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE raw_events (
    topic TEXT NOT NULL CHECK (length(topic) > 0),
    "partition" INTEGER NOT NULL CHECK ("partition" >= 0),
    "offset" INTEGER NOT NULL CHECK ("offset" >= 0),
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    message_key BLOB NOT NULL,
    message_value BLOB,
    kafka_timestamp_ms INTEGER CHECK (kafka_timestamp_ms IS NULL OR kafka_timestamp_ms >= 0),
    operation TEXT CHECK (operation IS NULL OR operation IN ('r', 'c', 'u', 'd')),
    source_table TEXT NOT NULL CHECK (source_table IN ('customers', 'orders', 'payments')),
    entity_key TEXT NOT NULL CHECK (length(entity_key) > 0),
    snapshot_marker TEXT,
    source_lsn INTEGER CHECK (source_lsn IS NULL OR source_lsn >= 0),
    source_sequence TEXT,
    source_tx_id INTEGER,
    transaction_id TEXT,
    transaction_total_order INTEGER CHECK (
        transaction_total_order IS NULL OR transaction_total_order >= 0
    ),
    transaction_data_collection_order INTEGER CHECK (
        transaction_data_collection_order IS NULL
        OR transaction_data_collection_order >= 0
    ),
    source_timestamp_ms INTEGER CHECK (
        source_timestamp_ms IS NULL OR source_timestamp_ms >= 0
    ),
    processing_status TEXT NOT NULL CHECK (
        processing_status IN ('applied', 'stale', 'tombstone')
    ),
    delivery_count INTEGER NOT NULL DEFAULT 1 CHECK (delivery_count >= 1),
    durable_committed_at TEXT NOT NULL,
    PRIMARY KEY (topic, "partition", "offset")
) STRICT;

CREATE INDEX raw_events_entity_history_idx
    ON raw_events (generation, source_table, entity_key, source_lsn);
CREATE INDEX raw_events_operation_idx
    ON raw_events (generation, operation, processing_status);

CREATE TABLE raw_headers (
    topic TEXT NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" INTEGER NOT NULL,
    header_index INTEGER NOT NULL CHECK (header_index >= 0),
    header_key TEXT NOT NULL CHECK (length(header_key) > 0),
    header_value BLOB,
    PRIMARY KEY (topic, "partition", "offset", header_index),
    FOREIGN KEY (topic, "partition", "offset")
        REFERENCES raw_events (topic, "partition", "offset")
        ON DELETE CASCADE
) STRICT;

CREATE TABLE ingestion_errors (
    topic TEXT NOT NULL CHECK (length(topic) > 0),
    "partition" INTEGER NOT NULL CHECK ("partition" >= 0),
    "offset" INTEGER NOT NULL CHECK ("offset" >= 0),
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    message_key BLOB,
    message_value BLOB,
    kafka_timestamp_ms INTEGER CHECK (kafka_timestamp_ms IS NULL OR kafka_timestamp_ms >= 0),
    error_class TEXT NOT NULL CHECK (length(error_class) > 0),
    error_message TEXT NOT NULL CHECK (length(error_message) > 0),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
    PRIMARY KEY (topic, "partition", "offset")
) STRICT;

CREATE TABLE customers_current (
    id INTEGER PRIMARY KEY,
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE orders_current (
    id INTEGER PRIMARY KEY,
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    total_amount TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE payments_current (
    id INTEGER PRIMARY KEY,
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    order_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    amount TEXT NOT NULL,
    provider_ref TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE entity_watermarks (
    generation TEXT NOT NULL CHECK (length(generation) > 0),
    source_table TEXT NOT NULL CHECK (source_table IN ('customers', 'orders', 'payments')),
    entity_key TEXT NOT NULL CHECK (length(entity_key) > 0),
    phase_rank INTEGER NOT NULL CHECK (phase_rank IN (0, 1)),
    source_lsn INTEGER NOT NULL CHECK (source_lsn >= 0),
    transaction_total_order INTEGER NOT NULL CHECK (transaction_total_order >= 0),
    transaction_data_collection_order INTEGER NOT NULL CHECK (
        transaction_data_collection_order >= 0
    ),
    last_was_delete INTEGER NOT NULL CHECK (last_was_delete IN (0, 1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (generation, source_table, entity_key)
) STRICT;

CREATE TRIGGER sink_metadata_binding_insert
BEFORE INSERT ON sink_metadata
WHEN EXISTS (SELECT 1 FROM sink_metadata)
BEGIN
    SELECT RAISE(ABORT, 'sink generation binding is immutable');
END;
CREATE TRIGGER sink_metadata_binding_update
BEFORE UPDATE OF generation ON sink_metadata
BEGIN
    SELECT RAISE(ABORT, 'sink generation binding is immutable');
END;
CREATE TRIGGER sink_metadata_binding_delete
BEFORE DELETE ON sink_metadata
BEGIN
    SELECT RAISE(ABORT, 'sink generation binding is immutable');
END;

CREATE TRIGGER raw_events_generation_insert
BEFORE INSERT ON raw_events
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'raw_events generation does not match sink');
END;
CREATE TRIGGER raw_events_generation_update
BEFORE UPDATE OF generation ON raw_events
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'raw_events generation does not match sink');
END;

CREATE TRIGGER ingestion_errors_generation_insert
BEFORE INSERT ON ingestion_errors
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'ingestion_errors generation does not match sink');
END;
CREATE TRIGGER ingestion_errors_generation_update
BEFORE UPDATE OF generation ON ingestion_errors
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'ingestion_errors generation does not match sink');
END;

CREATE TRIGGER customers_current_generation_insert
BEFORE INSERT ON customers_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'customers_current generation does not match sink');
END;
CREATE TRIGGER customers_current_generation_update
BEFORE UPDATE OF generation ON customers_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'customers_current generation does not match sink');
END;

CREATE TRIGGER orders_current_generation_insert
BEFORE INSERT ON orders_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'orders_current generation does not match sink');
END;
CREATE TRIGGER orders_current_generation_update
BEFORE UPDATE OF generation ON orders_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'orders_current generation does not match sink');
END;

CREATE TRIGGER payments_current_generation_insert
BEFORE INSERT ON payments_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'payments_current generation does not match sink');
END;
CREATE TRIGGER payments_current_generation_update
BEFORE UPDATE OF generation ON payments_current
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'payments_current generation does not match sink');
END;

CREATE TRIGGER entity_watermarks_generation_insert
BEFORE INSERT ON entity_watermarks
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'entity_watermarks generation does not match sink');
END;
CREATE TRIGGER entity_watermarks_generation_update
BEFORE UPDATE OF generation ON entity_watermarks
WHEN NOT EXISTS (
    SELECT 1 FROM sink_metadata
    WHERE singleton = 1 AND generation = NEW.generation
)
BEGIN
    SELECT RAISE(ABORT, 'entity_watermarks generation does not match sink');
END;
