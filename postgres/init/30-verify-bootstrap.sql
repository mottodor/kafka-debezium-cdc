\set ON_ERROR_STOP on

DO $$
DECLARE
    actual_columns TEXT[];
    actual_identity_triggers TEXT[];
    actual_publication_tables TEXT[];
    table_name TEXT;
    privilege_name TEXT;
    sequence_last_value BIGINT;
    sequence_is_called BOOLEAN;
BEGIN
    SELECT array_agg(
        format(
            '%s.%s:%s:%s:%s:%s',
            source_column.table_name,
            source_column.column_name,
            source_column.udt_name,
            source_column.is_nullable,
            source_column.is_identity,
            coalesce(source_column.identity_generation, '-')
        )
        ORDER BY
            CASE source_column.table_name
                WHEN 'customers' THEN 1
                WHEN 'orders' THEN 2
                WHEN 'payments' THEN 3
            END,
            source_column.ordinal_position
    )
    INTO actual_columns
    FROM information_schema.columns AS source_column
    WHERE source_column.table_schema = 'public'
      AND source_column.table_name IN ('customers', 'orders', 'payments');

    IF actual_columns IS DISTINCT FROM ARRAY[
        'customers.id:int8:NO:YES:ALWAYS',
        'customers.email:text:NO:NO:-',
        'customers.full_name:text:NO:NO:-',
        'customers.status:text:NO:NO:-',
        'customers.created_at:timestamptz:NO:NO:-',
        'customers.updated_at:timestamptz:NO:NO:-',
        'orders.id:int8:NO:YES:ALWAYS',
        'orders.customer_id:int8:NO:NO:-',
        'orders.status:text:NO:NO:-',
        'orders.total_amount:numeric:NO:NO:-',
        'orders.created_at:timestamptz:NO:NO:-',
        'orders.updated_at:timestamptz:NO:NO:-',
        'payments.id:int8:NO:YES:ALWAYS',
        'payments.order_id:int8:NO:NO:-',
        'payments.status:text:NO:NO:-',
        'payments.amount:numeric:NO:NO:-',
        'payments.provider_ref:text:NO:NO:-',
        'payments.created_at:timestamptz:NO:NO:-',
        'payments.updated_at:timestamptz:NO:NO:-'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'source column contract mismatch: %', actual_columns;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS monetary_column
        WHERE monetary_column.table_schema = 'public'
          AND monetary_column.table_name = 'orders'
          AND monetary_column.column_name = 'total_amount'
          AND monetary_column.numeric_precision = 12
          AND monetary_column.numeric_scale = 2
    ) OR NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS monetary_column
        WHERE monetary_column.table_schema = 'public'
          AND monetary_column.table_name = 'payments'
          AND monetary_column.column_name = 'amount'
          AND monetary_column.numeric_precision = 12
          AND monetary_column.numeric_scale = 2
    ) THEN
        RAISE EXCEPTION 'monetary column precision/scale mismatch';
    END IF;

    SELECT array_agg(format('%s.%s', c.relname, t.tgname) ORDER BY c.relname)
    INTO actual_identity_triggers
    FROM pg_catalog.pg_trigger AS t
    JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname IN ('customers', 'orders', 'payments')
      AND NOT t.tgisinternal
      AND t.tgenabled = 'O'
      AND t.tgfoid = 'public.protect_identity_and_touch_updated_at()'::regprocedure
      AND t.tgtype = 19;

    IF actual_identity_triggers IS DISTINCT FROM ARRAY[
        'customers.customers_protect_identity_and_touch_updated_at',
        'orders.orders_protect_identity_and_touch_updated_at',
        'payments.payments_protect_identity_and_touch_updated_at'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'immutable-ID trigger contract mismatch: %', actual_identity_triggers;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conname = 'orders_customer_fk'
          AND contype = 'f'
          AND conrelid = 'public.orders'::regclass
          AND confrelid = 'public.customers'::regclass
          AND confdeltype = 'r'
          AND conkey = ARRAY[(
              SELECT attnum::SMALLINT
              FROM pg_catalog.pg_attribute
              WHERE attrelid = 'public.orders'::regclass
                AND attname = 'customer_id'
          )]
          AND confkey = ARRAY[(
              SELECT attnum::SMALLINT
              FROM pg_catalog.pg_attribute
              WHERE attrelid = 'public.customers'::regclass
                AND attname = 'id'
          )]
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conname = 'payments_order_fk'
          AND contype = 'f'
          AND conrelid = 'public.payments'::regclass
          AND confrelid = 'public.orders'::regclass
          AND confdeltype = 'r'
          AND conkey = ARRAY[(
              SELECT attnum::SMALLINT
              FROM pg_catalog.pg_attribute
              WHERE attrelid = 'public.payments'::regclass
                AND attname = 'order_id'
          )]
          AND confkey = ARRAY[(
              SELECT attnum::SMALLINT
              FROM pg_catalog.pg_attribute
              WHERE attrelid = 'public.orders'::regclass
                AND attname = 'id'
          )]
    ) THEN
        RAISE EXCEPTION 'source foreign-key contract mismatch';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'cdc_connector'
          AND rolcanlogin
          AND rolreplication
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
          AND NOT rolinherit
          AND NOT rolbypassrls
    ) THEN
        RAISE EXCEPTION 'cdc_connector role attributes do not match the least-privilege contract';
    END IF;

    IF NOT has_database_privilege('cdc_connector', current_database(), 'CONNECT')
       OR has_database_privilege('cdc_connector', current_database(), 'CREATE') THEN
        RAISE EXCEPTION 'cdc_connector database privileges do not match the contract';
    END IF;

    IF NOT has_schema_privilege('cdc_connector', 'public', 'USAGE')
       OR has_schema_privilege('cdc_connector', 'public', 'CREATE') THEN
        RAISE EXCEPTION 'cdc_connector schema privileges do not match the contract';
    END IF;

    FOREACH table_name IN ARRAY ARRAY['customers', 'orders', 'payments']
    LOOP
        IF NOT has_table_privilege('cdc_connector', format('public.%I', table_name), 'SELECT') THEN
            RAISE EXCEPTION 'cdc_connector lacks SELECT on public.%', table_name;
        END IF;

        FOREACH privilege_name IN ARRAY ARRAY[
            'INSERT',
            'UPDATE',
            'DELETE',
            'TRUNCATE',
            'REFERENCES',
            'TRIGGER'
        ]
        LOOP
            IF has_table_privilege(
                'cdc_connector',
                format('public.%I', table_name),
                privilege_name
            ) THEN
                RAISE EXCEPTION 'cdc_connector has forbidden % privilege on public.%',
                    privilege_name,
                    table_name;
            END IF;
        END LOOP;

        FOREACH privilege_name IN ARRAY ARRAY['INSERT', 'UPDATE', 'REFERENCES']
        LOOP
            IF has_any_column_privilege(
                'cdc_connector',
                format('public.%I', table_name),
                privilege_name
            ) THEN
                RAISE EXCEPTION 'cdc_connector has forbidden column-level % privilege on public.%',
                    privilege_name,
                    table_name;
            END IF;
        END LOOP;
    END LOOP;

    SELECT array_agg(format('%I.%I', schemaname, tablename) ORDER BY schemaname, tablename)
    INTO actual_publication_tables
    FROM pg_catalog.pg_publication_tables
    WHERE pubname = 'postgres_cdc_g001_pub';

    IF actual_publication_tables IS DISTINCT FROM ARRAY[
        'public.customers',
        'public.orders',
        'public.payments'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'publication tables mismatch: %', actual_publication_tables;
    END IF;

    IF (SELECT count(*) FROM public.customers) <> 2
       OR NOT EXISTS (
           SELECT 1 FROM public.customers
           WHERE id = 1
             AND email = 'alice@example.test'
             AND full_name = 'Alice Adams'
             AND status = 'active'
       )
       OR NOT EXISTS (
           SELECT 1 FROM public.customers
           WHERE id = 2
             AND email = 'bob@example.test'
             AND full_name = 'Bob Brown'
             AND status = 'active'
       ) THEN
        RAISE EXCEPTION 'customers seed mismatch';
    END IF;

    IF (SELECT count(*) FROM public.orders) <> 2
       OR NOT EXISTS (
           SELECT 1 FROM public.orders
           WHERE id = 101
             AND customer_id = 1
             AND status = 'pending'
             AND total_amount = 120.50
       )
       OR NOT EXISTS (
           SELECT 1 FROM public.orders
           WHERE id = 102
             AND customer_id = 2
             AND status = 'paid'
             AND total_amount = 49.99
       ) THEN
        RAISE EXCEPTION 'orders seed mismatch';
    END IF;

    IF (SELECT count(*) FROM public.payments) <> 1
       OR NOT EXISTS (
           SELECT 1 FROM public.payments
           WHERE id = 1001
             AND order_id = 102
             AND status = 'captured'
             AND amount = 49.99
             AND provider_ref = 'pay-1001'
       ) THEN
        RAISE EXCEPTION 'payments seed mismatch';
    END IF;

    SELECT last_value, is_called
    INTO sequence_last_value, sequence_is_called
    FROM public.customers_id_seq;
    IF sequence_last_value <> 3 OR sequence_is_called THEN
        RAISE EXCEPTION 'customers identity sequence is not positioned for id=3';
    END IF;

    SELECT last_value, is_called
    INTO sequence_last_value, sequence_is_called
    FROM public.orders_id_seq;
    IF sequence_last_value <> 103 OR sequence_is_called THEN
        RAISE EXCEPTION 'orders identity sequence is not positioned for id=103';
    END IF;

    SELECT last_value, is_called
    INTO sequence_last_value, sequence_is_called
    FROM public.payments_id_seq;
    IF sequence_last_value <> 1002 OR sequence_is_called THEN
        RAISE EXCEPTION 'payments identity sequence is not positioned for id=1002';
    END IF;
END;
$$;
