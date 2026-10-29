\set ON_ERROR_STOP on

DO $$
DECLARE
    actual_columns TEXT[];
    actual_table_properties TEXT[];
    actual_identity_triggers TEXT[];
    actual_constraints TEXT[];
    actual_column_defaults TEXT[];
    actual_publication_settings TEXT;
    actual_publication_tables TEXT[];
    normalized_trigger_body TEXT;
    table_name TEXT;
    privilege_name TEXT;
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

    SELECT array_agg(
        format(
            '%s:%s:%s:%s:%s:%s:%s',
            source_table.relname,
            source_table.relkind,
            source_table.relpersistence,
            source_table.relreplident,
            source_table.relrowsecurity,
            source_table.relforcerowsecurity,
            source_table.relispartition
        )
        ORDER BY source_table.relname
    )
    INTO actual_table_properties
    FROM pg_catalog.pg_class AS source_table
    JOIN pg_catalog.pg_namespace AS source_namespace
      ON source_namespace.oid = source_table.relnamespace
    WHERE source_namespace.nspname = 'public'
      AND source_table.relname IN ('customers', 'orders', 'payments');

    IF actual_table_properties IS DISTINCT FROM ARRAY[
        'customers:r:p:d:f:f:f',
        'orders:r:p:d:f:f:f',
        'payments:r:p:d:f:f:f'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'source table properties mismatch: %', actual_table_properties;
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

    SELECT array_agg(
        format(
            '%s.%s:%s',
            source_table.relname,
            source_attribute.attname,
            pg_catalog.pg_get_expr(source_default.adbin, source_default.adrelid, true)
        )
        ORDER BY source_table.relname, source_attribute.attnum
    )
    INTO actual_column_defaults
    FROM pg_catalog.pg_attrdef AS source_default
    JOIN pg_catalog.pg_class AS source_table ON source_table.oid = source_default.adrelid
    JOIN pg_catalog.pg_namespace AS source_namespace ON source_namespace.oid = source_table.relnamespace
    JOIN pg_catalog.pg_attribute AS source_attribute
      ON source_attribute.attrelid = source_default.adrelid
     AND source_attribute.attnum = source_default.adnum
    WHERE source_namespace.nspname = 'public'
      AND source_table.relname IN ('customers', 'orders', 'payments');

    IF actual_column_defaults IS DISTINCT FROM ARRAY[
        'customers.created_at:CURRENT_TIMESTAMP',
        'customers.updated_at:CURRENT_TIMESTAMP',
        'orders.created_at:CURRENT_TIMESTAMP',
        'orders.updated_at:CURRENT_TIMESTAMP',
        'payments.created_at:CURRENT_TIMESTAMP',
        'payments.updated_at:CURRENT_TIMESTAMP'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'source column defaults mismatch: %', actual_column_defaults;
    END IF;

    SELECT btrim(regexp_replace(source_function.prosrc, E'\\s+', ' ', 'g'))
    INTO normalized_trigger_body
    FROM pg_catalog.pg_proc AS source_function
    JOIN pg_catalog.pg_namespace AS function_namespace
      ON function_namespace.oid = source_function.pronamespace
    JOIN pg_catalog.pg_language AS function_language
      ON function_language.oid = source_function.prolang
    WHERE function_namespace.nspname = 'public'
      AND source_function.proname = 'protect_identity_and_touch_updated_at'
      AND source_function.pronargs = 0
      AND source_function.prorettype = 'pg_catalog.trigger'::regtype
      AND function_language.lanname = 'plpgsql'
      AND source_function.prokind = 'f'
      AND source_function.provolatile = 'v'
      AND NOT source_function.prosecdef;

    IF normalized_trigger_body IS DISTINCT FROM
       'BEGIN IF NEW.id IS DISTINCT FROM OLD.id THEN RAISE EXCEPTION ''primary key id is immutable for %.%'', TG_TABLE_SCHEMA, TG_TABLE_NAME USING ERRCODE = ''integrity_constraint_violation''; END IF; NEW.updated_at := CURRENT_TIMESTAMP; RETURN NEW; END;' THEN
        RAISE EXCEPTION 'immutable-ID trigger function body mismatch: %', normalized_trigger_body;
    END IF;

    SELECT array_agg(
        format(
            '%s:%s:%s',
            source_table.relname,
            source_trigger.tgenabled,
            pg_catalog.pg_get_triggerdef(source_trigger.oid, true)
        )
        ORDER BY source_table.relname, source_trigger.tgname
    )
    INTO actual_identity_triggers
    FROM pg_catalog.pg_trigger AS source_trigger
    JOIN pg_catalog.pg_class AS source_table ON source_table.oid = source_trigger.tgrelid
    JOIN pg_catalog.pg_namespace AS source_namespace ON source_namespace.oid = source_table.relnamespace
    WHERE source_namespace.nspname = 'public'
      AND source_table.relname IN ('customers', 'orders', 'payments')
      AND NOT source_trigger.tgisinternal;

    IF actual_identity_triggers IS DISTINCT FROM ARRAY[
        'customers:O:CREATE TRIGGER customers_protect_identity_and_touch_updated_at BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION protect_identity_and_touch_updated_at()',
        'orders:O:CREATE TRIGGER orders_protect_identity_and_touch_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION protect_identity_and_touch_updated_at()',
        'payments:O:CREATE TRIGGER payments_protect_identity_and_touch_updated_at BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION protect_identity_and_touch_updated_at()'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'source trigger definitions mismatch: %', actual_identity_triggers;
    END IF;

    SELECT array_agg(
        format(
            '%s.%s:%s:%s:%s:%s:%s',
            source_table.relname,
            source_constraint.conname,
            source_constraint.contype,
            source_constraint.convalidated,
            source_constraint.condeferrable,
            source_constraint.condeferred,
            pg_catalog.pg_get_constraintdef(source_constraint.oid, true)
        )
        ORDER BY source_table.relname, source_constraint.conname
    )
    INTO actual_constraints
    FROM pg_catalog.pg_constraint AS source_constraint
    JOIN pg_catalog.pg_class AS source_table ON source_table.oid = source_constraint.conrelid
    JOIN pg_catalog.pg_namespace AS source_namespace ON source_namespace.oid = source_table.relnamespace
    WHERE source_namespace.nspname = 'public'
      AND source_table.relname IN ('customers', 'orders', 'payments')
      AND source_constraint.contype IN ('p', 'u', 'c', 'f');

    IF actual_constraints IS DISTINCT FROM ARRAY[
        'customers.customers_email_key:u:t:f:f:UNIQUE (email)',
        'customers.customers_full_name_check:c:t:f:f:CHECK (btrim(full_name) <> ''''::text)',
        'customers.customers_pkey:p:t:f:f:PRIMARY KEY (id)',
        'customers.customers_status_check:c:t:f:f:CHECK (status = ANY (ARRAY[''active''::text, ''inactive''::text]))',
        'orders.orders_customer_fk:f:t:f:f:FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE RESTRICT',
        'orders.orders_pkey:p:t:f:f:PRIMARY KEY (id)',
        'orders.orders_status_check:c:t:f:f:CHECK (status = ANY (ARRAY[''pending''::text, ''paid''::text, ''cancelled''::text]))',
        'orders.orders_total_amount_check:c:t:f:f:CHECK (total_amount >= 0::numeric)',
        'payments.payments_amount_check:c:t:f:f:CHECK (amount > 0::numeric)',
        'payments.payments_order_fk:f:t:f:f:FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE RESTRICT',
        'payments.payments_pkey:p:t:f:f:PRIMARY KEY (id)',
        'payments.payments_provider_ref_key:u:t:f:f:UNIQUE (provider_ref)',
        'payments.payments_status_check:c:t:f:f:CHECK (status = ANY (ARRAY[''authorized''::text, ''captured''::text, ''failed''::text, ''refunded''::text]))'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'source constraint definitions mismatch: %', actual_constraints;
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

    SELECT format(
        '%s:%s:%s:%s:%s:%s:%s',
        source_publication.pubname,
        source_publication.puballtables,
        source_publication.pubinsert,
        source_publication.pubupdate,
        source_publication.pubdelete,
        source_publication.pubtruncate,
        source_publication.pubviaroot
    )
    INTO actual_publication_settings
    FROM pg_catalog.pg_publication AS source_publication
    WHERE source_publication.pubname = 'postgres_cdc_g001_pub';

    IF actual_publication_settings IS DISTINCT FROM
       'postgres_cdc_g001_pub:f:t:t:t:t:f' THEN
        RAISE EXCEPTION 'publication settings mismatch: %', actual_publication_settings;
    END IF;

    SELECT array_agg(
        format(
            '%I.%I:%s:%s',
            source_namespace.nspname,
            source_table.relname,
            coalesce(source_relation.prattrs::TEXT, '-'),
            coalesce(
                pg_catalog.pg_get_expr(
                    source_relation.prqual,
                    source_relation.prrelid,
                    true
                ),
                '-'
            )
        )
        ORDER BY source_namespace.nspname, source_table.relname
    )
    INTO actual_publication_tables
    FROM pg_catalog.pg_publication_rel AS source_relation
    JOIN pg_catalog.pg_publication AS source_publication
      ON source_publication.oid = source_relation.prpubid
    JOIN pg_catalog.pg_class AS source_table
      ON source_table.oid = source_relation.prrelid
    JOIN pg_catalog.pg_namespace AS source_namespace
      ON source_namespace.oid = source_table.relnamespace
    WHERE source_publication.pubname = 'postgres_cdc_g001_pub';

    IF actual_publication_tables IS DISTINCT FROM ARRAY[
        'public.customers:-:-',
        'public.orders:-:-',
        'public.payments:-:-'
    ]::TEXT[] THEN
        RAISE EXCEPTION 'publication table definitions mismatch: %', actual_publication_tables;
    END IF;
END;
$$;
