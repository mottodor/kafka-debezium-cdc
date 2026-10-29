\set ON_ERROR_STOP on

DO $$
DECLARE
    actual_publication_tables TEXT[];
    table_name TEXT;
    privilege_name TEXT;
    sequence_last_value BIGINT;
    sequence_is_called BOOLEAN;
BEGIN
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
