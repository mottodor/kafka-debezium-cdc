\set ON_ERROR_STOP on

-- Preserve the observable child-to-parent delete order from docs/spec.md section 9.1.
DO $$
DECLARE
    affected_rows BIGINT;
BEGIN
    DELETE FROM public.payments WHERE id = 1002;
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    IF affected_rows <> 1 THEN
        RAISE EXCEPTION 'expected to delete payment 1002 exactly once, deleted % rows', affected_rows;
    END IF;
END;
$$;

DO $$
DECLARE
    affected_rows BIGINT;
BEGIN
    DELETE FROM public.orders WHERE id = 103;
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    IF affected_rows <> 1 THEN
        RAISE EXCEPTION 'expected to delete order 103 exactly once, deleted % rows', affected_rows;
    END IF;
END;
$$;

DO $$
DECLARE
    affected_rows BIGINT;
BEGIN
    DELETE FROM public.customers WHERE id = 3;
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    IF affected_rows <> 1 THEN
        RAISE EXCEPTION 'expected to delete customer 3 exactly once, deleted % rows', affected_rows;
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.payments WHERE id = 1002)
       OR EXISTS (SELECT 1 FROM public.orders WHERE id = 103)
       OR EXISTS (SELECT 1 FROM public.customers WHERE id = 3) THEN
        RAISE EXCEPTION 'delete exercise left fixture rows behind';
    END IF;
END;
$$;
