\set ON_ERROR_STOP on

-- Run only at the schema-change step from docs/spec.md section 5.5.
ALTER TABLE public.customers ADD COLUMN phone TEXT;

UPDATE public.customers
SET phone = '+15550000001'
WHERE id = 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.customers
        WHERE id = 1 AND phone = '+15550000001'
    ) OR NOT EXISTS (
        SELECT 1
        FROM public.customers
        WHERE id = 2 AND phone IS NULL
    ) THEN
        RAISE EXCEPTION 'schema-change exercise final state mismatch';
    END IF;
END;
$$;
