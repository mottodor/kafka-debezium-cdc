\set ON_ERROR_STOP on

-- Streaming fixture from docs/spec.md section 9.1.
DO $$
DECLARE
    new_id BIGINT;
BEGIN
    INSERT INTO public.customers (email, full_name, status)
    VALUES ('carol@example.test', 'Carol Clark', 'active')
    RETURNING id INTO new_id;

    IF new_id <> 3 THEN
        RAISE EXCEPTION 'expected customer id 3, received %', new_id;
    END IF;
END;
$$;

UPDATE public.customers
SET full_name = 'Alice Cooper',
    status = 'inactive'
WHERE id = 1;

DO $$
DECLARE
    new_id BIGINT;
BEGIN
    INSERT INTO public.orders (customer_id, status, total_amount)
    VALUES (3, 'pending', 15.00)
    RETURNING id INTO new_id;

    IF new_id <> 103 THEN
        RAISE EXCEPTION 'expected order id 103, received %', new_id;
    END IF;
END;
$$;

DO $$
DECLARE
    new_id BIGINT;
BEGIN
    INSERT INTO public.payments (order_id, status, amount, provider_ref)
    VALUES (103, 'authorized', 15.00, 'pay-1002')
    RETURNING id INTO new_id;

    IF new_id <> 1002 THEN
        RAISE EXCEPTION 'expected payment id 1002, received %', new_id;
    END IF;
END;
$$;

UPDATE public.orders
SET status = 'paid'
WHERE id = 103;

UPDATE public.payments
SET status = 'captured'
WHERE id = 1002;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.customers
        WHERE id = 1 AND full_name = 'Alice Cooper' AND status = 'inactive'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.customers
        WHERE id = 3 AND email = 'carol@example.test' AND full_name = 'Carol Clark' AND status = 'active'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.orders
        WHERE id = 103 AND customer_id = 3 AND status = 'paid' AND total_amount = 15.00
    ) OR NOT EXISTS (
        SELECT 1 FROM public.payments
        WHERE id = 1002 AND order_id = 103 AND status = 'captured' AND amount = 15.00 AND provider_ref = 'pay-1002'
    ) THEN
        RAISE EXCEPTION 'streaming exercise final state mismatch';
    END IF;
END;
$$;
