\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE public.customers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL CHECK (btrim(full_name) <> ''),
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'cancelled')),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT orders_customer_fk
        FOREIGN KEY (customer_id)
        REFERENCES public.customers (id)
        ON DELETE RESTRICT
);

CREATE TABLE public.payments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('authorized', 'captured', 'failed', 'refunded')),
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    provider_ref TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT payments_order_fk
        FOREIGN KEY (order_id)
        REFERENCES public.orders (id)
        ON DELETE RESTRICT
);

CREATE FUNCTION public.protect_identity_and_touch_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id THEN
        RAISE EXCEPTION 'primary key id is immutable for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER customers_protect_identity_and_touch_updated_at
BEFORE UPDATE ON public.customers
FOR EACH ROW
EXECUTE FUNCTION public.protect_identity_and_touch_updated_at();

CREATE TRIGGER orders_protect_identity_and_touch_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.protect_identity_and_touch_updated_at();

CREATE TRIGGER payments_protect_identity_and_touch_updated_at
BEFORE UPDATE ON public.payments
FOR EACH ROW
EXECUTE FUNCTION public.protect_identity_and_touch_updated_at();

INSERT INTO public.customers (id, email, full_name, status, created_at, updated_at)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 'alice@example.test', 'Alice Adams', 'active', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'),
    (2, 'bob@example.test', 'Bob Brown', 'active', '2026-01-01 00:01:00+00', '2026-01-01 00:01:00+00');

INSERT INTO public.orders (id, customer_id, status, total_amount, created_at, updated_at)
OVERRIDING SYSTEM VALUE
VALUES
    (101, 1, 'pending', 120.50, '2026-01-01 00:02:00+00', '2026-01-01 00:02:00+00'),
    (102, 2, 'paid', 49.99, '2026-01-01 00:03:00+00', '2026-01-01 00:03:00+00');

INSERT INTO public.payments (id, order_id, status, amount, provider_ref, created_at, updated_at)
OVERRIDING SYSTEM VALUE
VALUES
    (1001, 102, 'captured', 49.99, 'pay-1001', '2026-01-01 00:04:00+00', '2026-01-01 00:04:00+00');

SELECT setval(pg_get_serial_sequence('public.customers', 'id'), 3, false);
SELECT setval(pg_get_serial_sequence('public.orders', 'id'), 103, false);
SELECT setval(pg_get_serial_sequence('public.payments', 'id'), 1002, false);

COMMIT;
