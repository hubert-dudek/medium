BEGIN;

CREATE TABLE public.orders (
    order_id BIGINT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    amount_cents BIGINT NOT NULL
);

INSERT INTO public.orders (order_id, tenant_id, amount_cents)
SELECT id, (id % 32)::integer, (17 * id) % 100000
FROM generate_series(1::bigint, 1000000::bigint) AS seed(id);

CREATE INDEX orders_tenant_order_idx ON public.orders (tenant_id, order_id);
ANALYZE public.orders;

COMMIT;

SELECT count(*) AS seeded_orders FROM public.orders;
SELECT order_id, tenant_id, amount_cents FROM public.orders WHERE order_id = 42;

SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE contype = 'p' AND conrelid = 'public.orders'::regclass;
