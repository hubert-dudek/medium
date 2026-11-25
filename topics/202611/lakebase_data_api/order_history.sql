BEGIN;

CREATE TABLE IF NOT EXISTS public.orders_cdc (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    operation text NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    order_id bigint NOT NULL,
    old_amount_cents bigint,
    new_amount_cents bigint
);

CREATE INDEX IF NOT EXISTS orders_cdc_lookup
    ON public.orders_cdc (order_id, event_id DESC);

CREATE OR REPLACE FUNCTION public.capture_order_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    INSERT INTO public.orders_cdc (operation, order_id, old_amount_cents, new_amount_cents)
    VALUES (
        TG_OP,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END,
        CASE WHEN TG_OP <> 'INSERT' THEN OLD.amount_cents END,
        CASE WHEN TG_OP <> 'DELETE' THEN NEW.amount_cents END
    );
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION public.capture_order_change() FROM PUBLIC;

CREATE OR REPLACE TRIGGER orders_capture_change
AFTER INSERT OR UPDATE OR DELETE ON public.orders
FOR EACH ROW EXECUTE FUNCTION public.capture_order_change();

COMMIT;
