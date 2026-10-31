-- 0002_outbox_and_trigger.sql
-- Change capture outbox and row-level trigger on orders table

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'outbox_status') THEN
        CREATE TYPE outbox_status AS ENUM ('pending', 'claimed', 'done', 'dead');
    END IF;
END
$$;

-- Outbox table storing dirty (tenant_id, bucket_hour) hints
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    bucket_hour TIMESTAMPTZ NOT NULL,
    status outbox_status NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ NULL,
    claimed_by TEXT NULL,
    claim_token UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL
);

-- Index supporting fast FOR UPDATE SKIP LOCKED batch claiming and lease recovery
CREATE INDEX IF NOT EXISTS idx_outbox_claim 
    ON outbox (status, next_retry_at, id)
    WHERE status IN ('pending', 'claimed');

-- Index supporting fast retention cleanup
CREATE INDEX IF NOT EXISTS idx_outbox_cleanup
    ON outbox (status, processed_at)
    WHERE status = 'done';

-- Trigger function appending minimal dirty-bucket hints (zero joins, zero aggregation)
CREATE OR REPLACE FUNCTION notify_order_bucket_change()
RETURNS TRIGGER AS $$
DECLARE
    new_bucket TIMESTAMPTZ;
    old_bucket TIMESTAMPTZ;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        new_bucket := date_trunc('hour', NEW.created_at AT TIME ZONE 'Asia/Kolkata');
        INSERT INTO outbox (tenant_id, bucket_hour) VALUES (NEW.tenant_id, new_bucket);
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        new_bucket := date_trunc('hour', NEW.created_at AT TIME ZONE 'Asia/Kolkata');
        old_bucket := date_trunc('hour', OLD.created_at AT TIME ZONE 'Asia/Kolkata');

        -- Always append NEW bucket
        INSERT INTO outbox (tenant_id, bucket_hour) VALUES (NEW.tenant_id, new_bucket);

        -- If tenant changed or hour bucket changed, also append OLD bucket
        IF (NEW.tenant_id <> OLD.tenant_id OR new_bucket <> old_bucket) THEN
            INSERT INTO outbox (tenant_id, bucket_hour) VALUES (OLD.tenant_id, old_bucket);
        END IF;

        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        old_bucket := date_trunc('hour', OLD.created_at AT TIME ZONE 'Asia/Kolkata');
        INSERT INTO outbox (tenant_id, bucket_hour) VALUES (OLD.tenant_id, old_bucket);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_bucket_change ON orders;
CREATE TRIGGER trg_orders_bucket_change
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_bucket_change();

-- Maintenance procedures: bulk load helpers
CREATE OR REPLACE PROCEDURE disable_order_triggers()
LANGUAGE sql AS $$
    ALTER TABLE orders DISABLE TRIGGER trg_orders_bucket_change;
$$;

CREATE OR REPLACE PROCEDURE enable_order_triggers()
LANGUAGE sql AS $$
    ALTER TABLE orders ENABLE TRIGGER trg_orders_bucket_change;
$$;

-- Retention cleanup procedure: purges processed rows in batches
CREATE OR REPLACE FUNCTION purge_completed_outbox(batch_limit INT, older_than_minutes INT)
RETURNS INT AS $$
DECLARE
    deleted_rows INT;
BEGIN
    WITH to_delete AS (
        SELECT id FROM outbox
        WHERE status = 'done'
          AND processed_at < (NOW() - (older_than_minutes || ' minutes')::interval)
        LIMIT batch_limit
    )
    DELETE FROM outbox WHERE id IN (SELECT id FROM to_delete);
    GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;
