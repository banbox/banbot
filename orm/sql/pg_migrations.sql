-- TimescaleDB schema migrations for banbot v0.3+.
-- Each migration section starts with a numeric version marker comment.
-- Migrations are applied in a transaction with schema_migrations version tracking.

-- version 1
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ix_exsymbol_unique'
    ) THEN
        ALTER TABLE exsymbol DROP CONSTRAINT ix_exsymbol_unique;
    END IF;
END
$$;

DELETE FROM exsymbol a
USING exsymbol b
WHERE a.exchange = b.exchange
  AND a.market = b.market
  AND a.symbol = b.symbol
  AND a.id > b.id;

ALTER TABLE exsymbol
    ADD CONSTRAINT ix_exsymbol_unique UNIQUE (exchange, market, symbol);

-- version 3
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ix_exsymbol_unique'
    ) THEN
        ALTER TABLE exsymbol DROP CONSTRAINT ix_exsymbol_unique;
    END IF;
END
$$;

DELETE FROM exsymbol a
USING exsymbol b
WHERE a.exchange = b.exchange
  AND a.market = b.market
  AND a.symbol = b.symbol
  AND a.id > b.id;

ALTER TABLE exsymbol
    ADD CONSTRAINT ix_exsymbol_unique UNIQUE (exchange, market, symbol);

-- version 2
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ix_exsymbol_unique'
    ) THEN
        ALTER TABLE exsymbol DROP CONSTRAINT ix_exsymbol_unique;
    END IF;
END
$$;

ALTER TABLE exsymbol
    ADD CONSTRAINT ix_exsymbol_unique UNIQUE (exchange, market, symbol);
