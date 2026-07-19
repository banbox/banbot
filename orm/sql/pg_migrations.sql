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

-- version 4
ALTER TABLE exsymbol
    ADD COLUMN IF NOT EXISTS agg_rules text NOT NULL DEFAULT '';

-- version 5
ALTER TABLE exsymbol
    ADD COLUMN IF NOT EXISTS agg_rules text NOT NULL DEFAULT '';

-- version 6
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'calendars'
          AND column_name = 'name'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'calendars'
          AND column_name = 'market'
    ) THEN
        ALTER TABLE public.calendars RENAME COLUMN name TO market;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_calendars_market
    ON public.calendars (market);

CREATE UNIQUE INDEX IF NOT EXISTS idx_calendars_market_start
    ON public.calendars (market, start_ms);

CREATE UNIQUE INDEX IF NOT EXISTS idx_adj_factors_sid_sub_start
    ON public.adj_factors (sid, sub_id, start_ms);

CREATE UNIQUE INDEX IF NOT EXISTS ins_kline_sid_tf_pkey
    ON public.ins_kline (sid, timeframe);

-- version 7
DO $$
DECLARE
    target_table text;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_1d', 'kline_un'
    ] LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = target_table
              AND c.column_name = 'info'
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = target_table
              AND c.column_name = 'buy_volume'
        ) THEN
            EXECUTE format(
                'ALTER TABLE public.%I RENAME COLUMN info TO buy_volume',
                target_table
            );
        END IF;

        EXECUTE format(
            'ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS quote float8 NOT NULL DEFAULT 0',
            target_table
        );
        EXECUTE format(
            'ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS buy_volume float8 NOT NULL DEFAULT 0',
            target_table
        );
        EXECUTE format(
            'ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS trade_num int8 NOT NULL DEFAULT 0',
            target_table
        );

        IF target_table = 'kline_un' THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS expire_ms int8 NOT NULL DEFAULT 0',
                target_table
            );
        END IF;
    END LOOP;
END
$$;

-- version 8
DO $$
DECLARE
    target_table text;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_1d', 'kline_un'
    ] LOOP
        EXECUTE format(
            'ALTER TABLE public.%I ALTER COLUMN buy_volume SET DEFAULT 0',
            target_table
        );
    END LOOP;
END
$$;
