-- Örnek pipeline kaynak tabloları (examples/commerce-pipeline)
-- Bunlar Pipeline Sentinel'in izlediği "üretim" tablolarıdır; metadata
-- tablolarından (datasets/columns/profiles/...) ayrı bir şema altındadır.

CREATE SCHEMA IF NOT EXISTS commerce;

CREATE TABLE IF NOT EXISTS commerce.customers (
    customer_id     BIGINT PRIMARY KEY,
    email           TEXT NOT NULL,
    country         TEXT,
    signup_at       TIMESTAMPTZ NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS commerce.orders (
    -- Bilinçli olarak PRIMARY KEY DEĞİL: gerçek landing/staging tabloları
    -- gibi append-only'dir. Tekillik bir DB kısıtı değil, Sentinel'in
    -- contract seviyesindeki "uniqueness" kontrolüyle (§7.1) tespit edilen
    -- bir veri kalitesi kuralıdır — F04 (duplicate load) bunu simüle eder.
    order_id        BIGINT NOT NULL,
    customer_id     BIGINT,             -- FK bilinçli olarak zorunlu değil: referential_integrity testleri bunu yakalar
    status          TEXT NOT NULL,
    -- Bilinçli olarak DB-seviyesinde NOT NULL DEĞİL: "sessiz veri arızası"
    -- senaryosu (F03) job'ı teknik olarak patlatmadan null enjekte eder;
    -- eksiklik burada değil contract kuralında (§7.1 completeness) yakalanır.
    total_amount    NUMERIC(18, 2),
    currency        TEXT NOT NULL DEFAULT 'TRY',
    created_at      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS commerce.payments (
    payment_id      BIGINT NOT NULL,    -- yine bilinçli olarak PK değil, bkz. orders yorumu
    order_id        BIGINT,
    amount          NUMERIC(18, 2),
    method          TEXT NOT NULL,
    paid_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_order_id ON commerce.orders (order_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON commerce.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_payments_payment_id ON commerce.payments (payment_id);
CREATE INDEX IF NOT EXISTS idx_payments_order_id ON commerce.payments (order_id);
