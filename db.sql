/*********************************************************************/
/* 1. ONE‑TIME EXTENSION & SCHEMA SET‑UP                             */
/*********************************************************************/
CREATE SCHEMA IF NOT EXISTS addr;
SET search_path TO addr, public;

CREATE EXTENSION IF NOT EXISTS pg_trgm;   --  fast fuzzy / prefix matches
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- UUID generator

/*********************************************************************/
/* 2. MAIN TABLE – GENERIC ADDRESS ELEMENTS (UPU S42‑style)          */
/*    • hash‑partitioned for even write distribution                 */
/*    • text‑search vector + trigram column kept in sync            */
/*********************************************************************/
CREATE TABLE addr.addresses (
    /* Primary key --------------------------------------------------*/
    uuid             UUID    DEFAULT gen_random_uuid(),
    /* Generic address elements ------------------------------------*/
    country_code     CHAR(2)        NOT NULL, -- ISO‑3166‑1 α‑2
    postal_code      VARCHAR(20),
    admin_area_1     VARCHAR(80),  -- state / province / region
    admin_area_2     VARCHAR(80),  -- county / department / district
    locality         VARCHAR(120), -- city / town
    dependent_locality VARCHAR(120), -- suburb / neighbourhood
    thoroughfare     VARCHAR(160), -- street name (no house #)
    premise          VARCHAR(60),  -- house / building / lot
    sub_premise      VARCHAR(30),  -- unit / apartment
    /* Full‑string variants ----------------------------------------*/
    address_norm     TEXT            NOT NULL, -- libpostal‑normalised
    /* Search vector (full‑text) – STORED keeps it always in sync ---*/
    search_vector  TSVECTOR GENERATED ALWAYS AS
      ( to_tsvector('simple', address_norm) ) STORED,
    /* Primary key & partition key ---------------------------------*/
    PRIMARY KEY (uuid)
) PARTITION BY HASH (uuid);  --  light‑weight, write‑friendly partitioning

/*********************************************************************/
/* 3. CREATE 64 HASH PARTITIONS (≈ 10–15 M rows each is common)      */
/*********************************************************************/
DO
$$
DECLARE
    i integer;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS addr.addresses_p%02s
               PARTITION OF addr.addresses
               FOR VALUES WITH (MODULUS 64, REMAINDER %s);',
            i, i);
    END LOOP;
END
$$ LANGUAGE plpgsql;

/*********************************************************************/
/* 4. GLOBAL INDEXES (propagated to every partition)                 */
/*********************************************************************/
-- 4.1  Super‑fast fuzzy / prefix match for auto‑completion ----------
CREATE INDEX IF NOT EXISTS idx_addresses_norm_trgm
    ON addr.addresses USING GIN (address_norm gin_trgm_ops);

-- 4.2  Ordered prefix search on the street name (thoroughfare) ------
CREATE INDEX IF NOT EXISTS idx_addresses_thoroughfare_btree
    ON addr.addresses (thoroughfare text_pattern_ops);

-- 4.3  Full‑text search (tokens, not prefixes) ----------------------
CREATE INDEX IF NOT EXISTS idx_addresses_search_vector
    ON addr.addresses USING GIN (search_vector);

-- 4.4  Optional country + postal code filter (skip if rarely used) --
CREATE INDEX IF NOT EXISTS idx_addresses_country_postal
    ON addr.addresses (country_code, postal_code);

----------------------------------------------------------------------
--  NOTE:  All indexes are created on the parent table; each child
--         partition automatically receives its own physically‑local
--         copy.  Creating them *after* the partitions avoids
--         quadratic build time.  Use CONCURRENTLY for live systems.
----------------------------------------------------------------------

/*********************************************************************/
/* 5. BULK‑INGESTION STAGING TABLE (UNLOGGED)                        */
/*    - High‑throughput COPY here, then INSERT … ON CONFLICT         */
/*      into the partitioned base table.                             */
/*********************************************************************/
CREATE UNLOGGED TABLE addr.addresses_staging (LIKE addr.addresses INCLUDING ALL);

/* Example ingest cycle:
   1) COPY addr.addresses_staging FROM '/path/file.csv' CSV;
   2) INSERT INTO addr.addresses
        SELECT * FROM addr.addresses_staging
        ON CONFLICT DO NOTHING;
   3) TRUNCATE addr.addresses_staging;
*/