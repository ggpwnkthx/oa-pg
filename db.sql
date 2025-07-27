-- 1. Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. Drop old tables
DROP TABLE IF EXISTS openaddresses  CASCADE;
DROP TABLE IF EXISTS addresses      CASCADE;

-- 3. Recreate addresses
-- NOTE: unique_hash is a plain BYTEA (precomputed in ETL).
CREATE TABLE addresses (
    uuid                            UUID    PRIMARY KEY DEFAULT gen_random_uuid(),

    -- House number components
    address_number_prefix           TEXT,
    address_number                  TEXT,
    address_number_suffix           TEXT,

    -- Street name components
    street_name_pre_directional     TEXT,
    street_name_pre_modifier        TEXT,
    street_name_pre_type            TEXT,
    street_name                     TEXT,
    street_name_post_type           TEXT,
    street_name_post_modifier       TEXT,
    street_name_post_directional    TEXT,

    -- Building / landmark / intersection
    building_name                   TEXT,
    corner_of                       TEXT,
    intersection_separator          TEXT,
    landmark_name                   TEXT,
    place_name                      TEXT,

    -- Subaddress & occupancy
    subaddress_type                 TEXT,
    subaddress_identifier           TEXT,
    occupancy_type                  TEXT,
    occupancy_identifier            TEXT,

    -- USPS box grouping
    usps_box_group_id               TEXT,
    usps_box_group_type             TEXT,
    usps_box_id                     TEXT,
    usps_box_type                   TEXT,

    -- Region
    state_name                      TEXT,
    zipcode                         TEXT,

    unique_hash                     BYTEA NOT NULL,

    autocomplete_text   TEXT GENERATED ALWAYS AS (
        btrim(
            COALESCE(address_number_prefix, '')          || ' ' ||
            COALESCE(address_number, '')                 || ' ' ||
            COALESCE(address_number_suffix, '')          || ' ' ||
            COALESCE(street_name_pre_directional, '')    || ' ' ||
            COALESCE(street_name_pre_modifier, '')       || ' ' ||
            COALESCE(street_name_pre_type, '')           || ' ' ||
            COALESCE(street_name, '')                    || ' ' ||
            COALESCE(street_name_post_type, '')          || ' ' ||
            COALESCE(street_name_post_modifier, '')      || ' ' ||
            COALESCE(street_name_post_directional, '')   || ' ' ||
            COALESCE(building_name, '')                  || ' ' ||
            COALESCE(corner_of, '')                      || ' ' ||
            COALESCE(intersection_separator, '')         || ' ' ||
            COALESCE(landmark_name, '')                  || ' ' ||
            COALESCE(place_name, '')                     || ' ' ||
            COALESCE(subaddress_type, '')                || ' ' ||
            COALESCE(subaddress_identifier, '')          || ' ' ||
            COALESCE(occupancy_type, '')                 || ' ' ||
            COALESCE(occupancy_identifier, '')           || ' ' ||
            COALESCE(usps_box_group_id, '')              || ' ' ||
            COALESCE(usps_box_group_type, '')            || ' ' ||
            COALESCE(usps_box_id, '')                    || ' ' ||
            COALESCE(usps_box_type, '')                  || ' ' ||
            COALESCE(state_name, '')                     || ' ' ||
            COALESCE(zipcode, '')
        , ' '
        )
    ) STORED,

    CONSTRAINT unique_address_hash UNIQUE (unique_hash)
)
WITH (fillfactor = 80);

-- 4. Indexes on addresses
CREATE INDEX idx_addresses_autocomplete_trgm
    ON addresses USING gin (autocomplete_text gin_trgm_ops);

CREATE INDEX idx_addresses_city_street
    ON addresses (place_name, street_name);

CREATE INDEX idx_addresses_state_name
    ON addresses (state_name);

CREATE INDEX idx_addresses_zipcode
    ON addresses (zipcode);

-- 5. Recreate openaddresses (store GEOMETRY instead of GEOGRAPHY)
CREATE TABLE openaddresses (
    address_uuid   UUID      NOT NULL REFERENCES addresses (uuid),

    job_id         TEXT,
    source_name    TEXT,

    hash           TEXT,
    canonical      TEXT,

    -- Geometry values from OpenAddresses may be any geometry type, not just
    -- ``POINT``.  Use a general ``GEOMETRY`` column while keeping the SRID of
    -- 4326 which matches the source data.
    geometry       GEOMETRY (Geometry, 4326),

    CONSTRAINT unique_openaddresses_record
        UNIQUE (job_id, hash, geometry)
)
WITH (fillfactor = 90);

-- 6. Indexes on openaddresses
CREATE INDEX idx_openaddresses_canonical_trgm
    ON openaddresses USING gin (canonical gin_trgm_ops);

CREATE INDEX idx_openaddresses_geom_gist
    ON openaddresses USING gist (geometry);
