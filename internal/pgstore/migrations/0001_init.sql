-- Design §8 schema, copied exactly (TestMigrationMatchesDesign checks it).
-- Applied once, to an empty database, by `jetstream storage init`.

CREATE TABLE archive (
    id               smallint PRIMARY KEY CHECK (id = 1),
    archive_id       uuid NOT NULL,
    format_version   integer NOT NULL,         -- storage layout version, starts at 1
    schema_version   integer NOT NULL,
    writer_epoch     bigint NOT NULL DEFAULT 0,
    holder_id        uuid,
    lease_expires_at timestamptz,
    catalog_revision bigint NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE metadata_kv (
    key   bytea PRIMARY KEY,
    value bytea NOT NULL
) WITH (fillfactor = 80);

CREATE TABLE objects (
    object_id       bigserial PRIMARY KEY,
    key             uuid NOT NULL UNIQUE,
    sha256          bytea NOT NULL CHECK (length(sha256) = 32),
    byte_length     bigint NOT NULL CHECK (byte_length > 0),
    state           text NOT NULL CHECK (state IN ('uploading', 'available', 'deleting')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz
);
CREATE UNIQUE INDEX objects_sha256_available ON objects (sha256) WHERE state = 'available';
CREATE INDEX objects_gc ON objects (state, unreferenced_at);

CREATE TABLE segments (
    namespace             text NOT NULL CHECK (namespace IN ('main', 'bootstrap_live')),
    segment_index         bigint NOT NULL,
    state                 text NOT NULL CHECK (state IN ('active', 'sealed')),
    current_generation_id bigint,                 -- NULL while active
    revision              bigint NOT NULL,
    PRIMARY KEY (namespace, segment_index),
    CHECK ((state = 'active') = (current_generation_id IS NULL))
);
CREATE UNIQUE INDEX segments_one_active ON segments (namespace) WHERE state = 'active';
CREATE INDEX segments_revision ON segments (revision);

CREATE TABLE segment_generations (
    generation_id    bigserial PRIMARY KEY,
    namespace        text NOT NULL,
    segment_index    bigint NOT NULL,
    header           bytea NOT NULL CHECK (length(header) = 256),
    footer_object_id bigint NOT NULL REFERENCES objects (object_id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    revision         bigint NOT NULL,
    FOREIGN KEY (namespace, segment_index) REFERENCES segments (namespace, segment_index)
);
CREATE INDEX segment_generations_footer ON segment_generations (footer_object_id);

CREATE TABLE generation_blocks (
    generation_id     bigint NOT NULL REFERENCES segment_generations (generation_id) ON DELETE CASCADE,
    ordinal           integer NOT NULL,
    object_id         bigint NOT NULL REFERENCES objects (object_id),
    compressed_length bigint NOT NULL,
    PRIMARY KEY (generation_id, ordinal)
);
CREATE INDEX generation_blocks_object ON generation_blocks (object_id);

CREATE TABLE active_segment_blocks (
    namespace           text NOT NULL,
    segment_index       bigint NOT NULL,
    ordinal             integer NOT NULL,
    object_id           bigint NOT NULL REFERENCES objects (object_id),
    event_count         integer NOT NULL CHECK (event_count > 0),
    min_seq             bigint NOT NULL,
    max_seq             bigint NOT NULL,
    min_witnessed_us    bigint NOT NULL,
    max_witnessed_us    bigint NOT NULL,
    compressed_length   bigint NOT NULL,
    uncompressed_length bigint NOT NULL,
    revision            bigint NOT NULL,
    PRIMARY KEY (namespace, segment_index, ordinal),
    FOREIGN KEY (namespace, segment_index) REFERENCES segments (namespace, segment_index)
);
CREATE INDEX active_segment_blocks_object ON active_segment_blocks (object_id);
CREATE INDEX active_segment_blocks_revision ON active_segment_blocks (revision);

CREATE TABLE hot_batches (
    first_seq        bigint PRIMARY KEY,
    last_seq         bigint NOT NULL,
    event_count      integer NOT NULL CHECK (event_count > 0),
    min_witnessed_us bigint NOT NULL,
    max_witnessed_us bigint NOT NULL,
    epoch            bigint NOT NULL,
    revision         bigint NOT NULL,
    committed_at     timestamptz NOT NULL DEFAULT now(),
    frame            bytea,
    object_id        bigint REFERENCES objects (object_id),
    CHECK (last_seq - first_seq + 1 = event_count),
    CHECK ((frame IS NULL) <> (object_id IS NULL))
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX hot_batches_object ON hot_batches (object_id) WHERE object_id IS NOT NULL;
