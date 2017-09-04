\ir include/create_single_db.sql

-- Bogus chunk sizing function
CREATE OR REPLACE FUNCTION calculate_chunk_interval(
        dimension_id INTEGER,
        chunk_target_size BIGINT
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN -1;
END
$BODY$;

-- Chunk sizing function with bad signature
CREATE OR REPLACE FUNCTION bad_calculate_chunk_interval(
        dimension_id INTEGER
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    RETURN -1;
END
$BODY$;

CREATE TABLE test_adaptive(time timestamptz, temp float);

\set ON_ERROR_STOP 0
-- Bad signature of sizing func should fail
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         chunk_sizing_func => 'bad_calculate_chunk_interval');

\set ON_ERROR_STOP 1

-- Setting sizing func with correct signature should work
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB',
                         chunk_sizing_func => 'calculate_chunk_interval');

DROP TABLE test_adaptive;
CREATE TABLE test_adaptive(time timestamptz, temp float);

-- Size but no explicit func should use default func
SELECT create_hypertable('test_adaptive', 'time',
                         chunk_target_size => '1MB');
SELECT chunk_target_size FROM _timescaledb_catalog.hypertable
WHERE chunk_sizing_func = '_timescaledb_internal.calculate_chunk_interval'::REGPROC;

-- Change the target size
SELECT set_adaptive_chunk_sizing('test_adaptive', '2MB');
SELECT chunk_target_size FROM _timescaledb_catalog.hypertable;

-- Setting NULL func should disable adaptive chunking
SELECT set_adaptive_chunk_sizing('test_adaptive', '1MB', NULL);
SELECT chunk_target_size, chunk_sizing_func FROM _timescaledb_catalog.hypertable;

-- Setting NULL size disables adaptive chunking
SELECT set_adaptive_chunk_sizing('test_adaptive', NULL, '_timescaledb_internal.calculate_chunk_interval');
SELECT chunk_target_size FROM _timescaledb_catalog.hypertable
WHERE chunk_sizing_func = '_timescaledb_internal.calculate_chunk_interval'::REGPROC;

-- Setting 0 size should estimate size
SELECT set_adaptive_chunk_sizing('test_adaptive', '0MB');
SELECT chunk_target_size FROM _timescaledb_catalog.hypertable
WHERE chunk_sizing_func = '_timescaledb_internal.calculate_chunk_interval'::REGPROC;

-- Set a reasonable test value
SELECT set_adaptive_chunk_sizing('test_adaptive', '1MB');

INSERT INTO test_adaptive
SELECT time, random() * 35 FROM
generate_series('2017-03-07T18:18:03+00'::timestamptz - interval '175 days',
                '2017-03-07T18:18:03+00'::timestamptz,
                '5 minutes') as time;

SELECT * FROM chunk_relation_size('test_adaptive');
