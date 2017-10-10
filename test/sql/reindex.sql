CREATE TABLE reindex_test(time timestamp, temp float, PRIMARY KEY(time, temp));
CREATE UNIQUE INDEX reindex_test_time_unique_idx ON reindex_test(time);

-- create hypertable with three chunks
SELECT create_hypertable('reindex_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO reindex_test VALUES ('2017-01-20T09:00:01', 17.5),
                                ('2017-01-21T09:00:01', 19.1),
                                ('2017-04-20T09:00:01', 89.5),
                                ('2017-04-21T09:00:01', 17.1),
                                ('2017-06-20T09:00:01', 18.5),
                                ('2017-06-21T09:00:01', 11.0);

\d+ reindex_test

-- show reindexing
REINDEX (VERBOSE) TABLE reindex_test;

\set ON_ERROR_STOP 0
-- this one currently doesn't recurse to chunks and instead gives an
-- error
REINDEX (VERBOSE) INDEX reindex_test_time_unique_idx;
\set ON_ERROR_STOP 1

-- show reindexing on a normal table
CREATE TABLE reindex_norm(time timestamp, temp float);
CREATE UNIQUE INDEX reindex_norm_time_unique_idx ON reindex_norm(time);

INSERT INTO reindex_norm VALUES ('2017-01-20T09:00:01', 17.5),
                                ('2017-01-21T09:00:01', 19.1),
                                ('2017-04-20T09:00:01', 89.5),
                                ('2017-04-21T09:00:01', 17.1),
                                ('2017-06-20T09:00:01', 18.5),
                                ('2017-06-21T09:00:01', 11.0);

REINDEX (VERBOSE) TABLE reindex_norm;
REINDEX (VERBOSE) INDEX reindex_norm_time_unique_idx;

\d+ _timescaledb_internal._hyper_1_1_chunk

SELECT indexrelid AS original_indexoid FROM pg_index WHERE indexrelid = '_timescaledb_internal._hyper_1_1_chunk_reindex_test_time_unique_idx'::regclass
\gset

SELECT reindex_chunk('reindex_test', verbose=>true);

SELECT indexrelid AS reindex_indexoid FROM pg_index WHERE indexrelid = '_timescaledb_internal._hyper_1_1_chunk_reindex_test_time_unique_idx'::regclass
\gset

SELECT :original_indexoid = :reindex_indexoid;

SELECT reindex_chunk('reindex_test', verbose=>true, recreate=>true);

SELECT indexrelid AS remake_indexoid FROM pg_index WHERE indexrelid = '_timescaledb_internal._hyper_1_1_chunk_reindex_test_time_unique_idx'::regclass
\gset

SELECT :original_indexoid = :remake_indexoid;

--reindex only a particular index
SELECT reindex_chunk('reindex_test', 'reindex_test_pkey', verbose=>true, recreate=>true);

--from_time
SELECT reindex_chunk('reindex_test', 'reindex_test_pkey', TIMESTAMP '2017-06-20T07:00:01', verbose=>true, recreate=>true);

--to_time (note awkwardness with having to declare a NULL from_time)
SELECT reindex_chunk('reindex_test', 'reindex_test_pkey', NULL::timestamp, TIMESTAMP '2017-02-18 15:00:00', verbose=>true, recreate=>true);

--both from and to time
SELECT reindex_chunk('reindex_test', 'reindex_test_pkey', '2017-01-19 05:00:00'::timestamp, TIMESTAMP '2017-02-18 15:00:00', verbose=>true, recreate=>true);

--empty set
SELECT reindex_chunk('reindex_test', 'reindex_test_pkey', '2017-01-19 06:00:00'::timestamp, TIMESTAMP '2017-02-18 15:00:00', verbose=>true, recreate=>true);

\d+ _timescaledb_internal._hyper_1_1_chunk
