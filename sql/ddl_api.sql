-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
-- chunk_time_interval - (Optional) Initial time interval for a chunk
-- create_default_indexes - (Optional) Whether or not to create the default indexes.
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     anyelement = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
<<vars>>
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name                 NAME;
    schema_name                NAME;
    table_owner                NAME;
    tablespace_oid             OID;
    tablespace_name            NAME;
    main_table_has_items       BOOLEAN;
    is_hypertable              BOOLEAN;
    chunk_time_interval_actual BIGINT;
    time_type                  REGTYPE;
BEGIN
    SELECT relname, nspname, reltablespace
    INTO STRICT table_name, schema_name, tablespace_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT tableowner
    INTO STRICT table_owner
    FROM pg_catalog.pg_tables
    WHERE schemaname = schema_name
          AND tablename = table_name;

    IF table_owner <> session_user THEN
        RAISE 'Must be owner of relation %', table_name
        USING ERRCODE = 'insufficient_privilege';
    END IF;

    -- tables that don't have an associated tablespace has reltablespace OID set to 0
    -- in pg_class and there is no matching row in pg_tablespace
    SELECT spcname
    INTO tablespace_name
    FROM pg_tablespace t
    WHERE t.OID = tablespace_oid;

    EXECUTE format('SELECT TRUE FROM _timescaledb_catalog.hypertable WHERE
                    hypertable.schema_name = %L AND
                    hypertable.table_name = %L',
                    schema_name, table_name) INTO is_hypertable;

    IF is_hypertable THEN
       IF if_not_exists THEN
          RAISE NOTICE 'hypertable % already exists, skipping', main_table;
              RETURN;
        ELSE
              RAISE EXCEPTION 'hypertable % already exists', main_table
              USING ERRCODE = 'IO110';
          END IF;
    END IF;

    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO main_table_has_items;

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    time_type := _timescaledb_internal.dimension_type(main_table, time_column_name, true);

    chunk_time_interval_actual := _timescaledb_internal.time_interval_specification_to_internal(
        time_type, chunk_time_interval, INTERVAL '1 month', 'chunk_time_interval');

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _timescaledb_internal.create_hypertable_row(
            main_table,
            schema_name,
            table_name,
            time_column_name,
            partitioning_column,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            chunk_time_interval_actual,
            tablespace_name
        );
    EXCEPTION
        WHEN unique_violation THEN
            IF if_not_exists THEN
               RAISE NOTICE 'hypertable % already exists, skipping', main_table;
               RETURN;
            ELSE
               RAISE EXCEPTION 'hypertable % already exists', main_table
               USING ERRCODE = 'IO110';
            END IF;
        WHEN foreign_key_violation THEN
            RAISE EXCEPTION 'database not configured for hypertable storage (not setup as a data-node)'
            USING ERRCODE = 'IO101';
    END;

    PERFORM _timescaledb_internal.add_constraint(hypertable_row.id, oid)
    FROM pg_constraint
    WHERE conrelid = main_table;

   IF create_default_indexes THEN
        PERFORM _timescaledb_internal.create_default_indexes(hypertable_row, main_table, partitioning_column);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    interval_length         BIGINT = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
<<main_block>>
DECLARE
    table_name       NAME;
    schema_name      NAME;
    owner_oid        OID;
    hypertable_row   _timescaledb_catalog.hypertable;
BEGIN
    SELECT relname, nspname, relowner
    INTO STRICT table_name, schema_name, owner_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', main_table;
    END IF;

    SELECT *
    INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable h
    WHERE h.schema_name = main_block.schema_name
    AND h.table_name = main_block.table_name
    FOR UPDATE;

    PERFORM _timescaledb_internal.add_dimension(main_table,
                                                hypertable_row,
                                                column_name,
                                                number_partitions,
                                                interval_length);
END
$BODY$;

-- Update chunk_time_interval for a hypertable
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path=''
    AS
$BODY$
DECLARE
    main_table_name       NAME;
    main_schema_name      NAME;
    owner_oid             OID;
BEGIN
    SELECT relname, nspname, relowner
    INTO STRICT main_table_name, main_schema_name, owner_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', main_table;
    END IF;

    UPDATE _timescaledb_catalog.dimension d
    SET interval_length = set_chunk_time_interval.chunk_time_interval
    FROM _timescaledb_internal.dimension_get_time(
        (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE h.schema_name = main_schema_name AND
            h.table_name = main_table_name
    )) time_dim
    WHERE time_dim.id = d.id;
END
$BODY$;

CREATE OR REPLACE FUNCTION drop_chunks(
    older_than INTEGER,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    IF older_than IS NULL THEN
        RAISE 'The time provided to drop_chunks cannot be null';
    END IF;
    PERFORM _timescaledb_internal.drop_chunks_impl(older_than, table_name, schema_name, cascade);
END
$BODY$;

-- Drop chunks that are older than a timestamp.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than TIMESTAMPTZ,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_internal BIGINT;
BEGIN
    IF older_than IS NULL THEN
        RAISE 'The timestamp provided to drop_chunks cannot be null';
    END IF;

    SELECT (EXTRACT(epoch FROM older_than)*1e6)::BIGINT INTO older_than_internal;
    PERFORM _timescaledb_internal.drop_chunks_impl(older_than_internal, table_name, schema_name, cascade);
END
$BODY$;

-- Drop chunks older than an interval.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than  INTERVAL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade BOOLEAN = false
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_ts TIMESTAMPTZ;
BEGIN
    older_than_ts := now() - older_than;
    PERFORM drop_chunks(older_than_ts, table_name, schema_name, cascade);
END
$BODY$;

CREATE OR REPLACE FUNCTION attach_tablespace(
       hypertable REGCLASS,
       tablespace NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    main_schema_name  NAME;
    main_table_name   NAME;
    owner_oid         OID;
    hypertable_id     INTEGER;
    tablespace_oid    OID;
BEGIN
    SELECT nspname, relname, relowner
    FROM pg_class c INNER JOIN pg_namespace n
    ON (c.relnamespace = n.oid)
    WHERE c.oid = hypertable
    INTO STRICT main_schema_name, main_table_name, owner_oid;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', hypertable;
    END IF;

    SELECT id
    FROM _timescaledb_catalog.hypertable h
    WHERE (h.schema_name = main_schema_name)
    AND (h.table_name = main_table_name)
    INTO hypertable_id;

    IF hypertable_id IS NULL THEN
       RAISE EXCEPTION 'No hypertable "%" exists', main_table_name
       USING ERRCODE = 'IO101';
    END IF;

    PERFORM _timescaledb_internal.attach_tablespace(hypertable_id, tablespace);
END
$BODY$;

--reindex only some chunks of a hypertable
--main_table is the hypertable
--index_oid is the index on a hypertable to reindex (NULL for all)
--from_time is a lower bound for the time filter on a chunk
--to_time is a upper bound for the time filter on a chunk
--verbose is a flag to indicate whether to output a notice for each index processed
--recreate is a flag to force a CREATE INDEX followed by a DROP INDEX instead of a REINDEX.
----A reindex locks out reads during the procedure if those reads use the index.
----A recreate does not lock reads but may use more disk space.
CREATE OR REPLACE FUNCTION reindex_chunk(
    main_table REGCLASS,
    index_oid REGCLASS = NULL,
    from_time anyelement = NULL::bigint,
    to_time   anyelement = NULL,
    verbose   BOOLEAN = FALSE,
    recreate  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
    chunk_index_name NAME;
    verbose_mod TEXT = '';
    chunk_index_info RECORD;
    recreate_state_info RECORD;
    recreated_state_old OID[];
    recreated_state_new OID[];
    chunk_index_oid_new OID;
    time_dimension_row _timescaledb_catalog.dimension;
BEGIN
    hypertable_row := _timescaledb_internal.hypertable_from_main_table(main_table);
    time_dimension_row := _timescaledb_internal.dimension_get_time(hypertable_row.id);

    FOR chunk_index_info IN
        WITH chunks AS (
            SELECT *
            FROM _timescaledb_internal.get_chunks(
                hypertable_row.id,
                _timescaledb_internal.time_specification_to_internal(time_dimension_row.column_type, from_time, 'from_time'),
                _timescaledb_internal.time_specification_to_internal(time_dimension_row.column_type, to_time, 'to_time')
            )
        ),
        hypertable_idx AS (
            SELECT relname AS name
            FROM pg_index i
            INNER JOIN pg_class c ON (c.OID = i.indexrelid)
            WHERE indrelid = main_table
            AND (i.indexrelid = index_oid OR index_oid IS NULL)
        )
        SELECT c.schema_name, _timescaledb_internal.get_chunk_index_class_oid(c, h.name) AS chunk_index_oid
        FROM  chunks c, hypertable_idx h
        LOOP
            SELECT relname INTO STRICT chunk_index_name FROM pg_class WHERE OID = chunk_index_info.chunk_index_oid;
            IF recreate THEN
                -- the recreate phase is split into 2 parts a (1) CREATE INDEX followed by  (2) DROP INDEX + RENAME INDEX.
                -- you want to do phase 1 on all indexes before starting phase 2 since phase 2 takes heavy locks but is quick.
                SELECT _timescaledb_internal.chunk_index_recreate_create(chunk_index_info.chunk_index_oid)
                INTO chunk_index_oid_new;

                IF "verbose" = true THEN
                    RAISE INFO 'a new index was created for index "%"', chunk_index_name;
                END IF;
                recreated_state_old := recreated_state_old || chunk_index_info.chunk_index_oid;
                recreated_state_new := recreated_state_new || chunk_index_oid_new;
            ELSE
                IF "verbose" THEN
                    verbose_mod := '(VERBOSE)';
                END IF;
                EXECUTE FORMAT($$ REINDEX %s INDEX %I.%I $$, verbose_mod, chunk_index_info.schema_name, chunk_index_name);
            END IF;
    END LOOP;

    FOR recreate_state_info IN SELECT unnest(recreated_state_old) AS old_oid,  unnest(recreated_state_new) AS new_oid
        LOOP
        -- phase 2 of recreate index
        SELECT relname INTO STRICT chunk_index_name FROM pg_class WHERE OID = recreate_state_info.old_oid;
        PERFORM _timescaledb_internal.chunk_index_recreate_rename(recreate_state_info.old_oid, recreate_state_info.new_oid);
        IF "verbose" = true THEN
            RAISE INFO 'index "%" was renamed to use the new index', chunk_index_name;
        END IF;
    END LOOP;
END
$BODY$;
