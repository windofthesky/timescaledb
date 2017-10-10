-- This file contains functions related to getting information about the
-- schema of a hypertable, including columns, their types, etc.


-- Check if a given table OID is a main table (i.e. the table a user
-- targets for SQL operations) for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$;

-- Check if given table is a hypertable's main table
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$BODY$
     SELECT EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_main_table.schema_name AND 
               h.table_name = is_main_table.table_name
     );
$BODY$;

-- Get a hypertable given its main table OID
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.main_table_from_hypertable(
    hypertable_id int
)
    RETURNS regclass LANGUAGE SQL STABLE AS
$BODY$
    SELECT format('%I.%I',h.schema_name, h.table_name)::regclass
    FROM _timescaledb_catalog.hypertable h
    WHERE id = hypertable_id;
$BODY$;


-- Get the name of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_name_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS NAME LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_name NAME;
BEGIN
    SELECT h.time_column_name INTO STRICT time_col_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_name_for_chunk.schema_name AND
    c.table_name = time_col_name_for_chunk.table_name;
    RETURN time_col_name;
END
$BODY$;

-- Get the type of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_type_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_type REGTYPE;
BEGIN
    SELECT h.time_column_type INTO STRICT time_col_type
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_type_for_chunk.schema_name AND
    c.table_name = time_col_type_for_chunk.table_name;
    RETURN time_col_type;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunks(
    hypertable_id INT,
    from_time BIGINT,
    to_time   BIGINT
)
    RETURNS SETOF _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.*
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_internal.dimension_get_time(get_chunks.hypertable_id) time_dimension ON(true)
        INNER JOIN _timescaledb_catalog.dimension_slice ds
            ON (ds.dimension_id = time_dimension.id)
        INNER JOIN _timescaledb_catalog.chunk_constraint cc
            ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
        WHERE (from_time IS NULL OR ds.range_start >= from_time) AND
              (to_time IS NULL OR ds.range_end <= to_time) AND
              c.hypertable_id = get_chunks.hypertable_id;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunk_index_class_oid(
    chunk_row _timescaledb_catalog.chunk,
    hypertable_idx_name NAME
)
    RETURNS SETOF OID LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.oid
    FROM  _timescaledb_catalog.chunk_index ci
    INNER JOIN pg_class c ON (c.relname = ci.index_name)
    INNER JOIN pg_index i ON (c.oid = i.indexrelid AND i.indrelid = format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass)
    WHERE chunk_id = chunk_row.id AND
    hypertable_index_name = hypertable_idx_name

    UNION ALL

    SELECT chunk_index_class.oid
    FROM _timescaledb_catalog.chunk_constraint cc
    INNER JOIN pg_constraint con ON
    (cc.constraint_name = con.conname AND con.conrelid = format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass)
    INNER JOIN pg_class chunk_index_class ON (chunk_index_class.oid = con.conindid)
    WHERE cc.chunk_id = chunk_row.id AND
    cc.hypertable_constraint_name =
    (
        SELECT hyper_con.conname
        FROM pg_constraint hyper_con
        INNER JOIN pg_class hyper_index_class ON (hyper_index_class.oid = hyper_con.conindid)
        WHERE
        hyper_con.conrelid = (
            SELECT format ('%I.%I', h.schema_name, h.table_name)::regclass
            FROM _timescaledb_catalog.hypertable h
            WHERE h.id = chunk_row.hypertable_id
        )
        AND hyper_index_class.relname = hypertable_idx_name
    );
$BODY$;
