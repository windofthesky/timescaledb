CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_closed_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk(
    chunk_id int,
    is_cascade BOOLEAN
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    cascade_mod TEXT := '';
BEGIN
    -- when deleting the chunk row from the metadata table,
    -- also DROP the actual chunk table that holds data.
    -- Note that the table could already be deleted in case this
    -- is executed as a result of a DROP TABLE on the hypertable
    -- that this chunk belongs to.

    PERFORM _timescaledb_internal.drop_chunk_constraint(cc.chunk_id, cc.constraint_name)
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = drop_chunk.chunk_id;

    DELETE FROM _timescaledb_catalog.chunk WHERE id = chunk_id
    RETURNING * INTO STRICT chunk_row;

    PERFORM 1
    FROM pg_class c
    WHERE relname = quote_ident(chunk_row.table_name) AND relnamespace = quote_ident(chunk_row.schema_name)::regnamespace;

    IF FOUND THEN
        IF is_cascade THEN
            cascade_mod = 'CASCADE';
        END IF;

        EXECUTE format(
                $$
                DROP TABLE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
        );
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create(
    chunk_id INTEGER,
    hypertable_id INTEGER,
    schema_name NAME,
    table_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    tablespace_name NAME;
    main_table_oid  OID;
    dimension_slice_ids INT[];
BEGIN
    INSERT INTO _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name)
    VALUES (chunk_id, hypertable_id, schema_name, table_name) RETURNING * INTO STRICT chunk_row;

    SELECT array_agg(cc.dimension_slice_id)::int[] INTO STRICT dimension_slice_ids
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = chunk_create.chunk_id AND cc.dimension_slice_id IS NOT NULL;

    tablespace_name := _timescaledb_internal.select_tablespace(chunk_row.hypertable_id, dimension_slice_ids);

    PERFORM _timescaledb_internal.chunk_create_table(chunk_row.id, tablespace_name);

    --create the dimension-slice-constraints
    PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(cc)
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = chunk_create.chunk_id AND cc.dimension_slice_id IS NOT NULL;

    PERFORM _timescaledb_internal.create_chunk_index_row(chunk_row.schema_name, chunk_row.table_name,
                            hi.main_schema_name, hi.main_index_name, hi.definition)
    FROM _timescaledb_catalog.hypertable_index hi
    WHERE hi.hypertable_id = chunk_row.hypertable_id
    ORDER BY format('%I.%I',main_schema_name, main_index_name)::regclass;

    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = chunk_row.hypertable_id;
    main_table_oid := format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass;

    --create the hypertable-constraints copy
    PERFORM _timescaledb_internal.create_chunk_constraint(chunk_row.id, oid)
    FROM pg_constraint
    WHERE conrelid = main_table_oid
    AND _timescaledb_internal.need_chunk_constraint(oid);

    PERFORM _timescaledb_internal.create_chunk_trigger(chunk_row.id, tgname,
        _timescaledb_internal.get_general_trigger_definition(oid))
    FROM pg_trigger
    WHERE tgrelid = main_table_oid
    AND _timescaledb_internal.need_chunk_trigger(chunk_row.hypertable_id, oid);

END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.verify_chunk_sizing_func_signature(
        chunk_sizing_func REGPROC
)
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    -- Check that the function has the correct signature
    PERFORM * FROM pg_proc
    WHERE oid = chunk_sizing_func
    AND proargtypes = ARRAY['INT'::REGTYPE, 'BIGINT'::REGTYPE]::OIDVECTOR;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid chunk sizing function'
        USING HINT = 'Please verify the function signature';
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_initial_chunk_target_size()
    RETURNS BIGINT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    -- Simply set a quarter of estimated memory for now
    RETURN _timescaledb_internal.estimate_effective_memory() / 4;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(
        dimension_id INTEGER,
        chunk_target_size BIGINT
)
    RETURNS BIGINT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    dimension_row        _timescaledb_catalog.dimension;
    chunk_row            _timescaledb_catalog.chunk;
    chunk_interval       BIGINT;
    chunk_window         SMALLINT = 3;
    calculated_intervals BIGINT[];
BEGIN
    -- Get the dimension corresponding to the given dimension ID
    SELECT *
    INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_id;

    -- Get a window of most recent chunks
    FOR chunk_row IN
    SELECT * FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = dimension_row.hypertable_id
    ORDER BY c.id DESC LIMIT chunk_window
    LOOP
        DECLARE
            dimension_slice_row     _timescaledb_catalog.dimension_slice;
            chunk_relid             OID;
            max_dimension_value     BIGINT;
            min_dimension_value     BIGINT;
            chunk_interval          BIGINT;
            interval_fraction       FLOAT;
            chunk_size              BIGINT;
            extrapolated_chunk_size BIGINT;
            new_interval_length     BIGINT;
            chunk_size_fraction     FLOAT;
        BEGIN

        -- Get the chunk's min and max value for the dimension we are looking at
        EXECUTE format(
            $$
            SELECT _timescaledb_internal.to_internal_time(min(%1$I)),
                   _timescaledb_internal.to_internal_time(max(%1$I)) FROM %2$I.%3$I
            $$,
            dimension_row.column_name,
            chunk_row.schema_name,
            chunk_row.table_name
        ) INTO STRICT min_dimension_value, max_dimension_value;

        -- Get the chunk's slice for the given dimension
        SELECT * INTO STRICT dimension_slice_row
        FROM _timescaledb_catalog.dimension_slice s
        INNER JOIN _timescaledb_catalog.chunk_constraint cc
        ON (cc.dimension_slice_id = s.id)
        WHERE s.dimension_id = calculate_chunk_interval.dimension_id
        AND cc.chunk_id = chunk_row.id;

        -- Get approximate row count (cheaper than count() on table)
        SELECT c.oid
        FROM pg_class c, pg_namespace
        WHERE nspname = chunk_row.schema_name
        AND relname = chunk_row.table_name
        INTO STRICT chunk_relid;

        SELECT * FROM pg_total_relation_size(chunk_relid)
        INTO STRICT chunk_size;

        -- Calculate the chunk's actual interval in the
        -- dimension. This might be different from the interval we set
        -- because of chunk collisions and cutting when setting the
        -- new interval.
        chunk_interval = dimension_slice_row.range_end - dimension_slice_row.range_start;

        -- Only change the interval if the previous chunk had an
        -- interval that is more than 90% of the interval set for the
        -- dimension. A change in interval typically involves cuts in
        -- the chunk created immediately following the change due to
        -- collisions with previous chunks since all ranges are
        -- calculated from time epoch 0 irrespective of interval.
        IF (chunk_interval::FLOAT / dimension_row.interval_length) > 0.98 THEN
            interval_fraction := (max_dimension_value - min_dimension_value)::FLOAT / chunk_interval;

            -- Extrapolate the chunk relation size to the size the it
            -- would have if fully filled
            extrapolated_chunk_size := chunk_size + ((1.0 - interval_fraction) * chunk_size)::BIGINT;

            -- Now calculate the current size's fraction of the target size
            chunk_size_fraction := extrapolated_chunk_size::FLOAT / chunk_target_size;

            -- Apply some dampening. Do not change the interval if we
            -- are close to the target size or the previous chunk was
            -- partially filled
            IF interval_fraction > 0.7 AND abs(1.0 - chunk_size_fraction) > 0.15 THEN
                new_interval_length := (dimension_row.interval_length / chunk_size_fraction)::BIGINT;
                calculated_intervals := array_append(calculated_intervals, new_interval_length);
            END IF;
        END IF;
        END;
    END LOOP;

    chunk_interval = array_avg(calculated_intervals);

    IF chunk_interval IS NULL THEN
        RETURN -1;
    END IF;

    RETURN chunk_interval;
END
$BODY$;
