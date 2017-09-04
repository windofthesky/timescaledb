#include <unistd.h>

#include <postgres.h>
#include <fmgr.h>
#include <utils/datetime.h>
#include <catalog/pg_type.h>
#include <catalog/pg_trigger.h>
#include <catalog/namespace.h>
#include <utils/guc.h>
#include <utils/date.h>
#include <utils/builtins.h>

#include "utils.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

PG_FUNCTION_INFO_V1(pg_timestamp_to_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the Postgres epoch.
 */
Datum
pg_timestamp_to_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64		microseconds;

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp;
#else
	if (1)
	{
		int64		seconds = (int64) timestamp;

		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC);
	}
#endif
	PG_RETURN_INT64(microseconds);
}

PG_FUNCTION_INFO_V1(pg_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
pg_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64		microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds;
#else
	timestamp = microseconds / USECS_PER_SEC;
#endif

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

PG_FUNCTION_INFO_V1(pg_timestamp_to_unix_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the UNIX epoch.
 */
Datum
pg_timestamp_to_unix_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64		epoch_diff_microseconds = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
	int64		microseconds;

	if (timestamp < MIN_TIMESTAMP)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	if (timestamp >= (END_TIMESTAMP - epoch_diff_microseconds))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp + epoch_diff_microseconds;
#else
	if (1)
	{
		int64		seconds = (int64) timestamp;

		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC) + epoch_diff_microseconds;
	}
#endif
	PG_RETURN_INT64(microseconds);
}

PG_FUNCTION_INFO_V1(pg_unix_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64		microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

	/*
	 * Test that the UNIX us timestamp is within bounds. Note that an int64 at
	 * UNIX epoch and microsecond precision cannot represent the upper limit
	 * of the supported date range (Julian end date), so INT64_MAX is the
	 * natural upper bound for this function.
	 */
	if (microseconds < ((int64) USECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
#else
	/* Shift the epoch using integer arithmetic to reduce precision errors */
	timestamp = microseconds / USECS_PER_SEC;	/* seconds */
	microseconds = microseconds - ((int64) timestamp * USECS_PER_SEC);
	timestamp = (float8) ((int64) seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
		+ (float8) microseconds / USECS_PER_SEC;
#endif
	PG_RETURN_TIMESTAMPTZ(timestamp);
}

int64
time_value_to_internal(Datum time_val, Oid type)
{
	switch (type)
	{
		case INT8OID:
			return DatumGetInt64(time_val);
		case INT4OID:
			return DatumGetInt32(time_val);
		case INT2OID:
			return DatumGetInt16(time_val);
		case DATEOID:
			time_val = DirectFunctionCall1(date_timestamptz, time_val);
			return DatumGetInt64(DirectFunctionCall1(pg_timestamp_to_unix_microseconds, time_val));
		case TIMESTAMPOID:
			time_val = DirectFunctionCall1(timestamp_timestamptz, time_val);
			/* Fall through */
		case TIMESTAMPTZOID:
			return DatumGetInt64(DirectFunctionCall1(pg_timestamp_to_unix_microseconds, time_val));
		default:
			elog(ERROR, "unkown time type oid '%d'", type);
	}
}

PG_FUNCTION_INFO_V1(to_internal_time);

Datum
to_internal_time(PG_FUNCTION_ARGS)
{
	return time_value_to_internal(PG_GETARG_DATUM(0), get_fn_expr_argtype(fcinfo->flinfo, 0));
}

/* Make a RangeVar from a regclass Oid */
RangeVar *
makeRangeVarFromRelid(Oid relid)
{
	Oid			namespace = get_rel_namespace(relid);
	char	   *tableName = get_rel_name(relid);
	char	   *schemaName = get_namespace_name(namespace);

	return makeRangeVar(schemaName, tableName, -1);
}

int
int_cmp(const void *a, const void *b)
{
	const int  *ia = (const int *) a;
	const int  *ib = (const int *) b;

	return *ia - *ib;
}

FmgrInfo *
create_fmgr(char *schema, char *function_name, int num_args)
{
	FmgrInfo   *finfo = palloc(sizeof(FmgrInfo));
	FuncCandidateList func_list = FuncnameGetCandidates(list_make2(makeString(schema),
												  makeString(function_name)),
										num_args, NULL, false, false, false);

	if (func_list == NULL)
	{
		elog(ERROR, "couldn't find the function %s.%s", schema, function_name);
	}
	if (func_list->next != NULL)
	{
		elog(ERROR, "multiple functions found");
	}

	fmgr_info(func_list->oid, finfo);

	return finfo;
}

static inline int64
get_interval_period(Interval *interval)
{
	if (interval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval defined in terms of month, year, century etc. not supported")
				 ));
	}
#ifdef HAVE_INT64_TIMESTAMP
	return interval->time + (interval->day * USECS_PER_DAY);
#else
	if (interval->time != trunc(interval->time))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must not have sub-second precision")
				 ));
	}
	return interval->time + (interval->day * SECS_PER_DAY);
#endif
}

PG_FUNCTION_INFO_V1(timestamp_bucket);
Datum
timestamp_bucket(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;
	int64		period = -1;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	period = get_interval_period(interval);
	/* result = (timestamp / period) * period */
	TMODULO(timestamp, result, period);
	if (timestamp < 0)
	{
		/*
		 * need to subtract another period if remainder < 0 this only happens
		 * if timestamp is negative to begin with and there is a remainder
		 * after division. Need to subtract another period since division
		 * truncates toward 0 in C99.
		 */
		result = (result * period) - period;
	}
	else
	{
		result *= period;
	}
	PG_RETURN_TIMESTAMP(result);
}

PG_FUNCTION_INFO_V1(timestamptz_bucket);
Datum
timestamptz_bucket(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz result;
	int64		period = -1;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	period = get_interval_period(interval);
	/* result = (timestamp / period) * period */
	TMODULO(timestamp, result, period);
	if (timestamp < 0)
	{
		/*
		 * need to subtract another period if remainder < 0 this only happens
		 * if timestamp is negative to begin with and there is a remainder
		 * after division. Need to subtract another period since division
		 * truncates toward 0 in C99.
		 */
		result = (result * period) - period;
	}
	else
	{
		result *= period;
	}
	PG_RETURN_TIMESTAMPTZ(result);
}

static inline void
check_period_is_daily(int64 period)
{
#ifdef HAVE_INT64_TIMESTAMP
	int64		day = USECS_PER_DAY;
#else
	int64		day = SECS_PER_DAY;
#endif
	if (period < day)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must not have sub-day precision")
				 ));
	}
	if (period % day != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must be a multiple of a day")
				 ));

	}
}

PG_FUNCTION_INFO_V1(date_bucket);

Datum
date_bucket(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	DateADT		date = PG_GETARG_DATEADT(1);
	Datum		converted_ts,
				bucketed;
	int64		period = -1;

	if (DATE_NOT_FINITE(date))
		PG_RETURN_DATEADT(date);

	period = get_interval_period(interval);
	/* check the period aligns on a date */
	check_period_is_daily(period);

	/* convert to timestamp (NOT tz), bucket, convert back to date */
	converted_ts = DirectFunctionCall1(date_timestamp, PG_GETARG_DATUM(1));
	bucketed = DirectFunctionCall2(timestamp_bucket, PG_GETARG_DATUM(0), converted_ts);
	return DirectFunctionCall1(timestamp_date, bucketed);
}


PG_FUNCTION_INFO_V1(trigger_is_row_trigger);

Datum
trigger_is_row_trigger(PG_FUNCTION_ARGS)
{
	int16		tgtype = PG_GETARG_INT16(0);

	PG_RETURN_BOOL(TRIGGER_FOR_ROW(tgtype));
}

#if defined(WIN32)
#include "windows.h"
static int64
system_memory_bytes(void)
{
	MEMORYSTATUSEX status;

	status.dwLength = sizeof(status);
	GlobalMemoryStatusEx(&status);

	return status.ullTotalPhys;
}
#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
static int64
system_memory_bytes(void)
{
	int64		bytes;

	bytes = sysconf(_SC_PHYS_PAGES);
	bytes *= sysconf(_SC_PAGESIZE);

	return bytes;
}
#else
#error Unsupported platform
#endif

/*
 * Estimate the effective memory available to PostgreSQL based on the settings
 * of 'shared_buffers' and 'effective_cache_size'.
 *
 * Although we could rely solely on something like sysconf() to get the actual
 * system memory available, PostgreSQL will still be bound by 'shared_buffers'
 * and 'effective_cache_size' so might not effectively use the full memory on
 * the system anyway.
 *
 * If accurately set, 'effective_cache_size' is probably the best value to use
 * since it provides an estimate of the combined memory in both the shared
 * buffers and disk cache. A conservative setting of 'effective_cache_size' is
 * typically 1/2 the memory of the system, while a common recommended setting
 * for 'shared_buffers' is 1/4 of system memory. The caveat here is that it is
 * much more common to set 'shared_buffers', so therefore we try to use the max
 * of 'effective_cache_size' and twice the 'shared_buffers'.
 */
static int64
estimate_effective_memory(void)
{
	const char *val;
	const char *hintmsg;
	int			shared_buffers,
				effective_cache_size;
	int64		memory_bytes;
	int64		sysmem_bytes = system_memory_bytes();

	val = GetConfigOption("shared_buffers", false, false);

	if (NULL == val)
		elog(ERROR, "Missing configuration for 'shared_buffers'");

	if (!parse_int(val, &shared_buffers, GUC_UNIT_BLOCKS, &hintmsg))
		elog(ERROR, "Could not parse 'shared_buffers' setting: %s", hintmsg);

	val = GetConfigOption("effective_cache_size", false, false);

	if (NULL == val)
		elog(ERROR, "Missing configuration for 'effective_cache_size'");

	if (!parse_int(val, &effective_cache_size, GUC_UNIT_BLOCKS, &hintmsg))
		elog(ERROR, "Could not parse 'effective_cache_size' setting: %s", hintmsg);

	memory_bytes = Max((int64) shared_buffers * 4, (int64) effective_cache_size * 2);

	/* Both values are in blocks, so convert to bytes */
	memory_bytes *= BLCKSZ;

	if (memory_bytes > sysmem_bytes)
		memory_bytes = sysmem_bytes;

	return memory_bytes;
}

PG_FUNCTION_INFO_V1(estimate_effective_memory_bytes);

Datum
estimate_effective_memory_bytes(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(estimate_effective_memory());
}

PG_FUNCTION_INFO_V1(convert_text_memory_amount_to_bytes);

Datum
convert_text_memory_amount_to_bytes(PG_FUNCTION_ARGS)
{
	const char *val = text_to_cstring(PG_GETARG_TEXT_P(0));
	const char *hintmsg;
	int			nblocks;
	int64		bytes;

	if (!parse_int(val, &nblocks, GUC_UNIT_BLOCKS, &hintmsg))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid data amount"),
				 errhint("%s", hintmsg)));

	bytes = nblocks;
	bytes *= BLCKSZ;

	PG_RETURN_INT64(bytes);
}
