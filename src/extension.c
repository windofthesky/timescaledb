#include <postgres.h>
#include <access/xact.h>
#include <access/transam.h>
#include <commands/extension.h>
#include <commands/event_trigger.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>
#include <miscadmin.h>
#include <access/relscan.h>
#include <catalog/indexing.h>
#include <catalog/pg_extension.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>

#include "catalog.h"
#include "extension.h"
#include "version.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"

static Oid	extension_proxy_oid = InvalidOid;

/*
 * ExtensionState tracks the state of extension metadata in the backend.
 *
 * Since we want to cache extension metadata to speed up common checks (e.g.,
 * check for presence of the extension itself), we also need to track the
 * extension state to know when the metadata is valid.
 *
 * We use a proxy_table to be notified of extension drops/creates. Namely,
 * we rely on the fact that postgres will internally create RelCacheInvalidation
 * events when any tables are created or dropped. We rely on the following properties
 * of Postgres's dependency managment:
 *	* The proxy table will be created before the extension itself.
 *	* The proxy table will be dropped before the extension itself.
 */
enum ExtensionState
{
	/*
	 * NOT_INSTALLED means that this backend knows that the extension is not
	 * present.  In this state we know that the proxy table is not present.
	 * Thus, the only way to get out of this state is a RelCacheInvalidation
	 * indicating that the proxy table was added.
	 */
	EXTENSION_STATE_NOT_INSTALLED,

	/*
	 * UNKNOWN state is used only if we cannot be sure what the state is. This
	 * can happen in two cases: 1) at the start of a backend or 2) We got a
	 * relcache event outside of a transaction and thus could not check the
	 * cache for the presence/absence of the proxy table or extension.
	 */
	EXTENSION_STATE_UNKNOWN,

	/*
	 * TRANSITIONING only occurs when the proxy table exists but the extension
	 * does not. This can only happen in the middle of a create or drop
	 * extension.
	 */
	EXTENSION_STATE_TRANSITIONING,

	/*
	 * CREATED means we know the extension is loaded, metadata is up-to-date,
	 * and we therefore do not need a full check until a RelCacheInvalidation
	 * on the proxy table.
	 */
	EXTENSION_STATE_CREATED,
};

static enum ExtensionState extstate = EXTENSION_STATE_UNKNOWN;

static bool
proxy_table_exists()
{
	Oid			nsid = get_namespace_oid(CACHE_SCHEMA_NAME, true);
	Oid			proxy_table = get_relname_relid(EXTENSION_PROXY_TABLE, nsid);

	return OidIsValid(proxy_table);
}

static bool
extension_exists()
{
	return OidIsValid(get_extension_oid(EXTENSION_NAME, true));
}

static char *
extension_version(void)
{
	Datum		result;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];
	bool		is_null = true;
	static char *sql_version = NULL;

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(EXTENSION_NAME));

	scandesc = systable_beginscan(rel, ExtensionNameIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		result = heap_getattr(tuple, Anum_pg_extension_extversion, RelationGetDescr(rel), &is_null);

		if (!is_null)
		{
			sql_version = strdup(TextDatumGetCString(result));
		}
	}

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);
	return sql_version;
}

static bool inline
extension_is_transitioning()
{
	/*
	 * Determine whether the extension is being created or upgraded (as a
	 * misnomer creating_extension is true during upgrades)
	 */
	if (creating_extension)
	{
		Oid			extension_oid = get_extension_oid(EXTENSION_NAME, true);

		if (!OidIsValid(extension_oid))
		{
			/* be conservative */
			return true;
		}

		if (extension_oid == CurrentExtensionObject)
			return true;
	}
	return false;
}

static void
assert_extension_version(void)
{
	char	   *sql_version;

	if (extension_is_transitioning())
	{
		return;
	}

	sql_version = extension_version();

	if (strcmp(sql_version, TIMESCALEDB_VERSION_MOD) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Mismatched timescaledb version. Shared object file %s, SQL %s", TIMESCALEDB_VERSION_MOD, sql_version),
				 errhint("Restart postgres and then run 'ALTER EXTENSION timescaledb UPDATE'")));
	}

}

/* Returns the recomputed current state */
static enum ExtensionState
extension_new_state()
{
	/*
	 * NormalProcessingMode necessary to avoid accessing cache before its
	 * ready (which may result in an infinite loop). More concretely we need
	 * RelationCacheInitializePhase3 to have been already called.
	 */
	if (!IsNormalProcessingMode() || !IsTransactionState())
		return EXTENSION_STATE_UNKNOWN;

	if (proxy_table_exists())
	{
		if (!extension_exists())
			return EXTENSION_STATE_TRANSITIONING;
		else
			return EXTENSION_STATE_CREATED;
	}
	return EXTENSION_STATE_NOT_INSTALLED;
}

/* Sets a new state, returning whether the state has changed */
static bool
extension_set_state(enum ExtensionState newstate)
{
	if (newstate == extstate)
	{
		return false;
	}
	switch (newstate)
	{
		case EXTENSION_STATE_TRANSITIONING:
		case EXTENSION_STATE_UNKNOWN:
			break;
		case EXTENSION_STATE_CREATED:
			assert_extension_version();
			extension_proxy_oid = get_relname_relid(EXTENSION_PROXY_TABLE, get_namespace_oid(CACHE_SCHEMA_NAME, false));
			catalog_reset();
			break;
		case EXTENSION_STATE_NOT_INSTALLED:
			extension_proxy_oid = InvalidOid;
			catalog_reset();
			break;
	}
	extstate = newstate;
	return true;
}

/* Updates the state based on the current state, returning whether there had been a change. */
static bool
extension_update_state()
{
	return extension_set_state(extension_new_state());
}

/*
 *	Called upon all Relcache invalidate events.
 *	Returns whether or not to invalidate the entire extension.
 */
bool
extension_invalidate(Oid relid)
{
	switch (extstate)
	{
		case EXTENSION_STATE_NOT_INSTALLED:
			/* This event may mean we just added the proxy table */
		case EXTENSION_STATE_UNKNOWN:
			/* Can we recompute the state now? */
		case EXTENSION_STATE_TRANSITIONING:
			/* Has the create/drop extension finished? */
			extension_update_state();
			return false;
		case EXTENSION_STATE_CREATED:

			/*
			 * Here we know the proxy table oid so only listen to potential
			 * drops on that oid. Note that an invalid oid passed in the
			 * invalidation event applies to all tables.
			 */
			if (extension_proxy_oid == relid || !OidIsValid(relid))
			{
				extension_update_state();
				if (EXTENSION_STATE_CREATED != extstate)
				{
					/*
					 * note this state may be UNKNOWN but should be
					 * conservative
					 */
					return true;
				}
			}
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}

bool
extension_is_loaded(void)
{
	if (EXTENSION_STATE_UNKNOWN == extstate || EXTENSION_STATE_TRANSITIONING == extstate)
	{
		/* status may have updated without a relcache invalidate event */
		extension_update_state();
	}

	/*
	 * Turn off extension during upgrade scripts. This is necessary so that,
	 * for example, the catalog does not go looking for things that aren't yet
	 * there.
	 */
	if (extension_is_transitioning())
	{
		return false;
	}

	switch (extstate)
	{
		case EXTENSION_STATE_CREATED:
			return true;
		case EXTENSION_STATE_NOT_INSTALLED:
		case EXTENSION_STATE_UNKNOWN:
		case EXTENSION_STATE_TRANSITIONING:
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}
