#include <postgres.h>
#include <access/xact.h>
#include <commands/extension.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>

#include "extension.h"
#include "load.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"

static bool loaded = false;

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

void
extension_check()
{
	if (!loaded)
	{
		if (IsTransactionState() && proxy_table_exists() && extension_exists())
		{
			load_extension();
			loaded = true;
		}
	}
}
