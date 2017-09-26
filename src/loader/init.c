#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include <commands/extension.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <parser/analyze.h>


#include "extension.h"

#define MIN_SUPPORTED_VERSION_STR "9.6"
#define MIN_SUPPORTED_VERSION_NUM 90600

#if PG_VERSION_NUM < MIN_SUPPORTED_VERSION_NUM
#error "Unsupported version of PostgreSQL. Check src/init.c for supported versions."
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void _PG_init(void);
extern void _PG_fini(void);

static void
inval_cache_callback(Datum arg, Oid relid)
{
	extension_check();
}

static void
post_analyze_hook(ParseState *pstate, Query *query)
{
	extension_check();
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		/* cannot use GUC variable here since extension not yet loaded */
		char	   *allow_install_without_preload = GetConfigOptionByName("timescaledb.allow_install_without_preload", NULL, true);

		if (allow_install_without_preload == NULL ||
			strlen(allow_install_without_preload) != 2 ||
			strncmp(allow_install_without_preload, "on", 2) != 0)
		{
			char	   *config_file = GetConfigOptionByName("config_file", NULL, false);

			ereport(ERROR,
					(errmsg("The timescaledb library is not preloaded"),
					 errhint("Please preload the timescaledb library via shared_preload_libraries.\n\n"
					 "This can be done by editing the config file at: %1$s\n"
							 "and adding 'timescaledb' to the list in the shared_preload_libraries config.\n"
							 "	# Modify postgresql.conf:\n	shared_preload_libraries = 'timescaledb'\n\n"
							 "Another way to do this, if not preloading other libraries, is with the command:\n"
							 "	echo \"shared_preload_libraries = 'timescaledb'\" >> %1$s \n\n"
							 "(Will require a database restart.)\n\n"
							 "If you REALLY know what you are doing and would like to load the library without preloading, you can disable this check with: \n"
							 "	SET timescaledb.allow_install_without_preload = 'on';", config_file)));
			return;
		}
	}
	elog(INFO, "timescaledb loaded");

	/* cannot check for extension here since not inside a transaction yet */

	CacheRegisterRelcacheCallback(inval_cache_callback, PointerGetDatum(NULL));

	/*
	 * using the post_parse_analyze_hook since it's the earliest available
	 * hook
	 */
	post_parse_analyze_hook = post_analyze_hook;
}

void
_PG_fini(void)
{
	/* No way to unregister relcache callback */
}
