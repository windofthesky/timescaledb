-- this trigger function causes an invalidation event on the table whose name is
-- passed in as the first element.
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache_trigger()
 RETURNS TRIGGER AS '@MODULE_PATHNAME@', 'invalidate_relcache_trigger' LANGUAGE C;

-- This function is only used for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(proxy_oid OID)
 RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'invalidate_relcache' LANGUAGE C;
