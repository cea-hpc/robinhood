AM_CFLAGS= -static $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread -static

check_PROGRAMS=create_nostripe test_forcestripe test_uidgidcache test_params
TESTS=test_uidgidcache test_params

noinst_PROGRAMS=$(check_PROGRAMS)

test_forcestripe_LDADD=$(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_uidgidcache_SOURCES=test_uidgidcache.c ../common/uidgidcache.c ../common/RW_Lock.c
test_params_SOURCES=test_params.c ../common/rbh_params.c

indent:
	$(top_srcdir)/scripts/indent.sh
