AM_CFLAGS= -static $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread -static

# See autotools/m4/ax_valgrind_check.m4 for documentation
@VALGRIND_CHECK_RULES@
#VALGRIND_SUPPRESSIONS_FILES = my-project.supp
#EXTRA_DIST = my-project.supp

check_PROGRAMS=test_uidgidcache test_params \
    test_confparam test_parse
if LUSTRE
check_PROGRAMS+=create_nostripe test_forcestripe
endif
TESTS=test_parsing.sh test_uidgidcache test_params test_confparam

noinst_PROGRAMS=$(check_PROGRAMS)

test_forcestripe_LDADD=$(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_uidgidcache_SOURCES=test_uidgidcache.c ../common/uidgidcache.c ../common/RW_Lock.c
test_params_SOURCES=test_params.c ../common/rbh_params.c
test_confparam_SOURCES=test_confparam.c ../common/param_utils.c ../common/rbh_params.c
test_parse_SOURCES	    = test_parse.c
test_parse_LDADD         =  ../cfg_parsing/libconfigparsing.la


indent:
	$(top_srcdir)/scripts/indent.sh
