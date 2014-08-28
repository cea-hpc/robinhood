AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

check_PROGRAMS=create_nostripe test_forcestripe test_hash_func test_uidgidcache test_list_mgr test_entry_proc test_info_collector test_resource_monitor

all_libs=	../cfg_parsing/libconfigparsing.la \
                ../fs_scan/libfsscan.la \
                ../entry_processor/libentryproc.la \
                ../policies/libpolicies.la

if CHANGELOGS
all_libs += ../chglog_reader/libchglog_rd.la
endif
if HSM_LITE
all_libs += ../backend_ext/libbackend_ext.la
endif

all_libs += ../robinhood/librbhhelpers.la ../list_mgr/liblistmgr.la ../common/libcommontools.la



test_forcestripe_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_list_mgr_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_entry_proc_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_info_collector_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_resource_monitor_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_uidgidcache_LDADD=$(all_libs) $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

indent:
	$(top_srcdir)/scripts/indent.sh
