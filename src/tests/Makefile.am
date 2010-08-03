
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

check_PROGRAMS=test_hash_func test_list_mgr test_entry_proc test_info_collector test_resource_monitor

all_libs=       ../logs/liblogs.la \
                ../cfg_parsing/libconfigparsing.la \
                ../fs_scan/libfsscan.la \
                ../entry_processor/libentryproc.la \
                ../policy_modules/purge/libresmonitor.la \
                ../policy_matching/libpolicies.la \
                ../common/libcommontools.la \
                ../list_mgr/liblistmgr.la 
if TMP_FS_MGR
all_libs += ../policy_modules/rmdir/librmdir.la
else
all_libs += ../chglog_reader/libchglog_rd.la
all_libs += ../policy_modules/migration/libmigration.la
endif

test_list_mgr_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_entry_proc_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_info_collector_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_resource_monitor_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

indent:
	$(top_srcdir)/scripts/indent.sh
