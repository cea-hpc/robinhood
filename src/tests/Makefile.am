
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

check_PROGRAMS=test_hash_func test_list_mgr test_entry_proc test_info_collector test_resource_monitor

all_libs=       ../Logs/liblogs.la \
                ../ConfigParsing/libConfigParsing.la \
                ../InfoCollectors/fs_scan/libfsscan.la \
                ../EntryProcessor/libentryproc.la \
                ../ResourceMonitor/libresmonitor.la \
                ../Policies/libpolicies.la \
                ../Common/libcommontools.la \
                ../ListMgr/liblistmgr.la 
if TMP_FS_MGR
all_libs += ../Rmdir/librmdir.la
else
all_libs += ../InfoCollectors/chglog_reader/libchglog_rd.la
all_libs += ../Migration/libmigration.la
endif

test_list_mgr_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_entry_proc_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_info_collector_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_resource_monitor_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

indent:
	$(top_srcdir)/scripts/indent.sh
