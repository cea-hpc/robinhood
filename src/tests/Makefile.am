AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

check_PROGRAMS=create_nostripe test_forcestripe test_hash_func test_uidgidcache test_list_mgr test_entry_proc test_info_collector test_resource_monitor

all_libs=	../logs/liblogs.la \
	        ../cfg_parsing/libconfigparsing.la \
                ../fs_scan/libfsscan.la \
                ../entry_processor/libentryproc.la \
                ../policy_matching/libpolicies.la \
                ../policy_modules/purge/libresmonitor.la \
                ../list_mgr/liblistmgr.la


if CHANGELOGS
all_libs += ../chglog_reader/libchglog_rd.la
endif
if TMP_FS_MGR
all_libs += ../policy_modules/purge/libresmonitor.la
all_libs += ../policy_modules/rmdir/librmdir.la
endif
if LUSTRE_HSM
all_libs += ../policy_modules/purge/libresmonitor.la
all_libs += ../policy_modules/migration/libmigration.la
all_libs += ../policy_modules/hsm_remove/libhsmrm.la
endif
if HSM_LITE
all_libs += ../policy_modules/migration/libmigration.la
all_libs += ../policy_modules/hsm_remove/libhsmrm.la
all_libs += ../backend_ext/libbackend_ext.la
endif

all_libs += ../list_mgr/liblistmgr.la
all_libs += ../common/libcommontools.la


test_forcestripe_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_list_mgr_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_entry_proc_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_info_collector_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_resource_monitor_LDADD=$(all_libs)  $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

test_uidgidcache_LDADD=$(all_libs) $(DB_LDFLAGS) $(PURPOSE_LDFLAGS) $(FS_LDFLAGS)

indent:
	$(top_srcdir)/scripts/indent.sh
