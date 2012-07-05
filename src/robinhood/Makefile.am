
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

#if LUSTRE
#LVERSION=`rpm -qa "lustre[-_]modules*" --qf "%{Version}\n" | tail -1`
#FS_CFLAGS=-DLUSTRE_VERSION=\"$(LVERSION)\"
#endif

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

all_libs=	../logs/liblogs.la \
	        ../cfg_parsing/libconfigparsing.la \
                ../fs_scan/libfsscan.la \
                ../entry_processor/libentryproc.la \
                ../policy_matching/libpolicies.la

if CHANGELOGS
all_libs += ../chglog_reader/libchglog_rd.la
endif
if TMP_FS_MGR
all_libs += ../policy_modules/purge/libresmonitor.la
all_libs += ../policy_modules/rmdir/librmdir.la
endif
if SHERPA
all_libs += ../policy_modules/purge/libresmonitor.la
all_libs += ../policy_modules/rmdir/librmdir.la
all_libs += ../policy_modules/migration/libmigration.la
endif
if LUSTRE_HSM
all_libs += ../policy_modules/purge/libresmonitor.la
all_libs += ../policy_modules/migration/libmigration.la
all_libs += ../policy_modules/hsm_remove/libhsmrm.la
endif
if HSM_LITE
if SHOOK
all_libs += ../policy_modules/purge/libresmonitor.la
endif
all_libs += ../policy_modules/migration/libmigration.la
all_libs += ../policy_modules/hsm_remove/libhsmrm.la
all_libs += ../backend_ext/libbackend_ext.la
endif

all_libs += ../list_mgr/liblistmgr.la
all_libs += ../common/libcommontools.la

if TMP_FS_MGR

sbin_PROGRAMS=robinhood rbh-report
bin_PROGRAMS=rbh-find rbh-du

# dependencies:
robinhood_DEPENDENCIES=$(all_libs)
rbh_report_DEPENDENCIES=$(all_libs)
rbh_du_DEPENDENCIES=$(all_libs)
rbh_find_DEPENDENCIES=$(all_libs)

robinhood_SOURCES=rbh_daemon.c
robinhood_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
robinhood_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_report_SOURCES=rbh_report.c
rbh_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_find_SOURCES=rbh_find.c cmd_helpers.h cmd_helpers.c
rbh_find_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_find_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_du_SOURCES=rbh_du.c cmd_helpers.h cmd_helpers.c
rbh_du_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_du_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

endif

if LUSTRE_HSM
sbin_PROGRAMS=rbh-hsm rbh-hsm-report
bin_PROGRAMS=rbh-hsm-du rbh-hsm-find

# dependencies:
rbh_hsm_DEPENDENCIES=$(all_libs)
rbh_hsm_report_DEPENDENCIES=$(all_libs)
rbh_hsm_du_DEPENDENCIES=$(all_libs)
rbh_hsm_find_DEPENDENCIES=$(all_libs)

rbh_hsm_SOURCES=rbh_daemon.c
rbh_hsm_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_hsm_report_SOURCES=rbh_report.c cmd_helpers.c
rbh_hsm_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_hsm_du_SOURCES=rbh_du.c cmd_helpers.h cmd_helpers.c
rbh_hsm_du_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_du_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_hsm_find_SOURCES=rbh_find.c cmd_helpers.h cmd_helpers.c
rbh_hsm_find_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_find_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

endif

if SHOOK
sbin_PROGRAMS=rbh-shook rbh-shook-report rbh-shook-recov rbh-shook-undo-rm rbh-shook-import
bin_PROGRAMS=rbh-shook-du rbh-shook-find

# dependencies:
rbh_shook_DEPENDENCIES=$(all_libs)
rbh_shook_report_DEPENDENCIES=$(all_libs)
rbh_shook_recov_DEPENDENCIES=$(all_libs)
rbh_shook_du_DEPENDENCIES=$(all_libs)
rbh_shook_find_DEPENDENCIES=$(all_libs)

rbh_shook_SOURCES=rbh_daemon.c
rbh_shook_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_report_SOURCES=rbh_report.c cmd_helpers.c
rbh_shook_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_recov_SOURCES=rbh_recov.c
rbh_shook_recov_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_recov_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_undo_rm_SOURCES=rbh_undo_rm.c
rbh_shook_undo_rm_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_undo_rm_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_du_SOURCES=rbh_du.c cmd_helpers.h cmd_helpers.c
rbh_shook_du_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_du_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_find_SOURCES=rbh_find.c cmd_helpers.h cmd_helpers.c
rbh_shook_find_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_find_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_shook_import_SOURCES=rbh_import.c
rbh_shook_import_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_shook_import_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)
endif

if BACKUP
sbin_PROGRAMS=rbh-backup rbh-backup-report rbh-backup-recov rbh-backup-undo-rm rbh-backup-import
bin_PROGRAMS=rbh-backup-du rbh-backup-find

# dependencies:
rbh_backup_DEPENDENCIES=$(all_libs)
rbh_backup_report_DEPENDENCIES=$(all_libs)
rbh_backup_recov_DEPENDENCIES=$(all_libs)
rbh_backup_du_DEPENDENCIES=$(all_libs)
rbh_backup_find_DEPENDENCIES=$(all_libs)

rbh_backup_SOURCES=rbh_daemon.c
rbh_backup_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_report_SOURCES=rbh_report.c cmd_helpers.c
rbh_backup_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_recov_SOURCES=rbh_recov.c
rbh_backup_recov_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_recov_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_undo_rm_SOURCES=rbh_undo_rm.c
rbh_backup_undo_rm_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_undo_rm_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_du_SOURCES=rbh_du.c cmd_helpers.h cmd_helpers.c
rbh_backup_du_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_du_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_find_SOURCES=rbh_find.c cmd_helpers.h cmd_helpers.c
rbh_backup_find_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_find_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_backup_import_SOURCES=rbh_import.c
rbh_backup_import_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_backup_import_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)
endif



if SHERPA
sbin_PROGRAMS=rbh-sherpa rbh-sherpa-report
bin_PROGRAMS=rbh-sherpa-du rbh-sherpa-find

# dependencies:
rbh_sherpa_DEPENDENCIES=$(all_libs)
rbh_sherpa_report_DEPENDENCIES=$(all_libs)
rbh_sherpa_du_DEPENDENCIES=$(all_libs)
rbh_sherpa_find_DEPENDENCIES=$(all_libs)

rbh_sherpa_SOURCES=rbh_daemon.c
rbh_sherpa_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_sherpa_report_SOURCES=rbh_report.c cmd_helpers.c
rbh_sherpa_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_sherpa_du_SOURCES=rbh_du.c cmd_helpers.h cmd_helpers.c
rbh_sherpa_du_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_du_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_sherpa_find_SOURCES=rbh_find.c cmd_helpers.h cmd_helpers.c
rbh_sherpa_find_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_find_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

endif

new: clean all

indent:
	$(top_srcdir)/scripts/indent.sh
