AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread -rpath $(pkglibdir)

#if LUSTRE
#LVERSION=`rpm -qa "lustre[-_]modules*" --qf "%{Version}\n" | tail -1`
#FS_CFLAGS=-DLUSTRE_VERSION=\"$(LVERSION)\"
#endif

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

noinst_LTLIBRARIES=librbhhelpers.la
librbhhelpers_la_SOURCES=cmd_helpers.h cmd_helpers.c
librbhhelpers_la_LDFLAGS=-static

all_libs=   ../cfg_parsing/librbhcfg.la         \
            ../fs_scan/libfsscan.la             \
            ../entry_processor/libentryproc.la  \
            ../policies/libpolicies.la

if CHANGELOGS
all_libs += ../chglog_reader/libchglog_rd.la
endif

all_libs += ./librbhhelpers.la ../list_mgr/liblistmgr.la \
            ../common/libcommontools.la ../cfg_parsing/libconfigparsing.la

#sbin_PROGRAMS=robinhood rbh-report rbh-diff rbh-recov rbh-undelete rbh-import rbh-rebind
sbin_PROGRAMS=robinhood rbh-report rbh-diff rbh-undelete
bin_PROGRAMS=rbh-find rbh-du

# dependencies:
robinhood_DEPENDENCIES=$(all_libs)
rbh_report_DEPENDENCIES=$(all_libs)
rbh_du_DEPENDENCIES=$(all_libs)
rbh_find_DEPENDENCIES=$(all_libs)
rbh_diff_DEPENDENCIES=$(all_libs)
#rbh_recov_DEPENDENCIES=$(all_libs)
rbh_undelete_DEPENDENCIES=$(all_libs)
#rbh_import_DEPENDENCIES=$(all_libs)
#rbh_rebind_DEPENDENCIES=$(all_libs)
#
robinhood_SOURCES=rbh_daemon.c
robinhood_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
robinhood_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

rbh_report_SOURCES=rbh_report.c
rbh_report_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_report_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

rbh_find_SOURCES=rbh_find.c rbh_find_printf.c rbh_find.h
rbh_find_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_find_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

rbh_du_SOURCES=rbh_du.c
rbh_du_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_du_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

rbh_diff_SOURCES=rbh_diff.c
rbh_diff_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_diff_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAG) $(AM_LDFLAGS)

#rbh_recov_SOURCES=rbh_recov.c
#rbh_recov_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
#rbh_recov_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)
#
rbh_undelete_SOURCES=rbh_undelete.c
rbh_undelete_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_undelete_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)
#
#rbh_import_SOURCES=rbh_import.c
#rbh_import_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
#rbh_import_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)
#
#rbh_rebind_SOURCES=rbh_rebind.c
#rbh_rebind_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
#rbh_rebind_LDFLAGS=-rdynamic $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS) $(AM_LDFLAGS)

new: clean all

indent:
	$(top_srcdir)/scripts/indent.sh
