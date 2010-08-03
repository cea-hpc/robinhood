
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

#if LUSTRE
#LVERSION=`rpm -qa "lustre[-_]modules*" --qf "%{Version}\n" | tail -1`
#FS_CFLAGS=-DLUSTRE_VERSION=\"$(LVERSION)\"
#endif

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

all_libs=	../Logs/liblogs.la \
	        ../ConfigParsing/libConfigParsing.la \
                ../InfoCollectors/fs_scan/libfsscan.la \
                ../EntryProcessor/libentryproc.la \
                ../Policies/libpolicies.la \
                ../ResourceMonitor/libresmonitor.la \
                ../ListMgr/liblistmgr.la


if CHANGELOGS
all_libs += ../InfoCollectors/chglog_reader/libchglog_rd.la
endif
if TMP_FS_MGR
all_libs += ../Rmdir/librmdir.la
endif
if SHERPA
all_libs += ../Rmdir/librmdir.la
all_libs += ../Migration/libmigration.la
endif
if LUSTRE_HSM
all_libs += ../Migration/libmigration.la
all_libs += ../Remove/libhsmrm.la
endif

all_libs += ../Common/libcommontools.la

if TMP_FS_MGR

sbin_PROGRAMS=robinhood rbh-report

# dependencies:
robinhood_DEPENDENCIES=$(all_libs)
rbh_report_DEPENDENCIES=$(all_libs)

robinhood_SOURCES=robinhood_main.c
robinhood_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
robinhood_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_report_SOURCES=robinhood_report.c
rbh_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)
endif

if LUSTRE_HSM
sbin_PROGRAMS=rbh-hsm rbh-hsm-report

# dependencies:
rbh_hsm_DEPENDENCIES=$(all_libs)
rbh_hsm_report_DEPENDENCIES=$(all_libs)

rbh_hsm_SOURCES=robinhood_main.c
rbh_hsm_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_hsm_report_SOURCES=robinhood_report.c
rbh_hsm_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_hsm_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)
endif

if SHERPA
sbin_PROGRAMS=rbh-sherpa rbh-sherpa-report

# dependencies:
rbh_sherpa_DEPENDENCIES=$(all_libs)
rbh_sherpa_report_DEPENDENCIES=$(all_libs)

rbh_sherpa_SOURCES=robinhood_main.c
rbh_sherpa_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_LDFLAGS=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

rbh_sherpa_report_SOURCES=robinhood_report.c
rbh_sherpa_report_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
rbh_sherpa_report_LDFLAGS=-static $(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

endif

new: clean all

indent:
	$(top_srcdir)/scripts/indent.sh
