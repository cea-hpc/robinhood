AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

all_libs= ../logs/liblogs.la \
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

all_libs += ../list_mgr/liblistmgr.la ../common/libcommontools.la

#Lustre 2.x only
if LUSTRE
if USER_LOVEA
sbin_PROGRAMS=read_lovea set_lovea gen_lov_objid ost_fids_remap

set_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
read_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)

gen_lov_objid_DEPENDENCIES=$(all_libs)
gen_lov_objid_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
gen_lov_objid_LDADD=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

ost_fids_remap_DEPENDENCIES=$(all_libs)
ost_fids_remap_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)

endif
endif
