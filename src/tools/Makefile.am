AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

all_libs=   ../cfg_parsing/librbhcfg.la         \
            ../fs_scan/libfsscan.la             \
            ../entry_processor/libentryproc.la  \
            ../policies/libpolicies.la

if CHANGELOGS
all_libs += ../chglog_reader/libchglog_rd.la
endif

all_libs += ../robinhood/librbhhelpers.la ../list_mgr/liblistmgr.la \
            ../common/libcommontools.la ../cfg_parsing/libconfigparsing.la

sbin_PROGRAMS=

#Lustre 2.x only
if LUSTRE
if USER_LOVEA
sbin_PROGRAMS+=read_lovea set_lovea gen_lov_objid ost_fids_remap

set_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
read_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)

gen_lov_objid_DEPENDENCIES=$(all_libs)
gen_lov_objid_CFLAGS=-static $(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
gen_lov_objid_LDFLAGS=-static
gen_lov_objid_LDADD=$(all_libs) $(DB_LDFLAGS) $(FS_LDFLAGS) $(PURPOSE_LDFLAGS)

ost_fids_remap_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
ost_fids_remap_LDADD=../common/basename.o
endif

if LUSTRE_HSM
sbin_PROGRAMS+=lhsmtool_cmd
lhsmtool_cmd_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
lhsmtool_cmd_LDFLAGS=-lrt
lhsmtool_cmd_LDADD=$(FS_LDFLAGS)
endif

endif
