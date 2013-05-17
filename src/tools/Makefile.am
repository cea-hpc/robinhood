AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)
AM_LDFLAGS= -lpthread

DATE=`date '+%F %T'`
MISC_FLAGS="-DCOMPIL_DATE=\"$(DATE)\""

if LUSTRE
sbin_PROGRAMS=read_lovea set_lovea

set_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)
read_lovea_CFLAGS=$(AM_CFLAGS) $(FS_CFLAGS) $(MISC_FLAGS)

endif
