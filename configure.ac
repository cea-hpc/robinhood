# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT( [robinhood], [3.1.3], [robinhood-support@lists.sourceforge.net])
RELEASE="1"

AC_DEFINE_UNQUOTED(RELEASE, "$RELEASE", [release info])
AC_SUBST(RELEASE)

AC_CONFIG_AUX_DIR([autotools])
AC_CONFIG_MACRO_DIR([autotools/m4])
AC_CONFIG_HEADER([src/include/config.h])
AC_CONFIG_SRCDIR([src/list_mgr/listmgr_init.c])

AM_INIT_AUTOMAKE([-Wall -Werror tar-pax foreign subdir-objects])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT([/usr])

AC_GNU_SOURCE

AC_CONFIG_FILES([Makefile
         src/Makefile
         src/include/Makefile
         src/common/Makefile
         src/cfg_parsing/Makefile
         src/list_mgr/Makefile
         src/entry_processor/Makefile
         src/fs_scan/Makefile
         src/chglog_reader/Makefile
         src/modules/Makefile
         src/policies/Makefile
         src/robinhood/Makefile
         src/tools/Makefile
         src/tests/Makefile
         scripts/Makefile
         scripts/robinhood.init
         scripts/robinhood.init.sles
         scripts/robinhood.service
         scripts/robinhood@.service
         scripts/sysconfig_robinhood
         scripts/ld.so.robinhood.conf
         scripts/rbh_cksum.sh
         tests/Makefile
         tests/test_suite/Makefile
         web_gui/Makefile
         man/Makefile
         robinhood.spec
         doc/doxygen/doxygen.cfg])

AC_ARG_ENABLE([dist],
              AC_HELP_STRING([--enable-dist], [Only configure enough for make dist]),
              [],
              [enable_dist="no"])

AM_PROG_LEX
AC_PATH_PROG(LEX_INST, $LEX)
if test -z "$LEX_INST" -a "x$enable_dist" = xyes; then
  AC_MSG_ERROR([lex/flex not found])
fi

AC_PROG_YACC
AC_PATH_PROG(YACC_INST, $YACC)
if test -z "$YACC_INST" -a "x$enable_dist" = xyes; then
  AC_MSG_ERROR([yacc/bison not found])
fi

AS_IF([test "x$enable_dist" = xyes], [
AC_OUTPUT
exit
])

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

AC_PROG_LIBTOOL
AC_PROG_CC

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_CC_C_O], [AM_PROG_CC_C_O])

AC_C_INLINE

# define everything necessary for accessing large files (64bits offset)
AC_SYS_LARGEFILE

# gcc options
CC_OPT="-Wall -Werror -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -std=gnu99 -Wno-variadic-macros"
AC_SUBST(CC_OPT)

AC_CHECK_SIZEOF([nlink_t])
AC_CHECK_SIZEOF([off_t])
AC_CHECK_SIZEOF([ino_t])
AC_CHECK_SIZEOF([dev_t])
AC_CHECK_SIZEOF([time_t])
AC_CHECK_SIZEOF([size_t])
AC_CHECK_SIZEOF([pthread_t])

AX_ENABLE_FLAG([strict], [Compile with -Wall -Werror], [-Wall -Werror])

AC_CHECK_LIB(z, gzflush,
  [LIBS="-lz $LIBS"
   AC_DEFINE(HAVE_LIBZ, 1, [define if you have zlib])],
  [AC_MSG_WARN([zlib is required])])

PKG_CHECK_MODULES(GLIB2, [glib-2.0 >= 2.16])
PKG_CHECK_MODULES(GTHREAD2, [gthread-2.0])

CFLAGS="$CFLAGS $GLIB2_CFLAGS $GTHREAD2_CFLAGS"
LDFLAGS="$LDFLAGS $GLIB2_LIBS $GTHREAD2_LIBS"

# debug flags
AX_ENABLE_FLAG([debug-db], [enables debug traces for database operations], [-D_DEBUG_DB])
AX_ENABLE_FLAG([debug-parsing], [enables debug traces for configuration file parsing], [-D_DEBUG_PARSING])
AM_CONDITIONAL(DEBUG_PARSING, test "x$enable_debug_parsing" == "xyes" )

AX_ENABLE_FLAG([debug-pipeline], [enables debug traces for entry processor pipeline], [-D_DEBUG_ENTRYPROC])
AX_ENABLE_FLAG([debug-policies], [enables debug traces for entry processor pipeline], [-D_DEBUG_POLICIES])
AX_ENABLE_FLAG([debug-hash], [enables debug traces internal hash tables], [-D_DEBUG_HASH])

AX_ENABLE_FLAG([bench-scan], [test only: build special version for scan benchmarking], [-D_BENCH_SCAN])
AX_ENABLE_FLAG([bench-db], [test only: build special version for DB benchmarking], [-D_BENCH_DB])
AX_ENABLE_FLAG([bench-pipeline], [test only: build special version for pipeline benchmarking], [-D_BENCH_PIPELINE -D_BENCH_DB])
AX_ENABLE_FLAG([gprof], [test only: add gprof info to the binaries], [-g -pg])
AX_ENABLE_FLAG([gcov], [test only: add gcov info to the binaries], [--coverage])
AX_VALGRIND_CHECK

# behavior flags
AX_DISABLE_FLAG([atfunc], [Don't use 'at' functions for scanning], [-D_NO_AT_FUNC])

AX_ENABLE_FLAG( [fid2path-leading-slash], [must be enabled if fid2path() returns a leading slash], [-D_FID2PATH_LEADING_SLASH] )

AC_ARG_ENABLE( [data-version], AS_HELP_STRING([--disable-data-version],
               [Disable the use of llapi_get_data_version()]),
               [support_dv="$enableval"],[support_dv="yes"] )
DV_CMD="stat -c %Y-%s"

AC_ARG_ENABLE([common-rpms], AS_HELP_STRING([--disable-common-rpms],
              [Disable build of common RPMs, only build FS specific packages]),
              [common_rpms="$enableval"],[common_rpms="yes"])
AM_CONDITIONAL(COMMON_RPMS,test  "x$common_rpms" = "xyes")

build_lustre="OFF"
build_backup="OFF"
build_lhsm="OFF"
build_shook="OFF"

AC_ARG_ENABLE([lustre], AS_HELP_STRING([--disable-lustre],
              [Disable all lustre specific features]),
              [support_lustre="$enableval"],[support_lustre="yes"])
AC_ARG_ENABLE([shook], AS_HELP_STRING([--disable-shook],
              [Disable build of shook specific modules]),
              [support_shook="$enableval"],[support_shook="yes"])

# default input option is --scan
INPUT_OPT="--scan"

# shook requires Lustre + FID support + shook library
# hsm_lite requires Lustre + FID support
# lustre_hsm requires Lustre >= 2.5

if test "x$support_lustre" = "xyes" ; then
    # Lustre location
    AC_ARG_WITH( [lustre], AS_HELP_STRING([--with-lustre=<lustre_src_dir>],[indicate alternative location for lustre]),
                 LDIR="$withval")

    if test -n "$LDIR"; then
        CFLAGS="$CFLAGS -I$LDIR/lustre/include"
        LDFLAGS="$LDFLAGS -L$LDIR/lustre/utils"
    fi

    AC_CHECK_LIB([lustreapi], [llapi_obd_statfs], [have_liblustre_api="yes"])
    AM_CONDITIONAL( LUSTRE, test  "x$have_liblustre_api" = "xyes" )
    AC_SUBST(LUSTRE)

    # lustre relative information and checks
    if test "x$have_liblustre_api" = "xyes" ; then

        AC_DEFINE(_LUSTRE, 1, [liblustreapi is available])
        build_lustre="ON"

        AC_CHECK_HEADER([lustre/lustreapi.h], have_new_lustre_header="true", have_new_lustre_header="false")
        if test "$have_new_lustre_header" = "true"; then
            AC_DEFINE(_LUSTRE_API_HEADER, 1, [New lustreapi header])
            # this comes with Lustre 2.4, so disable features of Lustre < 2.4 at the same time
            AC_DEFINE(_MDT_SPECIFIC_LOVEA, 1, [MDT LOV EA is no longer the same as lov_user_md])
        fi

        AC_CHECK_HEADER([lustre/lustre_idl.h], have_lustre_idl="true", have_lustre_idl="false", [
            #define LPX64 "%#llx"
            #include <sys/types.h>
            #include <asm/types.h>
            #include <lustre/lustre_user.h>
        ])
        if test "$have_lustre_idl" = "true"; then
            AC_DEFINE(_LUSTRE_IDL_HEADER, 1, [lustre_idl header exists])
        fi

        # this defines LVERSION variable
        if test -z "$LDIR" ; then
            AX_LUSTRE_VERSION
        else
            # get version from sources
            AX_LUSTRE_SRC_VERSION("$LDIR")
        fi

        LDEFINES=""
        if test -n "$LVERSION"; then
            LDEFINES="--define=\"lversion $LVERSION\""
            AC_DEFINE_UNQUOTED(LUSTRE_VERSION, "$LVERSION",  [Lustre version])
        fi
        if test -n "$LPACKAGE"; then
            LDEFINES="$LDEFINES --define=\"lpackage $LPACKAGE\""
        fi
        AC_SUBST(LDEFINES)

        FS_LDFLAGS="-llustreapi"
        AC_SUBST(FS_LDFLAGS)

        # log functions
        AC_CHECK_LIB([lustreapi], [llapi_msg_set_level], [have_llapi_msg_level="yes"])
        test "x$have_llapi_msg_level" = "xyes" && AC_DEFINE(HAVE_LLAPI_MSG_LEVEL, 1, [llapi_msg_set_level is available])

        AC_CHECK_LIB([lustreapi], [llapi_error_callback_set],
                     [have_llapi_error_callback_set="yes"])
        test "x$have_llapi_error_callback_set" = "xyes" &&
            AC_DEFINE(HAVE_LLAPI_LOG_CALLBACKS, 1, [llapi log callbacks are available])

        AC_CHECK_LIB([lustreapi], [llapi_get_mdt_index_by_fid],
                     [have_llapi_get_mdt_index_by_fid="yes"])
        test "x$have_llapi_get_mdt_index_by_fid" = "xyes" &&
            AC_DEFINE(HAVE_LLAPI_GET_MDT_INDEX_BY_FID, 1,
                      [llapi_get_mdt_index_by_fid available])

        # check if struct statfs is defined in lustre user
        AC_CHECK_TYPE(struct obd_statfs,[have_obd_statfs="yes"],[have_obd_statfs="no"],[
            #define LPX64 "%#llx"
            #include <sys/types.h>
            #include <asm/types.h>
            #include <lustre/lustre_user.h>
        ])
        test "x$have_obd_statfs" = "xyes" && AC_DEFINE(HAVE_OBD_STATFS, 1, [struct obd_stafs is defined])

        AC_ARG_ENABLE( [fid-support], AS_HELP_STRING([--disable-fid-support],
                       [Don't address files by fid]),
                       fid_support="$enableval", fid_support="yes" )

            # only check for fid if fid support is not disabled
        if test "x$fid_support" = "xyes" ; then
                AC_CHECK_LIB([lustreapi], [llapi_fid2path], [have_fid="yes"])
                if test "x$have_fid" = "xyes"; then
                    AC_DEFINE(_HAVE_FID, 1, [lustre supports fids])
                    AC_DEFINE(_HSM_LITE, 1, [HSM lite support])
                    build_backup="ON"
                fi
        fi

        # check if fd2fid function exists
        AC_CHECK_LIB([lustreapi], [llapi_fd2fid], [have_fd2fid="yes"])
        test "x$have_fd2fid" = "xyes" && AC_DEFINE(HAVE_FD2FID, 1, [llapi_fd2fid function is available])

        # check for DNE support
        AC_CHECK_LIB([lustreapi], [llapi_file_fget_mdtidx], [have_dne_support="yes"])
        test "x$have_dne_support" = "xyes" && AC_DEFINE(HAVE_DNE, 1, [this version of Lustre supports DNE]) # FIXME wrong test (works with 2.1!)

        AC_ARG_ENABLE( [changelogs], AS_HELP_STRING([--disable-changelogs],[Don't use ChangeLogs]),
                   use_changelogs=$enableval, use_changelogs='yes' )

    	if test "x$use_changelogs" == "xyes" ; then

    		# test changelog functions

	    	AC_CHECK_LIB([lustreapi],[llapi_changelog_start],[have_llapi_changelog_start="yes"])
		    if test "x$have_llapi_changelog_start" = "xyes"; then
		        AC_DEFINE(HAVE_CHANGELOGS, 1, [Lustre changelogs records are structures])
                AC_CHECK_DECL([CL_IOCTL],AC_DEFINE(_HAVE_CL_IOCTL,1,[CL_IOCTL is defined]),[],[
                    #define LPX64 "%#llx"
                    #include <sys/types.h>
                    #include <asm/types.h>
                    #include <lustre/lustre_user.h>
                ])
                AC_CHECK_DECL([CL_LAYOUT],AC_DEFINE(HAVE_CL_LAYOUT,1,[Layout change emit changelog records]),[],[
                    #define LPX64 "%#llx"
                    #include <sys/types.h>
                    #include <asm/types.h>
                    #include <lustre/lustre_user.h>
                ])

                # changelog_ext_rec was removed by commit 0f22e4,
                # which added the flexible changelog format. That
                # commit added CLF_RENAME, so check for that symbol.
                AC_CHECK_DECLS([CLF_RENAME],[],[],[
                    #define LPX64 "%#llx"
                    #include <sys/types.h>
                    #include <asm/types.h>
                    #include <lustre/lustre_user.h>
                ])
                # default option for sysconfig
                INPUT_OPT="--readlog"
            fi

            if test "x$have_llapi_changelog_start" = "xyes" && \
               test "x$have_fid" != "xyes"; then
                AC_MSG_ERROR([Conflicting options: fid must be enabled for enabling MDT changelogs management])
            fi
    	fi
        if test "x$support_dv" = "xyes" ; then
            AC_CHECK_LIB([lustreapi], [llapi_get_data_version], [have_data_version="yes"])
        fi
        test "x$have_data_version" = "xyes" && DV_CMD="lfs data_version"

        # llapi_swap_layouts exists since Lustre2.4 release
        # since then, entry striping can change so scanning must update file stripe information
        AC_CHECK_LIB([lustreapi], [llapi_fswap_layouts], [have_llapi_fswap_layouts="yes"])
        test "x$have_llapi_fswap_layouts" = "xyes" && AC_DEFINE(HAVE_LLAPI_FSWAP_LAYOUTS, 1, [llapi_fswap_layouts is available])

        AX_ENABLE_FLAG( [llapi-fork-support], [Must be enabled if liblustreapi uses fork()], [-D_LLAPI_FORKS] )
        AX_ENABLE_FLAG( [mds-stat], [ioctl() to MDC instead of POSIX stat()], [-D_MDS_STAT_SUPPORT] )

        # pool functions
        AC_CHECK_LIB([lustreapi], [llapi_get_poollist], [have_llapi_poollist="yes"])
        AC_CHECK_LIB([lustreapi], [llapi_get_poolmembers], [have_llapi_poolmembers="yes"])
        test "x$have_llapi_poollist" = "xyes" && test "x$have_llapi_poolmembers" = "xyes"  && \
            AC_DEFINE(HAVE_LLAPI_GETPOOL_INFO, 1, [llapi_getpool functions are available])


        AC_CHECK_MEMBER([struct lov_user_ost_data_v1.l_object_seq], [have_obj_seq="yes"],  [have_obj_seq="no"], [
                    #define LPX64 "%#llx"
                    #include <sys/types.h>
                    #include <asm/types.h>
                    #include <lustre/lustre_user.h>
        ])
        test "x$have_obj_seq" = "xyes" && AC_DEFINE(HAVE_OBJ_SEQ, 1, [lov_user_ost_data_v1 has l_object_seq field])

        AC_CHECK_MEMBER([struct lov_user_ost_data_v1.l_object_id], [have_obj_id="yes"],  [have_obj_id="no"], [
                    #define LPX64 "%#llx"
                    #include <sys/types.h>
                    #include <asm/types.h>
                    #include <lustre/lustre_user.h>
        ])
        test "x$have_obj_id" = "xyes" && AC_DEFINE(HAVE_OBJ_ID, 1, [lov_user_ost_data_v1 has l_object_id field])

        # Lustre/HSM feature needs Lustre 2.5.0. As some hsm calls were already landed as empty nutshells in 2.4
        # we rely on this new call of 2.5.0: llapi_hsm_state_get_fd().
        AC_CHECK_LIB([lustreapi], llapi_hsm_state_get_fd, [have_lustre_hsm="yes"])
        if test "x$have_lustre_hsm" = "xyes"; then
            AC_DEFINE([_LUSTRE_HSM], [1], [Lustre/HSM feature is present])
            build_lhsm="ON"
        fi

        if test "x$support_shook" = "xyes" ; then
            # test if library is available for shook mode
            AC_CHECK_LIB([shooksvr], [shook_release], [have_shook="yes"])
            # fix with dynamic module management
            if test "x$have_shook" = "xyes"; then
                build_shook="ON"
            fi
            # check shook to lhsm conversion functions
            AC_CHECK_LIB([shooksvr], [shook_lhsmify], [have_shook_lhsmify="yes"])
            test "x$have_shook_lhsmify" = "xyes" && \
                AC_DEFINE([HAVE_SHOOK_LHSMIFY], [1], [shook_lhsmify function available])
        fi
    fi

    AM_CONDITIONAL(CHANGELOGS,  test  "x$have_llapi_changelog_start" = "xyes" )
    AM_CONDITIONAL(USER_LOVEA, [test  "x$have_new_lustre_header" != "xtrue" && test "x$have_llapi_changelog_start" = "xyes"])
    AM_CONDITIONAL(LUSTRE_HSM,  test "x$have_lustre_hsm" = "xyes")
    AM_CONDITIONAL(SHOOK,       test  "x$have_shook" = "xyes")
    # XXX HSM-LITE tagged sections of the code require Lustre 2.x
    AM_CONDITIONAL(HSM_LITE,    test "x$have_fid" = "xyes")
else
    AM_CONDITIONAL(LUSTRE, test 0 = 1 )
    AM_CONDITIONAL(CHANGELOGS, test 0 = 1 )
    AM_CONDITIONAL(USER_LOVEA, test  0 = 1 )
    AM_CONDITIONAL(LUSTRE_HSM,     test 0 = 1)
    AM_CONDITIONAL(SHOOK,       test  0 = 1)
    AM_CONDITIONAL(HSM_LITE,    test 0 = 1)
fi # end of Lustre support

AC_SUBST(DV_CMD)
AC_SUBST(INPUT_OPT)

AC_CHECK_HEADERS([attr/xattr.h],[],[AC_MSG_ERROR([libattr-devel is not installed.])])

AC_CHECK_LIB([pthread], [pthread_getsequence_np], [have_pthread_getsequence_np="yes"],[have_pthread_getsequence_np="no"])
    test "x$have_pthread_getsequence_np" = "xyes" && AC_DEFINE(HAVE_PTHREAD_GETSEQUENCE_NP, 1, [pthread_getsequence_np function exists])

AC_ARG_ENABLE([jemalloc], AS_HELP_STRING([--disable-jemalloc],
              [Use standard memory allocator instead of jemalloc]),
              [use_jemalloc="$enableval"],[use_jemalloc="yes"])
AM_CONDITIONAL(USE_JEMALLOC, test "x$use_jemalloc" == "xyes" )

if test "x$use_jemalloc" = "xyes" ; then
    AC_CHECK_LIB([jemalloc], [malloc_stats_print], LDFLAGS="$LDFLAGS -ljemalloc", \
        AC_MSG_ERROR([jemalloc library not found (needs jemalloc and jemalloc-devel)]))
fi

AC_SUBST(PURPOSE_CFLAGS)
AC_SUBST(PURPOSE_LDFLAGS)

CFLAGS="$CFLAGS -I\$(top_srcdir)/src/include"

# Db ?
#AC_ARG_WITH( [db], AS_HELP_STRING([--with-db=MYSQL|SQLITE (default=MYSQL)],[type of database engine] ),
#             DB="$withval", DB="MYSQL")
# SQLITE support is deprecated
DB="MYSQL"

AM_CONDITIONAL(USE_MYSQL_DB,    test "$DB" = "MYSQL")
AM_CONDITIONAL(USE_SQLITE_DB,   test "$DB" = "SQLITE")

# @TODO check database libraries and path

# Db dependent checks and flags
case $DB in
    MYSQL)

        # check mysql version and mysql_config program
        AX_MYSQL_INFO

        AC_CHECK_HEADERS([mysql/mysql.h])
        AC_CHECK_HEADER([mysql/mysql.h], HAVE_MYSQLCLNT="true",
                        AC_MSG_ERROR([MySQL client header not found (mysql/mysql.h). mysql-devel may not be installed.]))

        DB_CFLAGS="-D_MYSQL `$MYSQL_CONFIG --include`"
        DB_LDFLAGS=`mysql_config --libs_r`

        if test "$MYSQL_VERSION" -lt "5" ; then
            AC_MSG_WARN([MySQL version is too old (<5), optimized accounting won't be supported.])
        else
            DB_CFLAGS="$DB_CFLAGS -D_MYSQL5"
        fi
        ;;

    SQLITE)
        # check lib and headers
        AC_CHECK_HEADER([sqlite3.h], HAVE_SQLITE_HEADER="true",
                    AC_MSG_ERROR([sqlite-devel not installed]))
        AC_CHECK_LIB([sqlite3], [sqlite3_exec], HAVE_SQLITE_LIB="true",
                    AC_MSG_ERROR([sqlite3 library not found]))
        DB_CFLAGS="-D_SQLITE"
        DB_LDFLAGS="-lsqlite3"
        ;;
    *)
        AC_MSG_ERROR([This Database is not supported yet])
        ;;
esac

AC_SUBST(DB_CFLAGS)
AC_SUBST(DB_LDFLAGS)

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([string.h sys/param.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_UID_T
AC_TYPE_SIZE_T

# Check if getmntent_r exists
AC_CHECK_FUNC([getmntent_r],[getmntent_r=yes],[getmntent_r=no])
test "$getmntent_r" = "yes" && AC_DEFINE(HAVE_GETMNTENT_R, 1, [Reentrant version of getmntent exists])
AM_CONDITIONAL(MNTENTCOMPAT, test "$getmntent_r" = "no" )

# Check if fallocate(2) exists.
AC_CHECK_FUNC([fallocate],[fallocate=yes],[fallocate=no])
test "$fallocate" = "yes" && AC_DEFINE(HAVE_FALLOCATE, 1, [File preallocation available])

AS_AC_EXPAND(CONFDIR, $sysconfdir)
if test $prefix = NONE && test "$CONFDIR" = "/usr/etc"  ; then
    CONFDIR="/etc"
fi
AC_MSG_NOTICE([Using config dir $CONFDIR])
AC_DEFINE_UNQUOTED([SYSCONFDIR],"$CONFDIR", [Configuration directory])
AC_SUBST(CONFDIR)

AS_AC_EXPAND(SBINDIR, $sbindir)
AC_SUBST(SBINDIR)

AS_AC_EXPAND(LIBDIR, $libdir)
AC_SUBST(LIBDIR)

# for exporting to Makefile.in
AC_SUBST(ac_configure_args)

AC_OUTPUT

AC_MSG_NOTICE([Summary:])
AC_MSG_NOTICE([Lustre     support is $build_lustre])
AC_MSG_NOTICE([Backup     support is $build_backup])
AC_MSG_NOTICE([Lustre/HSM support is $build_lhsm])
test -d "/ccc" && AC_MSG_NOTICE([Shook      support is $build_shook])

exit 0
