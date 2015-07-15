# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT( [robinhood], [2.5.5], [robinhood-support@lists.sourceforge.net])
RELEASE="3"

AC_DEFINE_UNQUOTED(RELEASE, "$RELEASE", [release info] )
AC_SUBST(RELEASE)

AC_CONFIG_AUX_DIR([autotools])
AC_CONFIG_MACRO_DIR([autotools/m4])
AC_CONFIG_HEADER([src/include/config.h])
AC_CONFIG_SRCDIR([src/list_mgr/listmgr_init.c])

AM_INIT_AUTOMAKE([-Wall -Werror tar-pax foreign])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT([/usr])

AC_GNU_SOURCE

# disable shared libraries generation
AC_ENABLE_SHARED(no)

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

AC_PROG_LIBTOOL
AC_PROG_CC

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_CC_C_O], [AM_PROG_CC_C_O])

AC_C_INLINE
AM_PROG_LEX
AC_PROG_YACC

# define everything necessary for accessing large files (64bits offset)
AC_SYS_LARGEFILE

# gcc options
#CC_OPT="-Wall -Werror"
CC_OPT="-Wall -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations"
AC_SUBST(CC_OPT)

AC_CHECK_SIZEOF([nlink_t])
AC_CHECK_SIZEOF([off_t])
AC_CHECK_SIZEOF([ino_t])
AC_CHECK_SIZEOF([dev_t])
AC_CHECK_SIZEOF([time_t])
AC_CHECK_SIZEOF([size_t])
AC_CHECK_SIZEOF([pthread_t])

AX_ENABLE_FLAG([strict], [Compile with -Wall -Werror], [-Wall -Werror])

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

# behavior flags
AX_DISABLE_FLAG([atfunc], [Don't use 'at' functions for scanning], [-D_NO_AT_FUNC])

AX_ENABLE_FLAG( [fid2path-leading-slash], [must be enabled if fid2path() returns a leading slash], [-D_FID2PATH_LEADING_SLASH] )

# program to be used for mailing
AX_PROG_MAIL

AC_ARG_ENABLE( [lustre], AS_HELP_STRING([--disable-lustre],
               [Disable all lustre specific features]),
               [support_lustre="$enableval"],[support_lustre="yes"] )

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

        AC_CHECK_HEADER([lustre/lustreapi.h], have_new_lustre_header="true", have_new_lustre_header="false")
        if test "$have_new_lustre_header" = "true"; then
            AC_DEFINE(_LUSTRE_API_HEADER, 1, [New lustreapi header])
            # this comes with Lustre 2.4, so disable features of Lustre < 2.4 at the same time
            AC_DEFINE(_MDT_SPECIFIC_LOVEA, 1, [MDT LOV EA is no longer the same as lov_user_md])
        fi

        AC_CHECK_HEADER([lustre/lustre_idl.h], have_lustre_idl="true", have_lustre_idl="false")
        if test "$have_lustre_idl" = "true"; then
            AC_DEFINE(_LUSTRE_IDL_HEADER, 1, [lustre_idl header exists])
        fi

        # this defines LVERSION and LPACKAGE variables
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
            test "x$have_fid" = "xyes" && AC_DEFINE(_HAVE_FID, 1, [lustre supports fids])
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
            fi

            if test "x$have_llapi_changelog_start" = "xyes" && \
               test "x$have_fid" != "xyes"; then
                AC_MSG_ERROR([Conflicting options: fid must be enabled for enabling MDT changelogs management])
            fi
    	fi

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


    fi

    AM_CONDITIONAL(CHANGELOGS,  test  "x$have_llapi_changelog_start" = "xyes" )
    AM_CONDITIONAL(USER_LOVEA, [test  "x$have_new_lustre_header" != "xtrue" && test "x$have_llapi_changelog_start" = "xyes"])
else
    AM_CONDITIONAL(LUSTRE,     test 0 = 1)
    AM_CONDITIONAL(CHANGELOGS, test 0 = 1) 
    AM_CONDITIONAL(USER_LOVEA, test  0 = 1)
fi # end of Lustre support

AC_CHECK_HEADERS([attr/xattr.h],[],[AC_MSG_ERROR([libattr-devel is not installed.])])


AC_CHECK_LIB([pthread], [pthread_getsequence_np], [have_pthread_getsequence_np="yes"],[have_pthread_getsequence_np="no"])
    test "x$have_pthread_getsequence_np" = "xyes" && AC_DEFINE(HAVE_PTHREAD_GETSEQUENCE_NP, 1, [pthread_getsequence_np function exists])

# Purpose ?

AC_ARG_WITH( [purpose], AS_HELP_STRING([--with-purpose=TMPFS|LUSTRE_HSM|HSM_LITE|BACKUP|SHOOK],[specify the purpose of the daemon] ),
             PURPOSE="$withval", PURPOSE="TMP_FS_MGR")

AM_CONDITIONAL(LUSTRE_HSM,     test "$PURPOSE" = "LUSTRE_HSM")
AM_CONDITIONAL(TMP_FS_MGR,     test "$PURPOSE" = "TMP_FS_MGR" -o "$PURPOSE" = "TMPFS" )
AM_CONDITIONAL(HSM_LITE, test "$PURPOSE" = "HSM_LITE" -o  "$PURPOSE" = "SHOOK" -o  "$PURPOSE" = "BACKUP" )

# purpose dependent flags
case $PURPOSE in
        LUSTRE_HSM)
        # check pre-requisites
        if test "x$have_liblustre_api" != "xyes" ; then
            AC_MSG_ERROR([liblustreapi of lustre 2.x must be installed for compiling Lustre-HSM policy engine])
        fi
        if test "x$have_fid" != "xyes" ; then
            AC_MSG_ERROR([type lustre_fid is missing (needed for Lustre-HSM purpose)])
        fi

        PURPOSE_CFLAGS="-D_LUSTRE_HSM"
        PURPOSE_BIN="rbh-lhsm"
        PURPOSE_EXT="lhsm"
        PURPOSE_SVC="robinhood-lhsm"
        AC_DEFINE([HAVE_MIGR_POLICY], [1], [Migration policy for Lustre-HSM purpose])
        AC_DEFINE([HAVE_RM_POLICY], [1], [HSM removal policy for Lustre-HSM purpose])
        AC_DEFINE([HAVE_PURGE_POLICY], [1], [Purge policy for Lustre-HSM purpose])
        ;;
    TMP_FS_MGR | TMPFS)
        PURPOSE_CFLAGS="-D_TMP_FS_MGR"
        PURPOSE_BIN="robinhood"
        PURPOSE_EXT="tmpfs"
        PURPOSE_SVC="robinhood"
        AC_DEFINE([HAVE_RMDIR_POLICY], [1], [Dir removal policy for TMP_FS_MGR purpose])
        AC_DEFINE([HAVE_PURGE_POLICY], [1], [Purge policy for TMP_FS_MGR purpose])
        ;;
    
    BACKUP | HSM_LITE | SHOOK)
        # check pre-requisites
        if test "x$have_liblustre_api" != "xyes" ; then
            AC_MSG_ERROR([For now, HSM-lite purpose is only supported on Lustre v2 filesystem])
        fi
        if test "x$have_fid" != "xyes" ; then
            AC_MSG_ERROR([For now, HSM-lite purpose is only supported on Lustre v2 filesystem])
        fi

        PURPOSE_CFLAGS="-D_HSM_LITE"

        AC_DEFINE([HAVE_MIGR_POLICY], [1], [Migration policy for HSM-lite purpose])
        AC_DEFINE([HAVE_RM_POLICY], [1], [Backend removal policy for HSM-lite purpose])

        if test "$PURPOSE" = "SHOOK" -o "$PURPOSE" = "HSM_LITE"; then
            # test if library is available for shook mode
            AC_CHECK_LIB([shooksvr], [shook_release], [have_shook="yes"])

            if test "x$have_shook" != "xyes" ; then
                if test "$PURPOSE" != "HSM_LITE"; then
                    AC_MSG_ERROR([libshooksvr not found, cannot build robinhood using shook])
                fi
                # HSM_LITE mode without shook: OK (just display a warning)
                AC_MSG_WARN([libshooksvr not found, switching to backup mode only])

                PURPOSE_BIN="rbh-backup"
                PURPOSE_EXT="backup"
                PURPOSE_SVC="robinhood-backup"
            else
                # Shook mode with Shook library: OK
                AC_MSG_NOTICE([libshooksvr is installed, full HSM-lite support is enabled]) 
                PURPOSE_LDFLAGS="-lshooksvr"
                AC_DEFINE([HAVE_PURGE_POLICY], [1], [Purge policy for HSM-lite purpose])
                AC_DEFINE([HAVE_SHOOK], [1], [libshook based HSM binding])

                PURPOSE_BIN="rbh-shook"
                PURPOSE_EXT="shook"
                PURPOSE_SVC="robinhood-shook"
            fi
        else
            # backup mode
            AC_MSG_NOTICE([Compiling robinhood with backup mode])
            PURPOSE_BIN="rbh-backup"
            PURPOSE_EXT="backup"
            PURPOSE_SVC="robinhood-backup"
        fi
        ;;

    *)
        AC_MSG_ERROR([This purpose is not supported yet])
        ;;
esac

AM_CONDITIONAL(SHOOK, test  "$PURPOSE_EXT" = "shook")
AM_CONDITIONAL(BACKUP, test  "$PURPOSE_EXT" = "backup")

AC_SUBST(PURPOSE_CFLAGS)
AC_SUBST(PURPOSE_LDFLAGS)
AC_SUBST(PURPOSE_BIN)
AC_SUBST(PURPOSE_EXT)
AC_SUBST(PURPOSE_SVC)
AC_DEFINE_UNQUOTED([PURPOSE_EXT],"$PURPOSE_EXT", [Directory extension for the given usage])


CFLAGS="$CFLAGS -I\$(top_srcdir)/src/include"

# Db ?

AC_ARG_WITH( [db], AS_HELP_STRING([--with-db=MYSQL|SQLITE (default=MYSQL)],[type of database engine] ),
             DB="$withval", DB="MYSQL")

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

# for exporting to spec file
AC_SUBST(ac_configure_args)

AC_CONFIG_FILES([Makefile
         src/Makefile
         src/include/Makefile
         src/logs/Makefile
         src/common/Makefile
         src/cfg_parsing/Makefile
         src/list_mgr/Makefile
         src/entry_processor/Makefile
         src/fs_scan/Makefile
         src/chglog_reader/Makefile
         src/policy_matching/Makefile
         src/policy_modules/Makefile
         src/policy_modules/purge/Makefile
         src/policy_modules/migration/Makefile
         src/policy_modules/hsm_remove/Makefile
         src/policy_modules/rmdir/Makefile
         src/backend_ext/Makefile
         src/robinhood/Makefile
         src/tools/Makefile
         src/tests/Makefile
         scripts/Makefile
         scripts/robinhood.init
         scripts/robinhood.init.sles
         scripts/sysconfig_robinhood
         tests/Makefile
         web_gui/Makefile
         man/Makefile
         robinhood.spec
         doc/doxygen/doxygen.cfg
         scripts/make_mans.sh])
AC_OUTPUT
