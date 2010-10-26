# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT( [robinhood], [2.2.1], [robinhood-support@lists.sourceforge.net])
AC_SUBST( [RELEASE], [1] )

AC_CONFIG_AUX_DIR([autotools])
AC_CONFIG_MACRO_DIR([autotools/m4])
AC_CONFIG_HEADER([src/include/config.h])
AC_CONFIG_SRCDIR([src/list_mgr/listmgr_init.c])

AM_INIT_AUTOMAKE([-Wall -Werror foreign])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES])

AC_PREFIX_DEFAULT([/usr])

AC_GNU_SOURCE

# disable shared libraries generation
AC_ENABLE_SHARED(no)

# Checks for programs.
AC_PROG_CC
AC_C_INLINE
AM_PROG_LEX
AC_PROG_YACC
AC_PROG_LIBTOOL

# define everything necessary for accessing large files (64bits offset)
AC_SYS_LARGEFILE

AC_CHECK_SIZEOF([nlink_t])
AC_CHECK_SIZEOF([off_t])
AC_CHECK_SIZEOF([ino_t])
AC_CHECK_SIZEOF([dev_t])
AC_CHECK_SIZEOF([time_t])
AC_CHECK_SIZEOF([size_t])
AC_CHECK_SIZEOF([pthread_t])

# Flag for Lustre 1.4 with join() support
AX_ENABLE_FLAG([lustre14_bull], [Bull distribution of Lustre 1.4], [-DHAVE_LUSTRE_JOIN_TYPE])

# debug flags
AX_ENABLE_FLAG([debug-db], [enables debug traces for database operations], [-D_DEBUG_DB])
AX_ENABLE_FLAG([debug-parsing], [enables debug traces for configuration file parsing], [-D_DEBUG_PARSING])
AM_CONDITIONAL(DEBUG_PARSING, test "x$enable_debug_parsing" == "xyes" )

AX_ENABLE_FLAG([debug-pipeline], [enables debug traces for entry processor pipeline], [-D_DEBUG_ENTRYPROC])
AX_ENABLE_FLAG([debug-policies], [enables debug traces for entry processor pipeline], [-D_DEBUG_POLICIES])
AX_ENABLE_FLAG([debug-hash], [enables debug traces internal hash tables], [-D_DEBUG_HASH])

# behavior flags
AX_ENABLE_FLAG( [prep-stmts], [enables prepared statements for accessing the database], [-D_ENABLE_PREP_STMT] )
AX_ENABLE_FLAG( [fid2path-leading-slash], [must be enabled if fid2path() returns a leading slash], [-D_FID2PATH_LEADING_SLASH] )



# program to be used for mailing
AX_PROG_MAIL

AC_ARG_ENABLE( [lustre], AS_HELP_STRING([--disable-lustre],
               [Disable all lustre specific features]),
               [support_lustre="$enableval"],[support_lustre="yes"] )

if test "x$support_lustre" = "xyes" ; then

    AC_CHECK_LIB([lustreapi], [llapi_obd_statfs], [have_liblustre_api="yes"])
    AM_CONDITIONAL( LUSTRE, test  "x$have_liblustre_api" = "xyes" )
    AC_SUBST(LUSTRE)

    # lustre relative information and checks
    if test "x$have_liblustre_api" = "xyes" ; then

        AC_DEFINE(_LUSTRE, 1, [liblustreapi is available])

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

        AC_ARG_ENABLE( [changelogs], AS_HELP_STRING([--disable-changelogs],[Don't use ChangeLogs]),
                   use_changelogs=$enableval, use_changelogs='yes' )

    	if test "x$use_changelogs" == "xyes" ; then

    		# test changelog functions

	    	AC_CHECK_LIB([lustreapi],[llapi_changelog_start],[have_llapi_changelog_start="yes"])
		    if test "x$have_llapi_changelog_start" = "xyes"; then 
		        AC_DEFINE(HAVE_CHANGELOGS, 1, [Lustre changelogs records are structures])
                AC_CHECK_DECL([CL_MTIME],AC_DEFINE(CL_SPLITTED_TIME,1,[Changelog time events are splitted]),[],[
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

        AX_ENABLE_FLAG( [llapi-fork-support], [Must be enabled if liblustreapi uses fork()], [-D_LLAPI_FORKS] )
        AX_ENABLE_FLAG( [mds-stat], [ioctl() to MDC instead of POSIX stat()], [-D_MDS_STAT_SUPPORT] )

        # pool functions
        AC_CHECK_LIB([lustreapi], [llapi_get_poollist], [have_llapi_poollist="yes"])
        AC_CHECK_LIB([lustreapi], [llapi_get_poolmembers], [have_llapi_poolmembers="yes"])
        test "x$have_llapi_poollist" = "xyes" && test "x$have_llapi_poolmembers" = "xyes"  && \
            AC_DEFINE(HAVE_LLAPI_GETPOOL_INFO, 1, [llapi_getpool functions are available])


    fi

    AM_CONDITIONAL(CHANGELOGS,  test  "x$have_llapi_changelog_start" = "xyes" )

else
    AM_CONDITIONAL(LUSTRE, test 0 = 1 )
    AM_CONDITIONAL(CHANGELOGS, test 0 = 1 ) 

fi # end of Lustre support

AC_CHECK_HEADERS([attr/xattr.h],[],[AC_MSG_WARN([libattr-devel is not installed: the program will be compiled without xattr support.])])


AC_CHECK_LIB([pthread], [pthread_getsequence_np], [have_pthread_getsequence_np="yes"],[have_pthread_getsequence_np="no"])
    test "x$have_pthread_getsequence_np" = "xyes" && AC_DEFINE(HAVE_PTHREAD_GETSEQUENCE_NP, 1, [pthread_getsequence_np function exists])

# Purpose ?

AC_ARG_WITH( [purpose], AS_HELP_STRING([--with-purpose=TMP_FS_MGR|LUSTRE_HSM|BACKUP|SHERPA],[specify the purpose of the daemon] ),
             PURPOSE="$withval", PURPOSE="TMP_FS_MGR")

AM_CONDITIONAL(LUSTRE_HSM,     test "$PURPOSE" = "LUSTRE_HSM")
AM_CONDITIONAL(TMP_FS_MGR,     test "$PURPOSE" = "TMP_FS_MGR")
AM_CONDITIONAL(BACKUP_FS, test "$PURPOSE" = "BACKUP")
AM_CONDITIONAL(SHERPA, test "$PURPOSE" = "SHERPA")

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
        PURPOSE_BIN="rbh-hsm"
        PURPOSE_EXT="hsm"
        PURPOSE_SVC="robinhood-hsm"
        AC_DEFINE([HAVE_MIGR_POLICY], [1], [Migration policy for Lustre-HSM purpose])
        AC_DEFINE([HAVE_RM_POLICY], [1], [HSM removal policy for Lustre-HSM purpose])
        ;;
    TMP_FS_MGR)
        PURPOSE_CFLAGS="-D_TMP_FS_MGR"
        PURPOSE_BIN="robinhood"
        PURPOSE_EXT="tmpfs"
        PURPOSE_SVC="robinhood"
        AC_DEFINE([HAVE_RMDIR_POLICY], [1], [Dir removal policy for TMP_FS_MGR purpose])
        ;;
    SHERPA)
        PURPOSE_CFLAGS="-D_SHERPA"
        PURPOSE_LDFLAGS="-lSherpaCacheP -lsherpa_cfg_parsing -lSherpaCache"
        PURPOSE_BIN="rbh-sherpa"
        PURPOSE_EXT="sherpa"
        PURPOSE_SVC="robinhood-sherpa"

        AC_DEFINE([HAVE_MIGR_POLICY], [1], [Migration policy for SHERPA purpose])
        AC_DEFINE([HAVE_RMDIR_POLICY], [1], [Dir removal policy for SHERPA purpose])
        ;;

    BACKUP)
        PURPOSE_CFLAGS="-D_BACKUP_FS"
        PURPOSE_BIN="rbh-backup"
        PURPOSE_EXT="backup"
        PURPOSE_SVC="robinhood-backup"

        AC_DEFINE([HAVE_MIGR_POLICY], [1], [Migration policy for backup purpose])
        ;;

    *)
        AC_MSG_ERROR([This purpose is not supported yet])
        ;;
esac

AC_SUBST(PURPOSE_CFLAGS)
AC_SUBST(PURPOSE_LDFLAGS)
AC_SUBST(PURPOSE_BIN)
AC_SUBST(PURPOSE_EXT)
AC_SUBST(PURPOSE_SVC)
AC_DEFINE_UNQUOTED([PURPOSE_EXT],"$PURPOSE_EXT", [Directory extension for the given usage])


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
                        AC_MSG_ERROR([MySQL client package not installed]) )

        DB_CFLAGS="-D_MYSQL `$MYSQL_CONFIG --include`"
        DB_LDFLAGS=`mysql_config --libs_r`

        if test "$MYSQL_VERSION" = "5" ; then
            DB_CFLAGS="$DB_CFLAGS -D_MYSQL5"
        fi
        ;;

    SQLITE)
        # check lib and headers
        AC_CHECK_HEADER([sqlite3.h], HAVE_SQLITE_HEADER="true", 
                    AC_MSG_ERROR([sqlite-devel not installed]))
        AC_CHECK_LIB([sqlite3], [sqlite3_exec], HAVE_SQLITE_LIB="true", 
                    AC_MSG_ERROR([sqlite3 library not found]))
        DB_CFLAGS="-D_SQLITE -D_DISABLE_PREP_STMT"
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
         src/robinhood/Makefile
         src/tests/Makefile
         scripts/Makefile
         scripts/robinhood.init
         scripts/robinhood.init.sles
         scripts/sysconfig_robinhood
         tests/Makefile
         robinhood.spec
         doc/doxygen/doxygen.cfg])
AC_OUTPUT
