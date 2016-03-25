#
# This macro test installed lustre version and package name
#
AC_DEFUN([AX_LUSTRE_VERSION],
[
        # Check if any package provides 'lustre-client'
        # Since lustre 2.8, this symbol is provided by both 'lustre' and 'lustre-client'
        # so robinhood only needs to require it.
        AC_MSG_CHECKING(if any package provides lustre-client)

        if rpm -q --whatprovides lustre-client >/dev/null 2>/dev/null; then
            AC_MSG_RESULT(yes)
            LPACKAGE=lustre-client
            # Assume we want the same version as this package,
            # whatever 'lustre' or 'lustre-client'
            AC_MSG_CHECKING(Lustre version)
            LVERSION=`rpm -q --whatprovides lustre-client --qf "%{Version}\n" 2>/dev/null | grep -v "no package" | cut -d "." -f 1-2`
            AC_MSG_RESULT($LVERSION)
        else
            AC_MSG_RESULT(no)
            AC_MSG_CHECKING(if lustre is installed)

            # fallback to lustre package
            LPACKAGE=`rpm -q --whatprovides lustre --qf "%{Name}\n" 2>/dev/null | grep -v "no package"`
            if test -n "$LPACKAGE"; then
                LVERSION=`rpm -q $LPACKAGE --qf "%{Version}\n" 2>/dev/null | cut -d "." -f 1-2`
                AC_MSG_RESULT(found version $LVERSION)
            else
                AC_MSG_RESULT(no)
            fi
        fi
])

# Get lustre version from sources
# AX_LUSTRE_SRC_VERSION(LUSTRE_SRC_DIR)
AC_DEFUN([AX_LUSTRE_SRC_VERSION],
[
        AC_MSG_CHECKING(Lustre source version)

        if test -f $1/config.h ; then
            LVERSION=`grep "define VERSION " $1/config.h | awk '{print $(NF)}' | sed -e 's/"//g' | cut -d "." -f 1-2`
            # default RPM dependancy to lustre-client
            LPACKAGE="lustre-client"
 
            if test -z "$LVERSION"; then
                AC_MSG_RESULT(none installed)
            else
                AC_MSG_RESULT(source version $LVERSION)
            fi
        else
           AX_LUSTRE_EXPORT_VERSION([$1])
        fi
])

# Get lustre version from exported src directory
# AX_LUSTRE_EXPORT_VERSION(LUSTRE_SRC_DIR)
AC_DEFUN([AX_LUSTRE_EXPORT_VERSION],
[
        AC_MSG_CHECKING(Lustre exported src version)

        LVERSION=$([awk -F',' '/m._define\(\[LUSTRE_MINOR\]/ { minver=gensub( /[\[\]\)]/, "", "g", $(NF)) ; } /m._define\(\[LUSTRE_MAJOR\]/ { majver=gensub( /[\[\]\)]/, "", "g", $(NF)) ; } END { print majver "." minver ; }' $$1/usr/src/lustre-*/lustre/autoconf/lustre-version.ac])
        # default RPM dependancy to lustre-client
        LPACKAGE="lustre-client"

        if test -z "$LVERSION"; then
            AC_MSG_RESULT(none installed)
        else
            AC_MSG_RESULT(exported src version $LVERSION)
        fi

])

# -*- mode: shell; sh-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:
