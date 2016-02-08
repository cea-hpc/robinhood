#
# This macro test installed lustre version and package name
#
AC_DEFUN([AX_LUSTRE_VERSION],
[
        AC_MSG_CHECKING(Lustre version)

        # lustre and lustre-client are mutually exclusive
        LPACKAGE=`rpm -qa "lustre(-client)?" --qf "%{Name}\n" 2>/dev/null | tail -1`

        if test -z "$LPACKAGE"; then
            AC_MSG_RESULT(none installed)
        else
            LVERSION=`rpm -q "$LPACKAGE" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`
            AC_MSG_RESULT($LPACKAGE version $LVERSION)
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
