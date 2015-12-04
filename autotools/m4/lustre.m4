#
# This macro test installed lustre version and package name
#
AC_DEFUN([AX_LUSTRE_VERSION],
[
        AC_MSG_CHECKING(Lustre version)
        # special m4 sequences to get square brackets in output:
        # @<:@ => [
        # @:>@ => ]
        LVERSION=`rpm -q "lustre" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`
        LPACKAGE=`rpm -q "lustre" --qf "%{Name}\n"  2>/dev/null | tail -1`

        if test -z "$LVERSION"; then
            AC_MSG_RESULT(none installed)
        else
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
    	    unset LPACKAGE
 
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
        unset LPACKAGE

        if test -z "$LVERSION"; then
            AC_MSG_RESULT(none installed)
        else
            AC_MSG_RESULT(exported src version $LVERSION)
        fi

])

