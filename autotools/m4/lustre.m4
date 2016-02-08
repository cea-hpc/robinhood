#
# This macro test installed lustre version and package name
#
AC_DEFUN([AX_LUSTRE_VERSION],
[
        AC_MSG_CHECKING(Lustre version)

	# use "sort" to ensure 'lustre-client' is returned after 'lustre',
	# so it is selected by "tail -1"
        LPACKAGE=`rpm -qa "lustre(-client)?" --qf "%{Name}\n"  2>/dev/null | sort | tail -1`
        LVERSION=`rpm -qa "lustre(-client)?" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`

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

