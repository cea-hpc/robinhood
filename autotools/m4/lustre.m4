#
# This macro test installed lustre version and package name
#
AC_DEFUN([AX_LUSTRE_VERSION],
[
        AC_MSG_CHECKING(Lustre version)
        # special m4 sequences to get square brackets in output:
        # @<:@ => [
        # @:>@ => ]
        LVERSION=`rpm -qa "lustre@<:@-_@:>@*modules*" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`
        LPACKAGE=`rpm -qa "lustre@<:@-_@:>@*modules*" --qf "%{Name}\n"  2>/dev/null | tail -1`

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

	LVERSION=`grep "define VERSION " $1/config.h | awk '{print $(NF)}' | sed -e 's/"//g' | cut -d "." -f 1-2`
	unset LPACKAGE

        if test -z "$LVERSION"; then
            AC_MSG_RESULT(none installed)
        else
            AC_MSG_RESULT(source version $LVERSION)
        fi

])
