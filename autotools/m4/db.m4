#
# This macro test for MySQL config program and version
#
AC_DEFUN([AX_MYSQL_INFO],
[
        AC_CHECK_PROGS(MYSQL_CONFIG, mysql_config)

        if test -z "$MYSQL_CONFIG"; then
                AC_MSG_ERROR(MySQL must be installed)
        fi

        AC_MSG_CHECKING(for MySQL version)
        MYSQL_VERSION=`$MYSQL_CONFIG --version 2>/dev/null | cut -d "." -f 1`

        if test -z "$MYSQL_VERSION"; then
                MYSQL_VERSION="none"
        fi

        AC_MSG_RESULT($MYSQL_VERSION)
])

