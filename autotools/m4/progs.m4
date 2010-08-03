
AC_DEFUN([AX_PROG_MAIL],
[

	AC_MSG_CHECKING(for mail program)
	if test -x "/usr/bin/Mail"; then
        	MAIL=/usr/bin/Mail
	elif  test -x "/usr/bin/mailx"; then
	        MAIL=/usr/bin/mailx
	elif  test -x "/usr/sbin/mailx"; then
	        MAIL=/usr/sbin/mailx
	else
        	AC_MSG_ERROR([cannot find mailing program (Mail or mailx)])
	fi
	AC_MSG_RESULT($MAIL)
	c_mail_define=\"$MAIL\"
	AC_DEFINE_UNQUOTED([MAIL], $c_mail_define, [Mailing program])
])


AC_DEFUN([AX_PROG_RPCGEN],
[
	# looking for rpcgen program
	AC_CHECK_PROGS(RPCGEN, rpcgen.new rpcgen, rpcgen)
])
