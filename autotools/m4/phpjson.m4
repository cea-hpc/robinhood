#
# This macro test for php-json (used by webgui)
#
AC_DEFUN([AX_PHPJSON_CHECK],
[
	chkfile="/usr/lib64/php/modules/json.so"
	AC_CHECK_FILE([$chkfile],
	[
	], [
		AC_MSG_WARN(***********************************************)
		AC_MSG_WARN(php-json not found under $chkfile)
		AC_MSG_WARN(webgui will build however it may fail to render)
		AC_MSG_WARN(graphs yum install php-json?)
		AC_MSG_WARN(***********************************************)
	])
])
