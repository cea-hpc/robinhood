
#
# This macro is for features that are disabled by default
# and we want a CFLAG to be set if it is explicitely enabled
# on "configure" command line (with --enable-...)
#
# AX_ENABLE_FLAG( FEATURE_NAME, HELP_STRING, CFLAGS_IF_ENABLED )
#
# Example:
# AX_ENABLE_FLAG( [debug-memalloc], [enable debug traces for memory allocator], [-D_DEBUG_MEMALLOC] )
#
AC_DEFUN([AX_ENABLE_FLAG],
[

	AC_MSG_CHECKING($1 option)
	AC_ARG_ENABLE( [$1], AS_HELP_STRING([--enable-$1],[$2]),
		       [enable_]m4_bpatsubst([$1], -, _)=$enableval, [enable_]m4_bpatsubst([$1], -, _)='no' )

	if test "[$enable_]m4_bpatsubst([$1], -, _)" == yes ; then
		CFLAGS="$CFLAGS $3"
	    AC_MSG_RESULT(enabled)
    else
	    AC_MSG_RESULT(disabled)
	fi
])

#
# This macro is for features that are disabled by default
# and we want a CFLAG to be set if it is explicitely enabled
# on "configure" command line (with --enable-...)
#
# AX_ENABLE_FLAG_COND( FEATURE_NAME, HELP_STRING, CFLAGS_IF_ENABLED, COND )
#
# Example:
# AX_ENABLE_FLAG_COND( [debug-memalloc], [enable debug traces for memory allocator], [-D_DEBUG_MEMALLOC], DEBUG_MEMALLOC )
#
AC_DEFUN([AX_ENABLE_FLAG_COND],
[

	AC_MSG_CHECKING($1 option)
	AC_ARG_ENABLE( [$1], AS_HELP_STRING([--enable-$1],[$2]),
		       [enable_]m4_bpatsubst([$1], -, _)=$enableval, [enable_]m4_bpatsubst([$1], -, _)='no' )

	AM_CONDITIONAL( $4, test "[$enable_]m4_bpatsubst([$1], -, _)" == "yes" )

	if test "[$enable_]m4_bpatsubst([$1], -, _)" == yes ; then
		CFLAGS="$CFLAGS $3"
	    AC_MSG_RESULT(enabled)
        else
	    AC_MSG_RESULT(disabled)
	fi
])



#
# This macro is for features that are enabled by default
# and we want a CFLAG to be set if it is explicitely disabled
# on "configure" command line (with --disable-...)
#
# AX_DISABLE_FLAG( FEATURE_NAME, HELP_STRING, CFLAGS_IF_DISABLED )
#
# Example:
# AX_DISABLE_FLAG( [tcp-register], [disable registration of tcp services on portmapper], [-D_NO_TCP_REGISTER] )
#
AC_DEFUN([AX_DISABLE_FLAG],
[
	AC_MSG_CHECKING($1 option)
	AC_ARG_ENABLE( [$1], AS_HELP_STRING([--disable-$1],[$2]),
		       [enable_]m4_bpatsubst([$1], -, _)=$enableval, [enable_]m4_bpatsubst([$1], -, _)='yes' )

	if test "[$enable_]m4_bpatsubst([$1], -, _)" != yes ; then
		CFLAGS="$CFLAGS $3"
	    AC_MSG_RESULT(disabled)
    else
	    AC_MSG_RESULT(enabled)
	fi
])
