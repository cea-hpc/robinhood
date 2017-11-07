AM_YFLAGS		    = --defines
AM_LFLAGS		    = -i
AM_CFLAGS= -Wall -Werror $(DB_CFLAGS) $(PURPOSE_CFLAGS)

if DEBUG_PARSING
# not supported with automake 1.12
#AM_YFLAGS += --debug -v
AM_CFLAGS += -DYYDEBUG=1
endif

# yacc relative depedencies
conf_lex.l: conf_yacc.h
conf_yacc.h: conf_yacc.c

# cleaning FLEX/BISON generated files
#CLEANFILES = conf_yacc.h conf_yacc.c conf_lex.c

noinst_LTLIBRARIES          = libconfigparsing.la librbhcfg.la

libconfigparsing_la_SOURCES = conf_yacc.y conf_yacc.h conf_lex.l analyze.c config_parsing.c analyze.h \
			                  rbh_cfg_helpers.c rbh_boolexpr.c conf_yacc.c conf_lex.c

librbhcfg_la_SOURCES        = rbh_cfg.c

indent:
	$(top_srcdir)/scripts/indent.sh
