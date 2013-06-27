AM_YFLAGS		    = --defines
AM_LFLAGS		    = -i
# no -Werror because yacc's auto-generated code is not clean
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS) -DYY_NO_UNPUT

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

noinst_LTLIBRARIES          = libconfigparsing.la
libconfigparsing_la_SOURCES = conf_yacc.y conf_yacc.h conf_lex.l analyze.c config_parsing.c analyze.h \
			      RobinhoodConfig.c conf_yacc.c conf_lex.c

check_PROGRAMS		    = verif_syntax test_parsing

verif_syntax_SOURCES	    = verif_syntax.c
verif_syntax_LDADD         = libconfigparsing.la

test_parsing_SOURCES	    = test_parse.c
test_parsing_LDADD         =  libconfigparsing.la

indent:
	$(top_srcdir)/scripts/indent.sh
