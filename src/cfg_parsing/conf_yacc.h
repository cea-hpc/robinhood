/* A Bison parser, made by GNU Bison 2.7.  */

/* Bison interface for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2012 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_CONF_YACC_H_INCLUDED
# define YY_YY_CONF_YACC_H_INCLUDED
/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     _ERROR_ = 258,
     BEGIN_BLOCK = 259,
     END_BLOCK = 260,
     END_AFFECT = 261,
     BEGIN_SUB_BLOCK = 262,
     END_SUB_BLOCK = 263,
     BEGIN_PARENTHESIS = 264,
     END_PARENTHESIS = 265,
     VALUE_SEPARATOR = 266,
     AFFECT = 267,
     EQUAL = 268,
     DIFF = 269,
     GT = 270,
     GT_EQ = 271,
     LT = 272,
     LT_EQ = 273,
     AND = 274,
     OR = 275,
     NOT = 276,
     UNION = 277,
     INTER = 278,
     IDENTIFIER = 279,
     NON_IDENTIFIER_VALUE = 280,
     ENV_VAR = 281
   };
#endif
/* Tokens.  */
#define _ERROR_ 258
#define BEGIN_BLOCK 259
#define END_BLOCK 260
#define END_AFFECT 261
#define BEGIN_SUB_BLOCK 262
#define END_SUB_BLOCK 263
#define BEGIN_PARENTHESIS 264
#define END_PARENTHESIS 265
#define VALUE_SEPARATOR 266
#define AFFECT 267
#define EQUAL 268
#define DIFF 269
#define GT 270
#define GT_EQ 271
#define LT 272
#define LT_EQ 273
#define AND 274
#define OR 275
#define NOT 276
#define UNION 277
#define INTER 278
#define IDENTIFIER 279
#define NON_IDENTIFIER_VALUE 280
#define ENV_VAR 281



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{
/* Line 2058 of yacc.c  */
#line 52 "conf_yacc.y"

    char         str_val[MAXSTRLEN];
    list_items              *  list;
    generic_item            *  item;
    arg_list_t	            *  arg_list;


/* Line 2058 of yacc.c  */
#line 117 "conf_yacc.h"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;

#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */

#endif /* !YY_YY_CONF_YACC_H_INCLUDED  */
