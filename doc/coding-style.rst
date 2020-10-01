.. This file is part of the RobinHood Project
   Copyright (C) 2019 Commissariat a l'energie atomique et aux energies
                      alternatives

   SPDX-License-Identifer: LGPL-3.0-or-later

############
Coding style
############

*Code contains bugs. Bugs need fixing. Fixing bugs is hard.*

Since fixing bugs involves reading code, let's try and make this as easy as
possible.

-----

This document offers guidelines on how to write code for the RobinHood_ project.
It articulates around 4 majors points, in order of importance:

#. A consistent coding style;
#. A human friendly `code layout`_;
#. Smart naming_ conventions;
#. Comments.

.. _RobinHood: https://github.com/cea-hpc/robinhood

Enforcement of those rules in the robinhood project is still shy and mainly
works through careful programming and reviews. Tools to automate this process
are expected to be produced in the distant future. [#]_

Some rules described here may seem a bit "over-the-top". Everything is up for
debate, as long as the challenging party takes the time to document their claim,
bring new elements to the discussion, are willing to (at least) update this
document and (ideally) contribute scripts/patches to bring the current code
base to compliance.

This document borrows parts and ideas from the `Linux kernel coding style`_ and
Python's `PEP 8`_.

.. _Linux kernel coding style: https://www.kernel.org/doc/html/latest/process/coding-style.html
.. _PEP 8: https://www.python.org/dev/peps/pep-0008

.. [#] clang-format comes close but fails to provide all the necessary
       configuration switches robinhood would need.

Code layout
===========

Indentation
-----------

Use 4 spaces per indentation level.

Nesting more than 3 levels of indentation is a red flag nobody should dismiss.

Vertical alignment
~~~~~~~~~~~~~~~~~~

Align vertically code that is mostly going to be read vertically.

In my experience, this applies particularly well to static array declarations:

.. code:: C

    static const char * const messages[] = {
        [FIRST]     = "this is the first message",
        [SECOND]    = "this is the second message",
        [THIRD]     = "this is the third message",
    };

Where the expectation is that you only ever read this kind of array looking to
know what ``messages[FIRST]`` evaluates to; or which index yields the message
``this is the first message``. In the latter case, having the messages
vertically aligned allows easily scanning for the ``first`` part.

Preprocessor
~~~~~~~~~~~~

Add one space between the leading ``#`` and the first non-blank character for
every conditional the code appears under:

.. code:: C

    #no space outside of conditionals

    #ifdef HAVE_CONFIG_H /* One space */
    # include "config.h"
    #endif

    #ifndef HAVE_SOME_FUNCTION /* One space */
    # ifndef HAVE_SOME_OTHER_FUNCTION /* Two spaces */
    #  error "need at least of SOME_FUNCTION or SOME_OTHER_FUNCTION to compile"
    # endif /* One space */
    #endif

This gives a visual indication of which conditional applies to which code.
For long, nested conditionals, comments after ``#else`` and ``#endif`` are
welcome.

For multiline macros, although it might be tempting to right align
line-terminating ``\`` characters, refrain from doing so as it leads to more
(meaningless) changes when the macro is edited.

Avoid multiline macros in nested conditionals as this breaks alignment and
readability.

Maximum line length
-------------------

Lines should not exceed 80 characters (not including the newline character).

This limit is relaxed for user-visible strings which you should only break at
conversion specifiers (``%s``, ``%d``, ``%zu``, ...).

How to break lines?
~~~~~~~~~~~~~~~~~~~

Minimize the total number of lines:

.. code:: C

    /* This */
    long_function_name(long_argument_1, long_argument_2,
                       long_argument_3, long_argument_4);

    /* Is better than */
    long_function_name(long_argument_1,
                       long_argument_2,
                       long_argument_3,
                       long_argument_4);

Break at the highest level of grouping possible:

.. code:: C

    /* This */
    function_A(function_B(argument_1, argument_2),
               function_C(argument_3, argument_4));

    /* Is better than */
    function_A(function_B(argument_1, argument_2), function_C(argument_3,
                                                              argument_4));

Several styles of continuation lines are acceptable:

Break lines before binary operators rather than after. [#]_

.. code:: C

    /* Use an extra level of indentation */
    if (long_conditional_1
            && long_conditional_2)

    /* Or align vertically (mostly for mathematical formulas) */
    if (long_value_A + long_value_B + long_value_C
      + long_value_D + long_value_E + long_value_F)

    /* Or break right after an opening parens; add an extra level of
     * indentation; and leave the matching closing parens on a line of its own
     * in this case.
     */
    extremely_long_function_name_to_the_point_it_does_not_make_sense(
        argument_1, argument_2, argument_3, argument_4
        );

.. [#] https://www.python.org/dev/peps/pep-0008/#should-a-line-break-before-or-after-a-binary-operator

Spaces
------

Put spaces around binary operators, after commas, and after keywords except for:
``sizeof``, ``typeof``, ``__alignof__``, and ``__attribute__`` (because these
are used much like functions).

Refer to `this section`__ of the Linux kernel coding style for more details on
spacing in RobinHood.

__ https://www.kernel.org/doc/html/latest/process/coding-style.html#spaces

Blanks
------

Any variable/struct/function definition should be surrounded with blank lines.

Put a blank line after declaring your variables.

Never use two (or more) blank lines in a row. [#]_

.. [#] If you really wish to separate two sections of code, use two files, or
       consider `comment banners`_.

Braces
------

Always put the opening brace last on the line, and the closing brace first:

.. code:: C

    if (condition) {
        statement_1;
        statement_2;
    }

Except for functions, for which you should put the opening brace at the
beginning of the next line:

.. code:: C

    int
    function(int x)
    {
        ...
    }

Braces are optional where a single statement will do:

.. code:: C

    if (condition)
        return error;

If one conditional branch requires braces, they all do:

.. code:: C

    if (condition) {
        statement_1;
        statement_2;
    } else {
        statement_3;
    }

Also, use braces when a loop contains more than a single simple statement:

.. code:: C

    while (true) {
        if (condition)
            break;
    }

Function declaration
--------------------

When declaring (or defining) a function, place its return type on a line of its
own:

.. code:: C

    int
    function(int x, int y);

This allows to easily grep for declarations (and definitions) with ``grep``:

.. code:: shell

    grep '^function('

Variable declaration
--------------------

Always declare your variables at the top of a block. This might seem like "old
style programming" but there is little reason to have your variable declarations
closer to where the variables are used when your function definitions fit inside
a single screen.

Declaring more than 10 variables in a single function or a single block is
prohibited. Consider splitting your function or encapsulating some variables
in a ``struct``.

The only exception to this rule are ``for`` loops, for which you are encouraged
to declare loop-local variables in the initializer part of the ``for``
construct:

.. code:: C

    for (size_t i = 0; i < ; i++)
        something(i);

This allows hiding meaningless loop variables and restricting their scope to
a minimum.

Naming
======

Naming is notoriously hard:

    *There are only two hard things in Computer Science: cache invalidation and
    naming things. --Phil Karlton* [#]_

My personnal opinion is that a good name is **concise** and also **easy** to
**unambiguously** interpret.

While helpful to compare different options, this does nothing to actually *find*
options. AFAICT, conjuring inspiration is a hard problem, and we will not try to
solve it in this document.

The rest of this section details naming conventions that are meant to ease
navigating and refactoring code.

.. [#] https://skeptics.stackexchange.com/a/39178

Case
----

Use UPPER_CASE_WITH_UNDERSCORES for macros and variables defining static
constants, and for labels in enums.

.. code:: C

    enum my_enum {
        ME_FIRST,
        ME_SECOND,
        ME_THIRD,
    };

    static const char STRING[] = "example";
    static const size_t STRING_LEN = sizeof(STRING);

Use lower_case_with_underscores for everything else.

.. code:: C

    static char global_buffer[1 << 12];

    struct my_struct {
        int field;
    };

    void
    my_func(const char *string)
    {
        const size_t length = strlen(string);

        ...;
    }

In the previous example, ``length`` is not considered a "static constant" in
the sense that its value depends on the content of ``string``.

Prefixes
--------

Any public interface should bear the prefix ``rbh_``:

.. code:: C

    int
    rbh_public_function(...);

    struct rbh_public_structure;

    const int RBH_PUBLIC_CONSTANT;

Including enum values which should also bear a 2 to 3 letters prefix + an
underscore (``XY[Z]_``) indicative of their type:

.. code:: C

    enum rbh_public_enum {
        RBH_PE_ONE, /* PE_ for Public Enum */
        RBH_PE_TWO,
        RBH_PE_THREE,
    };

Feel free to omit prefixes for anything that is private.

Typedefs
--------

Only use typedefs to:

- actively hide what an object is;
- create new types for type-checking with sparse. [#]_

.. [#] https://www.kernel.org/doc/html/latest/dev-tools/sparse.html

Comments
========

.. TODO look for good heuristics as to when to write a comment and when not to

As much as possible, write code that does not need comments... And then add
some.

Before writing a comment, ponder whether the code actually needs commenting, or
refactoring.

Wherever you feel a bit of code is not trivial, annotate.

Wherever you know something is broken / left to do / questionable, leave a
``FIXME`` / ``TODO`` / ``XXX`` comment. [#]_

Be creative with your comments: use ASCII-art.

.. [#] They are special keywords to vim and will be highlighted as such.

Comment banners
---------------

Use comment banners to separate sections of code in a given file. Banners should
be 80 characters wide with a title at their center: [#]_

.. code:: C

    /*----------------------------------------------------------------------------*
     |                                    TITLE                                   |
     *----------------------------------------------------------------------------*/

Any public function should appear under its own banner. And as much as possible,
the code written specifically for this public function should also be placed
under that same banner.

.. [#] To vim users: the ``:center`` command helped me a lot (``:help :center``)
