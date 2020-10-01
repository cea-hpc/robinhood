.. This file is part of the RobinHood Project
   Copyright (C) 2020 Commissariat a l'energie atomique et aux energies
                      alternatives

   SPDX-License-Identifer: LGPL-3.0-or-later

############
Contributing
############

This document describes how to contribute to RobinHood and details a few
rules the project implements.

Overview
========

Issues_ should be submitted on GitHub__. Questions can be sent to one of
RobinHood's mailing lists:

- robinhood-support_ for questions about how to use RobinHood;
- robinhood-devel_ for questions about how to develop RobinHood.

Subscribe_ to either mailing lists to answer others' questions.

A third, low-traffic, moderated mailing list provides news and annoucements
about the project: robinhood-news_.

Reviews_ happen on GerritHub_, and so patches_ must be submitted there too.

.. __: https://github.com/cea-hpc/robinhood/issues
.. _subscribe: https://sourceforge.net/p/robinhood/mailman/
.. _robinhood-support: mailto:robinhood-support@lists.sourceforge.net
.. _robinhood-devel: mailto:robinhood-devel@lists.sourceforge.net
.. _robinhood-news: https://sourceforge.net/projects/robinhood/lists/robinhood-news
.. _gerrithub: https://review.gerrithub.io/cea-hpc/robinhood

Issues
======

There are no particular guidelines to submit good issues, at least for now. This
is meant to evolve as the project matures.

Patches
=======

To submit a patch, follow `Gerrit's documentation on the subject`__.
Alternatively, run the commands below:

.. code:: shell

    cd robinhood/
    git remote add gerrithub https://review.gerrithub.io/cea-hpc/robinhood
    git config remote.gerrithub.pushurl https://<login>@review.gerrithub.io/a/cea-hpc/robinhood
    git config remote.gerrithub.push HEAD:refs/for/main

Also note that you may need to log in at least once on GerritHub_ before you can
push changes.

.. __: https://gerrit-review.googlesource.com/Documentation/intro-user.html#upload-change

Coding style
------------

Roughly speaking, RobinHood's coding style is a mix of the `Linux kernel coding
style`_ and Python's `PEP 8`_. Refer to the `RobinHood coding style`_ for more
information.

.. _Linux kernel coding style: https://www.kernel.org/doc/html/latest/process/coding-style.html
.. _PEP 8: https://www.python.org/dev/peps/pep-0008
.. _RobinHood coding style: ./doc/coding-style.rst

Tests
-----

To be accepted upstream, your patch will need positive reviews_, and also to
pass tests. Those tests will be run (semi-)automatically [#]_ once you upload
your change, but you might want to make sure they pass before pushing.

To do so, you can run the following:

.. code:: shell

    meson builddir
    ninja -C builddir/ test

It is generally a good idea to use sanitizers with meson's
``-Db_sanitize=address,undefined`` option. It is also recommended to run clang's
static analyzer ``scan-build``. Finally, consider checking test coverage:

.. code:: shell

    meson -Db_sanitize=address,undefined -Db_coverage=true builddir
    ninja -C builddir test
    ninja -C builddir scan-build

Most of these options are well documented on meson_'s website.

.. [#] hopefully, the "semi-" part is only temporary
.. _meson: https://mesonbuild.com

Commit messages
---------------

RobinHood uses the following format for commit messages::

    [<component>: ]a short description

    [A long description that can span multiple lines.

    And contain several paragraphs.]

    [Change-type: {bugfix, feature}]
    [Breaking-change: description of the breakage]
    Signed-off-by: Your Name <your.name@example.com>
    Change-Id: I0123456789abcdef...

Where:

- ``component`` is the name of the component impacted by the change, if there is
  one; [#]_
- ``a short description`` usually starts with a verb;
- ``A long descrip...`` contains details of the patch (eg. the context,
  performance measures, references, ...);
- ``Change-type`` indicates the type of change introduced with this commit;
- ``Breaking-change`` indicates that the commit is a breaking change;
- ``Signed-off-by`` is used as a `Developer Certificate of Origin`_;
- ``Change-Id`` is used by Gerrit to track changes across revisions.

The order of the trailers at the end of a commit is unimportant. You can add
them manually, or use the `git interpret-trailer`_ command.

The ``Change-type`` trailer is only required if the commit clearly fits into one
of the supported categories (ie. ``bugfix`` or ``feature``). [#]_ It is meant to
be parsed programmatically to then generate a changelog.

The description after ``Breaking-change`` is also meant to appear in a release's
changelog.

You can use ``git commit``'s ``-s``/``--signoff`` option to add the
``Signed-off-by`` trailer automatically.

Gerrit provides a commit-msg hook to generate the ``Change-Id`` trailer. You
can fetch it with:

.. code:: shell

    curl -Lo path/to/robinhood/.git/hooks/ \
        https://review.gerrithub.io/tools/hooks/commit-msg

Refer to the documentation__ for more information.

Besides those mentionned above, you can add any git trailer you find relevant.
Here is a set of trailer tokens commonly used in RobinHood and their meaning:

- ``Fixes: #123``, the commit fixes issue #123 (it is interpreted by most
  platforms, like GitHub, and automatically closes an issue); [#]_
- ``Relates-to: #123``, the change is somehow related to issue #123 (platforms
  like GitHub may render it as a link to that particular issue, which is always
  nice).

.. [#] usually it will be the name of a file without its extension, ``tests``,
       or ``doc``
.. _Developer Certificate of Origin: https://developercertificate.org
.. _git interpret-trailer: https://git-scm.com/docs/git-interpret-trailers
.. [#] the list may grow in the future
.. __: https://gerrit-review.googlesource.com/Documentation/cmd-hook-commit-msg.html
.. [#] you may choose to use any other token that is supported by GitHub,
       although try to stick with ``fixes``

Reviews
=======

Google documents its review practices here__. RobinHood hopes to implement them.
It makes for an interesting read overall, whether you intend to submit a patch
or review one.

Key takeways are:

- patches do not have to be perfect, they just need to increase the overall
  quality of the project;
- make life easy for reviewers;
- be nice.

Landing
-------

RobinHood patches are systematically reviewed before they are merged.

Authors may negatively score their own patch to prevent it from landing. But
they must never positively score their own patch. [#]_

To be merged, a patch must:

- be fast-forwardable, or trivially rebasable;
- pass tests;
- not have any -1 or -2;
- be assigned to at least two (active) reviewers;
- have at least one +1.

Once these conditions are met if the patch has at least two +1s, it is merged
upstream. Otherwise, reviewers are granted 48h (or until the next +1) to oppose
to the patch's landing. If they do not, the patch will be merged upstream.

Reviewers can ask to extend the 48h period, in which case the patch will not
land until they submit their review or the extension expires.

.. __: https://google.github.io/eng-practices/review/
.. [#] it only makes it harder for the gatekeeper to find patches ready to land
