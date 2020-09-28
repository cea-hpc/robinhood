.. This file is part of the RobinHood project
   Copyright (C) 2020 Commissariat a l'energie atomique et aux energies
                      alternatives

   SPDX-License-Identifer: LGPL-3.0-or-later

#########
RobinHood
#########

RobinHood provides efficient and easy to use means to replicate and query any
filesystem's metadata.

When to use RobinHood?
======================

Filesystems are not particularly good at answering questions such as: "what are
the ten biggest files in this filesystem?", which will most likely trigger a
full filesystem scan.

In many setups, this is not a problem. Although traversing a whole data
structure to return only a few of its elements is not optimal, it can still be
very fast. But sometimes, especially with large distributed filesystems [#]_, a
full filesystem scan is not something you want to run more than once a day.

In those situations, RobinHood can provide tremendous gains.

.. [#] or filesystems backed by very slow hardware (eg. tapes)

Overview
========

Basically, what RobinHood does is replicate a filesystem's metadata into a
backend (usually a database), and provide the tools to easily and efficiently
query this backend.

Main use case
=============

Using RobinHood usually entails the following:

* Use rbh-sync_ to replicate a filesystem's metadata into a RobinHood backend;
* Use rbh-find_ to query this backend.

Here is an example:

.. code:: console

    $ tree /path/to/my/filesystem
    /path/to/my/filesystem/
    └── dir
        └── file.txt

    1 directory, 1 file
    $ rbh-sync rbh:posix:/path/to/my/filesystem rbh:mongo:my-filesystem
    $ rbh-find rbh:mongo:my-filesystem -name '*.txt'
    /dir/file.txt
    $ rbh-find rbh:mongo:my-filesystem -type d
    /dir

.. _rbh-sync: https://github.com/cea-hpc/rbh-sync
.. _rbh-find: https://github.com/cea-hpc/rbh-find

Project structure
=================

The project is divided into a core library and a set of applications:

* librobinhood_: the core library upon which applications are built;
* rbh-sync_: a tool to synchronize two robinhood backends;
* rbh-find_: a close twin of find(1).

Coming soon: rbh-fsevent_, a tool to incrementally update a robinhood backend
without having to issue a full rescan.

.. _librobinhood: https://github.com/cea-hpc/librobinhood
.. _rbh-fsevent: https://github.com/cea-hpc/rbh-fsevent
