.. This file is part of the RobinHood project
   Copyright (C) 2020 Commissariat a l'energie atomique et aux energies
                      alternatives

   SPDX-License-Identifer: LGPL-3.0-or-later

######
Rescan
######

Context
=======

For filesystems that support it, the best way to keep them up to date with
minimal overhead is to use an incremental approach. rbh-fsevents_ should support
most filesystems in that regard (or at least, will be able to).

That being said, if metadata events are missed, or if anything happens and the
backend is corrupted beyond salvation -- in the sense that the backend's
contents have irremediably diverged from the original filesystem, one may have
no other choice but to rescan the filesystem itself.

.. _rbh-fsevents: https://github.com/cea-hpc/rbh-fsevents

Problem
=======

If that happens, either the whole filesystem has to be rescanned, and then it is
basically like starting from scratch, regular procedures apply; or only part of
the filesystem needs rescanning, and things get interesting! (albeit, arguably,
more complicated)

The main issue is being able to distinguish entries that have been seen by the
scan from those that have not.

We suggest that a rescan should never actually delete entries from a backend and
instead only care about removing expired **namespace entries**. The actual
deletion of fsentries themselves should be deferred to a general purpose garbage
collector. [#]_

.. [#] such as https://github.com/cea-hpc/rbh-gc

Assumptions
===========

In order to simplify the design, we will make a number of assumptions. At the
end of the document, we will try to relax them one by one, and study how it
affects the overall design.

Filesystem freeze
-----------------

The filesystem may not be updated during a rescan. [#]_

.. [#] To avoid modifying access times when scanning entries, one can bind mount
       the root directory of the rescan in read-only mode.

Backend isolation
-----------------

The destination backend may not be updated during a rescan by any process other
than the rescan itself. [#]_

.. [#] The rescan itself may use multiple processes, so long as they cooperate
       with one another, and make sure not to step on each other's feet.

Solution
========

The currently agreed upon approach, is to tag namespace entries so that, at the
end of the scan, those with the tag (or without, depending on the
implementation) can be removed.

To tag a namespace entry, we suggest using a namespace xattr.

Whether to tag namespace entries as the rescan sees them, or beforehand, is left
for the implementation to decide. There does not seem to be any particular
reason to choose one over the other, except maybe for backend-specific
considerations.

The rest of the design will assume the implementation tags namespace entries
as it discovers them.

We suggest tagging namespace entries using an xattr of the form: ``seen: true``.

Then, at the end of the scan, iterate on all the entries in the backend that
appear under the root of the rescan [#]_ and process them so that: those without
the tag are unlinked; and those with the tag are untagged.

.. [#] most likely using the ``branch()`` backend operator

Relaxing assumptions
====================

Filesystem freeze
-----------------

If entries are renamed during a rescan, they may be missed, and if they are,
they won't appear in the destination backend. This may lead to awkward
situations later on:

.. code:: console

    $ tree /mnt
    /mnt
    ├── dir-0
    └── dir-1
        └── path
            └── to
                └── file-0

    4 directories, 1 file
    $ rescan rbh:posix:/mnt rbh:mongo:myfs &
    $ mv /mnt/dir-1/path /mnt/dir-0
    $ wait $!
    $ rbh-find rbh:mongo:myfs
    /dir-0
    /dir-1
    $
    $ rbh-fsevents fanotify:/mnt rbh:mongo:myfs &
    $ mv /mnt/dir-0/path/to/file-{0,1}
    $ rbh-find rbh:mongo:myfs
    /dir-0
    ???/file-0
    /dir-1

Note that registering some sort of filesystem event collector right before the
rescan will not be enough to discover every missing entry. The collector should
see the ``rename`` of ``path`` from ``dir-0`` to ``dir-1``, but this won't give
away the existence of ``to`` and ``file-0``.

Backend isolation
-----------------

If the destination backend is updated by an independent process, without any
cooperation, the backend may irremediably diverge from the filesystem. Nothing
can be guaranteed. All bets are off.

.. code:: console

    $ mkdir /mnt/dir-{0..1023}
    $ dd if=/dev/random of=/mnt/dir-1023/file bs=128K count=32K
    $ stat --format=%s /mnt/dir-1024/file | numfmt --to=iec-i
    4.0Gi
    $ tree /mnt
    /mnt
    ├── dir-0
    │   └── file
    ├── dir-1
    ├── ...
    └── dir-1023
    $ rbh-fsevents fanotify:/mnt rbh:mongo:myfs &
    $ rescan rbh:posix:/mnt rbh:mongo:myfs &
    $ echo "123" > file
    $ wait $!
    $ find /mnt -size 4G
    $ rbh-find rbh:mongo:myfs -size 4G rbh:mongo:myfs
    /mnt/dir-1023/file

In this example, ``rescan`` sees ``file`` with a size of ``4GiB``, whereas
``rbh-fsevents`` sees that ``file`` is ``4B`` long after being overwritten.
Unfortunately, nothing prevents ``rbh-fsevents`` from sending its update to
``rbh:mongo:myfs`` before ``rescan`` does, and so ``rescan`` overwrites
``file``'s actual size with the old ``4GiB`` value.

Note that unless ``file`` is ever operated on again, its size will remain set to
``4B`` in ``rbh:mongo:myfs`` forever.
