.. This file is part of the RobinHood project
   Copyright (C) 2020 Commissariat a l'energie atomique et aux energies
                      alternatives

   SPDX-License-Identifer: LGPL-3.0-or-later

############
xattrs: path
############

Context
=======

One of the main performance bottleneck of RobinHood v3 was the computation of
entries' path. Since the path itself was never stored, it had to be computed
from scratch each and everytime it was needed. That computation was not cheap
by any means: it involved a recursive traversal of an SQL table, one traversal
per entry (ie. ``path(entry) = path(entry.parent) + "/" + entry.name``).

In RobinHood v4, the goal is to maintain an eventually consistent ``path``
xattr. This attribute may temporarily be missing on some entries, or out-of-date
after a directory rename; but overall, it should be pretty close to the real
thing.

Applications that require a higher level of consistency can still emulate the
old behaviour themselves. Helpers for this may find their way into librobinhood,
if deemed appropriate.

Requirements
============

Consistency
-----------

Provided an entry and all of its parents are not renamed for long enough, its
``path`` xattr must converge to the value a recursive traversal would yield.

Solution
========

The idea is simple: an entry's path is that of its parent + a separator (``/``)
+ the entry's name. Therefore, in an infinite loop: look up entries without a
``path`` xattr or with an outdated one, and for each one of them, fetch their
parent's ``path`` xattr, compute the entry's path, and update the entry.

The complexity lies in three places:

- how to implement the lookup part of the algorithm;
- how to handle edge cases;
- how to make this somewhat efficient.

Because this is a lot of complexity, we will describe solutions to each part of
the problem separately.

Lookup
------

How to find all the entries which either lack a ``path`` xattr, or bear an
outdated one?

Of course, entries without a ``path`` xattr naturally stand out: they do not
have a ``path`` xattr. Those with an outdated path though, those are harder to
find.

Paths becomes outdated in a RobinHood backend if, and only if, a directory which
contains at least one entry is renamed. The rename operation (translated into
an ``UNLINK`` fsevent + a ``LINK`` fsevent in RobinHood's semantics) will reset
the directory's ``path`` xattr, but it will not update any of the entries inside
the directory. And yet, their paths have changed too.

::

    init:                         ----------------
                                  |     id: 0    |
                                  |--------------|
                                  | name:  dir-0 |
                                  | path: /dir-0 |
                                  ----------------
                                         |
                           -----------------------------
                           |                           |
                -----------------------     -----------------------
                |        id: 1        |     |        id: 2        |
                |---------------------|     |---------------------|
                | name:        file-0 |     | name:        file-1 |
                | path: /dir-0/file-0 |     | path: /dir-0/file-1 |
                -----------------------     -----------------------

    ============================================================================

    rename dir-0 dir-1:           ----------------
                                  |     id: 0    |
                                  |--------------|
                                  | name:  dir-1 |
                                  | XXXX: XXXXXX |
                                  ----------------
                                         |
                           -----------------------------
                           |                           |
                -----------------------     -----------------------
                |        id: 1        |     |        id: 2        |
                |---------------------|     |---------------------|
                | name:        file-0 |     | name:        file-1 |
                | path: /dir-0/file-0 |     | path: /dir-0/file-1 |
                -----------------------     -----------------------
                         ^^^^^                       ^^^^^
                           |                           |
                           --------- outdated ----------


The approach we recommend is to only look for entries without a ``path`` xattr,
and to remove their children's ``path`` xattr. Conceptually, this is the
equivalent of recursively *invalidating* the path of a whole directory tree.

Edge cases
----------

What if an entry does not have a parent?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This can happen if fsevents are applied out-of-order on a backend. This is a
valid and supported edge case in general, so our solution has to handle it as
well.

We cannot compute an entry's path without knowing its parent. But no entry
should stay indefinitely without a parent in a RobinHood backend. Therefore, we
can afford to simply skip the entry until its parent is known (or the entry is
deleted).

What if an entry's parent does not have a ``path`` xattr?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This can happen, if while iterating over entries without ``path`` xattrs, we
process an entry before its parent.

Just like the previous edge case: skip the entry for now, its parent will get a
path eventually.

What if an entry has multiple parents?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This can happen, if after a rename, we process the ``LINK`` fsevent before the
``UNLINK`` fsevent.

And once again, just like the other two edge cases: skip the entry, wait for it
to have only one parent again.

Efficiency
----------

There are three main avenues to make this algorithm efficient:

- skip as few entries as possible;
- batch queries as much as possible;
- distribute/parallelize.

Skipping as few entries as possible
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is equivalent to making sure we iterate first on entries which have exactly
one parent, whose path is up-to-date.

*This is an open problem, feel free to make suggestions.*

.. TODO I cannot think of a good way to do this.

Batch queries as much as possible
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Group entries by their ``parent_id``. This way, with a single query to the
backend, you can update several paths at once.

Distribute/parallelize
~~~~~~~~~~~~~~~~~~~~~~

Shard the processing using ``parent_id`` as an index key. Workers will operate
on distinct entries, and will need less queries to find parents' paths.

Pseudo-code implementation
--------------------------

As a reference point, here is an unoptimized version of the algorithm, in
pseudo-python:

.. code:: python

    def children(backend, entry):
        yield from backend.filter(FSEntryField.PARENT_ID == entry.id)

    def parent(backend, entry):
        parents = backend.filter(FSEntryField.ID == entry.parent_id)

        # Find a matching parent
        try:
            parent = next(parents)
        except StopIteration:
            raise MissingFSEntry(entry.parent_id)

        # Check the entry only has one parent
        try:
            next(parents)
        except StopIteration:
            return parent

        raise MultipleParents(entry)

    def path_fsevent(backend, entry):
        _parent = parent(backend, entry, FSEntryField.NAME)

        if "path" not in _parent.xattrs.ns:
            raise MissingPath(_parent)

        path = _parent.xattrs.ns["path"] + '/' + entry.name
        return NSXattrFSEvent(entry.id, entry.parent_id, entry.name,
                             {"path": path})

    def fsevents(backend):
        for entry in backend.filter(FSEntryField.ns_xattr("path") == None):
            # Invalidate the children's path
            for child in children(entry):
                yield NSXattrFSEvent(child.id, entry.id, child.name,
                                     {"path": None})

            try:
                yield path_fsevent(backend, entry)
            except MissingFSEntry, MissingPath, MultipleParents:
                # skip edge cases
                continue

    def main(backend):
        while True:
            backend.update(fsevents(backend))
