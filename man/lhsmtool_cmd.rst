==============
 lhsmtool_cmd
==============

------------------------------------------------------
Turn any data copy command into a Lustre/HSM copytool
------------------------------------------------------

:Author: henri.doreau@cea.fr
:Date: 2017-12-13
:Copyright: GPLv2
:Version: 0.1
:Manual section: 1

SYNOPSIS
========

    lhsmtool_cmd [options] <lustre mount point>

DESCRIPTION
===========

lhsmtool_cmd turns any command that can copy data from / to a file (or a file
descriptor) into a fully-fledged Lustre/HSM copytool.

The tool receives action items from the Lustre/HSM coordinator, opens the file
and notify Lustre that actions are starting, then spawns subcommands for the
actual data transfer, with lustre file descriptor and FID as parameters.

The commands to execute are specified in a configuration file, with well-known
patterns for the variable sections of the commands.

CONFIGURATION FILE
==================

By default, the lhsmtool_cmd configuration file lives in /etc/lhsmtool_cmd.conf.
It follows the syntax of .ini files, with a single section, named `[commands]`
which contains entries under the form `key = value`.

The keys are the actions that trigger the attached commands. They can be:

- archive
- restore
- remove
- cancel

The values are the commands to execute. They can be anything. The following
parameter templates are available: `{fid}` and `{fd}`.  The first template
`{fid}` is replaced by the lustre FID of the file that is being manipulated.
Commands can typically use it to derive a name for the corresponding object in
the archive.  The second template `{fd}` is an integer representing a file
descriptor to the file being manipulated in Lustre. Subcommands can directly use
this file descriptor to read (in the archive case) or write (for a restore) the
file in Lustre.

For commands that do only operate on paths, one may want to use
`/proc/self/{fd}` as demonstrated below with the ``dd`` command.

EXAMPLES
========

The following example illustrates a possible use of `dd(1)` to use a directory
as an archive, in which files are simply named after their FID in Lustre.

::

    [commands]
    archive = dd if=/proc/self/{fd} of=/tmp/arch/{fid}
    restore = dd if=/tmp/arch/{fid} of=/proc/self/{fd}

OPTIONS
=======

-A, --archive=<n>         Archive number to serve. This option can be repeated
--abort-on-error          Exit on first major error
-c, --config=<path>       Specify a non-default configuration file
--daemon                  Daemonize process: run in background
--dry-run                 Do not actually run but log what would have been done
-f, --event-fifo=<path>   Write event JSON descriptions at `<path>`
-F, --fanout=<n>          Max number of commands that can be spawned in parallel
-q, --quiet               Reduce verbosity
-v, --verbose             Increase verbosity
--help, -h                Show the help message and exit

SEE ALSO
========
* ``man lfs``
* ``man robinhood``

BUGS
====
Note that not all operations are fully supported. Typically, CANCEL is not yet
implemented, although it could easily be added using a fid/pid mapping and by
delivering signals to the subcommands.
