==============================
Robinhood testsuite for Lustre
==============================

How to run the tests for Lustre
-------------------------------


Have a Lustre filesystem
~~~~~~~~~~~~~~~~~~~~~~~~

Lustre must be mounted as /mnt/lustre before running setup step and
tests. The filesystem should have at least 4 OSTs.

If you don't have a Lustre filesystem handy, you can use llmount.sh to
create the filesystem.

For instance, as root, run::

  OSTSIZE=400000 OSTCOUNT=4 /usr/lib64/lustre/tests/llmount.sh


Set the purpose environment variable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The available values for PURPOSE are::

  - TMP_FS_MGR
  - LUSTRE_HSM
  - BACKUP
  - SHOOK

If unset, the default for PURPOSE is TMP_FS_MGR.

The PURPOSE variable can be exported like this::

  export PURPOSE=LUSTRE_HSM

or it can prefix the tests like that::

  PURPOSE=LUSTRE_HSM ... <test>


Setup the test environment
~~~~~~~~~~~~~~~~~~~~~~~~~~

As root, run::

  ./1-test_setup_lustre.sh

This will initialize the SQL database, create the changelog client,
start the Lustre posix copytool, and enable the HSM coordinator.


Run the tests
~~~~~~~~~~~~~

For brief output::

  ./2-run-tests.sh -q

For full output::

  ./2-run-tests.sh

To run only some tests (here, test 3 and 4)::

  ./2-run-tests.sh 3,4

To get a debug output, set DEBUG to 1::

  DEBUG=1 ./2-run-tests.sh

RBH_NUM_UIDGID can be set to use numerical UIDs and GIDs instead of their
strings counterparts::

  RBH_NUM_UIDGID=yes ./2-run-tests.sh 119


Run the tests under Valgrind
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The test suite, and just a subset of the tests, can be run under
valgrind by setting the WITH_VALGRIND environment variable. For
instance::

  WITH_VALGRIND=1 PURPOSE=LUSTRE_HSM ./2-run-tests.sh 301

Each test instance will create a valgrind log file called
`vg-test_<test number>-<pid>.log`

It is possible to pass some extra parameters to valgrind by setting
its VALGRIND_OPTS::

  WITH_VALGRIND=1 PURPOSE=LUSTRE_HSM VALGRIND_OPTS="--tool=cachegrind" ./2-run-tests.sh 100

By default, the output in the log files include a suppression rule for
each error. These suppressions can be added to `valgrind.supp` to
suppress the corresponding warning(s) in a subsequent run.

How to run the tests for non-lustre POSIX filesystem
----------------------------------------------------

Set environment variables
~~~~~~~~~~~~~~~~~~~~~~~~~

For POSIX tests, set the following environment variables:

* unset PURPOSE, or set PURPOSE=TMP_FS_MGR

* set POSIX_MODE=1

* set NOLOG=1

Setup the test environment
~~~~~~~~~~~~~~~~~~~~~~~~~~

As root, run::

  ./1-test_setup_posix.sh

This will initialize the SQL database, create and mount a loopback filesystem in /tmp.

Run the tests
~~~~~~~~~~~~~

Run the test as previously described in the Lustre section.

Running the tests with robinhood installed
------------------------------------------

After installing the three rpms robinhood, robinhood-adm and
robinhood-tests, create a test repository, cd into it, and run both
/usr/share/robinhood/tests/test_suite/1-test_setup_lustre.sh and
/usr/share/robinhood/tests/test_suite/2-run-tests.sh as shown in the
previous sections.
