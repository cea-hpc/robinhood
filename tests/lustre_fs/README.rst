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

  ./1-test_setup.sh

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
