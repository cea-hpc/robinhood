## ![robinhood logo](http://robinhood.sourceforge.net/images/logo_rh.gif) Robinhood Policy Engine
                                                                                 
Robinhood Policy Engine is a versatile tool to manage contents of large file systems. It maintains a replicate of filesystem medatada in a database that can be queried at will. It makes it possible to schedule mass action on filesystem entries by defining attribute-based policies, provides fast 'find' and 'du' enhanced clones, gives to administrators an overall view of filesystem contents through its web UI and command line tools.
It supports any POSIX filesystem and implements advanced features for  [Lustre](https://www.lustre.org) filesystems (list/purge files per OST or pool, read MDT changelogs...)
                                                                                 
Originally developped for HPC, it has been designed to perform all its tasks in parallel, so it is particularly adapted for running on large filesystems with millions of entries and petabytes of data. But of course, you can take benefits of all its features for managing smaller filesystems.
                                                                                 
Robinhood is distributed under the [CeCILL-C](http://www.cecill.info/licences/Licence_CeCILL-C_V1-en.html) license, which is a French transposition of the [GNU LGPL](http://www.gnu.org/licenses/lgpl.html) and is fully LGPL-compatible.
                                                                                 
### Main features                                                                
* Policy Engine: schedule actions on filesystem entries according to admin-defined criteria, based on entry attributes.
* User/group usage accounting, including file size profiling.                    
* Fast 'du' and 'find' clones.                                                   
* Customizable alerts on filesystem entries.                                     
* Aware of Lustre OSTs, pools and projects.                                      
                                                                                 
For more information, refer to the [online documentation](https://github.com/cea-hpc/robinhood/wiki).

I - License
===========

Copyright (C) 2004-2017 CEA/DAM.
Copyright 2013-2016 Cray Inc. All Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the CeCILL-C License.

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license (http://www.cecill.info) and that you
accept its terms.

II - Compiling
==============

2.1 - From source tarball
-------------------------

It is advised to build RobinHood on your target system, to ensure the best
compatibility with your Lustre and MySQL versions.

Build requirements: glib2-devel, libattr-devel, mysql-devel or mariadb-devel,
s-nail, bison, flex, jemalloc, jemalloc-devel.
For lustre support: lustre or lustre-client, lustre-devel (Lustre >= 2.15)
For running RobinHood's CI: lustre-tests (Lustre >= 2.15)

Unzip and untar the source distribution:
```
tar zxvf robinhood-3.x.x.tar.gz
cd robinhood-3.x.x
```

Configure and build:
```
./configure
make rpm
```

RPMs are generated in the 'rpms/RPMS/*arch*' directory.

2.2 - From git repository
-------------------------

Install git, and autotools stuff:
```
yum install git automake autoconf libtool
```

Retrieve robinhood sources
```
git clone https://github.com/cea-hpc/robinhood.git
cd robinhood
git checkout master *(or other branch)*
sh autogen.sh
```

Then refer to section 2.1 for next compilation steps.

2.3 - Build using src rpm
---------------------------

### 2.3.1 - Creating src rpm
Only requirements: rpm-build, gcc, flex, bison

To create src rpm on a host with minimal requirements, run:
```
./configure --enable-dist
make srpm
```

### 2.3.2 - Building from src rpm
By default, the src rpm builds robinhood for POSIX filesystems:
```
rpmbuild --rebuild robinhood-3.0*.src.rpm
```

To build robinhood binary RPM for a Lustre filsystem, run:
```
rpmbuild --rebuild robinhood-3.0*.src.rpm --with lustre --define "lversion x.y"
```

where x.y is your target lustre version (e.g. 2.7).

Note: if lustre-client is not installed on your target system, you can specify
an alternative lustre package by defining "lpackage", e.g.
```
rpmbuild [...] --define "lpackage lustre"
```

Note: 'lversion' can be omitted, by it is strongly recommanded to prevent
incompatibility issues between lustreapi versions.

III - Install
=============

For installing robinhood on your target system, install **robinhood-adm** RPM.
It includes configuration helper for DB, changelogs, ...

* For lustre filesystems install **robinhood-lustre** RPM.
* For other filesystems install **robinhood-posix** RPM.

IV - Database Configuration
===========================

Robinhood needs a MySQL database for storing information about files.
This database can run on a different node from Robinhood daemon.

* Install MySQL server on the machine (mysql-server and mysql packages).
* Start the DB engine:
    * systemctl start mariadb
* Run the configuration helper script as root on the database host to create
the database:
```
rbh-config create_db
```

* Write the DB password to a file with read access for root only (600)
  e.g. to /etc/robinhood.d/.dbpassword

Note: initially, the database schema is empty. Robinhood will create it the first time it is launched.

V - Lustre 2.x Filesystems only: enabling changelogs
====================================================
For Lustre, you must register robinhood as a MDT changelog consumer.

* Run the configuration helper script on Lustre MDS:
```
rbh-config enable_chglogs
```

  This registers a changelog consumer and activate required changelog records.

Note: by default, the script checks for a 'cl1' consumer.
If you need to register several changelog consumers on your file system,
refer to lustre documentation.

VI - Configuration file
=======================
Examples of config files are installed in /etc/robinhood.d/templates/

You can also use the '--template' option to generate a documented configuration file template:
```
robinhood --template=<template_file>
```

For more details, refer to [Robinhood v3 admin guide](https://github.com/cea-hpc/robinhood/wiki/robinhood_v3_admin_doc).

Note: by default, robinhood searches configuration files in "/etc/robinhood.d".

VII - First run
===============

Even if your filesystem is empty, you need to perform an initial scan in order to initialize robinhood database.
This prevents from having entries in filesystem that it wouldn't know about.
```
robinhood --scan --once
```

VIII - Start the daemon
=======================
* Configure per-filesystem daemon options in **/etc/sysconfig/robinhood.\<fsname\>**

Example: to read lustre changelogs and run all policies:
```
RBH_OPT="--readlog --run=all"
```

Example: to regularly scan filesystem and run all policies:
```
RBH_OPT="--scan --run=all"
```

Start the daemon:
```
systemctl start robinhood@*fsname*
```
