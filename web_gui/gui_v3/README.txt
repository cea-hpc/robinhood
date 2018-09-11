Robinhood new web interface (gui_v3)

I - License
===========

Copyright (C) 2016 CEA/DAM

This program is free software; you can redistribute it and/or modify
it under the terms of the CeCILL-C License.

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license (http://www.cecill.info) and that you
accept its terms.

libs:
jquery: MIT
Datatables.js: MIT
graphjs: MIT
bootstrap: MIT
bootstrap-treeview: Apache Licence 2
Moment.js: MIT

II - Installation
=================

2.1 Requirements

php, php-pdo, php-mysql

2.2 Install from tarball

copy web_gui/gui_v3 to your webserver folder (ex: /var/www/robinhood)
configure your web server (your can use the default configuration robinhood.conf in web_gui for apache)

2.3 Install from RPM

yum install robinhood-webgui

III - Configuration
===================

Local configuration:
You can setup a local config_local.php.template file instead of the config.php with default settings
Rename the config_local.php.template in config_local.php and use it instead of config.php

Database:
In "config.php" change the following fields with your db settings
$DB_HOST     = "";
$DB_NAME     = "";
$DB_USER     = "";
$DB_PWD      = "";

Supported DB: Mysql/MariaDB

Authentication:
You need to authenticate the user with your web server (apache: basic auth, ...)

Access list:
In "config.php" add permission to $ACCESS_LIST array

"*" is Everyone,
"$AUTH" is Authenticated user only
"username" Allow to give access to a specific person
"$SELF" Allow user to access to his own data (match the remote username to uid)

ex:
$ACCESS_LIST['api-ro'][] = '*'; // Give every access to the api in read only
$ACCESS_LIST['webgui'][] = 'marian'; // Give marian access to the webgui
$ACCESS_LIST['graphs'][] = '$SELF'; // Allow all users to see their own data as graphs

Example of permissions for a self service dashboard (with apache auth):
$ACCESS_LIST['webgui'][] = '*';
$ACCESS_LIST['api-ro'][] = '*';
$ACCESS_LIST['datatables'][] = '$SELF';
$ACCESS_LIST['graphs'][] = '$SELF';
$ACCESS_LIST['native_vars'][] = '$AUTH';
$ACCESS_LIST['native_accts'][] = '$SELF';


Misc:
MAX_ROWS: SQL max results
JSON_OPTIONS (default: JSON_PRETTY_PRINT): Set default json output
    JSON_PRETTY_PRINT is slower but human readable, set to 0 to disable

Apache:

The rpm install the configuration robinhood.conf in /etc/httpd/conf.d/
You might need to customize this file.

You can access the webgui at http://yourservername/robinhood


IV - API
=========

Functions provided by the API
    *<server-url>/api/robinhood
        -return a Robinhood quote (just for testing purpose)
    *<server-url>/api/test
        -return args as json (debug purposes)
    *<server-url>/api/current_auth
        -return your current authentification
    *<server-url>/api/db_info
        -return database configuration with status and errors
    *<server-url>/api/graph/(uid/gid/sizes/files/*_status)
        -return datas as json using graphjs datasets format
    *<server-url>/api/data/(uid/gid/files/*_status)
        -return datas as json using datatables.js format
    *<server-url>/api/native/table/fields.operator1.operator2/...
            ex: native/acct/gid.group/size.avg/ #return average size by group
            ex: ... (see bellow)
        -return direct data from ACCT_STAT

native syntax:

URL syntax: <server-url>/api/native/<table>/[field.operator[/operator...]]
native: request type
<table>: table requested (acct, vars, files , entries or names)
list of request / separated:
    *field.operator/operator_parameter
    *...
operators:
    *group        -Group result
    *groupbytime  -Floor a value in seconds by hour/day/week/month/year and group
    *groupbylog2  -Floor(log2(value)) and group (return NULL if value is 0)
    *max          -Get the max value when group is used
    *min          -Get the min value when group is used
    *count        -Get number of entries when group is used
    *avg          -Get average when group is used
    *sum          -Get sum when group is used
    *concat       -Concat string
    *remove       -Hide field from result
    *filter       -Filter result with sql "LIKE" (mandatory parameter, wildcard: *)
    *nfilter      -Filter result with sql "NOT LIKE" (mandatory parameter, wildcard: *)
    *equal        -Filter result with sql "=" (mandatory parameter)
    *less         -Filter result with sql "<" (mandatory parameter)
    *bigger       -Filter result with sql ">" (mandatory parameter)
    *soundslike   -Filter result with sql "SOUNDS LIKE" (mandatory parameters)
    *asc          -Sort asc by
    *desc         -Sort desc by

parameters:
    *whitelist  -hide all field by default, you have to select them explicitly
    *limit/int  -Limit the number of results
them

<server-url>/api/native/acct request all the table:
[
 {
        "uid": "jenkins",
        "gid": "jenkins",
        "type": "file",
        "lhsm_status": "new",
        "checksum_status": "",
        "size": "9148416",
        "blocks": "18280",
        "count": "138",
        "sz0": "1",
        "sz1": "0",
        "sz32": "0",
        "sz1K": "32",
        "sz32K": "105",
        "sz1M": "0",
        "sz32M": "0",
        "sz1G": "0",
        "sz32G": "0",
        "sz1T": "0"
    },...

Same, grouped by uid
<server-url>/api/native/acct/uid.group
[
    {
        "uid": "jenkins",
        "gid_set": "jenkins,mysql,testgroup,testuser",
        "type_set": "file",
        "lhsm_status_set": "new",
        "checksum_status_set": ",ok",
        "size": "61790208",
        "blocks": "123488",
        "count": "938",
        "sz0": "4",
        "sz1": "0",
        "sz32": "0",
        "sz1K": "212",
        "sz32K": "722",
        "sz1M": "0",
        "sz32M": "0",
        "sz1G": "0",
        "sz32G": "0",
        "sz1T": "0"
    },...
The api automatically makes comma separated list from strings and sum numbers

<server-url>/api/native/acct/uid.group/uid.filter/mysql (native/acct/uid.group.filter/mysql also works)
[
    {
        "uid": "mysql",
        "gid_set": "jenkins,mysql,testgroup,testuser",
        "type_set": "file",
        "lhsm_status_set": "new",
        "checksum_status_set": ",ok",
        "size": "62610432",
        "blocks": "125072",
        "count": "974",
        "sz0": "10",
        "sz1": "0",
        "sz32": "0",
        "sz1K": "231",
        "sz32K": "733",
        "sz1M": "0",
        "sz32M": "0",
        "sz1G": "0",
        "sz32G": "0",
        "sz1T": "0"
    }
]


All together:
<server-url>/api/native/acct/gid.group/size.avg/blocks.max/checksum_status.remove/lhsm_status.remove/type.remove.filter/file/
[
    {
        "gid": "jenkins",
        "uid_set": "jenkins,tcpdump,testuser,sshd,puppet,oprofile,mysql",
        "size_avg": "7059387.7333",
        "blocks_max": "19464",
        "count": "1617",
        "sz0": "12",
        "sz1": "0",
        "sz32": "0",
        "sz1K": "367",
        "sz32K": "1238",
        "sz1M": "0",
        "sz32M": "0",
        "sz1G": "0",
        "sz32G": "0",
        "sz1T": "0"
    },...

Side notes on groupbytime and groupbylog2:

You can specify the interval in groupbytime by adding .week/.day/.hour

The following query return the number of files (and size sum) by modification date
<server-url>/api/native/entries/whitelist/id.count/last_mod.groupbytime.day/size.sum
[{
        "last_mod_by": "1495929600",
        "id_count": "44",
        "size": "244133029"
    },
    {
        "last_mod_by": "1496016000",
        "id_count": "168",
        "size": "1747690453"
    },...
]

The last_mod_by value is an unix timestamp round to an interval.

You can group entries by size range using groupbylog2 (with option .unit and .hunit to divide the log by 10 or 5).

The following query return the number of size in range <B, B to KB, KB to MB, ... >:
<server-url>/api/native/entries/whitelist/id.count/size.groupbylog2.unit/size.max
[
    {
        "size_by": null,
        "id_count": "126131",
        "size_max": "0"
    },
    {
        "size_by": "0",
        "id_count": "172346",
        "size_max": "1023"
    },
    {
        "size_by": "1",
        "id_count": "659586",
        "size_max": "1048506"
    },
    {
        "size_by": "2",
        "id_count": "626440",
        "size_max": "1041403917"
    },
    {
        "size_by": "3",
        "id_count": "3935",
        ...
]

You can recreate accounting file size stats with:
<server-url>/api/native/entries/whitelist/id.count/size.groupbylog2.hunit/
[
  {
    "size_by": null,
    "id_count": "126131"
  },
  {
    "size_by": "0",
    "id_count": "21145"
  },
  {
    "size_by": "1",
    "id_count": "151201"
  },...
]
"size_by 0" count files from 1B to 32B, "size_by 1" 32B to 1K, "size by 2" 1K to 32KB ...

Common Robinhood queries:

rbh-report command:
    rbh-report -u foo -S
Equivalent URL:
    native/acct/uid.filter/foo

rbh-report command:
    rbh-report --top-users --by-count
Equivalent URL:
    native/acct/uid.group/count.desc/limit/20

V - Web UD & Plugins
====================
5.1 Link to a specific graph with filters

You can set the filter by passing parameters:
http://hostname/robinhood/index.php?formUID=ROBIN&formFilename=HOOD
will preset the UID and Filename filter to ROBIN and HOOD

You can choose the graph to display:
http://hostname/robinhood/index.php?formUID=user&callGraph=Sizes (uid, gid, Sizes, Files, ...)

5.2 Plugins

Please refer to the README.txt in plugins folder for development

VI - TODO
========

*Improve REST api
*Move from graph/data to native for graph and datatable
*Allow custom query directly in the interface
*Move builtin graphs in plugins (Files, ...)

VII - Changelog
==============
v0.3 @ 18/09/2018
     -Update GraphJS to 2.7.2 (previously 2.6)
      -Add plugins
        -tasks (Copy data in other databases)
        -netauth (Give access by ip source/hostname)
        -customgraph (create custom graphs)
     -Add parameter to disable files view in the UI
     -Improve console plugin
     -Partial support for multidatabases

v0.2 @ 25/09/2017
    -Add plugins
        -stackgraph (group small values)
        -internal stats (display robinhood vars)
        -browser (browse the filesystem)
        -colorgraph (set readable colors)
        -ldapauth (use ldap for data access)
        -console (run custom query from ui)
        -output (export data as csv)
    -Allow users to request a specific page
         and filters by settings parameter
         in the url
    -Improve UI
    -Add native/files API request
    -Upgrade libs

v0.1 @ 09/09/2016:
   -Ready for landing (robinhood 3.0-rc2)

