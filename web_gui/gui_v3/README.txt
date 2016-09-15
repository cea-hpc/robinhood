Robinhood new web interface

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

"*" is Everyone, "$AUTH" is Authenticated user and "username" match the authentified user username
ex:
$ACCESS_LIST['api-ro'][] = '*'; // Give every access to the api in read only
$ACCESS_LIST['webgui'][] = 'marian'; // Give marian access to the webgui

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
    *<server-url>/api/graph/(uid/gid/sizes/files/*_status)
        -return datas as json using graphjs datasets format
    *<server-url>/api/data/(uid/gid/files/*_status)
        -return datas as json using datatables.js format
    *<server-url>/api/native/fields.operator1.operator2/...
            ex: native/acct/gid.group/size.avg/ #return average size by group
            ex: ... (see bellow)
        -return direct data from ACCT_STAT

native syntax:

<server-url>/api/: baseURL
native: request type
acct: table requested (acct or var)
list of request / separated:
    *field.operator/operator_parameter
    *...
operators:
    *group -Group result
    *max -Get the max value when group is used
    *min -Get the min value when group is used
    *count -Get number of entries when group is used
    *avg -Get average when group is used
    *remove -Hide field from result
    *filter -Filter result with sql "LIKE" (mandatory parameter, wildcard: *)

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

V - TODO
========

*Improve REST api
*Move from graph/data to native for graph and datatable
*Allow custom query directly in the interface
*Remove all php from index.php

VI - Changelog
==============

v0.1 @ 09/09/2016:
   -Ready for landing (robinhood 3.0-rc2)

