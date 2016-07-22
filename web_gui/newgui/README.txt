Robinhood new web interface

I - License
===========

Copyright (C) 2016 CEA/DAM

This program is free software; you can redistribute it and/or modify
it under the terms of the CeCILL-C License.

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license (http://www.cecill.info) and that you
accept its terms.

II - Configuration
==================

Database:
In "config.php" change the following fields with your db settings
$DB_HOST     = "";
$DB_NAME     = "";
$DB_USER     = "";
$DB_PWD      = "";

Supported DB: Mysql/MariaDB

Authentifcation:
You need to authentify the user with your web server (apache: basic auth, ...)

access list:
In "config.php" add permission to $ACCESS_LIST array

"*" is Everyone, "$AUTH" is Authenticated user and "username" match the authentified user username
ex:
$ACCESS_LIST['api-ro'][] = '*'; // Give every access to the api in read only
$ACCESS_LIST['webgui'][] = 'marian'; // Give marian access to the webgui

III - API
=========

Functions provided by the API
    *robinhood
        -return a Robinhood quote (just for testing purpose)
    *test
        -return args as json (debug purposes)
    *current_auth
        -return your current authentification
    *grah/(owner/uid/gr_name/sizes/files/*_status)
        -return datas as json using graphjs datasets format
    *data/(owner/uid/gr_name/sizes/files/*_status)
        -return datas as json using datatables.js format
    *native/SELECT:field=TYPE!.../FILTER:field=val!/GROUP:uid!gid/
	-return direct data from ACCT_STAT
		ex: native/acct/SELECT:uid=VAL!gid=CONCAT/GROUP:uid return a list of user groups
		ex: native/acct/SELECT:uid=VAL!size=SUM/GROUP:uid return the total size used by uid	

IV - TODO
=========

*Fix label/color on graph
*Replacement for chartjs ?
*Remove older structure database references (owner, gr_name, ...)
*Set proprer format to datatable columns
*Fix issue with datable refresh when changing data fast 

V - Changelog
==============

v0.1 @ 20/07/2016:
   -First version (beta)

