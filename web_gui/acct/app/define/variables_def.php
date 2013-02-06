<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

//Number of users/groups/status to display in count and size section
define("LIMIT" , 7);

//Define tables
define("ACCT_TABLE" , "ACCT_STAT");
define("MAIN_TABLE" , "ENTRIES");
define("ANNEX_TABLE" , "ANNEX_INFO");
define("VAR_TABLE" , "VARS");

//Define acct table fields
define("OWNER" , "owner");
define("GROUP" , "gr_name");
define("TYPE" , "type");
define("BLOCKS" , "blocks");
define("SIZE" , "size");
define("COUNT" , "count");
define("STATUS" , "status");

define("VARNAME", "varname");
define("VARVALUE", "value");

//Define entries table fields
define("PATH" , "fullpath");

// system block size
define("DEV_BSIZE", 512);

// max search results
define("MAX_SEARCH_RESULT", 5000);

$hsmlite_status_tab = array( "unknown", "new", "modified", 
	     		     "retrieving", "archiving",
        		     "synchro", "released", "release_pending",
			     "removed" );
$hsm_status_tab = array( "unknown", "new", "modified",
			 "retrieving", "archiving",
			 "synchro", "released", "release_pending" );

$sz_range_fields = array( "sz0", "sz1", "sz32", "sz1K", "sz32K", "sz1M", "sz32M", "sz1G", "sz32G", "sz1T" );
$sz_range_name = array( "0", "1~31", "32~1K", "1K~31K", "32K~1M", "1M~31M", "32M~1G", "1G~31G", "32G~1T", "+1T" );


?>
