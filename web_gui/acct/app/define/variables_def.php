<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

//Number of users/groups/status to display in count and size section
define("LIMIT" , 4);

//Define tables
define("ACCT_TABLE" , "ACCT_STAT");
define("MAIN_TABLE" , "ENTRIES");

//Define acct table fields
define("OWNER" , "owner");
define("GROUP" , "gr_name");
define("TYPE" , "type");
define("BLOCKS" , "blocks");
define("SIZE" , "size");
define("COUNT" , "count");
define("STATUS" , "status");

//Define entries table fields
define("PATH" , "fullpath");

// system block size
define("DEV_BSIZE", 512);

?>
