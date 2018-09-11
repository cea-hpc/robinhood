<?php
/*
 * Copyright (C) 2018 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


require_once "config.php";
require_once "common.php";
require_once "plugin.php";

if ( (! PHP_SAPI === "cli") && $CONSOLE_CRON_ONLY == true) {
	echo "You are not allowed to run cron.php, please check config.php";
	exit(1);
}

plugins_call("init");


plugins_call("cron");

?>
