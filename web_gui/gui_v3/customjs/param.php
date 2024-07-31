<?php
/*
 * Copyright (C) 2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

include("../config.php");
include("../common.php");
include("../plugin.php");

// Firefox doesn't know what to do with a php file in <script> tag without a content-type header set.
header('Content-Type: application/javascript');

foreach($CHARTJS as $conf => $val)
{
        echo "$conf=$val;\n";
}

echo plugins_call("jscript", "<!--Javascript from plugins-->\n");
