<?php
/*
 * Copyright (C) 2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

require_once("robinhood.php");
require_once("../common.php");
require_once("../plugin.php");

if (!check_access("api-ro"))
    return "Permission denied";

if (!array_key_exists('request', $_REQUEST)) {
    $_REQUEST['request']="";
}

try {
    plugins_call("api_preprocess", $_REQUEST['request']);
    $API = new MyAPI($_REQUEST['request']);
    echo $API->processAPI();
} catch (Exception $e) {
    echo json_encode(Array('error' => $e->getMessage()));
}

?>
