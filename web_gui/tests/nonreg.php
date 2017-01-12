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


function check_error() {
        $err=error_get_last();
        switch($err['type']) {
        case E_ERROR: // 1 //
        case E_WARNING: // 2 //
                //Let's ignore the config_local warning
                if (startsWith($err['message'],"include(): Failed opening"))
                        return 0;
        case E_PARSE: // 4 //
        case E_NOTICE: // 8 //
        case E_CORE_ERROR: // 16 //
        case E_CORE_WARNING: // 32 //
        case E_COMPILE_ERROR: // 64 //
        case E_COMPILE_WARNING: // 128 //
        case E_USER_ERROR: // 256 //
        case E_USER_WARNING: // 512 //
        case E_USER_NOTICE: // 1024 //
        case E_STRICT: // 2048 //
        case E_RECOVERABLE_ERROR: // 4096 //
        case E_DEPRECATED: // 8192 //
        case E_USER_DEPRECATED: // 16384 //
                break;
        }
        print "[ERROR] Someting goes wrong:\n";
        print_r($err);
        exit($err['type']); 
}

$cwd = getcwd();


echo "[NOTICE] include API\n";
chdir("../gui_v3/api");
require_once("robinhood.php");
check_error();

chdir("..");
echo "[NOTICE] include Common\n";
require_once("common.php");
check_error();

echo "[NOTICE] Process API\n";
$API = new MyAPI("robinhood");
$result =  $API->processAPI();
if ($result != '"\"Fear not, my friends. This will be my greatest performance.\""') {
        echo "API is broken, return $result instead of status";
        echo "[ERROR] API Failed\n";
        exit(1);
}
check_error();
/* Check index.php */
echo "[NOTICE] Process index.php\n";
ob_start();
require_once("index.php");
$data = ob_get_clean();
check_error();

/* TODO check database connexion */
/* TODO check natives/vars       */

chdir($cwd);
echo "Success\n";
?>
