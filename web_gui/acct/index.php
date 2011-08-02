<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
    //ini_set("output_buffering",8989 );
    //ini_set("memory_limit","1G");
    require 'lib/Application.class.php';
    require 'app/define/variables_def.php';
    require 'app/define/functions_def.php';
    
    $app = new Application();
    $app->run();

?>
