<?php
/*
 * Copyright (C) 2016-2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/* Plugin template */

class Plugin {
    public $Name="Generic";
    public $Description="Something about your plugin";
    public $Version="0";

    /* Called from UI and api */
    function init() {
            /* Plugin init */
    }

    /* Called from jscript customlib */
    function jscript($param) {
        /* JS Required by the plugin
         * Called from param.php  */
    }

    /* Called from api before processing data */
    function api_preprocess() {
        /* Just before the API args parsing
         * Called from api/robinhood.php */
    }

    /* Called from api just before processing data */
    function api_process($param) {
           /* Just after API args parsing
            * $param = parsed args */

    }

    /* Called from api when sending the header */
    function api_header_type($param) {
            /* Just before sending the data
             * it's the html header (file type, ...) */
    }

    /* Called from api just before sending data */
    function api_response($param) {
           /* Just before API send data */
           /* $param = data */
    }

    /* Custom api call */
    function api_native($param) {
           /* Custom api call
            * $param = (request, parsed args)
            * Called when reaching something still undeclared from api/native/foo
            */

    }

    /* Called from API before sending json data of uid/gid for graphs*/
    function graph_postdata_uid($param) {
            /* Just before sending uid graph data */
            /* Whole json as args, for compatibility */
    }

    /* Called from API before sending json data of sizes for graphs */
    function graph_postdata_sizes($param) {
            /* Just before sending sizes graph data */
            /* Whole json as args, for compatibility */
    }

    /* Called from API before doing initial SQL request for uid/gid graphs */
    function graph_presql_uid($param) {
            /* The sql request as string */
    }

    /* Called from API before doing initial SQL request for sizes graphs */
    function graph_presql_sizes($param) {
            /* The sql request as string */
    }

    /* Called at each sql request to add where clause for access control */
    function access_sql_filter($param) {
            /* The sql filter for clause, should start with " AND " */
    }

    /* Called from UI in form filter */
    function ui_form_filter($param) {
            /* HTML in the filter form */
    }

    /* Called from UI in menu */
    function ui_menu_bottom($param) {
            /* HTML */
    }

    /* Called from UI in menu */
    function ui_menu_top($param) {
            /* HTML */
    }

    /* Called from UI in menu */
    function ui_header($param) {
            /* HTML inside <header></header> */
    }
}

/**
 *
 * Load all the plugins
 *
 * @return nothing
 */
function plugins_load() {
    global $PLUGINS_INST;
    global $PLUGINS_REG;

    foreach ($PLUGINS_REG as $p) {
        require_once "plugins/$p/plugin.php";
        $PLUGINS_INST[]=new $p();
    }
}

/**
 *
 * Call specific function from plugins
 *
 * @return data
 */
function plugins_call($function, $param=NULL) {
   global $PLUGINS_INST;
   foreach ($PLUGINS_INST as $p) {
           $reflection = new ReflectionMethod($p, $function);
           if (count($reflection->getParameters())==0) {
                $p->$function();
        } else {
                $ret = $p->$function($param);
                if ($ret)
                    $param=$ret;
        }
   }
   return $param;
}

plugins_load();
plugins_call("init");
