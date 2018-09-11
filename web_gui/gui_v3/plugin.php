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
    public $Name = "Generic";
    public $Description = "Something about your plugin";
    public $Version = "0";

    /* php lib required to run the plugin */
    public $Req_lib = array();
    /* plugin required by this plugin */
    public $Req_plug = array();

    /* Required table for plugin */
    /* *Create them if they don't exist */
    public $Req_table = array();

    /* pre_init status */
    const INIT_OK = 0;
    /* Non present php lib */
    const INIT_REQLIB = 1;
    /* Waiting for another plugin */
    const INIT_WAITPLUG = 2;
    /* The other plugin is missing */
    const INIT_MISSPLUG = 3;


    /* Called from UI and api */
    function pre_init() {
	global $db;
	global $DBA;
         /* Check if the plugin est loadable */
	foreach ($this->Req_lib as $lib) {
		if (!extension_loaded($lib)) {
			return $this::INIT_REQLIB;
		}
	}

	$confdb = getDB("config")[0];
	/* @TODO should failed if db doesn't work */
	foreach ($this->Req_table as $table=>$fields) {
		$result = $db[$confdb]->query("SELECT * FROM information_schema.columns WHERE (table_name = '$table') AND TABLE_SCHEMA = '".$DBA[$confdb]["DB_NAME"]."';");
		if ($result->rowCount()<1) {
			$db[$confdb]->query("CREATE TABLE IF NOT EXISTS $table $fields");
		}
	}

	return $this::INIT_OK;
    }

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

    /* Called from Common to identify the user */
    function get_user($param) {
	/* User identity */
    }

    /* Called from cron */
    function cron() {
	/* called each time cron.php is runned */
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
	$new_plugin = new $p();
	$init_result = $new_plugin->pre_init();
	if ($init_result==$new_plugin::INIT_REQLIB) {

	} elseif ($init_result==$new_plugin::INIT_OK) {
	        $PLUGINS_INST[] = $new_plugin;
	}

    }
}

/**
 *
 * Call specific function from plugins
 *
 * @return data
 */
function plugins_call($function, $param = NULL) {
   global $PLUGINS_INST;
   foreach ($PLUGINS_INST as $p) {
           $reflection = new ReflectionMethod($p, $function);
           if (count($reflection->getParameters()) == 0) {
                $p->$function();
        } else {
                $ret = $p->$function($param);
                if ($ret)
                    $param = $ret;
        }
   }
   return $param;
}

plugins_load();
plugins_call("init");
