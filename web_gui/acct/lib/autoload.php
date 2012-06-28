<?php
    require_once 'ezcomponents/Base/src/base.php';
    spl_autoload_register( array( 'ezcBase', 'autoload' ) );

    function autoload($class)
    {
        $classes = array (
            'Router' => '../lib/Router.class.php',
            'Page' => '../lib/Page.class.php',
            'Request' => '../lib/Request.class.php',
            'Response' => '../lib/Response.class.php',
            'ApplicationComponent' => '../lib/ApplicationComponent.class.php',
            'Controller' => '../lib/Controller.class.php',
            'Application' => '../lib/Application.class.php',
            'UserManager' => '../app/model/UserManager.class.php',
            'DatabaseRequest' => '../app/model/DatabaseRequest.class.php',
            'StatusManager' => '../app/model/StatusManager.class.php',
            'SearchManager' => '../app/model/SearchManager.class.php',
            'ConfigurationManager' => '../app/model/ConfigurationManager.class.php',
            'GroupManager' => '../app/model/GroupManager.class.php',
            'SizeManager' => '../app/model/SizeManager.class.php',
            'Statistics' => '../app/model/Statistics.class.php',
            'MenuManager' => '../app/model/MenuManager.class.php',
            'GroupController' => '../app/modules/group/GroupController.class.php',
            'StatusController' => '../app/modules/status/StatusController.class.php',
            'ConfigurationController' => '../app/modules/configuration/ConfigurationController.class.php',
            'SizeController' => '../app/modules/size/SizeController.class.php',
            'UserController' => '../app/modules/user/UserController.class.php',
            'SearchController' => '../app/modules/search/SearchController.class.php'
	    );
        require dirname(__FILE__).'/'.$classes[$class];
    }
    spl_autoload_register('autoload');
?>
