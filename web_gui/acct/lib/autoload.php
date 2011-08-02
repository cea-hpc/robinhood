<?php
    require_once 'ezcomponents/Base/src/base.php';
    spl_autoload_register( array( 'ezcBase', 'autoload' ) );

    function autoload($class)
    {
        $classes = array (
		'Application' => '../lib/Application.class.php',
		'Request' => '../lib/Request.class.php',
		'ApplicationComponent' => '../lib/ApplicationComponent.class.php',
		'Router' => '../lib/Router.class.php',
		'Page' => '../lib/Page.class.php',
		'Controller' => '../lib/Controller.class.php',
		'Response' => '../lib/Response.class.php',
		'GroupController' => '../app/modules/group/GroupController.class.php',
		'UserController' => '../app/modules/user/UserController.class.php',
		'MainController' => '../app/modules/main/MainController.class.php',
		'DatabaseRequest' => '../app/model/DatabaseRequest.class.php',
		'Statistics' => '../app/model/Statistics.class.php',
		'MenuManager' => '../app/model/MenuManager.class.php',
		'UserManager' => '../app/model/UserManager.class.php',
		'GroupManager' => '../app/model/GroupManager.class.php'
	);
	require dirname(__FILE__).'/'.$classes[$class];
    }
    spl_autoload_register('autoload');
?>
