<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class Router extends ApplicationComponent
{

   /**
    * This method returns the controller which matches the URL
    * @return  controller
    */
    public function getController()
    {
        if( !file_exists( dirname(__FILE__)."/../app/config/database.xml" ) )
        {
            require dirname(__FILE__).'/../app/modules/configuration/ConfigurationController.class.php';
            $controller = new ConfigurationController( $this->getApplication(), "configuration", "blank" );
        }
        else
        {
            $dom = new DOMDocument();
            if( $dom->load( 'app/config/routes.xml' ) )
            {
                $routesList = $dom->getElementsByTagName('route');

                foreach( $routesList as $route )
                {
                    if( $route->hasAttribute( 'url' ) )
                    {
                        if( preg_match( '`^.*/'.$route->getAttribute( "url" ).'$`', $this->getApplication()->getRequest()->getURL(), $matches))
                        {
                            $module = $route->getAttribute( "module" );
                            $type = $route->getAttribute( "type" );

                            $controller_name = ucfirst($module).'Controller';
                            $controller_file = dirname(__FILE__).'/../app/modules/'.$module.'/'.ucfirst($module).'Controller.class.php';
                            if (!file_exists($controller_file))
                                throw new Exception( "Module unreachable: ".$controller_file." does not exist" );
                            else
                                require $controller_file;

                            $controller = new $controller_name( $this->getApplication(), $module, $type );

                            //if there are $_GET variables
                            if ( $route->hasAttribute('vars') )
                            {
                                $vars = explode(',', $route->getAttribute('vars'));
                                
                                foreach ($matches as $key => $match)
                                {
                                    if ($key !== 0)
                                    {
                                        $this->getApplication()->getRequest()->addGetData( $vars[$key-1], $match );
                                    }
                                }
                            }
                            break;
                        }
                    }
                    else
                    {
                        throw new Exception( "Invalid XML file: Each route element must have an url attribute" );
                    }
                }
            }
        }
        if ( !isset( $controller ) )
        {
            $this->getApplication()->getResponse()->redirect404();
        }
        return $controller;
    }
    
}

?>
