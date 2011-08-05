<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/ConfigurationManager.class.php";

class ConfigurationController extends Controller
{
    public function executeBlank()
    {
        if( $this->getApplication()->getRequest()->hasPostValues() == TRUE )
        {
            $this->createDatabaseConfigFile();

            $configurationManager = new ConfigurationManager();
            if( $configurationManager->checkConnection() == TRUE )
            {
                $this->updateRoutesFile( $configurationManager->getHomePage() );
                $this->page->addVar( 'file_created' , TRUE );
            }
            else
            {
                 $this->page->addVar( 'file_created' , FALSE );
                 unlink ( "app/config/database.xml" );
            }                
        }
    }

   /**
    * This function add an xml node to a specific parent
    */
    private function addNode( $dom, $parent, $element, $value )
    {
        $node = $dom->createElement( $element );
        $node_value = $dom->createTextNode( $value );
        $parent->appendChild( $node );
        $node->appendChild( $node_value );
    }

   /*
    * This function creates the database configuration file
    */
    private function createDatabaseConfigFile()
    {
        //Create database.xml file
        //$conf_file = fopen( "app/config/database.xml", "w" );
        $dom_db = new DomDocument();
        $database = $dom_db->createElement("database");
        $dom_db->appendChild($database);

        if( $this->getApplication()->getRequest()->getPostData("dbms") )
        {
            $this-> addNode( $dom_db, $database, "dbms",
                    $this->getApplication()->getRequest()->getPostData("dbms") );
        }

        if( $this->getApplication()->getRequest()->getPostData("host") )
        {
            $this-> addNode( $dom_db, $database, "host",
                    $this->getApplication()->getRequest()->getPostData("host") );
        }

        if( $this->getApplication()->getRequest()->getPostData("db_name") )
        {
            $this-> addNode( $dom_db, $database, "dbname",
                    $this->getApplication()->getRequest()->getPostData("db_name") );
        }

        if( $this->getApplication()->getRequest()->getPostData("user_name") )
        {
            $this-> addNode( $dom_db, $database, "user",
                    $this->getApplication()->getRequest()->getPostData("user_name") );
        }

        if( $this->getApplication()->getRequest()->getPostData("password") )
        {
            $this-> addNode( $dom_db, $database, "password",
                    $this->getApplication()->getRequest()->getPostData("password") );
        }
        if( $this->getApplication()->getRequest()->getPostData("flavor") )
        {
            $this-> addNode( $dom_db, $database, "flavor",
                    $this->getApplication()->getRequest()->getPostData("flavor") );
        }

        $dom_db->formatOutput = TRUE;
        $dom_db->save( 'app/config/database.xml' );
	
	chmod('app/config/database.xml', 0640);
    }

   /*
    * This function update the routes file with a node containing the home page (index.php)
    */
    private function updateRoutesFile( $homePage )
    {
        $dom_routes = new DomDocument();
        $dom_routes->preserveWhiteSpace = FALSE;
        $dom_routes->load( 'app/config/routes.xml' );
        $root = $dom_routes->documentElement;
        $route_list = $dom_routes->getElementsByTagName("route");

        foreach( $route_list as $route )
        {
            //if the node already exists, remove it
            if ( $route->getAttribute("url") == "index.php" ) 
            {
                $root->removeChild( $route );
            }
        }

        $route = $dom_routes->createElement( "route" );
        $route->setAttribute( "url", "index.php" );
        $route->setAttribute( "module", $homePage );
        $route->setAttribute( "type", "main" );
        $root->appendChild( $route );

        $dom_routes->formatOutput = TRUE;
        $dom_routes->save( 'app/config/routes.xml' );

    }

    
}
