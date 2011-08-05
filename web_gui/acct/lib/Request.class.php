<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
class Request extends ApplicationComponent 
{
   /**
    * This method gets data from url
    */
    public function getGetData( $name )
    {
        if( isset( $_GET[$name] ) )
            return $_GET[$name];
        else
            return null;
    }

   /**
    * This method adds a new data from url
    */
    public function addGetData( $key, $value )
    {
        $_GET[$key] = $value;
    }

    public function getPostData($name)
    {
        if( isset( $_POST[$name] ) )
            return $_POST[$name];
        else
            return null;
    }

   /**
    * This method returns true if there are post values
    * @return boolean
    */
    public function hasPostValues()
    {
        if( sizeof( $_POST ) == 0 )
            return FALSE;
        else
            return TRUE;
    }

   /**
    * This method gets the current URL
    */
    public function getURL()
    {
        return $_SERVER['REQUEST_URI'];
    }

}

?>
