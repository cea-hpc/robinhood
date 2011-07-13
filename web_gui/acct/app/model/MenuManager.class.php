<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class MenuManager
{
    private $db_request;

    public function __construct()
    {
        $this->db_request = new DatabaseRequest( 'app/config/database.xml' );
    }

   /**
    * This method returns an array containing ACCT table fields
    * @return associative array
    */
    public function getSections()
    {
        $menu = array();
        $db_result = $this->db_request->getSchema( ACCT_TABLE );
        foreach( $db_result as $field => $value )
        {
            switch( $field )
            {
                case OWNER;
                    $menu[] = 'Users';
                    break;
                case GROUP;
                    $menu[] = 'Groups';
                    break;
                case STATUS;
                    $menu[] = 'Status';
                    break;
                default;
                    break;
            }
        } 
        return $menu;
    }

}
?>

