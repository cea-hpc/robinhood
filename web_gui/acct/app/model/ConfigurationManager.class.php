<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class ConfigurationManager
{

   /**
    * This method returns true if a connection can be established
    * @return boolean
    */
    public function checkConnection()
    {
        try
        {
            $db_request = new DatabaseRequest( 'app/config/database.xml' );
            return TRUE;
        }
        catch( PDOException $e )
        {
            return FALSE;
        }
    }

   /**
    * This method returns the home page module
    * @return boolean
    */
    public function getHomePage()
    {
        $db_request = new DatabaseRequest( 'app/config/database.xml' );
        $db_result = $db_request->getSchema( ACCT_TABLE );
        print_r ($db_result);
        foreach( $db_result as $homePage => $value )
        {
            switch( $homePage )
            {
                case OWNER;
                    return 'user';
                    break;
                case GROUP;
                    return 'group';
                    break;
                case STATUS;
                    return 'status';
                    break;
                default;
                    break;
            }
        }
        return null;
    }
}
?>
