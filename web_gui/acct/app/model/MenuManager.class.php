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
     * this returns FS name
     */
    public function getfsname()
    {
        $db_result = $this->db_request->select( array( VARNAME => 'FS_Path' ), VAR_TABLE, null, null );
        if ($db_result != null)
        {
            foreach( $db_result as $line )
            {
                return $line[VARVALUE];
            }
        }
    }

   /**
    * This method returns an array containing ACCT table fields
    * @return associative array
    */
    public function getSections()
    {
        global $sz_range_fields;
        $menu = array();
        $db_result = $this->db_request->getSchema( ACCT_TABLE );
        foreach( $db_result as $field => $value )
        {
            switch( $field )
            {
                case OWNER:
                    $menu[] = 'Users';
                    break;
                case GROUP:
                    $menu[] = 'Groups';
                    break;
                case STATUS:
                    $menu[] = 'Status';
                    break;
                case $sz_range_fields[0]:
                    $menu[] = 'Sizes';
                    break;
                default;
                    break;
            }
        } 
        return $menu;
    }

}
?>

