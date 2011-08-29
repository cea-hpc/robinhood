<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class SearchManager
{
    private $db_request;
    private $rowNumber;

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
    * This method returns the result of the mysql query
    * @return db_result
    */
    public function getStatistics( $user, $group, $path )
    {
        $db_result = $this->db_request->select( array( OWNER => $user, GROUP => $group, PATH => $path), MAIN_TABLE, null, null, null, MAX_SEARCH_RESULT );
        $this->rowNumber = $this->db_request->getRowNumber();
        return $db_result;
    }
   /**
    * This method returns the row number of the last query executed
    * @return rowNumber
    */
    public function getRowNumber()
    {
        return $this->rowNumber;
    }
}
?>
