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
