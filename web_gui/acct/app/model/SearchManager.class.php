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
        # added by azet@azet.org (18.6.2013):
        # escape _POST input to prevent SQL injection. 
        # using PDO::quote would be nice, but look at that select function m(
        $user_safe  = mysql_real_escape_string($user);
        $group_safe = mysql_real_escape_string($group);
        $path_safe  = mysql_real_escape_string($path);

        $db_result = $this->db_request->select(array(OWNER => $user_safe,
                                GROUP => $group_safe, PATH => $path_safe),
                                MAIN_TABLE, null, null, null, MAX_SEARCH_RESULT);
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
