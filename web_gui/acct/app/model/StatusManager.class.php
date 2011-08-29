<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class StatusManager
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
    * This method returns Statistics object to create the pie graph or the status list
    * @return Statistics
    */
    public function getStat()
    {
        $count = array();
        $size = array();
        $blks = array();
        $db_result = $this->db_request->select( null, ACCT_TABLE, array(STATUS), null );

        $stat = new Statistics();

        foreach( $db_result as $line )
        {
	    $stname = $this->db_request->statusName($line[STATUS]);
            $count[$stname] = $line['SUM('.COUNT.')'];
            $size[$stname] = $line['SUM('.SIZE.')'];
            $blks[$stname] = $line['SUM('.BLOCKS.')'];
        }
        $stat->setSize( $size );
        $stat->setBlocks( $blks );
        $stat->setCount( $count );

        return $stat;
    }

   /**
    * This method returns detailed statistics for a specific status
    * @param status
    * @return db_result
    */
    public function getDetailedStat( $status, $sort )
    {
	$st_idx = $this->db_request->statusIndex( $status );
        $db_result = $this->db_request->select( array( STATUS => $st_idx ), ACCT_TABLE, null, $sort );
        return $db_result;
    }

   /**
    * This method returns an associative array containing the schema of ACCT table
    * @return array
    */
    public function getAcctSchema()
    {
        return $this->db_request->getSchema( ACCT_TABLE );
    }

}
?>
