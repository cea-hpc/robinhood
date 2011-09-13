<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class GroupManager
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
    * This method returns Statistics object to create the pie graph or the user list
    * @return Statistics
    */
    public function getStat()
    {
        $count = array();
        $size = array();
        $blks = array();
        $db_result = $this->db_request->select( null, ACCT_TABLE, array(GROUP), null );

        $stat = new Statistics();

        foreach( $db_result as $line )
        {
            if ($line[GROUP] && $line['SUM('.COUNT.')'] && $line['SUM('.SIZE.')'] )
            {
                $count[$line[GROUP]] = $line['SUM('.COUNT.')'];
                $size[$line[GROUP]] = $line['SUM('.SIZE.')'];
                $blks[$line[GROUP]] = $line['SUM('.BLOCKS.')'];
            }
        }

        $stat->setSize( $size );
        $stat->setBlocks( $blks );
        $stat->setCount( $count );

        return $stat;
    }

   /**
    * This method returns detailed statistics for a specific group
    * @param group
    * @return db_result
    */
    public function getDetailedStat( $group, $sort )
    {
        $db_result = $this->db_request->select( array( GROUP => $group ), ACCT_TABLE, null, $sort );
	# translate status index into status name
        foreach( $db_result as  $key => $line )
	{
		if (isset($line[STATUS])) {
			$db_result[$key][STATUS] = $this->db_request->statusName($line[STATUS]);
		}
	}
        return $db_result;
    }

   /**
    * This method returns an associative array containing the schema of ACCT table
    * @return an array
    */
    public function getAcctSchema()
    {
        return $this->db_request->getSchema( ACCT_TABLE );
    }

}
?>
