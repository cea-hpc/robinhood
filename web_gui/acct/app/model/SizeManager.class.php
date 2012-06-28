<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class SizeManager
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
    public function getSizeProfile()
    {
        global $sz_range_fields;

        $count = array();
        $size = array();
        $blks = array();
        $ranges = array();
        $db_result = $this->db_request->select( array(TYPE => 'file'), ACCT_TABLE, array(TYPE), null );

        foreach( $db_result as $line )
        {
            foreach($sz_range_fields as $field)
            {
                $ranges[$field] = $line['SUM('.$field.')'];
            }
        }
        return $ranges;
    }

/**
    * This method returns Statistics object to create the pie graph or the status list
    * @return Statistics
    */
    public function getSizeProfileUsers($user = null)
    {
        global $sz_range_fields;

        $count = array();
        $size = array();
        $blks = array();
        $users = array();
        if ($user == null)
            $db_result = $this->db_request->select( array(TYPE => 'file'), ACCT_TABLE, array(OWNER), COUNT );
        else
            $db_result = $this->db_request->select( array(TYPE => 'file', OWNER => $user), ACCT_TABLE,  array(GROUP), null);

        foreach( $db_result as $line )
        {
            if ($user == null)
                $u=$line[OWNER];
            else
                $u=$line[GROUP];

            if (isset($users[$u]))
                $users[$u] = array();

            if (isset($users[$u][SIZE]))
                $users[$u][SIZE] += $line['SUM('.SIZE.')'];
            else
                $users[$u][SIZE] = $line['SUM('.SIZE.')'];

            if (isset($users[$u][COUNT]))
                $users[$u][COUNT] += $line['SUM('.COUNT.')'];
            else
                $users[$u][COUNT] = $line['SUM('.COUNT.')'];

            if (isset($users[$u][BLOCKS]))
                $users[$u][BLOCKS] += $line['SUM('.BLOCKS.')'];
            else
                $users[$u][BLOCKS] = $line['SUM('.BLOCKS.')'];

            foreach($sz_range_fields as $field)
            {
                if (isset($users[$u][$field]))
                    $users[$u][$field] += $line['SUM('.$field.')'];
                else
                    $users[$u][$field] = $line['SUM('.$field.')'];
            }
        }
        return $users;
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
