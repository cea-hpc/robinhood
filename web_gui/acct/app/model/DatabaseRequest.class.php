<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

/**
 * This class contains methods to execute requests on the database 
 */
class DatabaseRequest
{
    private $connection;
    private $dbms;
    private $host;
    private $dbname;
    private $user;
    private $password;
    private $rbh_mode;
    private $rowNumber;

   /**
    * The constructor creates a new database connection
    * @param string $config_file
    */
    public function __construct( $config_file_path )
    {
        $this->getParametersFromConfigFile( $config_file_path );
        $this->connection = new PDO($this->dbms.':host='.$this->host.';dbname='.$this->dbname, $this->user, $this->password );
        $this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    public function closeConnection()
    {
        $connection = null;
    }

   /**
    * This method get connection parameters from database.xml.
    */
    private function getParametersFromConfigFile( $config_file_path )
    {
        $dom = new DOMDocument();
        $dom->load( $config_file_path );
        
        $dbms = $dom->getElementsByTagName('dbms')->item(0);
        if( $dbms )
            $this->dbms = $dbms->nodeValue;
        else
            $this->dbms = "mysql";

        $host = $dom->getElementsByTagName('host')->item(0);
        if( $host )
            $this->host = $host->nodeValue;
        else
            $this->host = "localhost";

        $dbname = $dom->getElementsByTagName('dbname')->item(0);
        if( $dbname )
            $this->dbname = $dbname->nodeValue;
        else
            throw new Exception( "You must specify the database name in the config file" );

        $user = $dom->getElementsByTagName('user')->item(0);
        if( $user )
            $this->user = $user->nodeValue;
        else
            $this->user = "root";

        $password = $dom->getElementsByTagName('password')->item(0);
        if( $password )
            $this->password = $password->nodeValue;
        else
            $this->password = '';

        $flavor = $dom->getElementsByTagName('flavor')->item(0);
        if( $flavor )
            $this->rbh_mode = $flavor->nodeValue;
        else
            $this->rbh_mode = "tmp_fs_mgr";
    }
    
   /**
    * This method returns an associative array containing the fields of the specified table as key and the field type as value.
    * @param table
    * @return schema
    */ 
    public function getSchema( $table )
    {
       	$schema = array();
        try{
            $result = $this->connection->query( "DESCRIBE ".$table );
            $result->setFetchMode(PDO::FETCH_OBJ);
            while( $line = $result->fetch() )
            {
                $schema[$line->Field] = $line->Type;
            }
            $result->closeCursor();
        }
        catch( PDOException $e )
        {
            #echo 'Error: '.$e->getMessage().'</br>';
	    $schema = null;
        }
        return $schema;
    }

   /**
    * This method executes a select query.
    * @param filter associative array containing the table field as key and a value.
    * @param table
    * @param groupby array containing group by fields.
    * @param orderby field to sort
    * @param order ( desc or asc )
    * @return A double array containing the query result.
    */
    public function select( $filter, $table, $groupby, $orderby, $order='desc', $limit=0 )
    {
        $filter_str = "";
        $groupby_str = "";
        $field_str = "";
        $is_first = TRUE;
	$join_annex = 0;
        $i = 0;
	if ( $table == MAIN_TABLE )
		$annex_schema = $this->getSchema( ANNEX_TABLE );
	else
		$annex_schema = null;

        if( $filter )
        {
            foreach( $filter as $field => $value ) 
            {
		if (($annex_schema != null) && (in_array($field, array_keys($annex_schema))))
			# need to join on annex table
			$join_annex = 1;

                if( $value != "")
                {
                    if( preg_match( '`^.*(\*|\?).*$`', $value ) )
                    {
                        $value = str_replace( '*', '%', $value );
                        $value = str_replace( '?', '_', $value );
                        $filter_str = $filter_str.($is_first ? '' : ' AND ').$field.' LIKE \''.$value.'\'';
                    }
                    else
                    {
                        $filter_str = $filter_str.($is_first ? '' : ' AND ').$field.'=\''.$value.'\'';
                    }
                    $is_first = FALSE;
                }
            }
        }

        if( $groupby )
        {
            $is_first = TRUE;
            foreach( $groupby as $field )
            {
		if (($annex_schema != null) && (in_array($field, array_keys($annex_schema))))
			# need to join on annex table
			$join_annex = 1;

                $groupby_str = $groupby_str.($is_first ? '' : ',').$field;
                $is_first = FALSE;
            }
        }

	/* /!\ is there a field in ANNEX_INFO? */

        $is_first = TRUE;
        foreach( $this->getSchema( $table ) as $field => $type )
        {
            if( substr_count( $type, "int" ) != 0 && $groupby && $field != "status" ) //TODO status case (no meaning here)
                $field_str .= ( $is_first ? '' : ', ' )." SUM(".($join_annex ? "$table." : "").$field.")";
            else
                $field_str .= ( $is_first ? '' : ', ' ).($join_annex ? "$table." : "").$field;
            $is_first = FALSE;
        }
	if ($join_annex) {
	        foreach( $this->getSchema( ANNEX_TABLE ) as $field => $type )
		{
		    if( substr_count( $type, "int" ) != 0 && $groupby && $field != "status" ) //TODO status case (no meaning here)
			$field_str .= ( $is_first ? '' : ', ' )." SUM(".($join_annex ? ANNEX_TABLE."." : "").$field.")";
		    else
			$field_str .= ( $is_first ? '' : ', ' ).($join_annex ? ANNEX_TABLE."." : "").$field;
		    $is_first = FALSE;
		}
	}

        try
        {
	    if ($join_annex)
		    $query = "SELECT ".$field_str." FROM ".$table." LEFT JOIN ".ANNEX_TABLE." ON $table.id = ".ANNEX_TABLE.".id ".
				( $filter ? " WHERE ".$filter_str : "" ).
				( $groupby ? " GROUP BY ".$groupby_str : "" ).( $orderby ? " ORDER BY ".$orderby." ".$order : "" ).
				( $limit > 0 ? " LIMIT $limit" : "" );
	    else
		    $query = "SELECT ".$field_str." FROM ".$table.( $filter ? " WHERE ".$filter_str : "" ).
				( $groupby ? " GROUP BY ".$groupby_str : "" ).( $orderby ? " ORDER BY ".$orderby." ".$order : "" ).
				( $limit > 0 ? " LIMIT $limit" : "" );

            $result = $this->connection->query( $query );
            $this->rowNumber = $result->rowCount();

            if( $this->rowNumber > MAX_SEARCH_RESULT || $this->rowNumber == 0)
            {
                return null;
            }

            while( $line = $result->fetch( PDO::FETCH_ASSOC ) )
            {
                $returned_result[$i] = $line;
                $i++;
            }
            $result->closeCursor();

            return $returned_result;
        }
        catch( PDOException $e )
        {
            echo 'Error: '.$e->getMessage().'</br>';
        }
    }

    public function getRowNumber(){
        return $this->rowNumber;
    }

    public function statusName( $st_num )
    {
	global $hsm_status_tab ;
	global $sherpa_status_tab;
	global $hsmlite_status_tab ;

	switch( $this->rbh_mode )
	{
		case "tmp_fs_mgr":
			echo 'Error: unexpected status for robinhood flavor '.$this->rbh_mode.'<br>';
			return "Unknown status ".$st_num;
		case "backup":
		case "shook":
			return $hsmlite_status_tab[$st_num];
		case "hsm":
			return $hsm_status_tab[$st_num];
		case "sherpa":
			return $sherpa_status_tab[$st_num];
		default:
			echo 'Error: unknown robinhood flavor '.$this->rbh_mode.'<br>';
			return "Unknown status ".$st_num;
	}
    }

    public function statusIndex( $st_str )
    {
	global $hsm_status_tab ;
	global $sherpa_status_tab;
	global $hsmlite_status_tab ;

	switch( $this->rbh_mode )
	{
		case "tmp_fs_mgr":
			return -1;
		case "backup":
		case "shook":
			$arr = $hsmlite_status_tab;
			break;
		case "hsm":
			$arr = $hsm_status_tab;
			break;
		case "sherpa":
			$arr = $sherpa_status_tab;
			break;
		default:
			return -1;
	}

	$i=-1;
	foreach ($arr as $st)
	{
		$i++;
		if ($st == $st_str)
			return $i;
	}
	return -1;
    }

}

?>
