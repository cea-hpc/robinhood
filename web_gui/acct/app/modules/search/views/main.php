<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

if( ( $page == 'result' && $result == null ) || $page == 'form' )
{
?>    
    <form method="post" id="form">
        <fieldset>
            <?php
            if( isset( $rowNumber ) && $rowNumber > MAX_SEARCH_RESULT )
            {
                echo "<p id='error'>The result is too large to be displayed.<br/> Please, specify other filters.</p>";
            }
            else if( isset( $rowNumber ) && $rowNumber == 0 )
            {
                 echo "<p id='error'>No result available.</p>";
            }
            ?>
            <p>
                <label>User</label>
                <input type="text" name="user" />
            </p>
            <p>
             <label>Group</label>
             <input type="text" name="group"/>
          </p>
          <p>
             <label>Path</label>
             <input type="text" name="path"/>
          </p>

          <button type="submit">Submit</button>
          <button type="reset">Reset</button>

       </fieldset>

    </form>
        
<?php  
}
else
{

	if( isset( $rowNumber ) && $rowNumber == MAX_SEARCH_RESULT )
	{
		echo "<p id='error'>NOTICE: too many results!<br/>Display has been truncated to 500 entries.</p>";
	}

?>

<table cellpadding="0" cellspacing="0" border="0" class="display" id="jQueryTable">
    <thead>
        <tr>
            <th>User</th>
            <th>Group</th>
            <th>Path</th>
            <th>Size</th>
        </tr>
    </thead>
    <tbody>
        <?php
        foreach( $result as $line )
        {
            echo "<tr class='gradeB'>";
                echo "<td>".$line[OWNER]."</td>";
                echo "<td>".$line[GROUP]."</td>";
                echo "<td>".$line[PATH]."</td>";
                echo "<td>".$line[SIZE]."</td>";
            echo "</tr>";
        }
        ?>
    </tbody>
</table>

<?php
}
?>

