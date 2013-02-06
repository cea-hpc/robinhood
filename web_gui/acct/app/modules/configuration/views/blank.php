<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>

<?php
if( isset( $file_created ) && $file_created == TRUE )
{
?>
    <script language="javascript" type="text/javascript">
        window.location.replace("index.php");
    </script>
<?php
}
?>

<form method="post" id="form">
    <fieldset>
        <h3>Configuration</h3>

        <?php 
        if( isset( $file_created ) )
        {
            echo "<p id='error'><b> Error: Database connection failed </b></p>";
        }
        ?>
        <p> Enter database configuration </p>
      
        <p>
            <label>Host (localhost by default)</label>
            <input type="text" name="host" />
        </p>
        <p>
         <label>Database name *</label>
         <input type="text" name="db_name" required="required" />
      </p>
      <p>
         <label>User name *</label>
         <input type="text" name="user_name" required="required" />
      </p>
      <p>
         <label>Password</label>
         <input type="password" name="password" />
      </p>

      <p>
         <label>Robinhood mode:</label>
	 <select name="flavor">
		<OPTION VALUE="tmp_fs_mgr" SELECTED>robinhood-tmp_fs_mgr</OPTION>
		<OPTION VALUE="backup">robinhood-backup</OPTION>
		<OPTION VALUE="hsm">robinhood-hsm</OPTION>
		<OPTION VALUE="shook">robinhood-shook</OPTION>
	 </select>
      </p>
      
      <button type="submit">Submit</button>
      <button type="reset">Reset</button>

   </fieldset>

</form>


<script>
$("#form").validator();
</script>
