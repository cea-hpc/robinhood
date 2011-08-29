<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr">
    <head>
        <meta http-equiv="Content-type" content="text/html; charset=iso-8859-1" />
        <style type="text/css" title="currentStyle">
            @import "app/templates/css/style.css";
            @import "app/templates/css/table_jui.css";
            @import "app/templates/css/smoothness/jquery-ui-1.8.4.custom.css";
        </style>

        <script type="text/javascript" src="jQuery/jquery.tools.min.js"></script>
        <script type="text/javascript" src="jQuery/DataTable/media/js/jquery.dataTables.js"></script>
        <script type="text/javascript" charset="utf-8">
            var oTable;
            $(document).ready(function() {
                oTable = $('#jQueryTable').dataTable({
                    "bJQueryUI": true,
                    "sPaginationType": "full_numbers",
                });
            });
        </script>

        <title>Robinhood Report <?php echo $fsname; ?></title>
    <head>
    <body>
        <div><?php echo $content; ?></div>
    </body>
</html>	
