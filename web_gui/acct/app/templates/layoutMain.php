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
            @import "app/templates/css/tab.css";
            @import "app/templates/css/simple_table.css";
            @import "app/templates/css/popup.css";
            @import "app/templates/css/form.css";
            @import "app/templates/css/smoothness/jquery-ui-1.8.4.custom.css";
        </style>
        <script type="text/javascript" src="jQuery/jquery.tools.min.js"></script>
        <script type="text/javascript" src="jQuery/DataTable/media/js/jquery.dataTables.js"></script>
        <script type="text/javascript" charset="utf-8">
            var oTable;
            $(document).ready(function() {
                /*$('#example tr').click( function() {
                    var aData = oTable.fnGetData( this );
                    alert(aData[1]);
                });*/
                oTable = $('#jQueryTable').dataTable({
                    "bJQueryUI": true,
                    "sPaginationType": "full_numbers",
                });
            });
        </script>

        <!-- Test popup -->
        <script>
            $(function() {
                // if the function argument is given to overlay,
                // it is assumed to be the onBeforeLoad event listener
                $("a[rel]").overlay({

                    mask: 'darkred',
                    effect: 'apple',

                    onBeforeLoad: function() {

                        // grab wrapper element inside content
                        var wrap = this.getOverlay().find(".contentWrap");

                        // load the page specified in the trigger
                        wrap.load(this.getTrigger().attr("href"));
                    }

                });
            });
        </script>
        <!-- Fin test -->

        <!-- Menu -->
        <script type="text/javascript">
            $(document).ready(function () {

                $("#menu ul li a").mouseover(function () {
                    $(this).css("background-color","#FFFFFF");
                    $(this).stop().animate({ paddingLeft: "20px" }, 500 );
                });

                $("#menu ul li a").mouseout(function () {
                    $(this).css("background-color","#ECEFF5");
                    $(this).stop().animate({ paddingLeft: "4px" }, 500 );
                });

            });
        </script>

        <script type="text/javascript">
            $(function() {
                $("ul.tabs").tabs("div.panes > div", {effect: 'fade', fadeOutSpeed: 400});
            });
        </script>

        <title>Robinhood Report <?php echo $fsname; ?></title>

    </head>

    <body>

        <div id="top_panel">
            <img src="app/img/logo_rh_sf.gif" alt="Logo Robinhood"/>
            <h1>Robinhood Policy Engine</h1>
            <img src="app/img/logoCEA20.jpg" alt="Logo CEA"/>
        </div>
        <div align="center"><h3><?php echo $fsname; ?></h3></div>

        <div id="left_panel">
            <div id="menu">
                <ul>
                    <?php
                    foreach( $menu as $section )
                    {
                        echo "<li><a href='".strtolower($section).".php'>".$section."</a></li>";
                    }
                    ?>
                    <li><a href='search.php'>Search</a></li>
                </ul>
            </div>
        </div>

        <div id="right_panel">
            <?php echo $content; ?>
        </div>

    </body>
</html>
