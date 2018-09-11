/*
 * Copyright (C) 2016-2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/*************************
 * Graph VAR
 ************************/
var GraphState="empty";
var TableState="empty";
var myChart;
var GraphCTX;
var lastGet="empty";

//Used for background data loading
var tableData;
var tableDataCount;
var tableDataTaskId=0;
var graphData;
var graphDataCount;
var graphDataTaskId=0;

Chart.pluginService.register({
    beforeRender: function (chart) {
        if (chart.config.options.showAllTooltips && (chart.config.type == "doughnut" || chart.config.type == "bar" || chart.config.type == "pie" )) {
            // create an array of tooltips
            // we can't use the chart tooltip because there is only one tooltip per chart
            chart.pluginTooltips = [];
            chart.config.data.datasets.forEach(function (dataset, i) {
                chart.getDatasetMeta(i).data.forEach(function (sector, j) {
                    chart.pluginTooltips.push(new Chart.Tooltip({
                        _chart: chart.chart,
                        _chartInstance: chart,
                        _data: chart.data,
                        _options: chart.options.tooltips,
                        _active: [sector]
                    }, chart));
                });
            });
            // turn off normal tooltips
            chart.options.tooltips.enabled = false;
        }
    },
        afterDraw: function (chart, easing) {
            if (chart.config.options.showAllTooltips) {
                // we don't want the permanent tooltips to animate, so don't do anything till the animation runs atleast once
                if (!chart.allTooltipsOnce) {
                    if (easing !== 1)
                        return;
                    chart.allTooltipsOnce = true;
                }

                // turn on tooltips
                chart.options.tooltips.enabled = true;
                Chart.helpers.each(chart.pluginTooltips, function (tooltip) {
                    tooltip.initialize();
                    tooltip.update();
                    // we don't actually need this since we are not animating tooltips
                    tooltip.pivot();
                    tooltip.transition(easing).draw();
                });
                chart.options.tooltips.enabled = false;
            }
        }
});

function formatBytes(bytes,decimals) {
    if(bytes == 0) return '0 Byte';
    var k = 1024;
    var dm = decimals + 1 || 3;
    var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatCount(bytes,decimals) {
    if(bytes == 0) return 'None';
    var k = 1000;
    var dm = decimals + 1 || 3;
    var sizes = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function getRGB(str){
    var hash = $.md5(str);
    var rgb = '#' + hash.substring(0,2) + hash.substring(2,4) + hash.substring(4,6);
    return rgb;
}

var stringToColour = function(str) {
    var hash = 0;
    str=str+str+str+str+str+str+str+str;
    for (var i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    var colour = '#';
    for (var i = 0; i < 3; i++) {
        var value = (hash >> (i * 8)) & 0xFF;
        colour += ('00' + value.toString(16)).substr(-2);
    }
    return colour;
}


function msg_warning(str){
    $('#messagebox').append('<div class="alert alert-warning">'+
            '<a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a>' +
            '<strong>Warning!</strong> ' +
            str +
            '</div>');
}

function msg_danger(str){
    $('#messagebox').append('<div class="alert alert-danger">'+
            '<a href="#" class="close" data-dismiss="alert" aria-label="close">&times;</a>' +
            '<strong>Danger!</strong> ' +
            str +
            '</div>');
}


function msg_clean() {
    $('#messagebox').html('');
}

/*********************************
 * Helper to check the API State
 * and document state.
 ********************************/
$(function() {

    //Check if api website is available
    $.ajax({
        url: "api/index.php?request=robinhood"
    }).then(function(data) {
        $('#quote').append(data);
    });

    //Get the filesystem path
    $.ajax({
        url: "api/index.php?request=native/vars"
    }).then(function(data) {
        $('#main_title').append(": "+data.FS_Path);
        document.title = document.title + ": "+data.FS_Path;
    });

    //Get the current user
    $.ajax({
        url: "api/index.php?request=current_auth"
    }).then(function(data) {
        if (data!='$NOAUTH') {
            $('#loggedas').html("Logged as "+data);
        } else {
            $('#loggedas').html("Logged as Nobody");
        }
    });

    //Add keypress event for filter form
    $('#filterform input').on('keypress', function(event){
        if(event.key=="Enter" && !event.shiftKey){
            GetGraph(lastGet);
       }
    });


});

/****************************************
 * Logout from apache auth
 **************************************/
function logout() {
    var out = window.location.href.replace(/:\/\//, '://log:out@');
    jQuery.get(out);
}

/****************************************
 * Recursive background load of dataTable
 **************************************/
function loadDataTable(item, queryString, taskID) {
        if (tableData.limited!=false && tableDataCount<maxdisplayedrows && tableDataTaskId==taskID) {
                newoffset = tableData.offset + tableData.limited;
                console.log("Table Query offset:",newoffset);
                $.ajax({
                        url: "api/index.php?request=data/" + item + "/" +queryString + "/offset/" + newoffset + "/"
                }).then(function(data) {
                        tableData=data;
                        TableState.rows.add(data.datasets).draw();
                        tableDataCount=tableDataCount+data.limited;

                        //let's test the stack size ! (20 000 for IE10, 281 810 for FF42 )
                        loadDataTable(item, queryString, taskID);
                });
        } else if (tableDataCount>=maxdisplayedrows) {
                msg_warning("Partial result for graph and table,  limited to "+maxdisplayedrows+" entries by maxdisplayedrows (config.php). Please use filter !");
        }
}


/****************************************
 * Recursive background load of a graph
 **************************************/
function loadDataGraph(item, queryString, taskID) {
        if (graphData.limited!=false && graphDataCount<maxdisplayedrows && graphDataTaskId==taskID) {
                newoffset = graphData.offset + graphData.limited;
                console.log("Graph Query offset:",newoffset);
        $.ajax({
                url: "api/index.php?request=graph/" + item + "/" +queryString + "/offset/" + newoffset + "/"
        }).then(function(data) {
                graphData=data
                myChart.data.datasets[0].data = myChart.data.datasets[0].data.concat(data.datasets[0].data);
                myChart.data.datasets[0].backgroundColor = myChart.data.datasets[0].backgroundColor.concat(data.datasets[0].backgroundColor);
                console.log(data);
                myChart.update();
                graphDataCount=graphDataCount+data.limited;

        //let's test the stack size ! (20 000 for IE10, 281 810 for FF42 )
        loadDataGraph(item, queryString, taskID);
        });
        }
}

/****************************************
 * Async. function which request graph
 * and data, then update graph and table
 **************************************/
//GetGraph and Table Data async
function GetGraph(item){

    //Set the main_content frame
    document.getElementById("main_content").innerHTML = `
    <canvas style="max-height:640px; min-height:320px" id="ctx"></canvas> <!-- Canvas for Graph -->
    <table id="datalist" class="table table-striped table-bordered" width="100%"></table> <!-- Datalist-->
    `

    $('#filter').button('loading');
    lastGet=item;
    //Get filter
    var queryString="";
    var myForm = document.getElementById("filterform");
    for (var i = 0; i < myForm.elements.length; i++) {
        if (myForm.elements[i].name.length>0)
        {
            queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[i].value.replace("/", "-");
        }
    }


    //Clean Graph and Data table

    if (TableState!="empty"){
        TableState.destroy();
        $('#datalist').empty();
    }

     //Delete the old graph
     if (GraphState!="empty") {
         myChart.destroy();
     }


    graphDataTaskId++;

    //Get the Graph data
    $.ajax({
        url: "api/index.php?request=graph/" + item + "/" + queryString
    }).then(function(data) {
        var options = {
            responsive : responsiveChart,
            maintainAspectRatio: false,
            animation : animationChart,
            showAllTooltips: showAllTooltipsChart,
            tooltips: {
                enabled: true,
                mode: 'single',
                callbacks: {
                    label: function(tooltipItems, data) {
                        val = data.datasets[tooltipItems.datasetIndex].data[tooltipItems.index]
                        type = data.datasets[tooltipItems.datasetIndex].unit
                        if (type=="size") {
                            return formatBytes(val,0);
                        } else if (type=="count") {
                            return formatCount(val,0);
                        } else if (type=="date") {
                            return (new Date(val*1000));
                        }
                        return val
                    }
                }
            }
        }


        GraphCTX = document.getElementById("ctx").getContext("2d");
        graphData=data;
        //Create the new graph
        myChart = new Chart(GraphCTX,{
            type: data.default_graph,
                data: data,
                options: options
        });
        //Load the whole graph in backgroup
        graphDataCount = data.limited;
        loadDataGraph(item, queryString, tableDataTaskId);
        GraphState=data.default_graph;
        $('#filter').button('reset')
    });



    tableDataTaskId++;


    $.ajax({
            url: "api/index.php?request=data/" + item + "/" +queryString
    }).then(function(data) {
            tableData=data;
            TableState = $('#datalist').DataTable( {
                    destroy: true,
                       clear: true,
                       bAutoWidth: false,
                       bSortClasses: false,
                       bDeferRender: true,
                       data: data.datasets,
                       columns: data.columns,
                       columnDefs: data.columnsDefs
            } );
            //Load the whole table in background
            tableDataCount = data.limited;
            loadDataTable(item, queryString, tableDataTaskId);
            });


}

/*********************************
 * Clean the filter form
 ********************************/

function CleanForm() {
    var myForm = document.getElementById("filterform");
    for (var i = 0; i < myForm.elements.length; i++) {
        myForm.elements[i].value = ""
    }
}
