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
//Global Graph
//Chart.defaults.global.responsive = true;

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
        if(event.which==13 && !event.shiftKey){
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
 * Async. function which request graph
 * and data, then update graph and table
 **************************************/
//GetGraph and Table Data async
function GetGraph(item){
    $('#filter').button('loading');
    lastGet=item;
    $('#messagebox').html("");
    //Get filter
    var queryString="";
    var myForm = document.getElementById("filterform");
    for (var i = 0; i < myForm.elements.length; i++) {
        if (myForm.elements[i].name.length>0)
        {
            queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[i].value
        }
    }


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

        if (data.limited) {
            msg_warning("Partial result,  limited to "+data.limited+" entries by $MAX_ROWS (config.php). Please use filter !");
        } else {
            msg_clean();
        }

        //Delete the old graph
        if (GraphState!="empty") {
            myChart.destroy();
        }

        GraphCTX = document.getElementById("ctx").getContext("2d");

        //Create the new graph
        myChart = new Chart(GraphCTX,{
            type: data.default_graph,
                data: data,
                options: options
        });

        GraphState=data.default_graph;
        $('#filter').button('reset')
    });


    $.ajax({
        url: "api/index.php?request=data/" + item + "/" +queryString
    }).then(function(data) {
        if (TableState!="empty"){
            TableState.destroy();
            $('#datalist').empty();
        }

        TableState = $('#datalist').DataTable( {
            destroy: true,
                   clear: true,
                   data: data.datasets,
                   columns: data.columns,
                   columnDefs: data.columnsDefs
        } );
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
