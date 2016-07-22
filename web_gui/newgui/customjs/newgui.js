/*
 * Copyright (C) 2016 CEA/DAM
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
Chart.defaults.global.title.display = true;
Chart.defaults.global.defaultFontSize = 13;

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

/*********************************
 * Helper to check the API State
 * and document state.
 ********************************/
$(function() {
                $.ajax({
url: "api/index.php?request=robinhood"
}).then(function(data) {
        $('.check-api-id').append(data);
        });
		$.ajax({
url: "api/index.php?request=native/vars"
}).then(function(data) {
	newdata = JSON.parse(data);
	console.log(newdata)
        $('.check-api-id').append(" - Filesystem: "+newdata.FS_Path);
        });
        

	});

/****************************************
 * Async. function which request graph 
 * and data, then update graph and table
 **************************************/
//GetGraph and Table Data async
function GetGraph(item){

        lastGet=item;

        //Get filter
        var queryString=""
                var myForm = document.getElementById("filterform");
        for (var i = 0; i < myForm.elements.length; i++) {
                if (myForm.elements[i].name.length>0)
                {
                        queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[i].value
                }
        }


        //Get the Graph data
        $.ajax({
url: "api/index.php?request=graph/" + item + queryString
}).then(function(data) {

        var options = {
responsive : true,
animation : false,
showAllTooltips: true,
defaultFontSize: 14,
tooltips: {
                enabled: true,
                mode: 'single',
                callbacks: {
                    label: function(tooltipItems, data) {
                        val = data.datasets[tooltipItems.datasetIndex].data[tooltipItems.index]
                        type = data.datasets[tooltipItems.datasetIndex].unit
                        if (type=="size") {
                            return formatBytes(val,0);
                        }
                        if (type=="count") {
                            return formatCount(val,0);
                        }
                    return val
                    }
                }}
}
//Convert string to json
newdata = JSON.parse(data);

//Delete the old graph
if (GraphState!="empty") {
myChart.destroy();
}

GraphCTX = document.getElementById("ctx").getContext("2d");


//Create the new graph
myChart = new Chart(GraphCTX,{
type: newdata.default_graph,
data: newdata,
options: options
});

GraphState=newdata.default_graph;

});


$.ajax({
url: "api/index.php?request=data/" + item +queryString
}).then(function(data) {
        newdata = JSON.parse(data);
        if (TableState!="empty"){
            TableState.destroy();
            $('#datalist').empty();
        }

        TableState = $('#datalist').DataTable( {
            destroy: true,
            clear: true,
            data: newdata.datasets,
            columns: newdata.columns,
            columnDefs: newdata.columnsDefs 
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
