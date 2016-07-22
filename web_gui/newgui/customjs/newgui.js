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
Chart.defaults.global.responsive = true;

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
tooltipTemplate: "<%= value %>",

onAnimationComplete: function()
{
this.showTooltip(this.segments, true);

},
tooltipEvents: [],
showTooltips: true,
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

//Get the table data
$.ajax({
url: "api/index.php?request=data/" + item +queryString
}).then(function(data) {

        newdata = JSON.parse(data);

        $('#datalist').DataTable( {
destroy: true,
data: newdata.datasets,
columns: newdata.columns,
columnDefs: [
{ type: 'file-size', targets: '_all' }
]
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
