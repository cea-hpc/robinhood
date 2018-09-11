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

/***********************************
 * Build table and get data from API
 **********************************/
function customgraph_GetInfo(){

/* Create the table template */
    document.getElementById("main_content").innerHTML = `
<h1>Custom Graph</h1>
 <form id="graphform" name="graphform">
 <table class="table">
    <thead>
      <tr>
        <th>Id</th>
        <th>Graph name</th>
        <th>Chart type</th>
        <th>Request</th>
        <th>X</th>
        <th>Y</th>
        <th>Series</th>
        <th>Mapping</th>
        <th>Action</th>
      </tr>
    </thead>
    <tbody id=fsinfobody name=fsinfobody>
    </tbody>
  </table>
  </form>
 `;


/* Retrieve the data */
    $.ajax({
        url: "api/index.php?request=native/customgraph/get"
    }).then(function(data) {
    if (data[0]!='customgraph') {
	    for (var key in data) {

	$("#fsinfobody").append(`
	      <tr>
		<td>`+data[key]['Id']+` <a href='#' onclick='customgraph_GetGraph("`+data[key]['Id']+`")'><span class="glyphicon glyphicon-signal"></span></a> </td>
		<th>
			<input type="text" class="form-control" id="Name_`+data[key]['Id']+`" name="Name_`+data[key]['Id']+`" placeholder="Name" value=`+data[key]['Name']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="ChartType_`+data[key]['Id']+`" name="ChartType_`+data[key]['Id']+`" placeholder="ChartType" value=`+data[key]['ChartType']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="Request_`+data[key]['Id']+`" name="Request_`+data[key]['Id']+`" placeholder="Request" value=`+data[key]['Request']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="X_`+data[key]['Id']+`" name="X_`+data[key]['Id']+`" placeholder="X" value=`+data[key]['X']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="Y_`+data[key]['Id']+`" name="Y_`+data[key]['Id']+`" placeholder="Y" value=`+data[key]['Y']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="Series_`+data[key]['Id']+`" name="Series_`+data[key]['Id']+`" placeholder="Series" value=`+data[key]['Series']+`>
		</th>
		<th>
			<input type="text" class="form-control" id="Mapping_`+data[key]['Id']+`" name="Mapping_`+data[key]['Id']+`" placeholder="Mapping" value=`+data[key]['Mapping']+`>
		</th>
		<th> 
            <button type="button" class="btn btn-primary" onclick="updateCustomGraph(`+data[key]['Id']+`)">Modify</button>
            <button type="button" class="btn btn-primary" onclick="delCustomGraph(`+data[key]['Id']+`)">Delete</button>
        </th>
	      </tr>
	`);
    }
    }
$("#fsinfobody").append(`
      <tr>
        <th>#</th>
        <th>
                <input type="text" class="form-control" id="Name" name="Name" placeholder="Name">
	</th>
        <th>
                <input type="text" class="form-control" id="ChartType" name="ChartType" placeholder="ChartType">
	</th>
	<th>
		<input type="text" class="form-control" id="Request" name="Request" placeholder="Request">
	</th>
	<th>
		<input type="text" class="form-control" id="X" name="X" placeholder="X">
	</th>
	<th>
		<input type="text" class="form-control" id="Y" name="Y" placeholder="Y">
	</th>
	<th>
		<input type="text" class="form-control" id="Series" name="Series" placeholder="Series">
	</th>
	<th>
		<input type="text" class="form-control" id="Mapping" name="Mapping" placeholder="Mapping">
	</th>
        <th> <button type="button" class="btn btn-primary" onclick="setCustomGraph('')">Add</button></th>
      </tr>

`);
    });
}

/***********************************
 * Add or modify graphs
 **********************************/

function setCustomGraph(item){

	var queryString="/";
	var myForm = document.getElementById("graphform");
	for (var i = 0; i < myForm.elements.length; i++) {
		if (myForm.elements[i].name.length>0 && myForm.elements[i].name.search("_")==-1) {
            if (myForm.elements[i].value == "") {
                myForm.elements[i].value="None";
            }
			queryString = queryString + "" + myForm.elements[i].name + "/" + myForm.elements[i].value.replace(/\//g, "---") + "/";
		}
	}
   $.ajax({
        url: "api/index.php?request=native/customgraph/add" + "/" + queryString
    }).then(function(data) {
	customgraph_GetInfo()
	});
}


function updateCustomGraph(item){


var queryString="";
    var myForm = document.getElementById("graphform");
    for (var i = 0; i < myForm.elements.length; i++) {
        if (myForm.elements[i].name.length>0 && myForm.elements[i].name.search("_")==-1) {
            if (myForm.elements[myForm.elements[i].name+"_"+item].value=="") {
                myForm.elements[myForm.elements[i].name+"_"+item].value="None";
            }
            queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[myForm.elements[i].name+"_"+item].value.replace(/\//g, "---");
        }
    }
   $.ajax({
        url: "api/index.php?request=native/customgraph/add" + "/" + queryString
    }).then(function(data) {
        delCustomGraph(item);
    })



}




function delCustomGraph(item){
   $.ajax({
        url: "api/index.php?request=native/customgraph/del/" + item
    }).then(function(data) {
	customgraph_GetInfo()
	});
}



/***********************************
 * Display Graph
 **********************************/

function customgraph_GetGraph(Id){
    $.ajax({
        url: "api/index.php?request=native/customgraph/get"
    }).then(function(data) {
    if (data[0]!='customgraph') {
        for (var key in data) {
            if (data[key][`Id`]==Id) {
        document.getElementById("main_content").innerHTML = `
                <h1>Graph `+data[key]['Name']+`</h1>
                <div name="cgraph" id="cgraph"></div>
                `;
            ConsoleCGraphRun(data[key]);
            }


    }

}

})
}




function ConsoleCGraphRun(dgraph) {
        document.getElementById("cgraph").innerHTML='<canvas style="max-height:640px; min-height:320px" id="ctx"></canvas> <!-- Canvas for Graph -->'

        var request = dgraph['Request'];
        $.ajax({
                url: "api/index.php?request=" + request
        }).then(function(data) {

        xitem = dgraph['X'];
        yitem = dgraph['Y'];
        type = dgraph['ChartType'];
        series = dgraph['Series'];
        mapping = dgraph['Mapping'];
        /* scatter graph */
        if (type=="scatter") {

        dataxy = []
        for (var item in data)
        {
                dataxy.push({x: data[item][xitem], y: data[item][yitem]})
        }

       scatterChartData = {
            datasets: [{
                label: "Dataset",
                borderColor: "FF0000",
                backgroundColor: "00FF00",
                data: dataxy,
                }]};

        var ctx = document.getElementById("ctx").getContext("2d");
        window.myScatter = Chart.Scatter(ctx, {
                data: scatterChartData,
                options: {
                    title: {
                         display: true,
                         text: 'Chart.js Scatter Chart'
                        },
                }
        });

        }

        /* line graph */
        if (type=="line" || type=="line stacked" || type=="bar") {

        if (series != 'None')
        {
        //series_label =  {}
        series_title = {}
        series_x = {}
        series_index={}
        for (var item in data)
        {
            series_title[data[item][series]]=""
            series_x[data[item][xitem]]=""
            key = data[item][series]+"_"+data[item][xitem]
            series_index[key] = data[item][yitem]
        }

        labels = []
        for (var t in series_x)
        {
            labels.push(t)
        }
        datasets=[]
        for (var s in series_title)
        {
                datay=[]
                for (var t in series_x)
                {
                    key = s+"_"+t
                    if (key in series_index) {
                        datay.push(series_index[key])
                    } else {
                        datay.push(0)
                    }
                }

                if (mapping != "None") {
                    mapp = mapping.split(",");
                    imapp = mapp.indexOf(s);
                    
                    if (imapp==-1) {
                        label_s = s;
                    } else {
                        label_s = mapp[imapp+1];
                    } 
                } else {
                    label_s = s;
                }

                if (type=="line") {
                    dataset = {
                        label: label_s,
                        borderColor: stringToColour(s),
                        backgroundColor: "#00000000",
                        data: datay,
                    }
                } else {
                    dataset = {
                        label: label_s,
                        borderColor: "#000000",
                        backgroundColor: stringToColour(s),
                        data: datay,
                    }

                }

                datasets.push(dataset)
        }

       lineChartData = {
            labels: labels,
            datasets: datasets
       };

        } else {
        datay = []
        labels = []
        for (var item in data)
        {
                datay.push(data[item][yitem])
                labels.push(data[item][xitem])
        }

       lineChartData = {
            labels: labels,
            datasets: [{
                label: "Dataset",
                borderColor: "FF0000",
                backgroundColor: "00FF00",
                data: datay,
                }]};
        }


        var ctx = document.getElementById("ctx").getContext("2d");

        if (type=="line") {
        window.myBar = Chart.Line(ctx, {
                data: lineChartData,
                options: {
                    title: {
                         display: true,
                         text: 'Chart.js line Chart'
                        },
                }
        });
        } else if (type=="line stacked") {
        window.myBar = Chart.Line(ctx, {
                data: lineChartData,
                options: {
                    title: {
                         display: true,
                         text: 'Chart.js line stacked Chart'
                        },
                    scales: {
                        yAxes: [{stacked: true,}]
                    },
                }
        });
        } else if (type=="bar") {

        window.mybar = Chart.Bar(ctx, {
                data: lineChartData,
                options: {
                    title: {
                         display: true,
                         text: 'chart.js bar chart'
                        },
                }
        });


        }


        }

        });
}
