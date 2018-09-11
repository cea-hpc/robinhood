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


function ConsoleRun() {
        var request = document.getElementById("consolerequest").value;
        $.ajax({
                url: "api/index.php?request=" + request
        }).then(function(data) {
                defaultx = ""
                document.getElementById("consoledata").innerHTML="Number of result: " + data.length;
                if (Array.isArray(data)) {
                        document.getElementById("consoledatabox").value=JSON.stringify(data,null,2);
                        graphx = document.getElementById("consolerequestgraphx")
                        graphy = document.getElementById("consolerequestgraphy")
                        graphs = document.getElementById("consolerequestseries")
                        while (graphx.options.length) graphx.remove(0);
                        while (graphy.options.length) graphy.remove(0);
                        while (graphs.options.length) graphs.remove(0);
                        graphs.add(new Option("None", "None"));
                        for (var item in data[0]) {
                                if (defaultx=="" || item=="CronDate") { 
                                    defaultx=item
                                }
                                graphx.add(new Option(item, item));
                                graphy.add(new Option(item, item));
                                graphs.add(new Option(item, item));
                        }
                graphx.value=defaultx

                } else {
                        document.getElementById("consoledatabox").value=data;
                }
        });
}

function ConsoleGraphRun() {
        document.getElementById("consolegraph").innerHTML='<canvas style="max-height:640px; min-height:320px" id="ctx"></canvas> <!-- Canvas for Graph -->'

        var request = document.getElementById("consolerequest").value;
        $.ajax({
                url: "api/index.php?request=" + request
        }).then(function(data) {

        xitem = document.getElementById("consolerequestgraphx").value;
        yitem = document.getElementById("consolerequestgraphy").value;
        type = document.getElementById("consolerequestgraph").value;
        series = document.getElementById("consolerequestseries").value;
        mapping = document.getElementById("mapping").value;
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

function console_GetInfo(request){

        /* Create the table template */
        document.getElementById("main_content").innerHTML = `
                <h1>Console</h1>
                <h2>Request</h2>
                <form id="consoleform" name="consoleform" class="form-inline">
                <fieldset class="form-group">
                <label for="interative">Filter</label>
                <input type="text" class="form-control" id="consolerequest" name="consolerequest" placeholder="request" size=64>
                </fieldset>
                <button type="button" id="Run" class="btn btn-primary"  autocomplete="off" onclick="ConsoleRun()">Run</button>
                </form>
                <h2>Data</h2>
                <div name="consoledata" id="consoledata"></div>
                <textarea class="consoledatabox" name="consoledatabox" id="consoledatabox" rows="24" style='width:100%;'>
                </textarea>
                <h2>Graph</h2>
                <form id="consoleformgraph" name="consoleformgraph" class="form-inline">
                <fieldset class="form-group">
                 <div class="form-group">
                <label>Type</label>
                <select  class="form-control" id="consolerequestgraph" name="consolerequestgraph" placeholder="Type">
                    <option value="line">Line</option>
                    <option value="line stacked">Line stacked</option>
                    <option value="bar">Bar</option>
                    <option value="scatter">Scatter</option>
                </select>
                </div>
                 <div class="form-group">
                    <label>Label/X</label>
                    <select  class="form-control" id="consolerequestgraphx" name="consolerequestgraphx" placeholder="fieldx" placeholder="Label/X" ></select>
                </div>
                 <div class="form-group">
            <label>Values/Y</label>
                    <select  class="form-control" id="consolerequestgraphy" name="consolerequestgraphy" placeholder="fieldy" placeholder="Y"></select>
                </div>
                 <div class="form-group">
            <label>Series</label>
                    <select  class="form-control" id="consolerequestseries" name="consolerequestseries" placeholder="series" placeholder="series"></select>
                </div>
                 <div class="form-group">
                <button type="button" id="RunGraph" class="btn btn-primary" autocomplete="off" onclick="ConsoleGraphRun()">Refresh</button>
                <input type="text" class="form-control" id="graphname" name="graphname" placeholder="Name" size=24>
                <input type="text" class="form-control" id="mapping" name="mapping" placeholder="Mapping" size=24>
                <button type="button" id="AddGraph" class="btn btn-primary" autocomplete="off" onclick="AddCustomGraph()">Add to custom graphs</button>
                </div>
                </fieldset>
                </form>
                <div name="consolegraph" id="consolegraph"></div>
                <h2>Help</h2>
                <h3>Request</h3>
                <p>Example: native/acct/uid.group to get the stats by user.</p>
                <p>Example: native/files/uid.filter/robin/ to get all the files and directories of robin.</p>
                <p>Please refer to README.txt for more filter and request</p>
                <h3>Graph</h3>
                <p> Use series to automaticaly create differents dataset using with of the field </p>
                <p> Mapping is a coma separed list to change dataset labels </p>
                <p> Example: if you use groupbysize you can replace 0,1,2,... by B,KB,MB with 0,K,1,KB,2,MB, ...</p>
                `;

        if (request) {
            document.getElementById("consolerequest").value = request;
            ConsoleRun();
        }

        //Add keypress event for filter form
        $('#consoleform input').on('keypress', function(event){
                if(event.key == "Enter" && !event.shiftKey){
                        ConsoleRun();
                        return false;
                }
        });

}


function AddCustomGraph(){

    var queryString="";
    var myForm = document.getElementById("consoleformgraph");
    var myFormc = document.getElementById("consoleform");
    queryString = queryString + "/";
    queryString = queryString + "Name/" + myForm.elements["graphname"].value+ "/";
    queryString = queryString + "ChartType/" + myForm.elements["consolerequestgraph"].value+ "/";
    queryString = queryString + "Request/" + myFormc.elements["consolerequest"].value.replace(/\//g, "---")+ "/";
    queryString = queryString + "X/" + myForm.elements["consolerequestgraphx"].value+ "/";
    queryString = queryString + "Y/" + myForm.elements["consolerequestgraphy"].value+ "/";
    queryString = queryString + "Series/" + myForm.elements["consolerequestseries"].value+ "/";
    queryString = queryString + "Mapping/" + myForm.elements["mapping"].value+ "/";
   $.ajax({
        url: "api/index.php?request=native/customgraph/add" + "/" + queryString
    }).then(function(data) {
    customgraph_GetInfo()
    });
}

