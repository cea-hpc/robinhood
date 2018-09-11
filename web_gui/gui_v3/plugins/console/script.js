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

                document.getElementById("consoledata").innerHTML="Number of result: " + data.length;
                if (Array.isArray(data)) {
                        document.getElementById("consoledatabox").value=JSON.stringify(data,null,2);
                        graphx = document.getElementById("consolerequestgraphx")
                        graphy = document.getElementById("consolerequestgraphy")
                        while (graphx.options.length) graphx.remove(0);
                        while (graphy.options.length) graphy.remove(0);
                        for (var item in data[0]) {
                                graphx.options[graphx.options.length] = new Option(item, item)
                                graphy.options[graphy.options.length] = new Option(item, item)
                        }
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

        /* bar graph */
        if (type=="bar") {

        datay = []
        labels = []
        for (var item in data)
        {
                datay.push(data[item][yitem])
                labels.push(data[item][xitem])
        }

        console.log(datay)
        console.log(labels)

       barChartData = {
            labels: labels,
            datasets: [{
                label: "Dataset",
                borderColor: "FF0000",
                backgroundColor: "00FF00",
                data: datay,
                }]};

        var ctx = document.getElementById("ctx").getContext("2d");
        window.myBar = Chart.Bar(ctx, {
                data: barChartData,
                options: {
                    title: {
                         display: true,
                         text: 'Chart.js bar Chart'
                        },
                }
        });

        }

        /* line graph */
        if (type=="line") {

        datay = []
        labels = []
        for (var item in data)
        {
                datay.push(data[item][yitem])
                labels.push(data[item][xitem])
        }

        console.log(datay)
        console.log(labels)

       lineChartData = {
            labels: labels,
            datasets: [{
                label: "Dataset",
                borderColor: "FF0000",
                backgroundColor: "00FF00",
                data: datay,
                }]};

        var ctx = document.getElementById("ctx").getContext("2d");
        window.myBar = Chart.Line(ctx, {
                data: lineChartData,
                options: {
                    title: {
                         display: true,
                         text: 'Chart.js line Chart'
                        },
                }
        });

        }

        });
}

function console_GetInfo(){

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
                <label>Type</label>
                <select  class="form-control" id="consolerequestgraph" name="consolerequestgraph" placeholder="Type">
                    <option value="line">Line</option>
                    <option value="bar">Bar</option>
                    <option value="pie">Pie</option>
                    <option value="scatter">Scatter</option>
                </select>
		<br>
                <label>Label/X</label>
                <select  class="form-control" id="consolerequestgraphx" name="consolerequestgraphx" placeholder="fieldx" placeholder="Label/X" ></select>
		<br>
		<label>Values/Y</label>
                <select  class="form-control" id="consolerequestgraphy" name="consolerequestgraphy" placeholder="fieldy" placeholder="Y"></select>
		<br>
                <button type="button" id="RunGraph" class="btn btn-primary" autocomplete="off" onclick="ConsoleGraphRun()">Show in graph</button>
                </fieldset>
                </form>
                <div name="consolegraph" id="consolegraph"></div>
                <h2>Help</h2>
                <p>Example: native/acct/uid.group to get the stats by user.</p>
                <p>Example: native/files/uid.filter/robin/ to get all the files and directories of robin.</p>
                <p>Please refer to README.txt for more filter and request</p>
                `;

        //Add keypress event for filter form
        $('#consoleform input').on('keypress', function(event){
                if(event.key == "Enter" && !event.shiftKey){
                        ConsoleRun();
                        return false;
                }
        });

}
