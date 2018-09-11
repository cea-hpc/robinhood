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
function tasks_GetInfo(){

/* Create the table template */
    document.getElementById("main_content").innerHTML = `
<h1>Tasks</h1>
 <form id="taskform" name="taskform">
 <table class="table">
    <thead>
      <tr>
        <th>Id</th>
        <th>Name</th>
        <th>Trigger</th>
        <th>Request</th>
        <th>Action</th>
        <th>Database</th>
        <th>Last Run</th>
        <th>Last execution time</th>
        <th>Modify</th>
      </tr>
    </thead>
    <tbody id=fsinfobody name=fsinfobody>
    </tbody>
  </table>
  </form>
 `;


/* Retrieve the data */
    $.ajax({
        url: "api/index.php?request=native/tasks/get"
    }).then(function(data) {
    if (data[0]!='tasks') {
	    for (var key in data) {

	$("#fsinfobody").append(`
	      <tr>
		<td>`+data[key]['Id']+`<a href='#' onclick='console_GetInfo("native/task/`+data[key]['Name']+`")'><span class="glyphicon glyphicon-signal"></span></a></td>
		<th>
			<input type="text" class="form-control" id="Name_`+data[key]['Id']+`" name="Name_`+data[key]['Id']+`" placeholder="Name" value=`+data[key]['Name']+`>
		</th>
		<th>

		<select class="form-control" id="Trig_`+data[key]['Id']+`" name="Trig_`+data[key]['Id']+`">
			<option value="hourly">Hourly</option>
			<option value="daily">Daily</option>
			<option value="never">Never</option>
		</select>
		<script>document.getElementById("Trig_`+data[key]['Id']+`").value="`+data[key]['Trig']+`"</script>
		</th>
		<th>
			<input type="text" class="form-control" id="Request_`+data[key]['Id']+`" name="Request_`+data[key]['Id']+`" placeholder="Request" value=`+data[key]['Request']+`>
		</th>
		<th>
		<select class="form-control" id="Action_`+data[key]['Id']+`" name="Action_`+data[key]['Id']+`" value=`+data[key]['Action']+`>
			<option value="history">History</option>
			<option value="copy">Copy</option>
		</select>
		<script>document.getElementById("Action_`+data[key]['Id']+`").value="`+data[key]['Action']+`"</script>
		</th>
		<th>
		<select class="form-control" id="TargetDB_`+data[key]['Id']+`" name="TargetDB_`+data[key]['Id']+`">
        <script>
        $.ajax({
                url: "api/index.php?request=db_info"
            }).then(function(data) {
            for (var key in data) {
                var option = document.createElement('option');
                option.text = option.value = key;
                document.getElementById("TargetDB_`+data[key]['Id']+`").add(option,0);
                document.getElementById("TargetDB_`+data[key]['Id']+`").value="`+data[key]['TargetDB']+`";
            }
        });
        </script>
		</select>
		</th>
		<th> `+data[key]['LastRun']+` </th>
		<th> `+data[key]['ExecTime']+` Seconds </th>
		<th> <button type="button" class="btn btn-primary" onclick="updateTask(`+data[key]['Id']+`)">Modify</button>
             <button type="button" class="btn btn-primary" onclick="delTask(`+data[key]['Id']+`)">Delete</button>
        </th>
	      </tr>
	`);
    }
      //</form>
    }
$("#fsinfobody").append(`
      <tr>
        <th>#</th>
        <th>
                <input type="text" class="form-control" id="Name" name="Name" placeholder="Name">
	</th>
        <th>

        <select class="form-control" id="Trig" name="Trig">
                <option value="hourly">Hourly</option>
                <option selected value="daily">Daily</option>
                <option value="never">Never</option>
        </select>
	</th>
	<th>
		<input type="text" class="form-control" id="Request" name="Request" placeholder="Request">
	</th>
        <th>
        <select class="form-control" id="Action" name="Action">
                <option value="history">History</option>
                <option selected value="copy">Copy</option>
        </select>
	</th>
        <th>
        <select class="form-control" id="TargetDB" name="TargetDB">
        </select>
        <script>
        $.ajax({
                url: "api/index.php?request=db_info"
            }).then(function(data) {
            for (var key in data) {
                var option = document.createElement('option');
                option.text = option.value = key;
                document.getElementById("TargetDB").add(option,0);
        }
        });
        </script>
	</th>
        <th> # </th>
        <th> # </th>
        <th> <button type="button" class="btn btn-primary" onclick="setTask('')">Add</button></th>
      </tr>

`);
    });
}

/***********************************
 * Add or modify tasks
 **********************************/

function setTask(item){

	var queryString="";
	var myForm = document.getElementById("taskform");
	for (var i = 0; i < myForm.elements.length; i++) {
		if (myForm.elements[i].name.length>0 && myForm.elements[i].name.search("_")==-1) {
			queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[i].value.replace(/\//g, "---");
		}
	}
   $.ajax({
        url: "api/index.php?request=native/tasks/add" + "/" + queryString
    }).then(function(data) {
	tasks_GetInfo()
	})
}

function updateTask(item){


var queryString="";
    var myForm = document.getElementById("taskform");
    for (var i = 0; i < myForm.elements.length; i++) {
        if (myForm.elements[i].name.length>0 && myForm.elements[i].name.search("_")==-1) {
            queryString = queryString + "/" + myForm.elements[i].name + "/" + myForm.elements[myForm.elements[i].name+"_"+item].value.replace(/\//g, "---");
        }
    }
   $.ajax({
        url: "api/index.php?request=native/tasks/add" + "/" + queryString
    }).then(function(data) {
        delTask(item);
    })



}

function delTask(item){

   $.ajax({
        url: "api/index.php?request=native/tasks/del/" + item 
    }).then(function(data) {
	tasks_GetInfo()
	})
}
