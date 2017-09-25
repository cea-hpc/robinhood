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

                console.log(data);
                document.getElementById("consoledata").innerHTML="Number of result: " + data.length;
                if (Array.isArray(data)) {
                        document.getElementById("consoledatabox").value=JSON.stringify(data,null,2);
                } else {
                        document.getElementById("consoledatabox").value=data;
                }
        });
}

/***********************************
 * Build table and get data from API
 **********************************/
function console_GetInfo(){

        /* Create the table template */
        document.getElementById("main_content").innerHTML = `
                <h1>Console</h1>
                <h2>Request</h2>
                <form id="consoleform" name="consoleform">
                <fieldset class="form-group">
                <label for="interative">Filter</label>
                <input type="text" class="form-control" id="consolerequest" name="consolerequest" placeholder="request">
                </fieldset>
                <button type="button" id="Run" class="btn btn-primary"  autocomplete="off" onclick="ConsoleRun()">Run</button>
                </form>
                <h2>Data</h2>
                <div name="consoledata" id="consoledata"></div>
                <textarea class="consoledatabox" name="consoledatabox" id="consoledatabox" rows="24" style='width:100%;'>
                </textarea>
                <h2>Help</h2>
                <p>Example: native/acct/uid.group to get the stats by user.</p>
                <p>Example: native/files/uid.filter/robin/ to get all the files and directories of robin.</p>
                <p>Please refer to README.txt for more filter and request</p>
                `;

        //Add keypress event for filter form
        $('#consoleform input').on('keypress', function(event){
                if(event.which==13 && !event.shiftKey){
                        ConsoleRun();
                        return false;
                }
        });

}
