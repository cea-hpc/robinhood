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


function InteractiveRun() {
        var request = document.getElementById("interactiverequest").value;
        $.ajax({
                url: "api/index.php?request=" + request
        }).then(function(data) {

                console.log(data);
                document.getElementById("interactivedata").innerHTML="Number of result: " + data.length;
                if (Array.isArray(data)) {
                        document.getElementById("interactivedatabox").value=JSON.stringify(data,null,2);
                } else {
                        document.getElementById("interactivedatabox").value=data;
                }
        });
}

/***********************************
 * Build table and get data from API
 **********************************/
function interactive_GetInfo(){

        /* Create the table template */
        document.getElementById("main_content").innerHTML = `
                <h1>Interactive</h1>
                <h2>Data</h2>
                <div name="interactivedata" id="interactivedata"></div>
                <textarea class="interactivedatabox" name="interactivedatabox" id="interactivedatabox" rows="24" style='width:100%;'>
                </textarea>
                <h2>Request</h2>
                <form id="interactiveform" name="interactiveform">
                <fieldset class="form-group">
                <label for="interative">Filter</label>
                <input type="text" class="form-control" id="interactiverequest" name="interactiverequest" placeholder="request">
                </fieldset>
                <button type="button" id="Run" class="btn btn-primary"  autocomplete="off" onclick="InteractiveRun()">Run</button>
                </form>
                <h2>Help</h2>

                <p>Sample: native/acct/uid.group to get the stats by user.</p>
                <p>Sample: native/files/uid.filter/robin/ to get all the files and directories of robin.</p>
                <p>Please refer to README.txt for more filter and request</p>
                `;

        //Add keypress event for filter form
        $('#interactiveform input').on('keypress', function(event){
                if(event.which==13 && !event.shiftKey){
                        InteractiveRun();
                        return false;
                }
        });

}
