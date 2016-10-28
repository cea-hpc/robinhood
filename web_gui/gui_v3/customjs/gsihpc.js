
$(document).ready(function() {


  $('#filterform input').on('keypress', function(event){
    if(event.which==13 && !event.shiftKey){
      GetGraph(lastGet);
    }
  });

});
