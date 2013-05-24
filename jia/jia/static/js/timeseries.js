$(document).ready(function() {
  var redraw = function() {
    var retriever = SeriesRetriever(CSRF_TOKEN);
    var charter = TimeseriesCharter("chart", "y_axis", "legend");
    var dateParser = DateParser();
    var series = $('#series_name').val();
    var xSelector = '.data' + $('#x_axis_selector').val();
    var ySelector = '.data' + $('#y_axis_selector').val();
    var startTime = dateParser.parse($('#start_time').val()).getTime()/1000;
    var endTime = dateParser.parse($('#end_time').val()).getTime()/1000;
    
    retriever.get(series, startTime, endTime, function(data) {
      var xs = JSPath.apply(xSelector, data);
      var ys = JSPath.apply(ySelector, data);
      charter.chart(xs, ys);
    });
  };

  $('#create_chart').click(function(event) {
    event.preventDefault();
    redraw();
  });
});