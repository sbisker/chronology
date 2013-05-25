$(document).ready(function() {
  var retriever = SeriesRetriever(CSRF_TOKEN);
  var charter = TimeseriesCharter("chart", "y_axis", "legend");
  var dateParser = DateParser();
  var redraw = function() {
    var series = $('#series_name').val();
    var xSelector = $('#x_axis_selector').val();
    var ySelector = $('#y_axis_selector').val();
    var startTime = dateParser.parse($('#start_time').val()).getTime()/1000;
    var endTime = dateParser.parse($('#end_time').val()).getTime()/1000;
    
    retriever.get(series, startTime, endTime, function(data) {
      var xs = JSPath.apply(xSelector, data);
      var ys = JSPath.apply(ySelector, data);
      charter.chart(series, xs, ys);
    });
  };

  $('#create_chart').click(function(event) {
    event.preventDefault();
    redraw();
  });
});