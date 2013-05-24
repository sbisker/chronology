$(document).ready(function() {
  var retriever = SeriesRetriever(CSRF_TOKEN);
  var charter = TimeseriesCharter("chart", "y_axis", "legend");

  retriever.get(1, 2, 3, function(data) {
    var xs = JSPath.apply('.data._time', data);
    var ys = JSPath.apply('.data.data.money', data);
    charter.chart(xs, ys);
  });
});