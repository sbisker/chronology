var SeriesRetriever = function(csrf_token) {
  var path = '/kronos/get';
  
  var get = function(series, start, end, callback) {
    $.post(path,
           {series: series, start: start, end: end, _csrf_token: csrf_token},
           callback);
  };

  return {
    get: get
  };
};

var TimeseriesCharter = function(chartId, yAxisId, legendId) {
  var chart = function(xs, ys) {
    var pairs = _.zip(xs, ys);
    var series = _.map(pairs, function(pair) { return {x: pair[0], y: pair[1]}; });

    var graph = new Rickshaw.Graph( {
      element: document.getElementById(chartId),
      width: 580,
      height: 300,
      renderer: 'line',
      series: [ {
        color: 'steelblue',
        data: series
      } ]
    } );

    var axes = new Rickshaw.Graph.Axis.Time( { graph: graph } );
    var y_axis = new Rickshaw.Graph.Axis.Y( {
      graph: graph,
      orientation: 'left',
      tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
      element: document.getElementById(yAxisId),
    } );
    var legend = new Rickshaw.Graph.Legend( {
      element: document.getElementById(legendId),
      graph: graph
    });
    var hoverDetail = new Rickshaw.Graph.HoverDetail( {
      graph: graph
    });

    graph.render();
  };

  return {
    chart: chart
  };
};