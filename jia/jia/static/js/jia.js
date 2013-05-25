var SeriesRetriever = function(csrfToken) {
  var path = '/kronos/get';

  var get = function(series, start, end, callback) {
    $.post(path,
           {series: series, start: start, end: end, _csrf_token: csrfToken},
           function(data) {
             csrfToken = data.csrf_token;
             callback(data.data)
           });
  };

  return {
    get: get
  };
};

var TimeseriesCharter = function(chartId, yAxisId, legendId) {
  var chart = function(seriesName, xs, ys) {
    _.each([chartId, yAxisId, legendId], function(element) {
      $('#' + element).empty();
    });
    var pairs = _.zip(xs, ys);
    var series = _.map(pairs, function(pair) { return {x: pair[0], y: pair[1]}; });
    var palette = new Rickshaw.Color.Palette({scheme: 'spectrum14'});

    var graph = new Rickshaw.Graph( {
      element: document.getElementById(chartId),
      width: 580,
      height: 300,
      renderer: 'line',
      series: [ {
        color: palette.color(),
        name: seriesName,
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

var DateParser = function() {
  var regex = /([+-]?\d+)(days|hours)/;

  var parse = function(input) {
    if (input === 'now') {
      return Date.now();
    }
    var match = input.match(regex);
    if (match !== null) {
      if (match[2] === 'days') {
        return Date.now().addDays(parseFloat(match[1]));
      } else {
        return Date.now().addHours(parseFloat(match[1]));
      }
    }
    alert('Invalid time: ' + input);
  };

  return {
    parse: parse
  }
};