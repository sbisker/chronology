var module = angular.module('jia.barchart', ['angular-c3']);

module.factory('barchart', function () {

  var meta = {
    title: 'barchart',
    readableTitle: 'Bar Chart',
    template: 'barchart.html',

    css: [
      '/static/visualizations/barchart/barchart.css'
    ],

    requiredFields: [
      '@label',
      '@value'
    ],

    optionalFields: [
      '@group'
    ]
  };

  var visualization = function () {
    this.meta = meta;
    this.data = [];
    this.c3data = {
      columns: []
    };

    this.setData = function (data, msg) {
      this.data = data;
      var series = _.groupBy(data.events, function(event) {
        return event['@group'] || '';
      });

      var cats = [];
      if (_.size(series) > 0) {
        series = _.map(series, function(events, seriesName) {
          var i = 0;
          return {name: seriesName, data: _.map(events, function(event) {
            if (i == 0) {
              cats.push(event['@label']);
            }
            return event['@value'];
          })};
          i++;
        });
      } else {
        series = [];
        msg.warn("Data contains no events");
      }
      this.series = series;
      
      var cols = [];
      var groups = [];
      _.each(series, function (group) {
        var points = [group.name];
        points = points.concat(group.data);
        cols.push(points);
        groups.push(group.name);
      });

      this.c3data = {
        columns: cols,
        type: 'bar',
        groups: [groups]
      };

      this.c3axis = {
        x: {
          type: 'category',
          categories: cats
        }
      }
    }
  }

  return {
    meta: meta,
    visualization: visualization
  }
});
