$(function() {

//
// Models
//

var VisModel = Backbone.Model.extend({
    defaults : {
        "type" : "new",
        "title": "Add a new visualization",
        "start": "yesterday",
        "end"  : "today",
        "data" : null
    },
});


var VisCollection = Backbone.Collection.extend({
    model : VisModel
});


//
// Views
//

var VisView = Backbone.View.extend({
    tagName : "li",

    initialize : function() {
        this.listenTo(this.model, "change", this.render);
    },

    // Templates defined in templates/index.html
    viewTypeToTemplate : {
        "plot" : _.template($("#plot-vis").html()),
        "table": _.template($("#table-vis").html()),
        "new"  : _.template($("#new-vis").html()),
    },

    render : function() {
        var type = this.model.get("type");
        if (type == "plot") {
            this.render_plot();
        } else if (type == "table") {
            this.render_table();
        } else if (type == "new") {
            this.render_new();
        } else {
            console.log("VisView: Unknown model type ["+type+"]");
        }
        return this;
    },

    render_plot : function() {
        var template = this.viewTypeToTemplate["plot"];
        this.$el.html(template(this.model.attributes));

        var data = kronos_to_rickshaw(this.model.get("data"));
        var series = new Array();
        _.each(data, function(points, name) {
            series.push({
                name: name,
                data: points,
                color: "blue", // TODO(meelap) get random color
            });
        });

        var element = this.$(".plot")[0];

        var graph = new Rickshaw.Graph({
            element : element,
            width   : 400,
            height  : 250,
            series  : series,
            renderer: "line",
            min     : "auto",
        });
        graph.render();

        var legend = new Rickshaw.Graph.Legend({
            graph : graph,
            element : element,
        });

        // TODO(meelap) need jquery ui sortable for this
        //var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
            //graph : graph,
            //legend : legend,
        //});

        var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight({
            graph : graph,
            legend : legend,
        });
    },

    render_table : function() {
        var template = this.viewTypeToTemplate["table"];
        this.$el.html(template(this.model.attributes));
    },

    render_new : function() {
        var template = this.viewTypeToTemplate["new"];
        this.$el.html(template());
    },
});


var JiaView = Backbone.View.extend({
    el : $("#visualizations"),

    initialize : function() {
        this.collection = this.options.collection;
        this.listenTo(this.collection, "add", this.addOne);
        this.listenTo(this.collection, "reset", this.addAll);
    },

    addOne : function(vis) {
        var view = new VisView({model: vis});
        this.$el.append(view.render().el);
    },

    addAll : function() {
        this.collection.each(this.addOne, this);
    },
});

function kronos_to_rickshaw(kronos) {
    var rickshaw = {};
    _.each(kronos, function(datapoint) {
        _.each(datapoint, function(attrs, time) {
            if (typeof(attrs) == "object") {
                _.each(attrs, function(value, key) {
                    if (!_.has(rickshaw, key)) {
                        rickshaw[key] = new Array();
                    }
                    var t = parseInt(Date.parse(time).toString("yyyyMMdd"));
                    rickshaw[key].push({x: t, y: value});
                });
            } else {
                console.log("kronos_to_rickshaw: expecting dictionary ("+time+","+attrs+")");
            }
        });
    });

    return rickshaw;
}

f = kronos_to_rickshaw;

// Don't use `var` so that it is accessible from console for debugging
Visualizations = new VisCollection;
Jia = new JiaView({collection : Visualizations});
testdata = [
    { data : [ {x:0,y:0}, {x:1,y:1}, {x:2,y:4},{x:3,y:9}, {x:4,y:16},{x:5,y:25} ],
      name : "Test Data",
      color: "blue"}]
// if typeof(value) == "object":
//      do stacked plot
//         totals column in table
// else do regular plot/table
referrer_signups = [
    { "2013-01-01" : { "godaddy" : 10, "opentable" : 20 } },
    { "2013-01-02" : { "godaddy" : 11, "opentable" : 22 } },
    { "2013-01-03" : { "godaddy" : 12, "opentable" : 26 } },
    { "2013-01-04" : { "godaddy" : 14, "opentable" : 22 } },
    { "2013-01-05" : { "godaddy" : 15, "opentable" : 23 } }];

testnew = new VisModel({type: "new"});
testplot = new VisModel({type: "plot", data: referrer_signups});
testtable = new VisModel({type: "table", data: referrer_signups});

Visualizations.add(testplot);
Visualizations.add(testtable);
Visualizations.add(testnew);

});

