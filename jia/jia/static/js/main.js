$(function() {

Date.prototype.getUnixTime = function() {
    return Math.floor(this.getTime() / 1000);
}

var Kronos = new KronosClient;

//
// Models
//

var VisModel = Backbone.Model.extend({
    defaults : {
        "type" : "plot",
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

    className : "view",

    initialize : function() {
        this.listenTo(this.model, "change", this.render);
    },

    // Templates defined in templates/index.html
    viewTypeToTemplate : {
        "plot" : _.template($("#plot-vis").html()),
        "table": _.template($("#table-vis").html()),
    },

    render : function() {
        var type = this.model.get("type");
        if (type == "plot") {
            this.render_plot();
        } else if (type == "table") {
            this.render_table();
        } else {
            console.log("VisView: Unknown model type ["+type+"]");
        }
        return this;
    },

    render_plot : function() {
        var template = this.viewTypeToTemplate["plot"];
        this.$el.html(template(this.model.attributes));

        var palette = new Rickshaw.Color.Palette({scheme: "munin"});
        var data = kronos_to_rickshaw(this.model.get("data"));
        var series = new Array();
        _.each(data, function(points, name) {
            series.push({
                name: name,
                data: points,
                color: palette.color(),
            });
        });

        var graph = new Rickshaw.Graph({
            element : this.$(".plot")[0],
            interpolation : "linear",
            width   : 400,
            height  : 250,
            series  : series,
            renderer: "area",
            min     : 0,
        });
        graph.render();

        var legend = new Rickshaw.Graph.Legend({
            graph : graph,
            element : this.$(".legend")[0],
        });

        var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
            graph : graph,
            legend : legend,
        });

        var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight({
            graph : graph,
            legend : legend,
        });

        var hoverdetail = new Rickshaw.Graph.HoverDetail({graph: graph});

        var axes = new Rickshaw.Graph.Axis.Time({graph: graph});
        axes.render();
        //var xaxis = Rickshaw.Graph.Axis.X({
            //graph : graph,
            //ticks : 0, //TODO(meelap) number of ticks
        //});
        
        var yaxis = new Rickshaw.Graph.Axis.Y({
            graph : graph,
        }); 
        yaxis.render();
    },

    render_table : function() {
        var template = this.viewTypeToTemplate["table"];
        this.$el.html(template(this.model.attributes));

        var element = this.$(".table")[0];
        var thead = this.$("thead");
        
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
                    rickshaw[key].push({
                        x: Date.parse(time).getUnixTime(),
                        y: value
                    });
                });
            } else {
                console.log("kronos_to_rickshaw: expecting dictionary ("+time+","+attrs+")");
            }
        });
    });

    return rickshaw;
}

$("#new-vis-form").submit(function() {
    var start_time = Date.parse($("#start-time").text());
    var end_time = Date.parse($("#end-time").text());
    var stream_name = $("#stream-name").text();

    // TODO(meelap) echo errors back to the user.
    if (start_time == null) {
        console.log("Couldn't parse start time.");
    } else if (end_time == null) {
        console.log("Couldn't parse end time.");
    } else if (stream_name == "") {
        console.log("Stream name is empty.");
    } else {
        Kronos.get(stream_name,
                   start_time.getUnixTime(),
                   end_time.getUnixTime(),
                   create_new_visualization);
    }

    return false;
});

function create_new_visualization(responseText, xhrobj) {
    console.log("kronos_get: "+responseText);
}

referrer_signups = [
    { "2013-01-01" : { "godaddy" : 10, "opentable" : 20 } },
    { "2013-01-02" : { "godaddy" : 11, "opentable" : 22 } },
    { "2013-01-03" : { "godaddy" : 12, "opentable" : 26 } },
    { "2013-01-04" : { "godaddy" : 14, "opentable" : 22 } },
    { "2013-01-05" : { "godaddy" : 15, "opentable" : 23 } }];

testplot = new VisModel({type: "plot", title:"Customer signups", data: referrer_signups});
testtable = new VisModel({type: "table", data: referrer_signups});

var Visualizations = new VisCollection;
Jia = new JiaView({collection : Visualizations});

Visualizations.add(testplot);
Visualizations.add(testtable);

});
