if (typeof(Jia) === "undefined") {
    var Jia = {};
}

$(function() {

Jia.VisView = Backbone.View.extend({
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

        var sorteddata = _.sortBy(this.model.get("data"), "@time");
        var element = this.$(".table")[0];
        var thead = this.$("thead");
        
    },
});


Jia.MainView = Backbone.View.extend({
    el : $("#visualizations"),

    initialize : function() {
        this.collection = this.options.collection;
        this.listenTo(this.collection, "add", this.addOne);
        this.listenTo(this.collection, "reset", this.addAll);
        this.listenTo(this.collection, "all", this.render);
        this.render();
    },

    addOne : function(vis) {
        var view = new Jia.VisView({model: vis});
        this.$el.append(view.render().el);
    },

    addAll : function() {
        this.collection.each(this.addOne, this);
    },
});

});
