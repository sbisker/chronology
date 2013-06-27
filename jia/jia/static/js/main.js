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

        var graph = new Rickshaw.Graph({
            element : this.$(".plot")[0],
            width   : 400,
            height  : 250,
            series  : this.model.get("data"),
            renderer: "line",
            min     : "auto",
        });
        graph.render();
    },

    render_table : function() {
        var template = this.viewTypeToTemplate["table"];
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


// Don't use `var` so that it is accessible from console for debugging
Visualizations = new VisCollection;
Jia = new JiaView({collection : Visualizations});
testdata = [
    { data : [ {x:0,y:0}, {x:1,y:1}, {x:2,y:4},{x:3,y:9}, {x:4,y:16},{x:5,y:25} ],
      name : "Test Data",
      color: "blue"}]

testnew = new VisModel({type: "new"});
testplot = new VisModel({type: "plot", data: testdata});

Visualizations.add(testplot);
Visualizations.add(testnew);

});

