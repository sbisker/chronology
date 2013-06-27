$(function() {

var VisView = Backbone.View.extend({
    tagName : "li",

    initialize : function() {
        this.type = this.options.type;
        this.listenTo(this.model, "change", this.render);
    },

    // Templates defined in templates/index.html
    viewTypeToTemplate : {
        "plot" : _.template($("#plot-vis").html()),
        "table": _.template($("#table-vis").html()),
        "new"  : _.template($("#new-vis").html()),
    },

    render : function() {
        var type = this.model.type;
        if (type == "plot") {
            render_plot();
        } else if (type == "table") {
            render_table();
        } else if (type == "new") {
            render_new();
        } else {
            console.log("VisView: Unknown model type ["+type+"]");
        }
        return this;
    },

    render_plot : function() {
        var template = this.viewTypeToTemplate["plot"];
        this.$el.html(template(this.model.attributes));

        this.graph = new Rickshaw.Graph({
            element : this.$(".chart"),
            width   : 500,
            height  : 500,
            series  : this.model.data,
        });
        this.graph.render();
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
    $el : $("#visualizations"),

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

});
