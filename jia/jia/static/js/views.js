
// Defined in templates/index.html
var tableTemplate = _.template($("#table-vis").html());
var lineGraphTemplate = _.template($("#line-vis").html());
var barGraphTemplate = _.template($("#bar-vis").html());
var newVisTemplate = _.template($("new-vis").html());

var VisView = Backbone.View.extend({
    tagName : "li",

    initialize : function() {
        this.listenTo(this.model, "change", this.render);
    },

    viewTypeToTemplate : {
        "line" : lineGraphTemplate,
        "bar"  : barGraphTemplate,
        "table": tableTemplate
    },

    render : function() {
        var type = this.model.type;
        if (_.has(this.viewTypeToTemplate, type)) {
            var template = this.viewTypeToTemplate[type];
            this.$el.html(template(this.model.attributes));
        } else {
            console.log("VisView: Unknown model type ["+type+"]");
        }
        return this;
    },
});

var JiaView = Backbone.View.extend({
    $el : $("#visualizations"),

    collection: null,

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
