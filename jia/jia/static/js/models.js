$(function() {

var VisModel = Backbone.Model.extend({
    defaults : function() {
        return {
            type : "table", // table|bar|line
            title: "Title",
            start: "yesterday",
            end  : "today"
        }
    },
});


var VisCollection = Backbone.Collection.extend({
    model : VisModel
});

});
