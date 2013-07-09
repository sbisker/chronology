if (typeof(Jia) === "undefined") {
    var Jia = {};
}

function kronos_to_rickshaw(kronos) {
    // Kronos format is:
    // [ { "@time" : unix_timestamp, "@id" : event_id, attributes... }, ...]
    var rickshaw = {};
    var isKronosReservedKey = Jia.kronos.isKronosReservedKey;
    _.each(kronos, function(datapoint) {
        var timestamp = datapoint["@time"];
        _.each(datapoint, function(value, key) {
            if (isKronosReservedKey(key)) {
                return;
            }
            if (!_.has(rickshaw, key)) {
                rickshaw[key] = new Array();
            }
            rickshaw[key].push({
                x: timestamp,
                y: value
            });
        });
    });
    return rickshaw;
}

Jia.VisModel = Backbone.Model.extend({
    defaults : {
        "type" : "plot",
        "title": "Add a new visualization",
        "start": "yesterday",
        "end"  : "today",
        "data" : null
    },

    initialize : function() {
        this.set("data", _.sortBy(this.get("data"), "@time"));
    },
});


Jia.VisCollection = Backbone.Collection.extend({
    model : Jia.VisModel
});
