if (typeof(Jia) === "undefined") {
    var Jia = {};
}

Date.prototype.getUnixTime = function() {
    return Math.floor(this.getTime() / 1000);
}

Jia.kronos = new KronosClient;

$(function() {
$("#new-vis-form").submit(function(event) {
    event.preventDefault();
    event.stopPropagation();

    var vis_type = $("input[name=vistype]:checked").val();
    var start_time = Date.parse($("#start-time").val());
    var end_time = Date.parse($("#end-time").val());
    var stream_name = $("#stream-name").val();

    // TODO(meelap) echo errors back to the user.
    if (vis_type == null) {
        console.log("No visualization type chosen.");
    } else if (start_time == null) {
        console.log("Couldn't parse start time.");
    } else if (end_time == null) {
        console.log("Couldn't parse end time.");
    } else if (stream_name == "") {
        console.log("Stream name is empty.");
    } else {
        Jia.kronos.get(stream_name,
                       start_time.getUnixTime(),
                       end_time.getUnixTime(),
                       _.partial(create_new_visualization,
                                 vis_type,
                                 stream_name));
    }

    return false;
});
});

function create_new_visualization(type, title, data) {
    var newvis = new Jia.VisModel({type: type,
                                   title: title,
                                   data: data});
    Jia.main.collection.add(newvis);
}

referrer_signups = [
    { "@time" : 1372058730 , "godaddy" : 10, "opentable" : 20 },
    { "@time" : 1372158730 , "godaddy" : 11, "opentable" : 22 },
    { "@time" : 1372258730 , "godaddy" : 12, "opentable" : 26 },
    { "@time" : 1372358730 , "godaddy" : 14, "opentable" : 22 },
    { "@time" : 1372458730 , "godaddy" : 15, "opentable" : 23 }];

$(function() {
testplot = new Jia.VisModel({type: "plot", title:"Customer signups", data: referrer_signups});
testtable = new Jia.VisModel({type: "table", title:"Customer signups", data: referrer_signups});

Visualizations = new Jia.VisCollection;
Jia.main = new Jia.MainView({collection : Visualizations});

Visualizations.add(testplot);
Visualizations.add(testtable);
});

