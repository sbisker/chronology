var KronosClient = function() {
    var self = this;

    // TODO(meelap) real url of Kronos server
    // enable CORS on Kronos server for the
    // domain running this client
    //var url = "http://kronos.locu.com";
    self.url = "http://locudev2.locu.com:8150";

    self.put_url = self.url + "/1.0/events/put";
    self.get_url = self.url + "/1.0/events/get";

    // TODO(meelap) allow buffering of puts and flushing them on demand
    // Jia doesn't do puts, so this can wait until it's needed.
    self.put = function(stream, event) {
        var self = this;
        
        var payload = {};
        payload[stream] = [event];

        crossdomain.ajax(
            { url : self.put_url,
              type : "POST",
              data : JSON.stringify(payload),
              success : function(responsetext, xhrobj) {
                  // TODO(meelap)
                  console.log(responsetext);
              },
              error : function() {
                  // TODO(meelap) retry a few times?
                  // at least log the error somewhere
                  console.log("error");
              },
            }
        );
    }

    self.get = function(stream, start, end, callback) {
        // call callback with list of results
        var self = this;

        var payload = { stream     : stream,
                        start_time : start,
                        end_time   : end
                      };

        try {
            crossdomain.ajax({
                url : self.get_url,
                type : "POST",
                data : JSON.stringify(payload),
                success : function(responsetext, xhrobj) {
                    return self.get_cb(responsetext, xhrobj, callback);
                },
                error : function() {
                    // TODO(meelap) better error handling
                    console.log("kronos crossdomain call failed");
                },
            });
        } catch (e) {
            console.log("kronosclient.get: "+e);
        }
    };

    self.get_cb = function(responsetext, xhrobj, callback) {
        var events = [];
        for (var rawevent in responsetext.split()) {
            try {
                event = JSON.parse(rawevent);
            } catch(e) {
                continue;
            }
            events.push(event);
        }
        callback(events);
    };
};
