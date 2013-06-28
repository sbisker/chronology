var KronosClient = function() {
    var self = this;

    // TODO(meelap) real url of Kronos server
    // enable CORS on Kronos server for the
    // domain running this client
    //var url = "http://kronos.locu.com";
    var url = "http://ec2-54-226-161-30.compute-1.amazonaws.com";

    var put_url = url + "/1.0/events/put";
    var get_url = url + "/1.0/events/get";

    // TODO(meelap) allow buffering of puts and flushing them on demand
    // Jia doesn't do puts, so this can wait until it's needed.
    self.put = function(stream, event) {
        var self = this;
        
        var payload = {};
        payload[stream] = [event];

        crossdomain.ajax({ url : self.put_url,
                           type : "POST",
                           data : JSON.stringify(payload),
                           success : function(responsetext, xhrobj) {
                                    // TODO(meelap)
                                     },
                           error : function(responsetext, xhrobj) {
                                     // TODO(meelap) retry a few times?
                                     // at least log the error somewhere
                                     },
                         });
    };

    self.get = function(stream, start, end, callback) {
        // call callback with list of results
        var self = this;

        var payload = { stream     : stream,
                        start_time : start,
                        end_time   : end
                      };

        crossdomain.ajax({ url : self.get_url
                         , type : "POST"
                         , data : JSON.stringify(payload)
                         , success : function(responsetext, xhrobj) {
                                        return self.get_cb(responstext, xhrobj, callback);
                                     }
                         , error : function() {
                                     // TODO(meelap) better error handling
                                     console.log("kronos crossdomain call failed");
                             }
                                
                         });
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
