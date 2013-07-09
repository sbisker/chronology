var KronosClient = function(kronos_url) {
  var self = this;

  // enable CORS on Kronos server for the
  // domain running this client
  self.url = kronos_url;

  self.put_url = self.url + "/1.0/events/put";
  self.get_url = self.url + "/1.0/events/get";

  self.isKronosReservedKey = function(key) {
    if (key == "" || key[0] != "@") {
      return false;
    }
    return true;
  }

  // TODO(meelap) allow buffering of puts and flushing them on demand
  // Jia doesn't do puts, so this can wait until it's needed.
  self.put = function(stream, event, callback) {
    var self = this;
    
    var payload = {};
    payload[stream] = [event];

    crossdomain.ajax(
      { url : self.put_url,
        type : "POST",
        data : JSON.stringify(payload),
        success : function(payload) {
          callback(payload):;
        },
        error : function() {
          console.log("kronos crossdomain call failed");
        },
      }
    );
  }

  self.get = function(stream, start, end, callback) {
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
        success : function(payload) {
          var data = [];
          _.each(payload.split("\r\n"), function(rawevent) {
            try {
              var event = JSON.parse(JSON.parse(rawevent));
              data.push(event);
            } catch(e) {
              console.log("KronosClient.get failed to parse:"+rawevent);
            }
          });
          callback(data);
        },
        error : function() {
          console.log("kronos crossdomain call failed");
        },
      });
    } catch (e) {
      console.log("kronosclient.get: "+e);
    }
  };
};
