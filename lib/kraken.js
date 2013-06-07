var Memcached = require('memcaching');

var DELIMITER = "\r\n";

//
// Interface
//

var Kraken = function(host, port) {
  this._host = host;
  this._port = port;
  this._memcached = new Memcached({ servers: [ this._host + ":" + this._port ] });
};

Kraken.prototype.connect = function(callback) {
  this._memcached.pool.use('subscribe', callback);
};

Kraken.prototype.disconnect = function() {
  this._memcached.end();
};

/**
 * Topics must not include spaces.
 *
 * @param topics Array<String>
 */
Kraken.prototype.subscribe = function(topics, callback) {
  if (topics.length > 0) {
    this._memcached.set(
        "subscribe", topics.join(" "), 0, 0, callback);
  }
};

/**
 * Topics must not include spaces.
 *
 * @param topics Array<String>
 */
Kraken.prototype.unsubscribe = function(topics, callback) {
  if (topics.length > 0) {
    this._memcached.set(
        "unsubscribe", topics.join(" "), 0, 0, callback);
  }
};

/**
 * @param topics_message_pairs Array<[Array<String>, String]>
 */
Kraken.prototype.publish = function(topic_message_pairs, callback) {
  if (topic_message_pairs.length > 0) {
    var buffer = [];
    topic_message_pairs.forEach(function(topic_message_pair) {
      var topics = topic_message_pair[0];
      var message = topic_message_pair[1];
      buffer.push("MESSAGE ");
      buffer.push(topics.join(" "));
      buffer.push(" ");
      buffer.push(Buffer.byteLength(message));
      buffer.push(DELIMITER);
      buffer.push(message);
      buffer.push(DELIMITER);
    });
    this._memcached.set(
        "publish", buffer.join(""), 0, 0, callback);
  }
};

/**
 * @return Array<[Array<String>, String]> An array of topics, message pairs.
 */
Kraken.prototype.receiveMessages = function(callback) {
  this._memcached.get("messages", function(err, result) {
    if (err) {
      if (err.type == 'NOT_FOUND') {
        callback(null, []);
      } else {
        callback(err, null);
      }
    } else {
      if (result !== undefined) {
        var buffer = new Buffer(result[0]);

        var offset = 0;
        var results = [];
        while (true) {
          var line = processLine(buffer, offset);
          if (line == null) break;
          var line_parts = line.str.split(" ");
          var topics = line_parts.slice(1, line_parts.length - 1);
          var message_length = parseInt(line_parts[line_parts.length-1], 10);
          var message = buffer.toString('utf8', line.next, line.next+message_length);
          results.push([topics, message]);
          offset = line.next + message_length + 2;
        }
        callback(null, results);
      }
    }
  });
};

//
// Utility
//

function processLine(buffer, start) {
  var i = start;
  var max = buffer.length - 1;
  while (i < max) {
    if (buffer[i] === 0x0d && buffer[i + 1] === 0x0a) {
      return {
        str: buffer.toString('utf8', start, i),
        next: i + 2
      };
    }
    i++;
  }
  return null;
}


module.exports = Kraken;
