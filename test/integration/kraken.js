var Kraken = require("lib/kraken");
var assert = require("assert");

describe("KrakenClient", function() {
  var k1 = new Kraken("localhost", 12355);
  var k2 = new Kraken("localhost", 12355);

  /**
   * Messages are delivered asynchronously within Kraken, so we have
   * this little helper function to wait until we receive all expected
   * messages.
   */
  var receiveCountMessages = function(buffer, client, count, done) {
    client.receiveMessages(function(err, results) {
      assert.equal(undefined, err);
      buffer = buffer.concat(results);
      if (buffer.length >= count) {
        done(buffer);
      } else {
        setTimeout(function() {
          receiveCountMessages(buffer, client, count, done);
        }, 10);
      }
    });
  };

  before(function(done){
    k1.connect(function() {
      k2.connect(function() {
        done();
      });
    });
  });

  it("should subscribe successfully", function(done) {
    k1.subscribe(["foo", "bar"], function(err, result) {
      assert.equal(undefined, err);
      done();
    });
  });

  it("should unsubscribe successfully", function(done) {
    k1.unsubscribe(["bar"], function(err, result) {
      assert.equal(undefined, err);
      done();
    });
  });

  it("should publish successfully", function(done) {
    k2.publish([[["foo"], "hi"]], function(err, result) {
      assert.equal(undefined, err);
      k2.publish([[["foo"], "there"]], function(err, result) {
        assert.equal(undefined, err);
        done();
      });
    });
  });

  it("should receive messages successfully", function(done) {
    receiveCountMessages([], k1, 2, function(results) {
      assert.deepEqual(
          [ [[ 'foo' ], 'hi'], [[ 'foo' ], 'there'] ],
          results);
      done();
    });
  });
});

