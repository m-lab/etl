var sinon = require("sinon");
var assert = require("chai").assert;
var expect = require("chai").expect;
var enqueue = require("./enqueue.js");

const sample_data = { kind: 'storage#object', resourceState: 'exists', id: 'm-lab-sandbox/testfile/1491851987540929', selfLink: 'https://www.googleapis.com/storage/v1/b/m-lab-sandbox/o/testfile', name: 'testfile', bucket: 'm-lab-sandbox', generation: '1491851987540929', metageneration: '1', contentType: 'binary/octet-stream', timeCreated: '2017-04-10T19:19:47.519Z', updated: '2017-04-10T19:19:47.519Z', storageClass: 'REGIONAL', size: '0', md5Hash: 'ZDQxZDhjZDk4ZjAwYjIwNGU5ODAwOTk4ZWNmODQyN2U=', mediaLink: 'https://www.googleapis.com/storage/v1/b/m-lab-sandbox/o/testfile?generation=1491851987540929&alt=media', crc32c: 'AAAAAA==' }

describe("ProcessNotification", function () {
  it("calls the callback", function () {
    var callback = sinon.spy()
    enqueue.createStagingTaskOnFileNotification({"data": sample_data}, callback);
    assert(callback.called);
  });
});

describe("queueForFile", function () {
  it("won't return a queue for unparsed files", function () {
    assert.isNotOk(enqueue.queueForFile("test/2008/07/12/blah.tgz"));
    assert.isNotOk(enqueue.queueForFile("npad/2008/07/12/blah.tgz"));
  });
  it("will return a queue for files that should be parsed", function () {
    assert.isOk(enqueue.queueForFile("ndt/2008/07/12/blah.tgz"));
    assert.isOk(enqueue.queueForFile("sidestream/2008/07/12/blah.tgz"));
    assert.isOk(enqueue.queueForFile("paris-traceroute/2008/07/12/blah.tgz"));
  });
});

describe("fileIsProcessable", function() {
  it("won't be processable if it does not exist", function() {
    assert.isNotOk(enqueue.fileIsProcessable({"resourceState": "not_exists"}));
  });
});
