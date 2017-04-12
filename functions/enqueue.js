var google = require("googleapis");

exports.fileIsProcessable = function (file) {
  // TODO: make this better. I am sure this is not the only reason to not
  // process a file.
    return (file.resourceState !== "not_exists");
};

exports.queueForFile = function (filename) {
    const experiment_to_task_queue = {
        "ndt": "etl-ndt-queue",
        "sidestream": "etl-sidestream-queue",
        "paris-traceroute": "etl-paris-traceroute-queue"
    };
    var key, re;
    for (key in experiment_to_task_queue) {
        re = new RegExp("^" + key + "/\\d{4}/\\d{2}/\\d{2}/[0-9a-z_a-z:.-]+");
        if (re.test(filename)) {
            return experiment_to_task_queue[key];
        }
    }
    return null;
};


exports.enqueueFileTask = function (bucket, filename, queue, callback) {
  var http = require('http');
  var gsFilename = "gs://" + bucket + "/" + filename;
  var safeFilename = new Buffer(gsFilename).toString("base64");
  http.get('http://push-queue-dot-mlab-sandbox.appspot.com/receiver?queuename=etl-parser-queue&filename=' + safeFilename,
      function (res) {
        res.on('data', function (data) {});
        res.on('end',
               function() {
                 console.log('Enqueue GET done', gsFilename);
                 callback();
               });
      });
  /*
    // If you want things to be authenticated, then put them inside the callback
    // here.
    google.auth.getApplicationDefault(function (err, authClient, projectId) {
        if (err) {
            throw err;
        }

        // push taskqueues are currently non-functional for the REST API
        // TODO: complain about this
        var taskqueue = google.taskqueue({"version": "v1beta2", "auth": authClient})
        var storage = google.storage({"version": "v1", "auth": authClient});
        console.log(storage.buckets.list({"project": "mlab-sandbox"}));
        console.log(storage.buckets.list({"project": "mlab-sandbox"}));

        var gsFilename = "gs://" + bucket + "/" + filename;
        var params = {
            "project": "mlab-sandbox",
            "taskqueue": "etl-parser-queue",
            "payloadBase64": new Buffer(gsFilename).toString("base64")
        };
        taskqueue.tasks.insert(params, function (err, response) {
            if (err) {
                console.log("Failed to enqueue", filename, "error was", err);
            } else {
                console.log("Enqueued", filename);
            }
            callback();
        });
    });
    */
};

/**
 * Cloud Function to be triggered by Cloud Storage, enqueues the file to the
 * proper task queue (or none at all).
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} The callback function.
 */
exports.fileNotification = function fileNotification (event, callback) {
    const file = event.data;
    const queue = exports.queueForFile(file.name);

    if (exports.fileIsProcessable(file) && queue) {
        exports.enqueueFileTask(file.bucket, file.name, queue, callback);
    } else {
      callback();
    }
};
