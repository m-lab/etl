

'use strict';

var google = require("googleapis");

exports.fileIsProcessable = function (file) {
  // TODO: make this better. I am sure this is not the only reason to not
  // process a file.
    return (file.resourceState !== "not_exists");
};

exports.queueForFile = function (filename) {
    var key, re, experiment_to_task_queue;
    experiment_to_task_queue = {
        "switch": "etl-disco-queue",
        "ndt": "etl-ndt-queue",
        "sidestream": "etl-sidestream-queue",
        "paris-traceroute": "etl-paris-traceroute-queue"
    };
    // TODO - fix this.
    for (key in experiment_to_task_queue) {
        re = new RegExp("^" + key + "/\\d{4}/\\d{2}/\\d{2}/[0-9a-z_a-z:.-]+");
        if (re.test(filename)) {
            return experiment_to_task_queue[key];
        }
    }
    return null;
};

exports.enqueueFileTask = function (project, bucket, filename, callback) {
    var http, gsFilename, safeFilename;
    http = require('http');
    gsFilename = "gs://" + bucket + "/" + filename;
    safeFilename = new Buffer(gsFilename).toString("base64");
    http.get('http://queue-pusher-dot-' + project +
        '.appspot.com/receiver?filename=' + safeFilename,
        function (res) {
            res.on('data', function (data) {});
            res.on('end',
                function () {
                    console.log('Enqueue GET done', gsFilename);
                    callback();
                });
        });
};

/**
 * Cloud Function to be triggered by Cloud Storage, enqueues the file to the
 * proper task queue (or none at all).
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} The callback function.
 */
exports.createSandboxTaskOnFileNotification = function (event, callback) {
    var file = event.data;

    if (exports.fileIsProcessable(file) && exports.queueForFile(file.name)) {
        exports.enqueueFileTask('mlab-sandbox', file.bucket, file.name, callback);
    } else {
        callback();
    }
};

/**
 * Cloud Function to be triggered by Cloud Storage, enqueues the file to the
 * proper task queue (or none at all).
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} The callback function.
 */
exports.createStagingTaskOnFileNotification = function (event, callback) {
    var file = event.data;

    if (exports.fileIsProcessable(file) && exports.queueForFile(file.name)) {
        exports.enqueueFileTask('mlab-staging', file.bucket, file.name, callback);
    } else {
        callback();
    }
};

/**
 * Cloud Function to be triggered by Cloud Storage, enqueues the file to the
 * proper task queue (or none at all).
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} The callback function.
 */
exports.createProdTaskOnFileNotification = function (event, callback) {
    var file = event.data;

    if (exports.fileIsProcessable(file) && exports.queueForFile(file.name)) {
        exports.enqueueFileTask('mlab-oti', file.bucket, file.name, callback);
    } else {
        callback();
    }
};

exports.createSandboxTaskOnEmbargoFileNotification = exports.createSandboxTaskOnFileNotification
exports.createStagingTaskOnEmbargoFileNotification = exports.createStagingTaskOnFileNotification
exports.createProdTaskOnEmbargoFileNotification = exports.createProdTaskOnFileNotification