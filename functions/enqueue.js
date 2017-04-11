var google = require("googleapis");
var taskqueue = google.taskqueue("v1beta2")

exports.fileIsProcessable = function (file) {
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


exports.enqueueFileTask = function (bucket, filename, queue) {
    var gsFilename = "gs://" + bucket + "/" + filename;
    var params = {
        "project": "mlab-sandbox",
        "queue": "etl-parser-queue",
        "payloadBase64": new Buffer(gsFilename).toString("base64")
    };
    taskqueue.tasks.insert(params, function (err, response) {
        if (err) {
            console.log("Failed to enqueue", filename, "error was", err);
        } else {
            console.log("Enqueued", filename);
        }
    });
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
        exports.enqueueFileTask(file.bucket, file.name, queue);
    }
    callback();
};
