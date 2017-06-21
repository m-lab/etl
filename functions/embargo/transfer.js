/**
 * @fileoverview Description of this file.
  To deploy this cloud function to mlab-sandbox, try:

  gcloud beta functions deploy fileNotification \
    --stage-bucket=functions-mlab-oti \
    --trigger-bucket=scraper-mlab-oti \
    --project=mlab-oti
 */

var sys = require('sys');
var exec = require('child_process').exec;

var google = require('googleapis');

exports.fileIsProcessable = function (file) {
  // TODO: make this better. I am sure this is not the only reason to not
  // process a file.
    return (file.resourceState !== 'not_exists');
};

/**
 * Executes a lambda with auth.
 *
 * @param {function} func The function to invoke with auth.
 * @param {function} fail The function to call to indicate failure.
 */
exports.executeWithAuth = function (func, fail) {
    // If you want things to be authenticated, then put them inside the callback
    // here.
    google.auth.getApplicationDefault(
        function (err, authClient, projectId) {
            if (err) {
                fail();
                return;
            }

            func(authClient, projectId);
        }
    );
};

var puts = function(error, stdout, stderr) { sys.puts(stdout); };

/**
 * Moves a file object from current location to archive-mlab-oti bucket.
 *
 * @param {object} file The file to move.
 * @param {function} done The callback function to indicate cloud function is
 *                        complete.
 */
exports.moveFile = function (file, done) {
    // TODO move the file.
    var gsFilename = 'gs://' + file.bucket + '/' + file.name;
    var gsDest = 'gs://archive-mlab-oti'
    child = exec('gsutil mv ' + gsFilename + ' ' + gsDest,
        function (err, stdout, stderr) {
            if (err !== null) {
                console.log('gsutil error: ' + err);
            }
            done();
        });
};

/**
 * Determines whether a file object should be embargoed, or transfered
 * immediately.
 *
 * @param {object} file The file under consideration
 */
exports.shouldEmbargo = function (file) {
    return false;
};

/**
 * Cloud Function to be triggered by Cloud Storage,
 * moves the file to the
 * archive-mlab-oti bucket.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} done The callback function to indicate function complete.
 */
exports.fileNotification = function fileNotification (event, done) {
    const file = event.data;

    if (exports.fileIsProcessable(file)) {
        if (exports.shouldEmbargo()) {
            // TODO - notify the embargo system.
        } else {
            exports.moveFile(file, done);
        }
    } else {
      done();
    }
};
