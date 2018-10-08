/**
 * @fileoverview Description of this file.
 *
 * CAUTION: There are subtleties in deploying this, because of our intended
 * separation of sandbox, staging, and production pipelines.
 *
 * These functions process fileNotifications from google cloud storage,
 * determine whether a new file needs to be embargoed, and if not, moves
 * the file to the destination bucket. Only one function can be registered
 * to trigger on a bucket operation.
 *
 * The destination bucket is hard coded into different functions, one for each
 * project.  The trigger bucket and the project are both determined by the
 * deployment command.  Tried using projectId to determine destination bucket,
 * but the projectId is not reliably available.
 *
 * We should trigger buckets within the same project the function will be deployed
 * in. The cmds for triggering those functions can be found in README of this dir.
 *
 */

'use strict';

var google = require('googleapis');
var http = require('http');

/**
 * Checks whether a file is eligible for processing, e.g. if it exists, since we
 * get notifications on delete operations.
 *
 * @param {file} file The object to (possibly) move.
 */
exports.fileIsProcessable = function (file) {
  // TODO: make this better. I am sure this is not the only reason to not
  // process a file.
    return (file.resourceState !== 'not_exists');
};

/**
 * Executes a function with default auth.
 *
 * @param {function} func The function to invoke with auth.
 * @param {function} fail The function to call to indicate failure.
 */
exports.executeWithAuth = function (func, fail) {
    google.auth.getApplicationDefault(
        function (err, authClient, projectId) {
            if (err) {
                fail();
                return;
            }

            if (authClient.createScopedRequired && authClient.createScopedRequired()) {
                // This isn't actually executing.
                authClient = authClient.createScoped(
                    ['https://www.googleapis.com/auth/cloud-platform']
                );
            }

            func(authClient, projectId);
        }
    );
};

/**
 * Create a function to copy and delete a single file.
 * Ideally, the destination should be determined based on the project ID,
 * but the project ID does not seem to be reliably available.
 *
 * @param {file} file The object to move.
 * @param {function} done The callback called when the move completes.
 */
exports.makeMoveWithAuth = function (file, destBucket, done) {
    return function (authClient, projectId) {
        var storage;

        storage = google.storage(
            {"version": "v1", "auth": authClient, "project": projectId}
        );

        console.log('copying: ', file.name, ' to ', destBucket);
        // Copy the file.
        storage.objects.copy(
            {
                "sourceBucket": file.bucket,
                "sourceObject": encodeURIComponent(file.name),
                "destinationBucket": destBucket,
                "destinationObject": encodeURIComponent(file.name)
            },
            // This will be called when copy completes.  If the copy
            // is successful, this attempts to delete the source file.
            // Additional parameters msg, and incoming, are unused.
            function (err) {
                if (err) {
                    console.log('copy err: ', err);
                    done(err);
                } else {
                    // Delete the object, checking generation in case it changed.
                    // TODO - add check that this is running in the mlab-oti project,
                    // and don't delete from other projects.
                    // TODO - remove this condition when we are happy with
                    // deletion.
                    if (file.name.substring(0, 5) === 'test/') {
                        storage.objects.delete(
                            {
                                "bucket": file.bucket,
                                "object": encodeURIComponent(file.name),
                                "generation": file.generation,
                            },
                            // This will be called when delete completes.
                            // Additional parameters msg, and incoming, are unused.
                            function (err) {
                                if (err) {
                                    console.log('delete err: ', err);
                                    done(err);
                                } else {
                                    done(null);
                                }
                            }
                        );
                    } else {
                        done(null);
                    }
                }
            }
        );
    };
};

/**
 * Determines whether a file object should be embargoed, or transferred
 * immediately.
 *
 * @param {object} file The file under consideration
 */
exports.shouldEmbargo = function (file) {
    // Unconditionally return false.
    return false;
};

/**
 * Trigger the operation by embargo app engine.
 *
 * @param {string} project The cloud project ID
 * @param {string} sourceBucket The Cloud Storage bucket that holds the source file.
 * @param {string} filename The file name to be embargoed.
 * @param {function} callback The callback function called when this function completes.
 */
exports.triggerEmbargoHandler = function (project, sourceBucket, filename, callback) {
    var gsFilename, safeFilename;
    gsFilename = "gs://" + sourceBucket + "/" + filename;
    safeFilename = new Buffer(gsFilename).toString("base64");
    http.get('http://embargo-dot-' + project +
        '.appspot.com/submit?file=' + safeFilename,
        function (res) {
            res.on('data', function (data) {});
            res.on('end',
                function () {
                    console.log('Embargo done', gsFilename);
                    callback();
                });
        });
};


/**
 * Cloud Function to be triggered by Cloud Storage,
 * moves the file to the requested bucket.
 *
 * @param {object} event The Cloud Storage notification event.
 * @param {string} project The cloud project ID
 * @param {function} done The callback function called when this function completes.
 */
exports.embargoOnFileNotification = function (event, project, done) {
    var file = event.data;

    if (exports.fileIsProcessable(file)) {
        if (exports.shouldEmbargo(file)) {
            exports.triggerEmbargoHandler(project, file.bucket, file.name, done);
            console.log('Embargo: ', file.bucket, file.name);
        } else {
            exports.executeWithAuth(exports.makeMoveWithAuth(file, 'archive-' + project, done));
        }
    } else {
        done(null);
    }
};

/**
 * Cloud Function to be triggered by Cloud Storage notifications, for
 * mlab-sandbox.
 *
 * @param {object} event The Cloud Storage notification event.
 * @param {function} done The callback function called when this function completes.
 */
exports.embargoOnFileNotificationSandbox = function (event, done) {
    exports.embargoOnFileNotification(event, 'mlab-sandbox', done);
};

/**
 * Cloud Function to be triggered by Cloud Storage notifications, for
 * mlab-staging.
 *
 * @param {object} event The Cloud Storage notification event.
 * @param {function} done The callback function called when this function completes.
 */
exports.embargoOnFileNotificationStaging = function (event, done) {
    exports.embargoOnFileNotification(event, 'mlab-staging', done);
};

/**
 * Cloud Function to be triggered by Cloud Storage notifications, for
 * production (mlab-oti).
 *
 * @param {object} event The Cloud Storage notification event.
 * @param {function} done The callback function called when this function completes.
 */
exports.embargoOnFileNotificationOti = function (event, done) {
    exports.embargoOnFileNotification(event, 'mlab-oti', done);
};
