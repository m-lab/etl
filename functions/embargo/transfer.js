/**
 * @fileoverview Description of this file.
 * These functions process fileNotifications from google cloud storage,
 * determine whether a new file needs to be embargoed, and if not, moves
 * the file to the archive bucket.
 *
 * It currently supports two hard coded destination buckets, archive-mlab-oti, for GCF
 * deployed in mlab-oti (production), and destination-mlab-sandbox for any other
 * deployment (though the destination may fail for projects other than
 * mlab-sandbox).
 *
 * To deploy this cloud function to mlab-oti (until we get autodeploy set up):

 * // Create the buckets
 * gsutil mb -p mlab-oti archive-mlab-oti
 * gsutil mb -p mlab-oti scraper-mlab-oti
 * gsutil mb -p mlab-oti functions-mlab-oti
 * // Deploy the functions.
 * gcloud beta functions deploy transferOnFileNotification \
 *   --stage-bucket=functions-mlab-oti \
 *   --trigger-bucket=scraper-mlab-oti \
 *   --project=mlab-oti
 */

'use strict';

var google = require('googleapis');

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
 * Create a function to copy and delete a single file.  The destination
 * is determined by the projectId passed in from executeWithAuth.
 *
 * @param {file} file The object to move.
 * @param {function} done The callback called when the move completes.
 */
exports.makeMoveWithAuth = function (file, done) {
    return function (authClient, projectId) {
        // Choose the destination bucket, based on projectId.
        var destBucket, storage;

        if (projectId === 'mlab-oti') {
            destBucket = 'archive-mlab-oti';
        } else {
            // For projects other than mlab-oti, write files elsewhere.
            destBucket = 'destination-mlab-sandbox';
        }

        storage = google.storage(
            {"version": "v1", "auth": authClient, "project": projectId}
        );

        console.log('copying: ', destBucket, file.name);
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
            function (err, msg, incoming) {
                if (err) {
                    console.log('copy err: ', err);
                    done(err);
                } else {
                    // Delete the object, checking generation in case it changed.
                    // TODO - add check for mlab-oti project, and don't delete
                    // from other projects.
                    storage.objects.delete(
                        {
                            "bucket": file.bucket,
                            "object": encodeURIComponent(file.name),
                            "generation": file.generation,
                        },
                        // This will be called when delete completes.
                        function (err, msg, incoming) {
                            if (err) {
                                console.log('delete err: ', err);
                                done(err);
                            } else {
                                done(err);
                            }
                        }
                    );
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
    // All ndt files can bypass embargo.
    return file.name.substring(0, 4) !== 'ndt/';
};

/**
 * Cloud Function to be triggered by Cloud Storage,
 * moves the file to the archive-mlab-oti bucket.
 *
 * @param {object} event The Cloud Storage notification event.
 * @param {function} done The callback function called when this function completes.
 */
exports.transferOnFileNotification = function transferOnFileNotification(event, done) {
    var file = event.data;

    if (exports.fileIsProcessable(file)) {
        if (exports.shouldEmbargo(file)) {
            // TODO - notify the embargo system.
            console.log('Ignoring: ', file.bucket, file.name);
        } else {
            exports.executeWithAuth(exports.makeMoveWithAuth(file, done));
        }
    } else {
        done(null);
    }
};
