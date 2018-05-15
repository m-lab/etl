/**
 * @fileoverview Description of this file.
 *
 * CAUTION: There are subtleties in deploying this, because of our intended
 * separation of sandbox, staging, and production pipelines.
 *
 * This is far from ideal.  Also, beware that doing multiple transfers from
 * scraper-mlab-oti will soon stop working, because we intend for the production
 * transfer function to also DELETE the file from the scraper-mlab-oti source.
 * We just haven't enabled that yet, because we want to be comfortable that
 * there won't be any risk of data loss, and we haven't done the testing yet
 * to ensure that.
 *
 * These functions process fileNotifications from google cloud storage,
 * determine whether a new file needs to be embargoed, and if not, moves
 * the file to the destination bucket.
 *
 * The destination bucket is hard coded into different functions, one for each
 * project.  The trigger bucket and the project are both determined by the
 * deployment command.  Tried using projectId to determine destination bucket,
 * but the projectId is not reliably available.
 *
 * To deploy this cloud function to mlab-oti (until we get autodeploy set up):
 *
 * // Create the buckets
   export GCLOUD_PROJECT=mlab-oti
   gsutil mb -p $GCLOUD_PROJECT archive-$GCLOUD_PROJECT
   gsutil mb -p $GCLOUD_PROJECT scraper-$GCLOUD_PROJECT
   gsutil mb -p $GCLOUD_PROJECT functions-$GCLOUD_PROJECT
   // Deploy the functions.
   export FN_SUFFIX=${GCLOUD_PROJECT##*-}
   gcloud beta functions deploy embargoOnFileNotification${FN_SUFFIX^} \
     --stage-bucket=functions-$GCLOUD_PROJECT \
     --trigger-bucket=scraper-$GCLOUD_PROJECT \
     --project=$GCLOUD_PROJECT

 * Scraper pushes files into scraper-mlab-oti, but not to corresponding buckets
 * for staging or sandbox.  So staging and sandbox deployments require you to
 * choose what to trigger from.
 * When triggering on any mlab-oti bucket, we MUST NOT delete the file, so
 * please ensure that this will not happen.  (There should be ACLs to prevent
 * this, but please do not rely on them).
 * If we trigger on scraper-mlab-oti, we may miss some files, if they are
 * deleted (by the mlab-oti functions) before we handle them.
 * A simple way to get most of the files that prod is handling is to trigger on
 * archive-mlab-oti, so that the files are copied in a waterfall manner.  However,
 * any embargoed files won't show up in archive-mlab-oti, so use this strategy
 * with caution.
 *
 * // Create the buckets
   export GCLOUD_PROJECT=mlab-staging
   gsutil mb -p $GCLOUD_PROJECT archive-$GCLOUD_PROJECT
   gsutil mb -p $GCLOUD_PROJECT functions-$GCLOUD_PROJECT
   // Deploy the functions.
   export FN_SUFFIX=${GCLOUD_PROJECT##*-}
   gcloud beta functions deploy embargoOnFileNotification${FN_SUFFIX^} \
     --stage-bucket=functions-$GCLOUD_PROJECT \
     --trigger-bucket=scraper-mlab-oti \
     --project=$GCLOUD_PROJECT

 * Or for sandbox, also triggering by files appearing in scraper-mlab-oti:
 *
 * // Create the buckets
   export GCLOUD_PROJECT=mlab-sandbox
   gsutil mb -p $GCLOUD_PROJECT archive-$GCLOUD_PROJECT
   gsutil mb -p $GCLOUD_PROJECT functions-$GCLOUD_PROJECT
   // Deploy the functions.
   export FN_SUFFIX=${GCLOUD_PROJECT##*-}
   gcloud beta functions deploy embargoOnFileNotification${FN_SUFFIX^} \
     --stage-bucket=functions-$GCLOUD_PROJECT \
     --trigger-bucket=scraper-mlab-oti \
     --project=$GCLOUD_PROJECT

 * Alternatively, it might be more desireable to create another sandbox bucket,
 * designate it as the trigger source, and use file transfers to populate it.
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
    // Only sidestream files need to be embargoed.  All others can be
    // transferred.
    return (file.name.substring(0, 11) === 'sidestream/');
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
 * @param {string} destBucket The Cloud Storage bucket to move files to.
 * @param {function} done The callback function called when this function completes.
 */
exports.embargoOnFileNotification = function (event, project, done) {
    var file = event.data;

    if (exports.fileIsProcessable(file)) {
        if (exports.shouldEmbargo(file)) {
            exports.triggerEmbargoHandler(project, file.bucket, file.name, done);
            console.log('Embargo: ', file.bucket, file.name);
        } else {
            exports.executeWithAuth(exports.makeMoveWithAuth(file, destPublicBucket, done));
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
