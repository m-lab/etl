/**
 * @fileoverview Description of this file.
  To deploy this cloud function to mlab-sandbox, try:

  // Create the buckets
  gsutil mb -p mlab-oti archive-mlab-oti
  gsutil mb -p mlab-oti scraper-mlab-oti
  gsutil mb -p mlab-oti functions-mlab-oti
  // Deploy the functions.
  gcloud beta functions deploy transferOnFileNotification \
    --stage-bucket=functions-mlab-oti \
    --trigger-bucket=scraper-mlab-oti \
    --project=mlab-oti


    CURRENTLY BROKEN
    Things to try?
    GOOGLE_APPLICATION_CREDENTIALS ?

 */

var google = require('googleapis');

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
            console.log('inside auth, projectId = ', projectId);
            if (err) {
                fail();
                return;
            }


            if (authClient.createScopedRequired && authClient.createScopedRequired()) {
                // This isn't actually executing.
                console.log('createScopedRequired');
                authClient = authClient.createScoped(
                    ['https://www.googleapis.com/auth/cloud-platform']);
            }
            console.log(authClient);

            console.log('executing func');
            func(authClient, projectId);
        }
    );
};

/**
 * Executes a lambda with auth.
 *
 * @param {file} file The object to move.
 * @param {function} done The callback function to indicate cloud function is
 *                        complete.
 */
exports.makeMoveWithAuth = function (file, done) {
    return function (authClient, projectId) {
        var destBucket;  // The destination bucket, based on projectId.
        if (projectId === 'mlab-oti') {
            destBucket = 'archive-mlab-oti';
        } else {
            destBucket = 'destination-mlab-sandbox';
        }

        var storage = google.storage(
            {"version": "v1", "auth": authClient, "project": projectId});

        console.log('copying: ', destBucket, encodeURIComponent(file.name));
        storage.objects.copy({
            "sourceBucket": file.bucket,
            "sourceObject": encodeURIComponent(file.name),
            "destinationBucket": destBucket,
            "destinationObject": encodeURIComponent(file.name)
        },
        // This will be called when copy completes.
        function(err, msg, incoming) {
            if (err) {
                console.log('err: ', err);
                console.log('msg: ', msg);
                console.log('calling done after copy failed.');
                done(err, msg, incoming);
            } else {
                // Delete the object, checking generation in case it changed.
                console.log('deleting file')
                storage.objects.delete({
                    "bucket": file.bucket,
                    "object": encodeURIComponent(file.name),
                    "generation": file.generation,
                },
                // This will be called when delete completes.
                function(err, msg, incoming) {
                    if (err) {
                        console.log(err);
                        console.log('calling done after delete failed.');
                        done(err, msg, incoming);
                    } else {
                        console.log('delete succeeded');
                        done(err, msg, incoming);
                    }
                });
            }
        });
  }
}

var puts = function(error, stdout, stderr) { sys.puts(stdout); };


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
 * moves the file to the archive-mlab-oti bucket.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} done The callback function to indicate function complete.
 */
exports.transferOnFileNotification = function transferOnFileNotification (event, done) {
    const file = event.data;

    if (exports.fileIsProcessable(file)) {
        if (exports.shouldEmbargo()) {
            // TODO - notify the embargo system.
        } else {
            exports.executeWithAuth(exports.makeMoveWithAuth(file, done));
        }
    } else {
      done();
    }
};
