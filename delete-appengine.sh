#!/bin/bash
#
# delete-appengine.sh deletes all service versions after the 9 most recently
# deployed versions with zero traffic split. This script is meant to run as a
# late step of the cloudbuild deployment. Running this regularly will keep the
# number of versions around 10 per service (for rollbacks) and below 210
# globally (the hard limit imposed by AppEngine).

# Example: Below is example output from `gcloud app versions list`. The
# 'TRAFFIC_SPLIT' determines whether any traffic is directed to this service.
# Standard environment services (i.e. non-flex) are always in 'SERVING' state.
# The service versions are ordered by SERVICE name (ascending) then
# LAST_DEPLOYED (descending).
#
# SERVICE                 VERSION.ID       TRAFFIC_SPLIT  LAST_DEPLOYED              SERVING_STATUS
# annotator               20210713t053025  1.00           2021-07-13T01:40:43-04:00  SERVING
# annotator               20210702t202340  0.00           2021-07-02T16:33:46-04:00  STOPPED
# locate                  20211101t225708  1.00           2021-11-01T18:58:52-04:00  SERVING
# locate                  20211027t205713  0.00           2021-10-27T17:10:45-04:00  STOPPED
# locate                  20211026t195123  0.00           2021-10-26T16:04:28-04:00  STOPPED
# locate                  20211026t174157  0.00           2021-10-26T13:55:53-04:00  STOPPED
# locate                  20211020t220644  0.00           2021-10-20T18:20:54-04:00  STOPPED
# default                 20211012t163346  1.00           2021-10-12T12:35:18-04:00  SERVING
# default                 20211011t235453  0.00           2021-10-11T19:56:14-04:00  SERVING
# default                 20211011t224634  0.00           2021-10-11T18:47:58-04:00  SERVING
# default                 20211011t174445  0.00           2021-10-11T13:48:54-04:00  SERVING
# default                 20210812t202133  0.00           2021-08-12T16:22:14-04:00  SERVING
# default                 20210810t163607  0.00           2021-08-10T12:37:28-04:00  SERVING
# default                 20210518t153854  0.00           2021-05-18T11:39:56-04:00  SERVING
# default                 20210427t170445  0.00           2021-04-27T13:06:07-04:00  SERVING
# default                 20210420t212953  0.00           2021-04-20T17:30:45-04:00  SERVING
# default                 20210420t174553  0.00           2021-04-20T13:46:37-04:00  SERVING
# default                 20210409t180654  0.00           2021-04-09T14:07:38-04:00  SERVING

# Delete service versions after the most recent 9 with a zero traffic split.
# NOTE: any service version with a non-zero traffic split is never deleted.
gcloud --project=${PROJECT_ID} \
    app versions list --sort-by=SERVICE,~LAST_DEPLOYED | \
    awk '{
       if ($3 == 0) {
         count[$1]+=1
         if (count[$1] > 9) {
           print $1, $2
         }
       }
    }' | \
    while read service version ; do 
        echo gcloud app versions delete --service $service $version
    done
