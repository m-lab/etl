#!/bin/bash
#
# delete-appengine.sh deletes all service versions after the first 9 with a
# zero traffic split. This script is meant to run as a late step of the
# cloudbuild deployment. Running this regularly will keep the number of
# versions around 10 per service (for rollbacks) and below 210 globally (the
# hard limit imposed by AppEngine).

# Example: Below is example output from `gcloud app versions list`. The
# 'TRAFFIC_SPLIT' determines whether any traffic is directed to this service.
# Standard environment services (i.e. non-flex) are always in 'SERVING' state.
#
# SERVICE                 VERSION.ID       TRAFFIC_SPLIT  LAST_DEPLOYED              SERVING_STATUS
# annotator               20210702t202340  0.00           2021-07-02T16:33:46-04:00  STOPPED
# annotator               20210713t053025  1.00           2021-07-13T01:40:43-04:00  SERVING
# locate                  20180425t084144  0.00           2018-04-25T08:42:52-04:00  STOPPED
# locate                  20190305t132521  0.00           2019-03-05T13:26:34-05:00  STOPPED
# locate                  20190305t135528  0.00           2019-03-05T13:57:06-05:00  SERVING
# locate                  20190305t184900  0.00           2019-03-05T18:50:09-05:00  STOPPED
# locate                  20190314t100404  1.00           2019-03-14T10:05:20-04:00  SERVING
# default                 20200507t202419  0.00           2020-05-07T16:25:06-04:00  SERVING
# default                 20200507t210846  0.00           2020-05-07T17:09:25-04:00  SERVING
# default                 20200508t184639  0.00           2020-05-08T14:47:22-04:00  SERVING
# default                 20200508t192236  0.00           2020-05-08T15:23:13-04:00  SERVING
# default                 20200511t171409  0.00           2020-05-11T13:15:00-04:00  SERVING
# default                 20200514t140435  0.00           2020-05-14T10:05:25-04:00  SERVING
# default                 20200520t171631  0.00           2020-05-20T13:17:23-04:00  SERVING
# default                 20200520t174419  0.00           2020-05-20T13:45:05-04:00  SERVING
# default                 20200526t210545  0.00           2020-05-26T17:06:37-04:00  SERVING
# default                 20200630t221216  0.00           2020-06-30T18:13:18-04:00  SERVING
# default                 20200817t211428  0.00           2020-08-17T17:15:35-04:00  SERVING
# default                 20200817t212515  0.00           2020-08-17T17:25:59-04:00  SERVING

# Delete service versions after the first 9 with a zero traffic split.
gcloud --project=${PROJECT_ID} app versions list | \
    awk '{
       if ($3 == 0) {
         count[$1]+=1
       };
       if (count[$1] > 9 && $3 == 0) {
         print $1, $2
       }
    }' | \
    while read service version ; do 
        echo gcloud app versions delete --service $service $version
    done
