The cloud functions in this repository are used to index prow job results for time-sharded
retrieval by the ci-search project. Deployment requires editor permissions in the
openshift-gce-devel project.

The functions operate on origin-ci-test and so must be deployed in the openshift-gce-devel
project. The service account search-index-gcs-writer@openshift-gce-devel.iam.gserviceaccount.com
was created ahead of time and given storage creator/viewer on the bucket. During first deployment
the function should *not* be accessible to external viewers.