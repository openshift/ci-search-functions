build:
	go build .
.PHONY: build

deploy-functions: build
	gcloud functions deploy RecordFailingJob \
		--project openshift-gce-devel --runtime go113 \
		--service-account search-index-gcs-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 128MB --timeout=15s \
		--trigger-resource origin-ci-test --trigger-event google.storage.object.finalize
.PHONY: deploy-functions

delete-functions:
	gcloud functions delete RecordFailingJob \
		--project openshift-gce-devel
.PHONY: delete-functions