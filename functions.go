// Package cisearch provides GCS indexer functions
package cisearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	// SelfLink                string                 `json:"selfLink"`
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
	// Generation              string                 `json:"generation"`
	// Metageneration          string                 `json:"metageneration"`
	ContentType string    `json:"contentType"`
	TimeCreated time.Time `json:"timeCreated"`
	Updated     time.Time `json:"updated"`
	// TemporaryHold           bool                   `json:"temporaryHold"`
	// EventBasedHold          bool                   `json:"eventBasedHold"`
	// RetentionExpirationTime time.Time              `json:"retentionExpirationTime"`
	// StorageClass            string                 `json:"storageClass"`
	// TimeStorageClassUpdated time.Time              `json:"timeStorageClassUpdated"`
	Size      string `json:"size"`
	MD5Hash   string `json:"md5Hash"`
	MediaLink string `json:"mediaLink"`
	// ContentEncoding         string                 `json:"contentEncoding"`
	// ContentDisposition      string                 `json:"contentDisposition"`
	// CacheControl            string                 `json:"cacheControl"`
	Metadata map[string]interface{} `json:"metadata"`
	// CRC32C                  string                 `json:"crc32c"`
	// ComponentCount          int                    `json:"componentCount"`
	// Etag                    string                 `json:"etag"`
	// CustomerEncryption      struct {
	// 	EncryptionAlgorithm string `json:"encryptionAlgorithm"`
	// 	KeySha256           string `json:"keySha256"`
	// }
	// KMSKeyName    string `json:"kmsKeyName"`
	// ResourceState string `json:"resourceState"`
}

// RecordFailingJob creates a date sharded index of all failed jobs within
// a bucket. Jobs that fail are linked from
//
//   gs://BUCKET/index/job-failures/RFC3339_DATE_OF_FAILURE/JOB_NAME/BUILD_NUMBER
//
// with the contents of that file being the finished.json (for now) and a 'link'
// metadata attribute pointing to a gs:// path to the source. Readers should not
// assume anything about the contents of the object or that the link is in the
// same bucket.
func RecordFailingJob(ctx context.Context, e GCSEvent) error {
	// meta, err := metadata.FromContext(ctx)
	// if err != nil {
	// 	return fmt.Errorf("metadata.FromContext: %v", err)
	// }
	base := path.Base(e.Name)
	switch base {
	case "finished.json":
		parts := strings.Split(e.Name, "/")
		if len(parts) < 4 {
			return nil
		}
		client, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		r, err := client.Bucket(e.Bucket).Object(e.Name).NewReader(ctx)
		if err != nil {
			return err
		}
		data, err := ioutil.ReadAll(r)
		var finished Finished
		if err := json.Unmarshal(data, &finished); err != nil {
			return err
		}
		if finished.Passed == nil || *finished.Passed || finished.Timestamp == nil || *finished.Timestamp == 0 {
			return nil
		}

		// build index components
		build := parts[len(parts)-2]
		job := parts[len(parts)-3]
		finishedAt := time.Unix(*finished.Timestamp, 0)
		key := finishedAt.UTC().Format(time.RFC3339)
		u := (&url.URL{
			Scheme: "gs",
			Host:   e.Bucket,
			Path:   path.Dir(e.Name),
		}).String()
		indexPath := path.Join("index", "job-failures", key, job, build)

		// write the link
		w := client.Bucket(e.Bucket).Object(indexPath).NewWriter(ctx)
		w.ObjectAttrs.Metadata = map[string]string{"link": u}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("failed to link %s to %s", indexPath, u)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to link %s to %s", indexPath, u)
		}
		log.Printf("Indexed failed job %s to gs://%s/%s", u, e.Bucket, indexPath)
	}
	return nil
}
