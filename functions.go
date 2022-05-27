// Package cisearch provides GCS indexer functions
package cisearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
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

// IndexJobs creates a date sharded index of all jobs within
// a bucket. Jobs that have completed are linked from
//
//   gs://BUCKET/index/job-failures/RFC3339_DATE_OF_FAILURE/JOB_NAME/BUILD_NUMBER
//
// with the contents of that file a job result object and a 'link'
// metadata attribute pointing to a gs:// path to the source. The
// 'state' metadata attribute is set to 'success', 'failure', or
// 'error' if the passed attribute is not set in the finished.json.
//
// Readers should not assume anything about the contents of the
// object or that the link is in the same bucket.
func IndexJobs(ctx context.Context, e GCSEvent) error {
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
		client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
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
		if finished.Timestamp == nil || *finished.Timestamp == 0 {
			return nil
		}

		var state string
		switch {
		case finished.Passed == nil:
			state = "error"
		case *finished.Passed:
			state = "success"
		default:
			state = "failed"
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
		indexPath := path.Join("index", "job-state", key, job, build)

		// set the data for the job to the result
		if data, err = json.Marshal(JobResult{
			State:       state,
			CompletedAt: finishedAt.Unix(),
			Link:        u,
		}); err != nil {
			return fmt.Errorf("could not serialize job result: %v", err)
		}

		// write the link with the metadata contents
		w := client.
			Bucket(e.Bucket).
			Object(indexPath).
			If(storage.Conditions{DoesNotExist: true}).
			NewWriter(ctx)
		w.ObjectAttrs.Metadata = map[string]string{
			"link":      u,
			"state":     state,
			"completed": strconv.FormatInt(finishedAt.Unix(), 10),
		}
		if _, err := w.Write(data); err != nil {
			defer w.Close()
			return fmt.Errorf("failed to link %s to %s: %v", indexPath, u, err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to link %s to %s: %v", indexPath, u, err)
		}
		log.Printf("Indexed job %s with state %s to gs://%s/%s", u, state, e.Bucket, indexPath)

	case "job_metrics.json":
		// only process job metrics that appear to be in a smaller set of logs
		parts := strings.Split(e.Name, "/")
		if len(parts) < 4 {
			return nil
		}
		var u, build, job string
		switch {
		case parts[0] == "logs":
			u = (&url.URL{
				Scheme: "gs",
				Host:   e.Bucket,
				Path:   path.Join(parts[:3]...),
			}).String()
			job = parts[1]
			build = parts[2]
			switch {
			case strings.HasPrefix(job, "periodic-ci-openshift-release-"),
				strings.HasPrefix(job, "release-openshift-"):
			default:
				// log.Printf("Skip job that is not a release job: %s", e.Name)
				return nil
			}
		default:
			//log.Printf("Skip job that is not postsubmit/periodic: %s", e.Name)
			return nil
		}

		client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
		if err != nil {
			return err
		}

		// read the raw output and transform into the consolidated form
		// {
		//	 "<name>[{<label>="<value>"]": {"timestamp":<int64>,"value":"<float64 string>"},
		//   ...
		// }
		r, err := client.Bucket(e.Bucket).Object(e.Name).NewReader(ctx)
		if err != nil {
			return err
		}
		metrics := make(map[string]PrometheusResult)
		d := json.NewDecoder(r)
		var rows int
		for err = d.Decode(&metrics); err == nil; err = d.Decode(&metrics) {
			rows++
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to decode metric on line %d: %v", rows+1, err)
		}

		outputMetrics := make(map[string]OutputMetric, len(metrics))
		for name, v := range metrics {
			if v.Status != "success" {
				continue
			}
			if v.Data.ResultType != "vector" {
				continue
			}
			if len(v.Data.Result) == 0 {
				continue
			}
			if len(v.Data.Result) == 1 && len(v.Data.Result[0].Metric) == 0 {
				outputMetrics[name] = OutputMetric{
					Value:     v.Data.Result[0].Value.Value,
					Timestamp: v.Data.Result[0].Value.Timestamp,
				}
				//log.Printf("%s %s @ %d", name, v.Data.Result[0].Value.Value, v.Data.Result[0].Value.Timestamp)
				continue
			}
			var label string
			for i, result := range v.Data.Result {
				if len(label) == 0 {
					for k := range result.Metric {
						label = k
						break
					}
					if len(label) == 0 {
						continue
					}
				}
				value, ok := result.Metric[label]
				if !ok {
					log.Printf("warn: Dropped result %d from %s because no value for metric %s", i, name, label)
					continue
				}
				outputMetrics[fmt.Sprintf("%s{%s=%q}", name, label, value)] = OutputMetric{
					Value:     result.Value.Value,
					Timestamp: result.Value.Timestamp,
				}
				//log.Printf("%s{%s=%q} %s @ %d", name, label, value, result.Value.Value, result.Value.Timestamp)
			}
		}

		duration, ok := outputMetrics["job:duration:total:seconds"]
		if !ok {
			return fmt.Errorf("job not indexed, does not have metric %q", "job:duration:total:seconds")
		}

		data, err := json.Marshal(outputMetrics)
		if err != nil {
			return fmt.Errorf("unable to marshal output metrics: %v", err)
		}

		// build index components
		finishedAt := time.Unix(duration.Timestamp, 0)
		key := finishedAt.UTC().Format(time.RFC3339)
		indexPath := path.Join("index", "job-metrics", key, job, build)

		// write the link with the metadata contents
		w := client.
			Bucket(e.Bucket).
			Object(indexPath).
			If(storage.Conditions{DoesNotExist: true}).
			NewWriter(ctx)
		w.ObjectAttrs.Metadata = map[string]string{
			"link":      u,
			"completed": strconv.FormatInt(finishedAt.Unix(), 10),
		}
		if _, err := w.Write(data); err != nil {
			defer w.Close()
			return fmt.Errorf("failed to write metrics %s to %s: %v", indexPath, u, err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to write metrics %s to %s: %v", indexPath, u, err)
		}

		log.Printf("Indexed %d job metrics %s in %d bytes to gs://%s/%s (link to %s)", len(metrics), e.Name, len(data), e.Bucket, indexPath, u)
	}
	return nil
}

type JobResult struct {
	State       string `json:"state"`
	CompletedAt int64  `json:"completed_at"`
	Link        string `json:"link"`
}

type OutputMetric struct {
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"value"`
}

type PrometheusResult struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
}

type PrometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []PrometheusMetric `json:"result"`
}

type PrometheusMetric struct {
	Metric PrometheusLabels `json:"metric"`
	Value  PrometheusValue  `json:"value"`
}

type PrometheusValue struct {
	Timestamp int64
	Value     string
}

// PrometheusLabels avoids deserialization allocations
type PrometheusLabels map[string]string

var _ json.Marshaler = PrometheusLabels(nil)
var _ json.Unmarshaler = &PrometheusLabels{}

func (l PrometheusLabels) MarshalJSON() ([]byte, error) {
	if len(l) == 0 {
		return []byte(`{}`), nil
	}
	return json.Marshal(map[string]string(l))
}

func (l *PrometheusLabels) UnmarshalJSON(data []byte) error {
	switch {
	case len(data) == 4 && bytes.Equal(data, []byte("null")):
		return nil
	case len(data) == 2 && bytes.Equal(data, []byte("{}")):
		if l == nil {
			return nil
		}
		for k := range *l {
			delete(*l, k)
		}
		return nil
	}
	if l == nil {
		*l = make(map[string]string)
	}
	var m *map[string]string = (*map[string]string)(l)
	return json.Unmarshal(data, m)
}

type parseState int

const (
	startState parseState = iota
	timestampState
	stringNumberState
	closeState
	doneState
)

func (l *PrometheusValue) UnmarshalJSON(data []byte) error {
	switch {
	case len(data) == 4 && bytes.Equal(data, []byte("null")):
		return nil
	case len(data) == 2 && bytes.Equal(data, []byte("[]")):
		return fmt.Errorf("unexpected value")
	}
	var state parseState = startState

	data = bytes.TrimSpace(data)
	for len(data) > 0 {
		switch data[0] {
		case '[':
			switch state {
			case startState:
				if l == nil {
					*l = PrometheusValue{}
				}
				data = bytes.TrimSpace(data[1:])
				state = timestampState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		case ']':
			switch state {
			case closeState:
				data = bytes.TrimSpace(data[1:])
				state = doneState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		default:
			switch state {
			case timestampState:
				pos := bytes.Index(data, []byte(","))
				if pos == -1 {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find comma")
				}
				timestampBytes := bytes.TrimSpace(data[:pos])
				var err error
				l.Timestamp, err = strconv.ParseInt(string(timestampBytes), 10, 64)
				if err != nil {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], timestamp was not an int64: %v", err)
				}
				data = data[pos+1:]
				state = stringNumberState
			case stringNumberState:
				pos := bytes.Index(data, []byte("]"))
				if pos == -1 {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find ending bracket in %q", string(data))
				}
				numberBytes := bytes.TrimSpace(data[:pos])
				if len(numberBytes) < 2 || numberBytes[0] != '"' || numberBytes[len(numberBytes)-1] != '"' {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find number string")
				}
				b := numberBytes[1 : len(numberBytes)-1]
				if len(b) != len(bytes.TrimSpace(b)) {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], number was not a valid float64: whitespace in string")
				}
				s := string(b)
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], number was not a valid float64: %v", err)
				}
				l.Value = s
				data = data[pos:]
				state = closeState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		}
	}
	if state != doneState {
		return fmt.Errorf("expected [<timestamp int>, \"<number string>\"]")
	}
	return nil
}
