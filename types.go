package cisearch

// Finished holds the finished.json values of the build
type Finished struct {
	// Timestamp is UTC epoch seconds when the job finished.
	// An empty value indicates an incomplete job.
	Timestamp *int64 `json:"timestamp,omitempty"`
	// Passed is true when the job completes successfully.
	Passed *bool `json:"passed"`
	// Metadata holds data computed by the job at runtime.
	// For example, the version of a binary downloaded at runtime
	Metadata Metadata `json:"metadata,omitempty"`
}

// Metadata holds the finished.json values in the metadata key.
//
// Metadata values can either be string or string map of strings
//
// Special values: infra-commit, repos, repo, repo-commit, links, others
type Metadata map[string]interface{}

// String returns the name key if its value is a string, and true if the key is present.
func (m Metadata) String(name string) (*string, bool) {
	if v, ok := m[name]; !ok {
		return nil, false
	} else if t, good := v.(string); !good {
		return nil, true
	} else {
		return &t, true
	}
}

// Meta returns the name key if its value is a child object, and true if they key is present.
func (m Metadata) Meta(name string) (*Metadata, bool) {
	if v, ok := m[name]; !ok {
		return nil, false
	} else if t, good := v.(Metadata); good {
		return &t, true
	} else if t, good := v.(map[string]interface{}); good {
		child := Metadata(t)
		return &child, true
	}
	return nil, true
}

// Keys returns an array of the keys of all valid Metadata values.
func (m Metadata) Keys() []string {
	ka := make([]string, 0, len(m))
	for k := range m {
		if _, ok := m.Meta(k); ok {
			ka = append(ka, k)
		}
	}
	return ka
}

// Strings returns the submap of values in the map that are strings.
func (m Metadata) Strings() map[string]string {
	bm := map[string]string{}
	for k, v := range m {
		if s, ok := v.(string); ok {
			bm[k] = s
		}
		// TODO(fejta): handle sub items
	}
	return bm
}
