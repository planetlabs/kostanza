package coster

import (
	"bytes"

	"go.opencensus.io/tag"
	"k8s.io/client-go/util/jsonpath"
)

// Mapping models how to map a destination field from a source field within
// a  kubernetes resource. The source is typically a jsonPath expression.
type Mapping struct {
	Default     string
	Destination string
	Source      string
}

// Mapper is a used to manage a set of mappings from source fields in
// a generic interface{} to a destination.
type Mapper struct {
	Entries []Mapping
}

// TagKeys returns a slice of tag.Key structs, useful when preparing your
// opencensus view to accept the dimensions derived from your mapping.
func (m *Mapper) TagKeys() ([]tag.Key, error) {
	tags := []tag.Key{}
	for _, mp := range m.Entries {
		t, err := tag.NewKey(mp.Destination)
		if err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return tags, nil
}

// MapData returns a string map by applying the mappers rules to the obj
// provided. The resulting map should have a corresponding field for every
// source object.
func (m *Mapper) MapData(obj interface{}) (map[string]string, error) {
	res := map[string]string{}
	for _, mp := range m.Entries {
		buf := new(bytes.Buffer)

		j := jsonpath.New(mp.Destination)
		j.AllowMissingKeys(true)

		if err := j.Parse(mp.Source); err != nil {
			return nil, err
		}

		if err := j.Execute(buf, obj); err != nil {
			return nil, err
		}

		res[mp.Destination] = buf.String()
		if res[mp.Destination] == "" {
			res[mp.Destination] = mp.Default
		}
	}
	return res, nil
}
