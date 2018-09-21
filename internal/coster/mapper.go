package coster

import (
	"bytes"
	"context"

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

func (m *Mapper) mapData(obj interface{}) (map[string]string, error) {
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

// MapContext augments a context with opencensus Tags according to the mapping.
// The `Destination` attribute maps directly to the name of the tag.
func (m *Mapper) MapContext(ctx context.Context, obj interface{}) (context.Context, error) {
	d, err := m.mapData(obj)
	if err != nil {
		return nil, err
	}

	tags := []tag.Mutator{}
	for k, v := range d {
		t, err := tag.NewKey(k)
		if err != nil {
			return nil, err
		}

		tags = append(tags, tag.Upsert(t, v))
	}

	return tag.New(ctx, tags...)
}
