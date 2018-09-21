package coster

import (
	"reflect"
	"testing"
)

type mapperTestMetadata struct {
	Labels      map[string]string
	Annotations map[string]string
}

type mapperTestStruct struct {
	Metadata mapperTestMetadata
}

var testStruct = mapperTestStruct{
	Metadata: mapperTestMetadata{
		Labels: map[string]string{
			"service": "svc-via-label",
		},
		Annotations: map[string]string{
			"service": "svc-via-annotation",
		},
	},
}

var mapperTestCases = []struct {
	name     string
	obj      mapperTestStruct
	mapper   Mapper
	expected map[string]string
}{
	{
		name: "using service annotation",
		obj:  testStruct,
		mapper: Mapper{
			Entries: []Mapping{
				Mapping{
					Source:      "{.Metadata.Annotations.service}",
					Destination: "service",
				},
			},
		},
		expected: map[string]string{
			"service": "svc-via-annotation",
		},
	},
	{
		name: "non-existent with default",
		obj:  testStruct,
		mapper: Mapper{
			Entries: []Mapping{
				Mapping{
					Source:      "{.Metadata.Annotations.nonexistent}",
					Default:     "fresh-default",
					Destination: "service",
				},
			},
		},
		expected: map[string]string{
			"service": "fresh-default",
		},
	},
}

func TestMapperMapping(t *testing.T) {
	for _, tt := range mapperTestCases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.mapper.mapData(tt.obj)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("expected %#v, got %#v", tt.expected, got)
			}
		})
	}
}
