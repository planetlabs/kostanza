package aggregator

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/go-test/deep"

	"github.com/jacobstr/kostanza/internal/coster"
)

var mapperToSchemaCases = []struct {
	name           string
	mapper         *coster.Mapper
	expectedSchema bigquery.Schema
}{
	{
		name:           "empty mapper",
		mapper:         &coster.Mapper{},
		expectedSchema: defaultSchema(),
	},
	{
		name: "mapper with service dimension",
		mapper: &coster.Mapper{
			Entries: []coster.Mapping{
				coster.Mapping{
					Source:      "service",
					Destination: "Service",
				},
			},
		},
		expectedSchema: append(
			defaultSchema(),
			&bigquery.FieldSchema{Name: "Dimensions_Service", Type: bigquery.StringFieldType},
		),
	},
}

func TestMapperToSchema(t *testing.T) {
	for _, tt := range mapperToSchemaCases {
		t.Run(tt.name, func(t *testing.T) {
			s := MapperToSchema(tt.mapper)
			if diff := deep.Equal(s, tt.expectedSchema); diff != nil {
				t.Fatal(diff)
			}
		})
	}
}
