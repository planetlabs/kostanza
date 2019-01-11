// Copyright 2018 Planet Labs Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/go-test/deep"

	"github.com/planetlabs/kostanza/internal/coster"
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
