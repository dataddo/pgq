package pgq

import (
	"reflect"
	"testing"
)

func TestGetTags(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected []string
	}{
		{
			name:     "Single Tag",
			line:     "Here's a tag:tagName",
			expected: []string{"tagName"},
		},
		{
			name:     "Multiple Tags No Spaces",
			line:     "Multiple :tag1:tag2:tag3",
			expected: []string{"tag1", "tag2", "tag3"},
		},
		{
			name:     "Invalid Tag With Space",
			line:     "This should not be a tag: tagWithSpace",
			expected: nil, // ": tagWithSpace" is not valid due to the space
		},
		{
			name:     "Tags With Mixed Characters",
			line:     "Tags with numbers  :tag123 and underscores:tag_name",
			expected: []string{"tag123", "tag_name"},
		},
		{
			name:     "Mixed Valid and Invalid Tags",
			line:     "Valid:tag1 and invalid: tag2",
			expected: []string{"tag1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getParams(tt.line)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("getTags(%q) got %v, want %v", tt.line, result, tt.expected)
			}
		})
	}
}
