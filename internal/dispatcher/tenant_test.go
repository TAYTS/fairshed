package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrDefault(t *testing.T) {
	t.Run("float64", func(t *testing.T) {
		tests := []struct {
			name       string
			v          float64
			defaultVal float64
			want       float64
		}{
			{"positive returns v", 0.5, 1.0, 0.5},
			{"zero returns defaultVal", 0.0, 1.0, 1.0},
			{"negative returns defaultVal", -3.0, 1.0, 1.0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.want, orDefault(tc.v, tc.defaultVal))
			})
		}
	})

	t.Run("int", func(t *testing.T) {
		tests := []struct {
			name       string
			v          int
			defaultVal int
			want       int
		}{
			{"positive returns v", 50, 100, 50},
			{"zero returns defaultVal", 0, 100, 100},
			{"negative returns defaultVal", -1, 100, 100},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.want, orDefault(tc.v, tc.defaultVal))
			})
		}
	})
}
