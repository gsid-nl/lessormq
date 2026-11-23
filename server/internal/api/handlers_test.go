package api

import (
	"reflect"
	"testing"
)

func TestOnlyTenants(t *testing.T) {
	all := []string{"acme", "beta", "gamma"}
	cases := map[string][]string{
		"gamma,acme":     {"acme", "gamma"},
		" beta , nope ,": {"beta"},
		"":               {},
		",":              {},
		"nope":           {},
	}
	for list, want := range cases {
		if got := onlyTenants(all, list); !reflect.DeepEqual(got, want) {
			t.Errorf("onlyTenants(%q) = %v, want %v", list, got, want)
		}
	}
}
