package testutil

import (
	"reflect"
	"testing"
)

func AssertEqual[T comparable](t testing.TB, got, expected T, explanation string, failnow bool) {
	t.Helper()
	if got != expected {
		t.Logf("%v: got %v, expected %v", explanation, got, expected)
		if failnow {
			t.FailNow()
		} else {
			t.Fail()
		}
	}
}

func AssertDeepEqual[T comparable](t testing.TB, got, expected []T, explanation string, failnow bool) {
	t.Helper()
	if !reflect.DeepEqual(got, expected) {
		for i := range got {
			if got[i] == expected[i] {
				continue
			}
			t.Logf("%v, first difference at index %d: got %v, expected %v", explanation, i, got[i], expected[i])
			if failnow {
				t.FailNow()
			} else {
				t.Fail()
			}
			break
		}
	}
}
