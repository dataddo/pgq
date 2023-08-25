// Package require is a minimal alternative to github.com/stretchr/testify/require.
package require

import (
	"errors"
	"reflect"
	"testing"
)

// NoError fails the test if err is not nil.
func NoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// Error fails the test if err is nil.
func Error(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ErrorIs fails the test if err is nil or does not match target.
func ErrorIs(t testing.TB, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("expected error %v, got %v", target, err)
	}
}

// Equal fails the test if expected is not equal to actual.
func Equal(t testing.TB, expected interface{}, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected:\n\t%[1]T(%#[1]v)\ngot:\n\t%[2]T(%#[2]v)", expected, actual)
	}
}
