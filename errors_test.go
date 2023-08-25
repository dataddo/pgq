package pgq

import (
	"errors"
	"fmt"
	"testing"
)

type legacyPGErrorImplementation struct {
	err   error
	fatal bool
	codes map[byte]string
}

func (l *legacyPGErrorImplementation) Error() string {
	return l.err.Error()
}

func (l *legacyPGErrorImplementation) Fatal() bool {
	return l.fatal
}

func (l *legacyPGErrorImplementation) Get(k byte) (v string) {
	return l.codes[k]
}

type pgErrorImplementation struct {
	sqlState string
}

func (p *pgErrorImplementation) Error() string {
	return fmt.Sprintf("pg error: %s", p.sqlState)
}

func (p *pgErrorImplementation) SQLState() string {
	return p.sqlState
}

var (
	_ legacyPGError = (*legacyPGErrorImplementation)(nil)
	_ pgError       = (*pgErrorImplementation)(nil)
)

func Test_isErrorCode(t *testing.T) {
	type args struct {
		err   error
		codes []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test wrapped pg error",
			args: args{
				codes: []string{"42P01"},
				err: fmt.Errorf("foo: %w",
					&pgErrorImplementation{
						sqlState: "42P01",
					},
				),
			},
			want: true,
		},
		{
			name: "test wrapped legacy error",
			args: args{
				codes: []string{"42P01"},
				err: fmt.Errorf("foo: %w",
					&legacyPGErrorImplementation{
						err:   errors.New("test"),
						fatal: true,
						codes: map[byte]string{
							'C': "42P01",
						},
					},
				),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isErrorCode(tt.args.err, tt.args.codes...); got != tt.want {
				t.Errorf("isErrorCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
