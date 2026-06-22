package rootset

import (
	"reflect"
	"testing"
)

func TestNormalizeRoots(t *testing.T) {
	tests := []struct {
		name    string
		roots   []string
		want    []string
		wantErr bool
	}{
		{
			name:  "single absolute",
			roots: []string{"/data/app/../app"},
			want:  []string{"/data/app"},
		},
		{
			name:  "multiple absolutes",
			roots: []string{"/data/app", "/var/log/myapp"},
			want:  []string{"/data/app", "/var/log/myapp"},
		},
		{
			name:    "relative rejected",
			roots:   []string{"data/app"},
			wantErr: true,
		},
		{
			name:    "duplicate rejected",
			roots:   []string{"/data/app", "/data/app/."},
			wantErr: true,
		},
		{
			name:    "nested rejected",
			roots:   []string{"/data", "/data/app"},
			wantErr: true,
		},
		{
			name:    "empty rejected",
			roots:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Normalize(tt.roots)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Normalize() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Normalize() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Normalize() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSplitList(t *testing.T) {
	got := SplitList(" /data/app, /var/log/myapp\n/srv/data ")
	want := []string{"/data/app", "/var/log/myapp", "/srv/data"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitList() = %#v, want %#v", got, want)
	}
}
