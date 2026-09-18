// OmniWatch — Agent
// Component: Beyla kernel check unit tests
// Phase: industry-ready (IND-6)
// Purpose: Table-test kernel version parsing incl. sub-5.10 releases
// Inputs: Version strings
// Outputs: go test PASS/FAIL
package beyla

import (
	"strings"
	"testing"
)

func TestParseKernelVersion(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		major   int
		minor   int
		patch   int
		wantErr bool
	}{
		{name: "ubuntu generic suffix", in: "5.15.0-91-generic", major: 5, minor: 15, patch: 0},
		{name: "plain release", in: "6.8.0", major: 6, minor: 8, patch: 0},
		{name: "floor exact short", in: "5.10", major: 5, minor: 10, patch: 0},
		{name: "sub-floor generic", in: "5.4.0-42-generic", major: 5, minor: 4, patch: 0},
		{name: "old lts", in: "4.19.0-21-amd64", major: 4, minor: 19, patch: 0},
		{name: "whitespace padded", in: "  5.10.0  ", major: 5, minor: 10, patch: 0},
		{name: "plus suffix", in: "6.1.0+deb12", major: 6, minor: 1, patch: 0},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "abc", wantErr: true},
		{name: "single number", in: "5", wantErr: true},
		{name: "non-numeric minor", in: "5.x.0", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ParseKernelVersion(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseKernelVersion(%q) = %v, want error", tc.in, v)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseKernelVersion(%q) error: %v", tc.in, err)
			}
			if v.Major != tc.major || v.Minor != tc.minor || v.Patch != tc.patch {
				t.Fatalf("ParseKernelVersion(%q) = %v, want %d.%d.%d",
					tc.in, v, tc.major, tc.minor, tc.patch)
			}
		})
	}
}

func TestCheckKernelRelease(t *testing.T) {
	cases := []struct {
		name      string
		release   string
		wantErr   bool
		errSubstr string
	}{
		{name: "above floor", release: "6.8.0", wantErr: false},
		{name: "floor exact", release: "5.10.0", wantErr: false},
		{name: "floor short", release: "5.10", wantErr: false},
		{name: "above minor same major", release: "5.15.0-91-generic", wantErr: false},
		{name: "sub-floor minor", release: "5.4.0-42-generic", wantErr: true, errSubstr: "kernel 5.10+ required for eBPF, current: 5.4"},
		{name: "sub-floor major", release: "4.19.0", wantErr: true, errSubstr: "kernel 5.10+ required for eBPF, current: 4.19"},
		{name: "garbage release", release: "abc", wantErr: true, errSubstr: "cannot verify kernel version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckKernelRelease(tc.release)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("CheckKernelRelease(%q) error: %v", tc.release, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("CheckKernelRelease(%q) = nil, want error", tc.release)
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Fatalf("CheckKernelRelease(%q) = %q, want substring %q",
					tc.release, err.Error(), tc.errSubstr)
			}
			if !strings.Contains(err.Error(), "remediation") {
				t.Fatalf("CheckKernelRelease(%q) = %q, want remediation steps", tc.release, err.Error())
			}
		})
	}
}

func TestVersionSupported(t *testing.T) {
	if !(Version{Major: 5, Minor: 10}.Supported()) {
		t.Fatal("5.10 must be supported")
	}
	if (Version{Major: 5, Minor: 9}.Supported()) {
		t.Fatal("5.9 must not be supported")
	}
	if !(Version{Major: 6, Minor: 0}.Supported()) {
		t.Fatal("6.0 must be supported")
	}
	if (Version{Major: 4, Minor: 19}.Supported()) {
		t.Fatal("4.19 must not be supported")
	}
}
