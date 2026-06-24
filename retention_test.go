package parter

import "testing"

// retention.yass.yaml parter.RetentionPass RETURN (WHEN Retention > 0): the
// integer Retention is converted to a calendar interval per Strategy —
// Monthly -> "<r> months", Weekly -> "<r> weeks", Daily -> "<r> days". The
// spec's worked example uses Retention=3 -> "3 months"/"3 weeks"/"3 days".
func TestRetentionInterval(t *testing.T) {
	cases := []struct {
		name string
		r    int
		s    RangeStrategy
		want string
	}{
		// retention.yass.yaml RETURN: the spec's r=3 worked examples.
		{"monthly_3", 3, Monthly, "3 months"},
		{"weekly_3", 3, Weekly, "3 weeks"},
		{"daily_3", 3, Daily, "3 days"},
		// Additional r values exercising the same unit mapping.
		{"monthly_1", 1, Monthly, "1 months"},
		{"monthly_12", 12, Monthly, "12 months"},
		{"weekly_1", 1, Weekly, "1 weeks"},
		{"weekly_8", 8, Weekly, "8 weeks"},
		{"daily_1", 1, Daily, "1 days"},
		{"daily_90", 90, Daily, "90 days"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retentionInterval(tc.r, tc.s); got != tc.want {
				t.Errorf("retentionInterval(%d, %v) = %q, want %q", tc.r, tc.s, got, tc.want)
			}
		})
	}
}
