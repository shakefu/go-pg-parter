package parter

import (
	"testing"
	"time"
)

// reconcile.yass.yaml RETURN: "truncate now() to the start of the current
// month, ISO week, or day per Strategy, computing ALL period-boundary
// computations in UTC."
func TestTruncatePeriodMonthly(t *testing.T) {
	// Mid-month timestamp truncates to first-of-month midnight UTC.
	now := time.Date(2026, 6, 23, 14, 37, 9, 123456789, time.UTC)
	got := truncatePeriod(now, Monthly)
	want := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("truncatePeriod Monthly = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("truncatePeriod Monthly location = %v, want UTC", got.Location())
	}
}

// reconcile.yass.yaml RETURN: Daily truncates to midnight UTC.
func TestTruncatePeriodDaily(t *testing.T) {
	now := time.Date(2026, 6, 23, 14, 37, 9, 123456789, time.UTC)
	got := truncatePeriod(now, Daily)
	want := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("truncatePeriod Daily = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("truncatePeriod Daily location = %v, want UTC", got.Location())
	}
}

// reconcile.yass.yaml RETURN: Weekly truncates to the ISO-week Monday 00:00 UTC.
func TestTruncatePeriodWeekly(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			// Wednesday 2026-06-24 -> Monday 2026-06-22.
			name: "wednesday",
			now:  time.Date(2026, 6, 24, 9, 0, 0, 0, time.UTC),
			want: time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC),
		},
		{
			// A Monday maps to itself.
			name: "monday",
			now:  time.Date(2026, 6, 22, 9, 0, 0, 0, time.UTC),
			want: time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC),
		},
		{
			// A Sunday maps to the PRECEDING Monday (2026-06-28 Sun -> 06-22 Mon).
			name: "sunday",
			now:  time.Date(2026, 6, 28, 23, 59, 59, 0, time.UTC),
			want: time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncatePeriod(c.now, Weekly)
			if !got.Equal(c.want) {
				t.Fatalf("truncatePeriod Weekly(%v) = %v, want %v", c.now, got, c.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("truncatePeriod Weekly location = %v, want UTC", got.Location())
			}
		})
	}
}

// reconcile.yass.yaml RETURN / MUST-NOT: "truncate or compute any period
// boundary in the local timezone" — a non-UTC input zone must not shift the
// resulting UTC boundary. The instant 2026-06-22 02:00 +05:00 is 2026-06-21
// 21:00 UTC, so the Daily boundary is 2026-06-21 (UTC day), not 06-22.
func TestTruncatePeriodNonUTCInput(t *testing.T) {
	zone := time.FixedZone("plus5", 5*3600)

	dailyNow := time.Date(2026, 6, 22, 2, 0, 0, 0, zone)
	gotDaily := truncatePeriod(dailyNow, Daily)
	wantDaily := time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC)
	if !gotDaily.Equal(wantDaily) {
		t.Fatalf("truncatePeriod Daily non-UTC = %v, want %v", gotDaily, wantDaily)
	}
	if gotDaily.Location() != time.UTC {
		t.Fatalf("truncatePeriod Daily non-UTC location = %v, want UTC", gotDaily.Location())
	}

	// 2026-07-01 03:00 +05:00 == 2026-06-30 22:00 UTC, so the Monthly boundary
	// is June, not July.
	monthlyNow := time.Date(2026, 7, 1, 3, 0, 0, 0, zone)
	gotMonthly := truncatePeriod(monthlyNow, Monthly)
	wantMonthly := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if !gotMonthly.Equal(wantMonthly) {
		t.Fatalf("truncatePeriod Monthly non-UTC = %v, want %v", gotMonthly, wantMonthly)
	}
}

// reconcile.yass.yaml RETURN: "advance each subsequent period boundary one
// strategy-unit from the previous boundary."
func TestAdvancePeriodMonthly(t *testing.T) {
	// Plain month step.
	if got, want := advancePeriod(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), Monthly),
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Monthly = %v, want %v", got, want)
	}
	// Year rollover: Dec -> Jan, year++.
	if got, want := advancePeriod(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), Monthly),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Monthly Dec->Jan = %v, want %v", got, want)
	}
	// Leap February: 2024-02-01 -> 2024-03-01.
	if got, want := advancePeriod(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), Monthly),
		time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Monthly leap Feb = %v, want %v", got, want)
	}
}

// reconcile.yass.yaml RETURN: Weekly advances by 7 days, including across month
// and year boundaries.
func TestAdvancePeriodWeekly(t *testing.T) {
	// Across a month boundary: Mon 2026-06-29 + 7d = Mon 2026-07-06.
	if got, want := advancePeriod(time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC), Weekly),
		time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Weekly month boundary = %v, want %v", got, want)
	}
	// Across a year boundary: Mon 2026-12-28 + 7d = Mon 2027-01-04.
	if got, want := advancePeriod(time.Date(2026, 12, 28, 0, 0, 0, 0, time.UTC), Weekly),
		time.Date(2027, 1, 4, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Weekly year boundary = %v, want %v", got, want)
	}
}

// reconcile.yass.yaml RETURN: Daily advances by 1 day.
func TestAdvancePeriodDaily(t *testing.T) {
	// Plain day step.
	if got, want := advancePeriod(time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC), Daily),
		time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Daily = %v, want %v", got, want)
	}
	// Across a leap-day boundary: 2024-02-28 -> 2024-02-29.
	if got, want := advancePeriod(time.Date(2024, 2, 28, 0, 0, 0, 0, time.UTC), Daily),
		time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("advancePeriod Daily leap = %v, want %v", got, want)
	}
}

// reconcile.yass.yaml RETURN: desiredPeriods returns Lookahead+1 contiguous
// periods starting at the current period boundary, and treats Lookahead=3
// Monthly as the current month plus 3 ahead (4 partitions total).
func TestDesiredPeriodsMonthlyExample(t *testing.T) {
	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	lookahead := 3
	got := desiredPeriods(now, Monthly, lookahead)

	if len(got) != lookahead+1 {
		t.Fatalf("desiredPeriods count = %d, want %d", len(got), lookahead+1)
	}

	// First Start == truncatePeriod(now).
	wantFirst := truncatePeriod(now, Monthly)
	if !got[0].Start.Equal(wantFirst) {
		t.Fatalf("desiredPeriods[0].Start = %v, want %v", got[0].Start, wantFirst)
	}

	// Exact expected boundaries for the Monthly Lookahead=3 example: Jun, Jul,
	// Aug, Sep 2026; the last End is Oct 2026.
	wantStarts := []time.Time{
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}
	wantLastEnd := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	for i, p := range got {
		if !p.Start.Equal(wantStarts[i]) {
			t.Errorf("desiredPeriods[%d].Start = %v, want %v", i, p.Start, wantStarts[i])
		}
		// Each period's End equals advancePeriod(Start).
		if !p.End.Equal(advancePeriod(p.Start, Monthly)) {
			t.Errorf("desiredPeriods[%d].End = %v, want advancePeriod(Start) = %v",
				i, p.End, advancePeriod(p.Start, Monthly))
		}
	}
	if !got[len(got)-1].End.Equal(wantLastEnd) {
		t.Errorf("desiredPeriods last End = %v, want %v", got[len(got)-1].End, wantLastEnd)
	}
}

// reconcile.yass.yaml RETURN: periods are contiguous — period[i].End ==
// period[i+1].Start — for every strategy and a non-trivial lookahead.
func TestDesiredPeriodsContiguous(t *testing.T) {
	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	for _, s := range []RangeStrategy{Monthly, Weekly, Daily} {
		lookahead := 5
		got := desiredPeriods(now, s, lookahead)
		if len(got) != lookahead+1 {
			t.Fatalf("strategy %d: count = %d, want %d", s, len(got), lookahead+1)
		}
		if !got[0].Start.Equal(truncatePeriod(now, s)) {
			t.Errorf("strategy %d: first Start = %v, want %v", s, got[0].Start, truncatePeriod(now, s))
		}
		for i := 0; i+1 < len(got); i++ {
			if !got[i].End.Equal(got[i+1].Start) {
				t.Errorf("strategy %d: period[%d].End = %v != period[%d].Start = %v",
					s, i, got[i].End, i+1, got[i+1].Start)
			}
		}
	}
}

// reconcile.yass.yaml RETURN: Lookahead=1 yields exactly two periods (current +
// one ahead); the minimum valid lookahead per config.yass.yaml.
func TestDesiredPeriodsLookaheadOne(t *testing.T) {
	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	got := desiredPeriods(now, Daily, 1)
	if len(got) != 2 {
		t.Fatalf("desiredPeriods Lookahead=1 count = %d, want 2", len(got))
	}
	if !got[0].Start.Equal(time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("got[0].Start = %v, want 2026-06-23", got[0].Start)
	}
	if !got[1].Start.Equal(time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("got[1].Start = %v, want 2026-06-24", got[1].Start)
	}
}
