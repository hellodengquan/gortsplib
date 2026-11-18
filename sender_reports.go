package gortsplib

import (
	"time"

	"github.com/pion/rtcp"
)

// maximum number of sender reports that are kept while waiting for the first
// RTP packet of their source.
const maxPendingSenderReports = 128

type pendingSenderReport struct {
	report     rtcp.SenderReport
	receivedAt time.Time
}

// pendingSenderReports keeps RTCP sender reports whose source is not known
// yet, because the first RTP packet of that source has not been received.
// Reports are kept per source, so that each source can be initialized with
// its own reports, and a source change can be handled without waiting for the
// next report.
type pendingSenderReports struct {
	entries []pendingSenderReport
}

func (b *pendingSenderReports) add(report *rtcp.SenderReport, receivedAt time.Time) {
	for i := range b.entries {
		if b.entries[i].report.SSRC == report.SSRC {
			b.entries[i] = pendingSenderReport{
				report:     *report,
				receivedAt: receivedAt,
			}
			return
		}
	}

	if len(b.entries) >= maxPendingSenderReports {
		b.entries = b.entries[1:]
	}

	b.entries = append(b.entries, pendingSenderReport{
		report:     *report,
		receivedAt: receivedAt,
	})
}

func (b *pendingSenderReports) take(ssrc uint32) (pendingSenderReport, bool) {
	for i := range b.entries {
		if b.entries[i].report.SSRC == ssrc {
			entry := b.entries[i]
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			return entry, true
		}
	}
	return pendingSenderReport{}, false
}

func (b *pendingSenderReports) remove(ssrc uint32) {
	for i := range b.entries {
		if b.entries[i].report.SSRC == ssrc {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			return
		}
	}
}
