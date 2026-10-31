package helps

import (
	"strings"
	"testing"
)

func TestCodexFirstResponseEligibleUsesFinalPayload(t *testing.T) {
	// A mistaken broad enablement would cancel high-effort and large-context requests.
	for _, tc := range []struct {
		name  string
		body  string
		limit int
		want  bool
	}{
		{"ordinary", `{"input":"hello"}`, 262144, true},
		{"medium", `{"reasoning":{"effort":"medium"}}`, 262144, true},
		{"none", `{"reasoning":{"effort":"none"}}`, 262144, true},
		{"high", `{"reasoning":{"effort":"high"}}`, 262144, false},
		{"xhigh", `{"reasoning":{"effort":"xhigh"}}`, 262144, false},
		{"unknown", `{"reasoning":{"effort":"future-effort"}}`, 262144, false},
		{"effective high update", `{"reasoning":{"effort":"medium"},"input":[{"type":"configuration_update","reasoning":{"effort":"high"}}]}`, 262144, false},
		{"effective low update", `{"reasoning":{"effort":"high"},"input":[{"type":"configuration_update","reasoning":{"effort":"low"}}]}`, 262144, true},
		{"unknown update", `{"reasoning":{"effort":"medium"},"input":[{"type":"configuration_update","reasoning":{"effort":"future-effort"}}]}`, 262144, false},
		{"invalid", `{"input":`, 262144, false},
		{"too large", `{"input":"` + strings.Repeat("x", 100) + `"}`, 100, false},
		{"at boundary", `{}`, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CodexFirstResponseEligible([]byte(tc.body), tc.limit); got != tc.want {
				t.Fatalf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}
