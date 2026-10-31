package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	"github.com/tidwall/gjson"
)

// CodexFirstResponseEligible protects deep reasoning and large requests from the
// opt-in first-response deadline. Call only after the final payload rules run.
// The size guard counts JSON bytes, not tokens.
func CodexFirstResponseEligible(payload []byte, maxBytes int) bool {
	if maxBytes <= 0 || len(payload) > maxBytes || !gjson.ValidBytes(payload) {
		return false
	}
	effort := strings.ToLower(strings.TrimSpace(thinking.ExtractTranslatedReasoningEffort(payload, "codex")))
	// The canonical extractor preserves unknown labels and the last effective
	// configuration_update; a top-level value must not override that update.
	switch effort {
	case "", "none", "minimal", "low", "medium":
		return true
	default:
		return false
	}
}
