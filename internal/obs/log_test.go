package obs

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLogger_JSONLevels(t *testing.T) {
	t.Parallel()
	subTests := []struct {
		name      string
		level     string
		log       func(buf *bytes.Buffer, level string)
		wantMsg   string
		wantLevel string
		wantSeen  bool
	}{
		{
			name:  "info passes",
			level: "info",
			log: func(buf *bytes.Buffer, level string) {
				NewLogger(buf, level).Info("hello", "k", "v")
			},
			wantMsg: "hello", wantLevel: "INFO", wantSeen: true,
		},
		{
			name:  "debug filtered out at info",
			level: "info",
			log: func(buf *bytes.Buffer, level string) {
				NewLogger(buf, level).Debug("dbg")
			},
			wantSeen: false,
		},
		{
			name:  "debug visible at debug",
			level: "debug",
			log: func(buf *bytes.Buffer, level string) {
				NewLogger(buf, level).Debug("dbg")
			},
			wantMsg: "dbg", wantLevel: "DEBUG", wantSeen: true,
		},
		{
			name:  "default is info",
			level: "",
			log: func(buf *bytes.Buffer, level string) {
				NewLogger(buf, level).Info("ok")
			},
			wantMsg: "ok", wantLevel: "INFO", wantSeen: true,
		},
	}
	for _, tc := range subTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			tc.log(&buf, tc.level)
			out := strings.TrimSpace(buf.String())
			if !tc.wantSeen {
				require.Empty(t, out)
				return
			}
			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(out), &rec))
			require.Equal(t, tc.wantMsg, rec["msg"])
			require.Equal(t, tc.wantLevel, rec["level"])
		})
	}
}
