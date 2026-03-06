package stream

import (
	"fmt"
	"time"

	"github.com/Neumenon/cowrie/go/glyph"
)

// ============================================================
// Standard UI Event Types
// ============================================================
//
// These are recommended payload schemas for kind=ui frames.
// They provide a consistent way to stream agent/workflow status.

// Progress represents a progress update.
// Payload: Progress@(pct 0.42 msg "processing step 3")
func Progress(pct float64, msg string) *glyph.GValue {
	return glyph.Struct("Progress",
		glyph.MapEntry{Key: "pct", Value: glyph.Float(pct)},
		glyph.MapEntry{Key: "msg", Value: glyph.Str(msg)},
	)
}

// Log represents a log message.
// Payload: Log@(level "info" msg "decoded 1000 rows" ts "2025-06-20T10:30:00Z")
func Log(level, msg string) *glyph.GValue {
	return glyph.Struct("Log",
		glyph.MapEntry{Key: "level", Value: glyph.Str(level)},
		glyph.MapEntry{Key: "msg", Value: glyph.Str(msg)},
		glyph.MapEntry{Key: "ts", Value: glyph.Time(time.Now().UTC())},
	)
}

// LogInfo is a convenience for info-level logs.
func LogInfo(msg string) *glyph.GValue {
	return Log("info", msg)
}

// LogWarn is a convenience for warning-level logs.
func LogWarn(msg string) *glyph.GValue {
	return Log("warn", msg)
}

// LogError is a convenience for error-level logs.
func LogError(msg string) *glyph.GValue {
	return Log("error", msg)
}

// LogDebug is a convenience for debug-level logs.
func LogDebug(msg string) *glyph.GValue {
	return Log("debug", msg)
}

// Metric represents a numeric metric.
// Payload: Metric@(name "latency_ms" value 12.5 unit "ms")
func Metric(name string, value float64, unit string) *glyph.GValue {
	entries := []glyph.MapEntry{
		{Key: "name", Value: glyph.Str(name)},
		{Key: "value", Value: glyph.Float(value)},
	}
	if unit != "" {
		entries = append(entries, glyph.MapEntry{Key: "unit", Value: glyph.Str(unit)})
	}
	return glyph.Struct("Metric", entries...)
}

// Counter is a convenience for count metrics.
func Counter(name string, count int64) *glyph.GValue {
	return glyph.Struct("Metric",
		glyph.MapEntry{Key: "name", Value: glyph.Str(name)},
		glyph.MapEntry{Key: "value", Value: glyph.Int(count)},
		glyph.MapEntry{Key: "unit", Value: glyph.Str("count")},
	)
}

// Artifact represents a reference to an artifact (file, blob, etc).
// Payload: Artifact@(mime "image/png" ref "blob:sha256:..." name "plot.png")
func Artifact(mime, ref, name string) *glyph.GValue {
	return glyph.Struct("Artifact",
		glyph.MapEntry{Key: "mime", Value: glyph.Str(mime)},
		glyph.MapEntry{Key: "ref", Value: glyph.Str(ref)},
		glyph.MapEntry{Key: "name", Value: glyph.Str(name)},
	)
}

// ============================================================
// Resync Events
// ============================================================

// ResyncRequest is sent when a receiver needs a fresh snapshot.
// Payload: ResyncRequest@(sid 1 seq 42 want "sha256:..." reason "BASE_MISMATCH")
func ResyncRequest(sid, seq uint64, want string, reason string) *glyph.GValue {
	return glyph.Struct("ResyncRequest",
		glyph.MapEntry{Key: "sid", Value: glyph.Int(int64(sid))},
		glyph.MapEntry{Key: "seq", Value: glyph.Int(int64(seq))},
		glyph.MapEntry{Key: "want", Value: glyph.Str(want)},
		glyph.MapEntry{Key: "reason", Value: glyph.Str(reason)},
	)
}

// ============================================================
// Emit helpers
// ============================================================

// EmitUI emits a UI event value as GLYPH bytes.
func EmitUI(v *glyph.GValue) []byte {
	return []byte(glyph.Emit(v))
}

// EmitProgress emits a progress event as GLYPH bytes.
func EmitProgress(pct float64, msg string) []byte {
	return EmitUI(Progress(pct, msg))
}

// EmitLog emits a log event as GLYPH bytes.
func EmitLog(level, msg string) []byte {
	return EmitUI(Log(level, msg))
}

// EmitMetric emits a metric event as GLYPH bytes.
func EmitMetric(name string, value float64, unit string) []byte {
	return EmitUI(Metric(name, value, unit))
}

// EmitArtifact emits an artifact event as GLYPH bytes.
func EmitArtifact(mime, ref, name string) []byte {
	return EmitUI(Artifact(mime, ref, name))
}

// ============================================================
// Error Events
// ============================================================

// Error represents an error event for kind=err frames.
// Payload: Error@(code "BASE_MISMATCH" msg "state hash mismatch" sid 1 seq 42)
func Error(code, msg string, sid, seq uint64) *glyph.GValue {
	return glyph.Struct("Error",
		glyph.MapEntry{Key: "code", Value: glyph.Str(code)},
		glyph.MapEntry{Key: "msg", Value: glyph.Str(msg)},
		glyph.MapEntry{Key: "sid", Value: glyph.Int(int64(sid))},
		glyph.MapEntry{Key: "seq", Value: glyph.Int(int64(seq))},
	)
}

// EmitError emits an error event as GLYPH bytes.
func EmitError(code, msg string, sid, seq uint64) []byte {
	return []byte(glyph.Emit(Error(code, msg, sid, seq)))
}

// ============================================================
// Parse helpers
// ============================================================

// ParseUIEvent parses a UI event payload and returns its type and fields.
func ParseUIEvent(payload []byte) (typeName string, fields map[string]interface{}, err error) {
	result, err := glyph.Parse(string(payload))
	if err != nil {
		return "", nil, fmt.Errorf("parse ui event: %w", err)
	}
	if result.HasErrors() {
		return "", nil, fmt.Errorf("parse ui event: %s", result.Errors[0].Message)
	}
	if result.Value == nil {
		return "", nil, fmt.Errorf("parse ui event: no value")
	}

	v := result.Value
	if v.Type() != glyph.TypeStruct {
		return "", nil, fmt.Errorf("ui event must be a struct, got %s", v.Type())
	}

	sv, err := v.AsStruct()
	if err != nil {
		return "", nil, fmt.Errorf("parse ui event struct: %w", err)
	}
	typeName = sv.TypeName
	fields = make(map[string]interface{})

	for _, f := range sv.Fields {
		switch f.Value.Type() {
		case glyph.TypeStr:
			s, err := f.Value.AsStr()
			if err != nil {
				return "", nil, fmt.Errorf("parse field %s: %w", f.Key, err)
			}
			fields[f.Key] = s
		case glyph.TypeInt:
			i, err := f.Value.AsInt()
			if err != nil {
				return "", nil, fmt.Errorf("parse field %s: %w", f.Key, err)
			}
			fields[f.Key] = i
		case glyph.TypeFloat:
			fl, err := f.Value.AsFloat()
			if err != nil {
				return "", nil, fmt.Errorf("parse field %s: %w", f.Key, err)
			}
			fields[f.Key] = fl
		case glyph.TypeBool:
			b, err := f.Value.AsBool()
			if err != nil {
				return "", nil, fmt.Errorf("parse field %s: %w", f.Key, err)
			}
			fields[f.Key] = b
		case glyph.TypeTime:
			t, err := f.Value.AsTime()
			if err != nil {
				return "", nil, fmt.Errorf("parse field %s: %w", f.Key, err)
			}
			fields[f.Key] = t
		default:
			fields[f.Key] = f.Value
		}
	}

	return typeName, fields, nil
}
