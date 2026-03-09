package stream

import (
	"testing"
)

func TestProgress_Coverage(t *testing.T) {
	v := Progress(0.42, "processing")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestLog_Coverage(t *testing.T) {
	v := Log("info", "test message")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestLogInfo_Coverage(t *testing.T) {
	v := LogInfo("test")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestLogWarn_Coverage(t *testing.T) {
	v := LogWarn("warning")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestLogError_Coverage(t *testing.T) {
	v := LogError("error msg")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestLogDebug_Coverage(t *testing.T) {
	v := LogDebug("debug msg")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestMetric_Coverage(t *testing.T) {
	v := Metric("latency_ms", 12.5, "ms")
	if v == nil {
		t.Fatal("nil")
	}

	// Without unit
	v2 := Metric("count", 42, "")
	if v2 == nil {
		t.Fatal("nil")
	}
}

func TestCounter_Coverage(t *testing.T) {
	v := Counter("items", 100)
	if v == nil {
		t.Fatal("nil")
	}
}

func TestArtifact_Coverage(t *testing.T) {
	v := Artifact("image/png", "blob:sha256:abc", "plot.png")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestResyncRequest_Coverage(t *testing.T) {
	v := ResyncRequest(1, 42, "sha256:abc", "BASE_MISMATCH")
	if v == nil {
		t.Fatal("nil")
	}
}

func TestEmitUI_Coverage(t *testing.T) {
	data := EmitUI(Progress(0.5, "half"))
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestEmitProgress_Coverage(t *testing.T) {
	data := EmitProgress(0.5, "half")
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestEmitLog_Coverage(t *testing.T) {
	data := EmitLog("info", "test")
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestEmitMetric_Coverage(t *testing.T) {
	data := EmitMetric("lat", 10.0, "ms")
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestEmitArtifact_Coverage(t *testing.T) {
	data := EmitArtifact("text/plain", "ref:1", "file.txt")
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestError_Coverage(t *testing.T) {
	v := Error("E001", "something failed", 1, 42)
	if v == nil {
		t.Fatal("nil")
	}
}

func TestEmitError_Coverage(t *testing.T) {
	data := EmitError("E001", "fail", 1, 42)
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestParseUIEvent_Coverage(t *testing.T) {
	data := EmitProgress(0.42, "processing")
	typeName, fields, err := ParseUIEvent(data)
	if err != nil {
		t.Fatalf("ParseUIEvent error: %v", err)
	}
	if typeName != "Progress" {
		t.Errorf("expected Progress, got %s", typeName)
	}
	if fields["msg"] != "processing" {
		t.Errorf("msg: got %v", fields["msg"])
	}
}

func TestParseUIEvent_Error(t *testing.T) {
	data := EmitError("E001", "bad", 1, 42)
	typeName, fields, err := ParseUIEvent(data)
	if err != nil {
		t.Fatalf("ParseUIEvent error: %v", err)
	}
	if typeName != "Error" {
		t.Errorf("expected Error, got %s", typeName)
	}
	if fields["code"] != "E001" {
		t.Errorf("code: got %v", fields["code"])
	}
}

func TestParseUIEvent_InvalidInput(t *testing.T) {
	// Not a struct
	_, _, err := ParseUIEvent([]byte("42"))
	if err == nil {
		t.Error("expected error for non-struct")
	}
}
