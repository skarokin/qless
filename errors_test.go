package qless

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil")
	}

	base := errors.New("boom")
	perm := Permanent(base)
	if !IsPermanent(perm) {
		t.Error("Permanent error not detected by IsPermanent")
	}
	if IsPermanent(base) {
		t.Error("plain error should not be permanent")
	}
	if !errors.Is(perm, base) {
		t.Error("Permanent should unwrap to the original error")
	}
	if perm.Error() != base.Error() {
		t.Errorf("Error() = %q, want %q", perm.Error(), base.Error())
	}

	// Double wrapping is a no-op.
	if Permanent(perm) != perm {
		t.Error("Permanent(Permanent(err)) should return the same error")
	}

	// Detection through fmt.Errorf wrapping.
	wrapped := fmt.Errorf("outer: %w", perm)
	if !IsPermanent(wrapped) {
		t.Error("IsPermanent should see through wrapped errors")
	}
}

func TestShutdownError(t *testing.T) {
	err := &ShutdownError{Queued: 3, Active: 2, PendingEnqueues: 1, Cause: context.DeadlineExceeded}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Error("ShutdownError should unwrap to its cause")
	}
	msg := err.Error()
	for _, want := range []string{"3 queued", "2 active", "1 pending"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing %q", msg, want)
		}
	}
}
