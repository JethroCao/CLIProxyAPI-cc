package executor

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestFirstResponseBudgetSharesTotalAcrossAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		time.Sleep(2 * time.Second)
		ctx, first, err := budget.StartAttempt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if ctx.Err() == nil || !IsFirstResponseTimeout(first.Err(ctx.Err())) {
			t.Fatal("first attempt did not time out")
		}
		if first.Commit() {
			t.Fatal("timeout must win over late response")
		}
		first.Close()
		time.Sleep(3 * time.Second)
		ctx, second, err := budget.StartAttempt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if ctx.Err() == nil || !IsFirstResponseTimeout(second.Err(ctx.Err())) {
			t.Fatal("second attempt reset total budget")
		}
		second.Close()
		if !IsFirstResponseTimeout(budget.Check()) {
			t.Fatal("exhausted budget was not rejected")
		}
	})
}

func TestFirstResponseBudgetCommitPreservesLongStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		ctx, a, err := b.StartAttempt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		time.Sleep(14 * time.Second)
		if !a.Commit() {
			t.Fatal("response before deadline lost")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if ctx.Err() != nil || b.Check() != nil || a.Pending() {
			t.Fatalf("committed stream expired: %v", ctx.Err())
		}
	})
}

func TestFirstResponseBudgetParentCancellationIsNotTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		parent, cancel := context.WithCancel(context.Background())
		ctx, a, err := b.StartAttempt(parent)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		cancel()
		synctest.Wait()
		if got := a.Err(ctx.Err()); !errors.Is(got, context.Canceled) || IsFirstResponseTimeout(got) {
			t.Fatalf("caller cancellation rewritten: %v", got)
		}
		if a.Commit() {
			t.Fatal("canceled attempt committed")
		}
	})
}

func TestFirstResponseBudgetDisabledAndAttemptLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disabled := NewFirstResponseBudget(0, 15*time.Second)
		_, a, err := disabled.StartAttempt(context.Background())
		if err != nil || a != nil {
			t.Fatal("disabled budget created attempt")
		}
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		for i := 0; i < 2; i++ {
			_, attempt, e := b.StartAttempt(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			attempt.Close()
		}
		if !IsFirstResponseTimeout(b.Check()) {
			t.Fatal("third attempt permitted")
		}
		_, _, err = b.StartAttempt(context.Background())
		if !IsFirstResponseTimeout(err) {
			t.Fatal("attempt cap not enforced")
		}
	})
}

func TestFirstResponseBudgetDisableAndCloseReleaseTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		ctx, a, err := b.StartAttempt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		b.Disable()
		time.Sleep(time.Minute)
		synctest.Wait()
		if ctx.Err() != nil || b.Check() != nil {
			t.Fatal("excluded request timed out")
		}
		a.Close()
		if ctx.Err() == nil {
			t.Fatal("closed attempt context leaked")
		}
	})
}

func TestFirstResponseBudgetContextAndExpiredBeforeFirstAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		ctx := WithFirstResponseBudget(context.Background(), b)
		if FirstResponseBudgetFromContext(ctx) != b {
			t.Fatal("budget missing from context")
		}
		time.Sleep(31 * time.Second)
		if b.Check() != nil {
			t.Fatal("ineligible request budget activated early")
		}
		_, _, err := b.StartAttempt(ctx)
		if !IsFirstResponseTimeout(err) {
			t.Fatal("initial scheduling time excluded from budget")
		}
	})
}

func TestFirstResponseBufferLimitIsTerminalLocalPolicy(t *testing.T) {
	err := &FirstResponseBufferLimitError{}
	if !err.IsRequestScoped() || err.StatusCode() != 502 || !IsFirstResponsePolicyError(err) || IsFirstResponseTimeout(err) {
		t.Fatal("buffer limit must stop replay without pretending timeout")
	}
}

func TestFirstResponseBudgetControlContextUsesRemainingTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
		ctx, a, err := b.StartAttempt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		a.Close()
		selection, cancel := b.ControlContext(context.Background())
		defer cancel()
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if selection.Err() == nil || !IsFirstResponseTimeout(context.Cause(selection)) {
			t.Fatal("selection exceeded total budget")
		}
	})
}

func TestFirstResponseCommittedBudgetRejectsReplay(t *testing.T) {
	b := NewFirstResponseBudget(30*time.Second, 15*time.Second)
	_, a, err := b.StartAttempt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if !a.Commit() {
		t.Fatal("first response failed to commit")
	}
	_, _, err = b.StartAttempt(context.Background())
	if err == nil || IsFirstResponseTimeout(err) {
		t.Fatalf("committed budget allowed replay or misreported timeout: %v", err)
	}
}
