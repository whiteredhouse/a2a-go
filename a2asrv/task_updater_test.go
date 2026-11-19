// Copyright 2026 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package a2asrv

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// asUser runs a call as the named user, as an authenticating interceptor
// would.
func asUser(name string) context.Context {
	ctx, callCtx := NewCallContext(context.Background(), nil)
	callCtx.User = NewAuthenticatedUser(name, nil)
	return ctx
}

// newWaitingTask makes a handler whose agent asks for input, and a task of
// owner's that waits for it.
func newWaitingTask(t *testing.T, owner string, execute func(context.Context, *ExecutorContext) iter.Seq2[a2a.Event, error]) (RequestHandler, *a2a.Task) {
	t.Helper()
	agent := &mockAgentExecutor{ExecuteFunc: execute}
	if execute == nil {
		agent.ExecuteFunc = func(ctx context.Context, execCtx *ExecutorContext) iter.Seq2[a2a.Event, error] {
			return func(yield func(a2a.Event, error) bool) {
				if execCtx.StoredTask == nil && !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
					return
				}
				yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateInputRequired, nil), nil)
			}
		}
	}
	store := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{Authenticator: NewTaskStoreAuthenticator()})
	h := NewHandler(agent, WithTaskStore(store))
	res, err := h.SendMessage(asUser(owner), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hi"))})
	if err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok || task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("SendMessage() = %v, want a task waiting for input", res)
	}
	return h, task
}

func TestUpdateTaskEndsAWaitingTask(t *testing.T) {
	h, task := newWaitingTask(t, "alice", nil)
	updater, ok := h.(TaskUpdater)
	if !ok {
		t.Fatal("the handler is not a TaskUpdater")
	}
	reason := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("timed out"))
	failed := a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, reason)
	// The server's own call: no caller, whoever owns the task.
	got, err := updater.UpdateTask(context.Background(), task.ID, failed)
	if err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if got.Status.State != a2a.TaskStateFailed {
		t.Fatalf("UpdateTask() state = %v, want failed", got.Status.State)
	}
	// The owner sees it, and still owns the task.
	stored, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatalf("GetTask() as owner error = %v", err)
	}
	if stored.Status.State != a2a.TaskStateFailed {
		t.Errorf("GetTask() state = %v, want failed", stored.Status.State)
	}
	if _, err := h.GetTask(asUser("mallory"), &a2a.GetTaskRequest{ID: task.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("GetTask() as another user error = %v, want ErrTaskNotFound", err)
	}
	// An ended task takes no more updates.
	if _, err := updater.UpdateTask(context.Background(), task.ID, failed); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Errorf("UpdateTask() of an ended task error = %v, want ErrUnsupportedOperation", err)
	}
}

func TestUpdateTaskValidatesEvents(t *testing.T) {
	h, task := newWaitingTask(t, "alice", nil)
	updater := h.(TaskUpdater)
	other := &a2a.Task{ID: "other", ContextID: task.ContextID}
	for name, events := range map[string][]a2a.Event{
		"none":            nil,
		"not final":       {a2a.NewStatusUpdateEvent(task, a2a.TaskStateWorking, nil)},
		"another task":    {a2a.NewStatusUpdateEvent(other, a2a.TaskStateFailed, nil)},
		"not task update": {a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("x"))},
	} {
		if _, err := updater.UpdateTask(context.Background(), task.ID, events...); !errors.Is(err, a2a.ErrInvalidParams) {
			t.Errorf("UpdateTask(%s) error = %v, want ErrInvalidParams", name, err)
		}
	}
	got, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("after refused updates: %v %v, want the task still waiting", got, err)
	}
	// Several events, the last final.
	artifact := a2a.NewArtifactEvent(task, a2a.NewTextPart("partial"))
	done := a2a.NewStatusUpdateEvent(task, a2a.TaskStateCompleted, nil)
	got, err = updater.UpdateTask(context.Background(), task.ID, artifact, done)
	if err != nil || got.Status.State != a2a.TaskStateCompleted || len(got.Artifacts) != 1 {
		t.Errorf("UpdateTask(artifact, completed) = %v %v", got, err)
	}
}

func TestUpdateTaskRefusesATaskWithAnActiveExecution(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	h, task := newWaitingTask(t, "alice", func(ctx context.Context, execCtx *ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if execCtx.StoredTask == nil {
				if yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
					yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateInputRequired, nil), nil)
				}
				return
			}
			// The answer's turn runs until released.
			if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
				return
			}
			close(started)
			<-release
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
		}
	})
	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("blue"))
	answer.TaskID = task.ID
	done := make(chan error, 1)
	go func() {
		_, err := h.SendMessage(asUser("alice"), &a2a.SendMessageRequest{Message: answer})
		done <- err
	}()
	<-started
	_, err := h.(TaskUpdater).UpdateTask(context.Background(), task.ID, a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil))
	close(release)
	if err == nil {
		t.Error("UpdateTask() during an execution succeeded")
	}
	if err := <-done; err != nil {
		t.Errorf("the execution: %v", err)
	}
}
