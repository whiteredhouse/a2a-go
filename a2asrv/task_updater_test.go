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
	"time"

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

// newWaitingTask makes a handler whose agent runs execute for a message to
// the task, and a task of owner's that waits for input. The task is put into
// the store directly, so no execution of it is still being torn down when the
// test starts.
func newWaitingTask(t *testing.T, owner string, execute func(context.Context, *ExecutorContext) iter.Seq2[a2a.Event, error]) (RequestHandler, *a2a.Task) {
	t.Helper()
	store := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{Authenticator: NewTaskStoreAuthenticator()})
	h, task, _ := newWaitingTaskIn(t, owner, &mockAgentExecutor{ExecuteFunc: execute}, store)
	return h, task
}

// newWaitingTaskIn is [newWaitingTask] with the agent and the store given.
func newWaitingTaskIn(t *testing.T, owner string, agent *mockAgentExecutor, store taskstore.Store) (RequestHandler, *a2a.Task, *mockAgentExecutor) {
	t.Helper()
	task := &a2a.Task{
		ID:        a2a.NewTaskID(),
		ContextID: a2a.NewContextID(),
		Status:    a2a.TaskStatus{State: a2a.TaskStateInputRequired},
	}
	if _, err := store.Create(asUser(owner), task); err != nil {
		t.Fatalf("store.Create() error = %v", err)
	}
	return NewHandler(agent, WithTaskStore(store)), task, agent
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
	otherContext := &a2a.Task{ID: task.ID, ContextID: "another-context"}
	var nilStatus *a2a.TaskStatusUpdateEvent
	var nilArtifact *a2a.TaskArtifactUpdateEvent
	for name, events := range map[string][]a2a.Event{
		"none":            nil,
		"not final":       {a2a.NewStatusUpdateEvent(task, a2a.TaskStateWorking, nil)},
		"another task":    {a2a.NewStatusUpdateEvent(other, a2a.TaskStateFailed, nil)},
		"another context": {a2a.NewStatusUpdateEvent(otherContext, a2a.TaskStateFailed, nil)},
		"not task update": {a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("x"))},
		"early final": {
			a2a.NewStatusUpdateEvent(task, a2a.TaskStateInputRequired, nil),
			a2a.NewStatusUpdateEvent(task, a2a.TaskStateCompleted, nil),
		},
		"nil status":   {nilStatus},
		"nil artifact": {nilArtifact, a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil)},
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

// blockingStore holds the server's own reads of a task until released, so a
// test can act while an update is under way.
type blockingStore struct {
	taskstore.Store
	reading chan struct{}
	release chan struct{}
}

func (s *blockingStore) Get(ctx context.Context, taskID a2a.TaskID) (*taskstore.StoredTask, error) {
	if taskstore.IsServerAccess(ctx) {
		close(s.reading)
		<-s.release
	}
	return s.Store.Get(ctx, taskID)
}

// A message which comes while an update is under way waits for it, and then
// sees the task the update ended.
func TestUpdateTaskHoldsTheTaskUntilItEnds(t *testing.T) {
	store := &blockingStore{
		Store:   taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{Authenticator: NewTaskStoreAuthenticator()}),
		reading: make(chan struct{}),
		release: make(chan struct{}),
	}
	executed := make(chan struct{}, 1)
	agent := &mockAgentExecutor{ExecuteFunc: func(ctx context.Context, execCtx *ExecutorContext) iter.Seq2[a2a.Event, error] {
		executed <- struct{}{}
		return func(yield func(a2a.Event, error) bool) {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
		}
	}}
	h, task, _ := newWaitingTaskIn(t, "alice", agent, store)

	updated := make(chan error, 1)
	go func() {
		_, err := h.(TaskUpdater).UpdateTask(context.Background(), task.ID, a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil))
		updated <- err
	}()
	<-store.reading

	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("blue"))
	answer.TaskID = task.ID
	answered := make(chan error, 1)
	go func() {
		_, err := h.SendMessage(asUser("alice"), &a2a.SendMessageRequest{Message: answer})
		answered <- err
	}()
	select {
	case err := <-answered:
		t.Fatalf("SendMessage() during an update returned %v before the update ended", err)
	case <-executed:
		t.Fatal("the answer executed during an update")
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	if err := <-updated; err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if err := <-answered; err == nil {
		t.Error("SendMessage() to the task the update ended succeeded")
	}
	select {
	case <-executed:
		t.Error("the answer executed after the update ended the task")
	default:
	}
	got, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateFailed {
		t.Errorf("GetTask() = %v %v, want failed", got, err)
	}
}

// An update runs none of the agent's execution cleanup.
func TestUpdateTaskRunsNoExecutionCleanup(t *testing.T) {
	store := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{Authenticator: NewTaskStoreAuthenticator()})
	cleaned := false
	agent := &mockAgentExecutor{CleanupFn: func(context.Context, a2a.SendMessageResult, error) { cleaned = true }}
	h, task, _ := newWaitingTaskIn(t, "alice", agent, store)
	if _, err := h.(TaskUpdater).UpdateTask(context.Background(), task.ID, a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil)); err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if cleaned {
		t.Error("an update ran the agent's execution cleanup")
	}
}
