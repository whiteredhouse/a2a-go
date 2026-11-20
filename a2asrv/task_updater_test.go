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
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// asUser runs a call as the named user, as an authenticating interceptor
// would.
func asUser(name string) context.Context {
	ctx, callCtx := NewCallContext(context.Background(), nil)
	callCtx.User = NewAuthenticatedUser(name, nil)
	return ctx
}

// newUpdaterStore makes a store which knows its owners' tasks and the
// server's own calls apart, as the handler's default store does.
func newUpdaterStore() *taskstore.InMemory {
	return taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		CallerAuthenticator: NewTaskStoreCallerAuthenticator(),
	})
}

// newWaitingTask makes a handler whose agent runs execute for a message to
// the task, and a task of owner's that waits for input. The task is put into
// the store directly, so no execution of it is still being torn down when the
// test starts.
func newWaitingTask(t *testing.T, owner string, execute func(context.Context, *ExecutorContext) iter.Seq2[a2a.Event, error]) (RequestHandler, *a2a.Task) {
	t.Helper()
	store := newUpdaterStore()
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
	got, err := updater.UpdateTask(context.Background(), failed)
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
	if _, err := updater.UpdateTask(context.Background(), failed); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Errorf("UpdateTask() of an ended task error = %v, want ErrUnsupportedOperation", err)
	}
}

func TestUpdateTaskValidatesTheEvent(t *testing.T) {
	h, task := newWaitingTask(t, "alice", nil)
	updater := h.(TaskUpdater)
	otherContext := &a2a.Task{ID: task.ID, ContextID: "another-context"}
	for name, event := range map[string]*a2a.TaskStatusUpdateEvent{
		"nil":             nil,
		"no task":         a2a.NewStatusUpdateEvent(&a2a.Task{ContextID: task.ContextID}, a2a.TaskStateFailed, nil),
		"not final":       a2a.NewStatusUpdateEvent(task, a2a.TaskStateWorking, nil),
		"another context": a2a.NewStatusUpdateEvent(otherContext, a2a.TaskStateFailed, nil),
	} {
		if _, err := updater.UpdateTask(context.Background(), event); !errors.Is(err, a2a.ErrInvalidParams) {
			t.Errorf("UpdateTask(%s) error = %v, want ErrInvalidParams", name, err)
		}
	}
	got, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("after refused updates: %v %v, want the task still waiting", got, err)
	}
	// Another task the store does not have.
	other := a2a.NewStatusUpdateEvent(&a2a.Task{ID: "other", ContextID: task.ContextID}, a2a.TaskStateFailed, nil)
	if _, err := updater.UpdateTask(context.Background(), other); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("UpdateTask(unknown task) error = %v, want ErrTaskNotFound", err)
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
	_, err := h.(TaskUpdater).UpdateTask(context.Background(), a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil))
	close(release)
	if !errors.Is(err, ErrTaskActive) {
		t.Errorf("UpdateTask() during an execution error = %v, want ErrTaskActive", err)
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
	if callCtx, ok := CallContextFrom(ctx); ok && callCtx.IsServer() {
		close(s.reading)
		<-s.release
	}
	return s.Store.Get(ctx, taskID)
}

// A message which comes while an update is under way is refused as busy,
// and a retry sees the task the update ended.
func TestUpdateTaskHoldsTheTaskUntilItEnds(t *testing.T) {
	store := &blockingStore{
		Store:   newUpdaterStore(),
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
		_, err := h.(TaskUpdater).UpdateTask(context.Background(), a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil))
		updated <- err
	}()
	<-store.reading

	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("blue"))
	answer.TaskID = task.ID
	if _, err := h.SendMessage(asUser("alice"), &a2a.SendMessageRequest{Message: answer}); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Errorf("SendMessage() during an update error = %v, want a busy refusal", err)
	}
	if _, err := h.CancelTask(asUser("alice"), &a2a.CancelTaskRequest{ID: task.ID}); !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Errorf("CancelTask() during an update error = %v, want a busy refusal", err)
	}

	close(store.release)
	if err := <-updated; err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if _, err := h.SendMessage(asUser("alice"), &a2a.SendMessageRequest{Message: answer}); err == nil {
		t.Error("SendMessage() to the task the update ended succeeded")
	}
	select {
	case <-executed:
		t.Error("the answer executed")
	default:
	}
	got, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateFailed {
		t.Errorf("GetTask() = %v %v, want failed", got, err)
	}
}

// replyingSender answers the task in each push notification before it
// acknowledges it, as a synchronous recipient may.
type replyingSender struct {
	h       RequestHandler
	replied chan error
}

func (s *replyingSender) SendPush(ctx context.Context, config *a2a.PushConfig, event a2a.Event) error {
	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("blue"))
	answer.TaskID = event.TaskInfo().TaskID
	// Bounded, so that a deadlock fails the test rather than hanging it.
	replyCtx, cancel := context.WithTimeout(asUser("alice"), 2*time.Second)
	defer cancel()
	_, err := s.h.SendMessage(replyCtx, &a2a.SendMessageRequest{Message: answer})
	s.replied <- err
	return nil
}

// A recipient which answers a pushed update before acknowledging it does
// not deadlock the update.
func TestUpdateTaskPushRecipientMayAnswer(t *testing.T) {
	store := newUpdaterStore()
	pushes := push.NewInMemoryStore()
	sender := &replyingSender{replied: make(chan error, 1)}
	agent := &mockAgentExecutor{}
	task := &a2a.Task{ID: a2a.NewTaskID(), ContextID: a2a.NewContextID(), Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	if _, err := store.Create(asUser("alice"), task); err != nil {
		t.Fatal(err)
	}
	if _, err := pushes.Save(asUser("alice"), task.ID, &a2a.PushConfig{URL: "https://example.com/push"}); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(agent, WithTaskStore(store), WithPushNotifications(pushes, sender))
	sender.h = h

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := h.(TaskUpdater).UpdateTask(ctx, a2a.NewStatusUpdateEvent(task, a2a.TaskStateInputRequired, nil))
	if err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if got.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("UpdateTask() state = %v, want input required", got.Status.State)
	}
	if err := <-sender.replied; !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Errorf("the recipient's answer during the update: %v, want a busy refusal", err)
	}
}

// An update runs none of the agent's execution cleanup.
func TestUpdateTaskRunsNoExecutionCleanup(t *testing.T) {
	store := newUpdaterStore()
	cleaned := false
	agent := &mockAgentExecutor{CleanupFn: func(context.Context, a2a.SendMessageResult, error) { cleaned = true }}
	h, task, _ := newWaitingTaskIn(t, "alice", agent, store)
	if _, err := h.(TaskUpdater).UpdateTask(context.Background(), a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil)); err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	if cleaned {
		t.Error("an update ran the agent's execution cleanup")
	}
}

// A store which knows only user names refuses the server, also for a task
// of the anonymous user, whose name is empty as well.
func TestUpdateTaskNeedsAStoreWhichKnowsTheServer(t *testing.T) {
	byName := func(ctx context.Context) (string, error) {
		if callCtx, ok := CallContextFrom(ctx); ok {
			return callCtx.User.Name, nil
		}
		return "", a2a.ErrUnauthenticated
	}
	configs := map[string]func() *taskstore.InMemoryStoreConfig{
		"caller from name": func() *taskstore.InMemoryStoreConfig {
			return &taskstore.InMemoryStoreConfig{CallerAuthenticator: TaskStoreCallerFromName(byName)}
		},
		"deprecated name authenticator": func() *taskstore.InMemoryStoreConfig {
			return &taskstore.InMemoryStoreConfig{Authenticator: NewTaskStoreAuthenticator()} //nolint:staticcheck // the deprecated path must refuse the server too
		},
	}
	for name, config := range configs {
		for _, owner := range []string{"alice", ""} {
			store := taskstore.NewInMemory(config())
			h, task, _ := newWaitingTaskIn(t, owner, &mockAgentExecutor{}, store)
			if _, err := h.(TaskUpdater).UpdateTask(context.Background(), a2a.NewStatusUpdateEvent(task, a2a.TaskStateFailed, nil)); !errors.Is(err, a2a.ErrUnauthenticated) {
				t.Errorf("%s: UpdateTask() of %q's task error = %v, want ErrUnauthenticated", name, owner, err)
			}
			// The owner, also the anonymous one, still reaches the task, unchanged.
			got, err := store.Get(asUser(owner), task.ID)
			if err != nil || got.Task.Status.State != a2a.TaskStateInputRequired {
				t.Errorf("%s: Get() as owner %q = %v %v, want the task still waiting", name, owner, got, err)
			}
		}
	}
}

// A request can't make itself the server: only NewServerCallContext makes it.
func TestServerCallCannotBeClaimed(t *testing.T) {
	_, callCtx := NewCallContext(context.Background(), nil)
	callCtx.User = NewAuthenticatedUser("server", nil)
	var none *CallContext
	if callCtx.IsServer() || none.IsServer() {
		t.Error("a request's call context is the server's")
	}
	if _, server := NewServerCallContext(context.Background()); !server.IsServer() {
		t.Error("NewServerCallContext() is not the server's")
	}
}

// failingSender fails every push notification, as an unreachable webhook does.
type failingSender struct{}

func (failingSender) SendPush(context.Context, *a2a.PushConfig, a2a.Event) error {
	return errors.New("webhook unreachable")
}

// A push notification which is not delivered leaves the update standing, and
// the caller can tell it from a refused update.
func TestUpdateTaskKeepsTheUpdateWhenPushFails(t *testing.T) {
	for _, state := range []a2a.TaskState{a2a.TaskStateCompleted, a2a.TaskStateInputRequired} {
		store := newUpdaterStore()
		pushes := push.NewInMemoryStore()
		task := &a2a.Task{ID: a2a.NewTaskID(), ContextID: a2a.NewContextID(), Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
		if _, err := store.Create(asUser("alice"), task); err != nil {
			t.Fatal(err)
		}
		if _, err := pushes.Save(asUser("alice"), task.ID, &a2a.PushConfig{URL: "https://example.com/push"}); err != nil {
			t.Fatal(err)
		}
		h := NewHandler(&mockAgentExecutor{}, WithTaskStore(store), WithPushNotifications(pushes, failingSender{}))
		got, err := h.(TaskUpdater).UpdateTask(context.Background(), a2a.NewStatusUpdateEvent(task, state, nil))
		if !errors.Is(err, ErrPushNotDelivered) {
			t.Errorf("UpdateTask(%v) error = %v, want ErrPushNotDelivered", state, err)
		}
		if got == nil || got.Status.State != state {
			t.Errorf("UpdateTask(%v) task = %v, want the stored update", state, got)
		}
		stored, err := h.GetTask(asUser("alice"), &a2a.GetTaskRequest{ID: task.ID})
		if err != nil || stored.Status.State != state {
			t.Errorf("GetTask() after UpdateTask(%v) = %v %v, want the update stored", state, stored, err)
		}
	}
}
