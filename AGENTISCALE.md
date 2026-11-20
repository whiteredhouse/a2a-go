# agentiscale's fork of the A2A Go SDK

This branch (`agentiscale`) is [a2aproject/a2a-go](https://github.com/a2aproject/a2a-go)
release **v2.9.0** (commit `c746bfd`) plus the commits below. agentiscaled serves
its agents' A2A endpoints with it. `main` stays a copy of upstream; changes land on
`agentiscale` through pull requests. Licensed under Apache-2.0; see [LICENSE](LICENSE).

agentiscaled keeps the upstream module path and pins this branch with
`replace github.com/a2aproject/a2a-go/v2 => github.com/whiteredhouse/a2a-go/v2 <version>`.

## What differs from upstream

| Change | Why |
|---|---|
| `a2asrv.TaskUpdater` (`UpdateTask(ctx, event)`) in `a2asrv/handler.go`, passed through in `a2asrv/intercepted_handler.go`; `CreateUpdater` in `a2asrv/agentexec.go`; `Manager.Update`, `Factory.CreateUpdater`, `Updater`, `UpdateRequest` and `ErrExecutionActive` and `ErrUpdateInProgress` in `internal/taskexec`; `a2asrv.ErrTaskActive`; `a2asrv.NewServerCallContext` / `CallContext.IsServer`, `taskstore.Caller` / `CallerAuthenticator` (`InMemoryStoreConfig.CallerAuthenticator`, used by the handler's default store), `a2asrv.NewTaskStoreCallerAuthenticator` and `a2asrv.TaskStoreCallerFromName`; `taskstore.Authenticator`, `InMemoryStoreConfig.Authenticator` and `a2asrv.NewTaskStoreAuthenticator` marked `Deprecated:` (still work, adapted to a caller); callers in tests and `itk` moved to the caller authenticator; tests in `a2asrv/task_updater_test.go` and `a2asrv/taskstore/store_test.go` | Upstream runs an agent's events only inside a `SendMessage` or `CancelTask` request, so an agent cannot end a task on its own, for example when a question it asked gets no answer in time. `UpdateTask` publishes one final status update of the agent's task (ending it, or interrupting it such as input required) through the same processing as an execution's events: the task store and push notifications. It takes one event, not a batch, so the update is written whole or not at all: a2a-go writes events one at a time, and checking a batch up front would mean copying its rules. It checks the event is for the task and its context and that its state is final; an event the processor still can't apply fails the call. It refuses a task that has ended and a task with an active execution, cancelation or update (`a2asrv.ErrTaskActive`, retryable). While it runs, the update holds the task under the lock that admits executions and cancelations, so a message or cancelation that comes meanwhile is refused as busy rather than queued (a push recipient may answer before it acknowledges), and a retry sees the update's result. It runs none of the agent's execution cleanup hooks. Once the event is stored nothing after it changes the task (an execution would fail it on a push failure): the stored task is read before delivery with a context that outlives the call, every delivery is checked including failures reported only in `DeliveryResult.Err`, and a failure after the store wraps `a2asrv.ErrUpdateStored` (push: `ErrPushNotDelivered`) with the stored task returned. It runs as the server's own identity: a call context only `NewServerCallContext` can make, which a store that knows callers (`Caller.Server`) lets reach every task, keeping the task's owner. Stores learn who a call is for through one authenticator returning a structured caller (user name, or the server); the name-only authenticator is deprecated and kept as an adapter, the usual way to evolve a public Go API. `TaskStoreCallerFromName` and the deprecated `NewTaskStoreAuthenticator` refuse the server (a name cannot stand for it, and the anonymous user's name is empty too), so name-only stores fail closed. Cluster mode returns unsupported. Candidate for upstream. |
| This file | Records the fork. |

## Updating to a new upstream release

1. Sync `main` with upstream, then merge the new release tag into `agentiscale`
   in a pull request. Merge rather than rebase, so that commits agentiscaled
   pinned earlier stay on the branch.
2. Run `go build ./... && go vet ./...` and
   `go test -race ./a2asrv/... ./internal/taskexec/...`.
   If the execution manager or factory changed upstream, carry the update path
   along: it mirrors how a cancelation is set up and processed.
3. Update the release and commit at the top of this file, tag the result
   (for example `vX.Y.Z-agentiscale.1`) and move the pin in agentiscaled.
