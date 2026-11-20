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
| `a2asrv.TaskUpdater` (`UpdateTask(ctx, taskID, events...)`) in `a2asrv/handler.go`, passed through in `a2asrv/intercepted_handler.go`; `CreateUpdater` in `a2asrv/agentexec.go`; `Manager.Update`, `Factory.CreateUpdater`, `Updater`, `UpdateRequest` and `ErrExecutionActive` and `ErrUpdateInProgress` in `internal/taskexec`; `a2asrv.ErrTaskActive`; `taskstore.WithServerAccess` / `IsServerAccess`, honoured by the in-memory store; tests in `a2asrv/task_updater_test.go` and `a2asrv/taskstore/store_test.go` | Upstream runs an agent's events only inside a `SendMessage` or `CancelTask` request, so an agent cannot end a task on its own, for example when a question it asked gets no answer in time. `UpdateTask` publishes the agent's events to its task through the same processing as an execution's: the task store, subscribers and push. It checks the whole batch before writing anything: status and artifact events of the task and its context, none nil, no artifact empty, and only the last one final; an event the processor still can't apply fails the call. It refuses a task that has ended and a task with an active execution, cancelation or update (`a2asrv.ErrTaskActive`, retryable). While it runs, the update holds the task under the lock that admits executions and cancelations, so a message or cancelation that comes meanwhile is refused as busy rather than queued (a push recipient may answer before it acknowledges), and a retry sees the update's result. It runs none of the agent's execution cleanup hooks. The store's owner check is skipped for this server-side write, and the task keeps its owner. Cluster mode returns unsupported. Candidate for upstream. |
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
