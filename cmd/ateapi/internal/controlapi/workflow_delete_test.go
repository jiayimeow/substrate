// Copyright 2026 Google LLC
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

package controlapi

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestDeleteActorWorkflow_ExecutionPaths(t *testing.T) {
	tests := []struct {
		name        string
		seedState   ateapipb.ActorState
		anyState    bool
		missingTmpl bool
		wantErr     bool
		wantCode    codes.Code
	}{
		{
			name:      "delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete deleting actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_DELETING,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete running actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "delete paused actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "any_state delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete running actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete paused actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:        "delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    false,
			missingTmpl: true,
			wantErr:     false,
		},
		{
			name:        "any_state delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    true,
			missingTmpl: true,
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			tmplName := "tmpl1"
			if tc.missingTmpl {
				tmplName = "missing-tmpl"
			}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", tmplName, tc.seedState)

			deleted, err := w.DeleteActor(ctx, actorRef, tc.anyState, store.DeletePreconditions{})
			if tc.wantErr {
				if got := status.Code(err); got != tc.wantCode {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tc.wantCode, err)
				}
			} else {
				if err != nil {
					t.Fatalf("DeleteActor failed: %v", err)
				}
				if deleted == nil {
					t.Fatalf("expected non-nil deleted actor")
				}
				if _, err := st.GetActor(ctx, actorRef); err == nil {
					t.Errorf("expected actor to be deleted from store, but it still exists")
				}
			}
		})
	}
}

func TestEnsureMarkedDeleting_StateMatrix(t *testing.T) {
	tests := []struct {
		name     string
		anyState bool
		allowed  map[ateapipb.ActorState]bool
	}{
		{
			name:     "standard delete",
			anyState: false,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED: true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:   true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:  true, // skipped
			},
		},
		{
			name:     "any_state delete",
			anyState: true,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
				ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
				ateapipb.ActorState_ACTOR_STATE_RESUMING:    true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  true,
				ateapipb.ActorState_ACTOR_STATE_PAUSING:     true,
				ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:     true,
				ateapipb.ActorState_ACTOR_STATE_REVERTING:   true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:    true, // skipped
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, seedState := range allActorStates {
				ctx := context.Background()
				st, cleanup := storetest.SetupTestStore(t)
				w := newTestActorWorkflow(t, st, "ns", "tmpl1")

				actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
				seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState)
				actor, err := st.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatalf("state %v: get seeded actor: %v", seedState, err)
				}

				updated, err := w.ensureMarkedDeleting(ctx, actorRef, actor, tc.anyState)
				assertPrerequisiteResult(t, seedState, err, tc.allowed[seedState])
				if err == nil && seedState != ateapipb.ActorState_ACTOR_STATE_DELETING {
					if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
						t.Errorf("state %v: ensureMarkedDeleting returned actor in %v, want DELETING", seedState, updated.GetStatus().GetState())
					}
				}
				cleanup()
			}
		})
	}
}

// TestEnsureExternalSnapshotsReleased covers what a delete collects on its way
// out: the external snapshot the actor owns, and any snapshot an abandoned
// suspend left in flight, but never one the actor is only borrowing from a tag.
func TestEnsureExternalSnapshotsReleased(t *testing.T) {
	// inFlightSnapshotName names a snapshot written during a suspend operation
	// that never finalized.
	const inFlightSnapshotName = "2026-01-01t00-00-00z-abandoned"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the actor's external snapshot a tag's rather than one
		// it took itself, which is how an actor created from a tag starts out.
		tagOwnedSnapshot bool
		// inFlight, when set, names a snapshot an abandoned suspend left
		// behind. It must always be collected by the delete.
		inFlight            string
		wantCurrentReleased bool
	}{
		{
			name:                "releases the external snapshot the actor owns",
			wantCurrentReleased: true,
		},
		{
			// The actor can't delete a snapshot it only borrows from a tag:
			// that snapshot goes away with the tag.
			name:                "leaves an external snapshot borrowed from a tag in place",
			tagOwnedSnapshot:    true,
			wantCurrentReleased: false,
		},
		{
			name:                "collects the external snapshot an abandoned suspend left in flight",
			inFlight:            inFlightSnapshotName,
			wantCurrentReleased: true,
		},
		{
			name:                "collects an in-flight external snapshot even while borrowing",
			tagOwnedSnapshot:    true,
			inFlight:            inFlightSnapshotName,
			wantCurrentReleased: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
			})

			// The actor's prefix is keyed on the UID the store just assigned, so
			// its snapshots can only be placed now.
			current := mustActorSnapshotURI(t, template, actor, "current")
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: current.String()}
				s.InProgressSnapshotUri = inFlight.String()
			})

			if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
				t.Fatalf("ensureExternalSnapshotsReleased: %v", err)
			}
			if released := len(objects.Snapshot(t, current)) == 0; released != tt.wantCurrentReleased {
				t.Errorf("current external snapshot released = %v, want %v", released, tt.wantCurrentReleased)
			}
			if tt.inFlight != "" && len(objects.Snapshot(t, inFlight)) != 0 {
				t.Errorf("in-flight external snapshot %v was not released", inFlight)
			}
		})
	}
}

// TestEnsureExternalSnapshotsReleased_CollectsStrandedSnapshots covers that
// any leaked resource in external storage is cleaned upon actor deletion because
// they're under the same storage prefix:
//   - objects that dont have an entry in the DB
//   - a suspend that died before it recorded anything in the DB
func TestEnsureExternalSnapshotsReleased_CollectsStrandedSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
	})
	otherActor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-2"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	currentSnapshot := mustActorSnapshotURI(t, template, actor, "current")
	strandedSnapshot := mustActorSnapshotURI(t, template, actor, "stranded")
	otherActorsSnapshot := mustActorSnapshotURI(t, template, otherActor, "current")
	for _, uri := range []resources.SnapshotURI{currentSnapshot, strandedSnapshot, otherActorsSnapshot} {
		objects.PutSnapshot(t, uri, "manifest.json")
	}
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: currentSnapshot.String()}
	})

	if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
		t.Fatalf("ensureExternalSnapshotsReleased: %v", err)
	}
	if remaining := objects.Snapshot(t, strandedSnapshot); len(remaining) != 0 {
		t.Errorf("stranded external snapshot %v was not released, %v remains", strandedSnapshot, remaining)
	}
	// Deletion should not release snapshots from other actors
	if len(objects.Snapshot(t, otherActorsSnapshot)) == 0 {
		t.Errorf("another actor's external snapshot %v was released", otherActorsSnapshot)
	}
}

// TestDeleteActor_CollectsSnapshotWithoutActorTemplate checks that snapshots
// are collected even if the actor template is gone.
func TestDeleteActor_CollectsInFlightSnapshotWithoutTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	objects := objectstoretest.New()
	w := NewActorWorkflow(persistence, nil, nil, nil, nil, nil, "", nil, objects)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})

	// The template that holds the storage location is gone (was never written to storage).
	// We should still be able to access/delete the current snapshot for this actor.
	inFlight := mustActorSnapshotURI(t, &ateapipb.ActorTemplate{
		SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation},
	}, actor, "abandoned")
	objects.PutSnapshot(t, inFlight, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.InProgressSnapshotUri = inFlight.String()
	})

	if _, err := w.DeleteActor(ctx, actorRef, true, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if left := objects.Prefix(t, inFlight.OwnerPrefix()); len(left) != 0 {
		t.Errorf("Deleting the actor left %v under its own prefix, want the in-flight snapshot collected", left)
	}
}

// TestEnsureExternalSnapshotsReleased_DeletePrefixFailure verifies that if
// objectstore.DeletePrefix fails with a transient error during actor deletion,
// ensureExternalSnapshotsReleased returns that error, preserves the un-deleted
// objects, and cleanly completes the deletion on a subsequent retry.
func TestEnsureExternalSnapshotsReleased_DeletePrefixFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
	})

	current := mustActorSnapshotURI(t, template, actor, "current")
	objects.PutSnapshot(t, current, "manifest.json", "memory.zst")
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: current.String()}
	})

	errTransient := errors.New("simulated transient delete failure")
	objects.OnDelete = func(bucket, object string) error {
		return errTransient
	}

	err := w.ensureExternalSnapshotsReleased(ctx, actor)
	if !errors.Is(err, errTransient) {
		t.Fatalf("ensureExternalSnapshotsReleased error = %v, want error wrapping %v", err, errTransient)
	}

	// Objects should not have been deleted
	if len(objects.Snapshot(t, current)) == 0 {
		t.Fatal("objects were unexpectedly deleted despite OnDelete failure")
	}

	// Retry without failure: should successfully clean up the snapshot objects
	objects.OnDelete = nil
	if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
		t.Fatalf("ensureExternalSnapshotsReleased on retry failed: %v", err)
	}
	if remaining := objects.Snapshot(t, current); len(remaining) != 0 {
		t.Errorf("external snapshot objects remain after retry: %v", remaining)
	}
}

// TestDeleteActor_CollectsSnapshotsAfterWorkerDelete verifies that
// deleting an actor whose suspend a worker delete crashed mid-finalize deletes
// every object that suspend wrote. When an actor crashes mid-suspend, only
// DeleteActor or RevertActor can delete the in-progress snapshot
// (in_progress_snapshot_uri): whatever they cannot name is leaked for good.
func TestDeleteActor_CollectsSnapshotsAfterWorkerDelete(t *testing.T) {
	tests := []struct {
		name string
		// hasBeenSuspended means the actor was replacing a snapshot it took
		// itself, rather than suspending for the first time. Only the first
		// suspend needs the retained in-progress name to find what it wrote; a
		// replacement is already reachable through the snapshot it owns.
		hasBeenSuspended bool
	}{
		{
			name:             "replacing a snapshot the actor owned",
			hasBeenSuspended: true,
		},
		{
			name:             "first suspend, nothing to replace",
			hasBeenSuspended: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			objects := objectstoretest.New()

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			workerName := testWorkerUID("pod-1")
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker:          &ateapipb.ObjectRef{Name: workerName},
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-1",
						WorkerPodUid:    workerName,
					},
				},
			})
			if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-1",
				WorkerPodUid:    workerName,
				Status:          &ateapipb.WorkerStatus{},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			seedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid: actor.GetMetadata().GetUid(),
			})

			if tt.hasBeenSuspended {
				previous := mustActorSnapshotURI(t, template, actor, "old")
				objects.PutSnapshot(t, previous, "manifest.json")
				actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
					s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: previous.String()}
				})
			}

			actorWorkflow := NewActorWorkflow(persistence, nil, nil, nil, nil, nil, "", nil, objects)
			// Suspend the actor as far as it gets: MarkSuspending mints the
			// in-progress URI, and the checkpoint writes under it
			actor, err := actorWorkflow.ensureMarkedSuspending(ctx, actorRef, actor, template)
			if err != nil {
				t.Fatalf("ensureMarkedSuspending: %v", err)
			}
			fresh := mustParseSnapshotURI(t, actor.GetStatus().GetInProgressSnapshotUri())
			objects.PutSnapshot(t, fresh, "manifest.json")

			// The worker's pod goes away with the commit still outstanding, so
			// the suspend never gets to finish.
			if _, err := NewWorkerWorkflow(persistence).DeleteWorker(ctx, workerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker: %v", err)
			}

			// The actor is CRASHED; DeleteActor deletes the in-progress snapshot
			// (in_progress_snapshot_uri).
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Fatalf("state = %v, want CRASHED", got)
			}
			if _, err := actorWorkflow.DeleteActor(ctx, actorRef, true, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteActor: %v", err)
			}
			if left := objects.Prefix(t, fresh.OwnerPrefix()); len(left) != 0 {
				t.Errorf("deleting the actor left %v under its own prefix, want everything it wrote collected", left)
			}
		})
	}
}

// reclaimRecordingAtelet is a fake atelet that records every ReclaimActorDirs
// request and fails each with err while it is set.
type reclaimRecordingAtelet struct {
	ateletpb.UnimplementedAteomHerderServer

	mu       sync.Mutex
	err      error
	requests []*ateletpb.ReclaimActorDirsRequest
}

func (f *reclaimRecordingAtelet) ReclaimActorDirs(_ context.Context, req *ateletpb.ReclaimActorDirsRequest) (*ateletpb.ReclaimActorDirsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, proto.Clone(req).(*ateletpb.ReclaimActorDirsRequest))
	if f.err != nil {
		return nil, f.err
	}
	return &ateletpb.ReclaimActorDirsResponse{}, nil
}

func (f *reclaimRecordingAtelet) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *reclaimRecordingAtelet) reclaims() []*ateletpb.ReclaimActorDirsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// TestEnsureActorDirsReclaimed covers which nodes the step asks to reclaim an
// actor's directory. Only node-1 runs an atelet.
func TestEnsureActorDirsReclaimed(t *testing.T) {
	snapshotOn := func(nodes ...string) *ateapipb.LocalSnapshot {
		return &ateapipb.LocalSnapshot{SnapshotName: "pause-1", NodeVmsWithLocalSnapshots: nodes}
	}
	tests := []struct {
		name         string
		status       *ateapipb.ActorStatus
		ateletErr    error
		wantReclaims int
		wantErr      bool
	}{
		{
			name:         "reclaims on the node holding the snapshot",
			status:       &ateapipb.ActorStatus{LocalSnapshot: snapshotOn("node-1")},
			wantReclaims: 1,
		},
		{
			// The node and its disk are most likely gone; failing would keep
			// the actor DELETING for as long as they stay gone.
			name:         "skips a node without an atelet",
			status:       &ateapipb.ActorStatus{LocalSnapshot: snapshotOn("node-gone")},
			wantReclaims: 0,
		},
		{
			name:         "reclaims past a node without an atelet",
			status:       &ateapipb.ActorStatus{LocalSnapshot: snapshotOn("node-gone", "node-1")},
			wantReclaims: 1,
		},
		{
			name:         "reports a failed reclaim",
			status:       &ateapipb.ActorStatus{LocalSnapshot: snapshotOn("node-1")},
			ateletErr:    status.Error(codes.Internal, "simulated disk failure"),
			wantReclaims: 1,
			wantErr:      true,
		},
		{
			// Terminate reclaims the directory of an actor its worker hosts.
			name:         "leaves an assigned actor to Terminate",
			status:       &ateapipb.ActorStatus{WorkerAssignment: wireTestAssignment(), LocalSnapshot: snapshotOn("node-1")},
			wantReclaims: 0,
		},
		{
			name:         "skips an actor without a local snapshot",
			status:       &ateapipb.ActorStatus{},
			wantReclaims: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atelet := &reclaimRecordingAtelet{err: tt.ateletErr}
			w := &ActorWorkflow{dialer: newBufconnAteletDialer(t, "node-1", atelet)}
			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1", Uid: someActorUID},
				Status:   tt.status,
			}

			err := w.ensureActorDirsReclaimed(context.Background(), resources.ActorRefFromActor(actor), actor)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("ensureActorDirsReclaimed error = %v, want error: %v", err, tt.wantErr)
			}
			got := atelet.reclaims()
			if len(got) != tt.wantReclaims {
				t.Fatalf("atelet served %d reclaims, want %d", len(got), tt.wantReclaims)
			}
			want := &ateletpb.ReclaimActorDirsRequest{Atespace: "team-a", ActorName: "id1", ActorUid: someActorUID}
			for _, req := range got {
				if !proto.Equal(req, want) {
					t.Errorf("reclaim request = %v, want %v", req, want)
				}
			}
		})
	}
}

// TestDeleteActor_ReclaimsPausedActorsDirs covers deleting a paused actor: no
// worker is assigned, so no Terminate reaches the node holding its local
// snapshot and the delete must reclaim the actor's directory there. A failed
// reclaim keeps the actor DELETING so that the retry reclaims again.
func TestDeleteActor_ReclaimsPausedActorsDirs(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")
	atelet := &reclaimRecordingAtelet{err: status.Error(codes.Internal, "simulated disk failure")}
	w.dialer = newBufconnAteletDialer(t, "node-1", atelet)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_PAUSED, func(a *ateapipb.Actor) {
		a.Status.LocalSnapshot = &ateapipb.LocalSnapshot{SnapshotName: "pause-1", NodeVmsWithLocalSnapshots: []string{"node-1"}}
	})
	paused, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	if _, err := w.DeleteActor(ctx, actorRef, true, store.DeletePreconditions{}); err == nil {
		t.Fatal("DeleteActor succeeded despite the failed reclaim, want an error")
	}
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor after the failed delete: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_DELETING {
		t.Errorf("state after the failed delete = %v, want DELETING", got)
	}

	atelet.setErr(nil)
	if _, err := w.DeleteActor(ctx, actorRef, true, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActor retry: %v", err)
	}
	if _, err := st.GetActor(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActor after the retry = %v, want %v", err, store.ErrNotFound)
	}
	got := atelet.reclaims()
	if len(got) != 2 {
		t.Fatalf("atelet served %d reclaims, want 2 (the failed one and the retry)", len(got))
	}
	want := &ateletpb.ReclaimActorDirsRequest{Atespace: "team-a", ActorName: "id1", ActorUid: paused.GetMetadata().GetUid()}
	for _, req := range got {
		if !proto.Equal(req, want) {
			t.Errorf("reclaim request = %v, want %v", req, want)
		}
	}
}
