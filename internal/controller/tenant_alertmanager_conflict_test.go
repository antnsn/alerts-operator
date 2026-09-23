package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	fakebackend "github.com/antnsn/alerts-operator/internal/backend/fake"
	"github.com/antnsn/alerts-operator/internal/backend/mimir"
)

// amPeer builds a second Tenant CR sharing (tenantId, mimir address) with addr but using its own
// rules namespace prefix -- the configuration rulesNamespacePrefix exists to support, and the one
// where the rule halves of two Tenants genuinely do not collide.
func amPeer(name, tenantID, addr, prefix string) *observabilityv1alpha1.Tenant {
	return &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: observabilityv1alpha1.TenantSpec{
			TenantID: tenantID, RulesNamespacePrefix: prefix,
			Mimir: &observabilityv1alpha1.BackendSpec{Address: addr},
		},
	}
}

// withWrittenAlertmanager marks a Tenant as having itself written the Alertmanager document that
// is live at addr -- the two status fields syncAlertmanager records after a successful POST, and
// the exact evidence writingClaimants reads. A peer without them is a Tenant that has never
// written the document and cannot be harmed by someone else writing it.
func withWrittenAlertmanager(tn *observabilityv1alpha1.Tenant, addr string) *observabilityv1alpha1.Tenant {
	tn.Status.AlertmanagerConfigHash = "sha256:" + tn.Name
	tn.Status.AlertmanagerConfigAddress = addr
	return tn
}

// TestTenantAlertmanagerIgnoresPeerThatHasNeverWritten covers the over-restriction the first
// version of this guard introduced: it counted ANY Tenant sharing (tenantId, mimir address) as a
// claimant, including one that is structurally incapable of writing the document.
//
// A Tenant with spec.mimir set but no accepted NotificationPolicy short-circuits at
// syncAlertmanager's ch.Policy == nil branch -- False/NoNotificationPolicy, before any backend I/O
// -- so it never POSTs /api/v1/alerts and never records a hash. A "platform" Tenant owning the
// notification config alongside a rules-only "team-a" on the same org, each with its own
// rulesNamespacePrefix, is exactly the arrangement rulesNamespacePrefix exists to support; the
// unnarrowed guard froze platform's Alertmanager document at Ready=False/Conflict with no remedy
// that preserves the design.
//
// The rule the guard actually enforces is "never destroy on an unverified claim". A peer that has
// never written the document has nothing there to destroy, so it is not a claimant -- which is the
// same narrowing the finalizer has always applied via writingClaimants.
func TestTenantAlertmanagerIgnoresPeerThatHasNeverWritten(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	platform := amPeer("am-platform", "1", s.URL, "platform")
	platform.Generation = 1
	// Rules only: no ContactPoint, no NotificationPolicy, no alertmanagerConfigHash. Its own
	// AlertmanagerSynced is False/NoNotificationPolicy and it never touches the backend.
	teamA := amPeer("am-team-a", "1", s.URL, "team-a")
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: platform.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://a"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: platform.Name, Route: observabilityv1alpha1.Route{Receiver: "cp"}}}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, platform, teamA, cp, pol), Recorder: record.NewFakeRecorder(20)}
	ch := &children{Policy: pol, ContactPoints: []observabilityv1alpha1.ContactPoint{*cp}}
	mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})

	if err := r.syncAlertmanager(context.Background(), platform, mc, ch); err != nil {
		t.Fatal(err)
	}

	if c := findCond(platform, observabilityv1alpha1.ConditionAlertmanagerSynced); c.Status != metav1.ConditionTrue {
		t.Fatalf("a rules-only peer must not block the Tenant that owns notifications, got %+v", c)
	}
	if am := s.Alertmanager("1"); am == nil || !strings.Contains(am.Config, "default/cp") {
		t.Fatalf("the sole writer must write its document, got %+v", am)
	}
	if platform.Status.AlertmanagerConfigHash == "" {
		t.Fatal("a successful write must record an alertmanagerConfigHash")
	}
}

// TestTenantAlertmanagerRefusesWriteOnOwnershipCollision is the steady-state counterpart to
// TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes: the finalizer has always known the
// Alertmanager document's ownership key is (tenantId, address) with no prefix component, but
// syncAlertmanager had no such check and simply POSTed over whatever it found.
func TestTenantAlertmanagerRefusesWriteOnOwnershipCollision(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	mine := amPeer("am-conflict-a", "1", s.URL, "team-a")
	mine.Generation = 1
	// A real writer: it holds a confirmed hash for the document at this address, so writing here
	// would replace its routing. A peer without one is covered by
	// TestTenantAlertmanagerIgnoresPeerThatHasNeverWritten.
	peer := withWrittenAlertmanager(amPeer("am-conflict-b", "1", s.URL, "team-b"), s.URL)
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: mine.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://a"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: mine.Name, Route: observabilityv1alpha1.Route{Receiver: "cp"}}}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, mine, peer, cp, pol), Recorder: record.NewFakeRecorder(20)}
	ch := &children{Policy: pol, ContactPoints: []observabilityv1alpha1.ContactPoint{*cp}}
	mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})

	if err := r.syncAlertmanager(context.Background(), mine, mc, ch); err != nil {
		t.Fatal(err)
	}

	if n := countPrefix(s.Requests(), "POST /api/v1/alerts"); n != 0 {
		t.Fatalf("an ambiguously-owned Alertmanager document must never be written: %v", s.Requests())
	}
	if s.Alertmanager("1") != nil {
		t.Fatalf("backend holds a document this Tenant was not entitled to write: %+v", s.Alertmanager("1"))
	}
	c := findCond(mine, observabilityv1alpha1.ConditionAlertmanagerSynced)
	if c.Status != metav1.ConditionFalse || c.Reason != observabilityv1alpha1.ReasonConflict {
		t.Fatalf("want AlertmanagerSynced=False/Conflict, got %+v", c)
	}
	if !strings.Contains(c.Message, peer.Name) {
		t.Fatalf("condition must name the other claimant: %q", c.Message)
	}
	if mine.Status.AlertmanagerConfigHash != "" {
		t.Fatalf("a refused write must not record an alertmanagerConfigHash: %q", mine.Status.AlertmanagerConfigHash)
	}

	var gotPol observabilityv1alpha1.NotificationPolicy
	if err := r.Get(context.Background(), clientKey(pol), &gotPol); err != nil {
		t.Fatal(err)
	}
	if pc := findCond(&gotPol, observabilityv1alpha1.ConditionSynced); pc.Status != metav1.ConditionFalse || pc.Reason != observabilityv1alpha1.ReasonConflict {
		t.Fatalf("children of a refused document must not report Synced=True: %+v", pc)
	}
}

// TestTenantAlertmanagerOwnershipKeyHasNoPrefixComponent pins the key itself: the Alertmanager
// document is addressed by X-Scope-OrgID and the backend URL alone, so the prefix must not narrow
// the claimant set (that is what conflictingTenant's rule-namespace key does) while a different
// tenantId or a different address must.
func TestTenantAlertmanagerOwnershipKeyHasNoPrefixComponent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		peer       *observabilityv1alpha1.Tenant
		wantWrites bool
	}{
		{"different prefix still collides", amPeer("peer", "1", "ADDR", "other-prefix"), false},
		{"same prefix collides", amPeer("peer", "1", "ADDR", "team-a"), false},
		{"different tenantId does not collide", amPeer("peer", "2", "ADDR", "team-a"), true},
		{"different address does not collide", amPeer("peer", "1", "http://elsewhere", "team-a"), true},
		// The writing narrowing is orthogonal to the key and is pinned here too, so a future change
		// to either cannot quietly absorb the other.
		{"same key but never wrote does not collide", amPeer("peer", "1", "ADDR", "team-b"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fakebackend.New()
			t.Cleanup(s.Close)
			mine := amPeer("mine", "1", s.URL, "team-a")
			mine.Generation = 1
			peer := tc.peer.DeepCopy()
			if peer.Spec.Mimir.Address == "ADDR" {
				peer.Spec.Mimir.Address = s.URL
			}
			// Every peer in this table except the "never wrote" row holds a confirmed hash at its
			// OWN address, so the only thing deciding each row is the key itself: a peer pointed
			// elsewhere is excluded by the key, not by the hash.
			if !strings.Contains(tc.name, "never wrote") {
				withWrittenAlertmanager(peer, peer.Spec.Mimir.Address)
			}
			cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
				Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: mine.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://a"}}}}
			pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
				Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: mine.Name, Route: observabilityv1alpha1.Route{Receiver: "cp"}}}

			r := &TenantReconciler{Client: newChildrenFakeClient(t, mine, peer, cp, pol), Recorder: record.NewFakeRecorder(20)}
			ch := &children{Policy: pol, ContactPoints: []observabilityv1alpha1.ContactPoint{*cp}}
			mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})
			if err := r.syncAlertmanager(context.Background(), mine, mc, ch); err != nil {
				t.Fatal(err)
			}
			wrote := countPrefix(s.Requests(), "POST /api/v1/alerts") > 0
			if wrote != tc.wantWrites {
				t.Fatalf("wrote=%v want %v (requests %v, condition %+v)", wrote, tc.wantWrites,
					s.Requests(), findCond(mine, observabilityv1alpha1.ConditionAlertmanagerSynced))
			}
		})
	}
}

// TestTenantAlertmanagerFirstWriterKeepsTheDocument is the whole-reconcile view, against the live
// envtest manager. Two things have to hold at once:
//
//   - the Tenant that actually owns the Alertmanager document is *not* disturbed by a second Tenant
//     appearing on its org -- only the newcomer is flagged, and the live routing keeps working;
//   - the guard is scoped to the one target that is genuinely shared: both Tenants use their own
//     rulesNamespacePrefix, so both keep syncing their rule groups normally throughout.
func TestTenantAlertmanagerFirstWriterKeepsTheDocument(t *testing.T) {
	srv := fakebackend.New()
	defer srv.Close()

	newPair := func(name, prefix string) (*observabilityv1alpha1.Tenant, []client.Object) {
		tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: observabilityv1alpha1.TenantSpec{TenantID: "1",
				Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: prefix,
				// A conflict is cleared by the peer's CR disappearing, and the survivor is not
				// enqueued by that deletion (bead alerts-operator-29k) -- only its own resync
				// re-evaluates ownership. The production default is 5m; this keeps the last leg of
				// the test inside a poll window without weakening what it asserts.
				ResyncInterval: &metav1.Duration{Duration: 2 * time.Second}}}
		cp, pol := writingContactPointAndPolicy(name, name)
		arg := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "amx-g-" + name, Namespace: "default"},
			Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: name, Backend: observabilityv1alpha1.BackendMimir,
				Groups: ruleGroups("up == 0")}}
		return tn, []client.Object{tn, cp, pol, arg}
	}

	// owner is created and synced alone, so its write is genuine and its hash is real.
	owner, ownerObjs := newPair("amx-a", "amx-prefix-a")
	for _, o := range ownerObjs {
		if err := testClient.Create(testCtx, o); err != nil {
			t.Fatal(err)
		}
	}
	waitCondition(t, owner, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if owner.Status.AlertmanagerConfigHash == "" {
		t.Fatal("precondition: owner must hold a confirmed hash")
	}

	// newcomer shares (tenantId, address) and brings its own policy: it wants the same document.
	newcomer, newcomerObjs := newPair("amx-b", "amx-prefix-b")
	for _, o := range newcomerObjs {
		if err := testClient.Create(testCtx, o); err != nil {
			t.Fatal(err)
		}
	}

	waitCondition(t, newcomer, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	waitCondition(t, newcomer, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	waitCondition(t, newcomer, observabilityv1alpha1.ConditionMimirRulesSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if c := findCond(newcomer, observabilityv1alpha1.ConditionAlertmanagerSynced); !strings.Contains(c.Message, owner.Name) {
		t.Fatalf("the condition must name the Tenant that owns the document: %q", c.Message)
	}
	if newcomer.Status.AlertmanagerConfigHash != "" {
		t.Fatalf("a refused write must not record a hash: %q", newcomer.Status.AlertmanagerConfigHash)
	}

	// The owner is untouched: still True, still Ready, still the document in the backend. Polled
	// over several of its 2s resyncs so a delayed flip to Conflict would be caught rather than
	// raced past.
	for range 3 {
		waitCondition(t, owner, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
		waitCondition(t, owner, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	}
	if am := srv.Alertmanager("1"); am == nil || !strings.Contains(am.Config, "default/fin-cp-amx-a") {
		t.Fatalf("the live document must still be the owner's, got %+v", am)
	}
	rules := srv.Rules("1")
	for _, ns := range []string{"amx-prefix-a/default/amx-g-amx-a", "amx-prefix-b/default/amx-g-amx-b"} {
		if len(rules[ns]) != 1 {
			t.Fatalf("rule sync must be unaffected by an Alertmanager ownership conflict: %+v", rules)
		}
	}

	// Removing the owner clears the conflict: its finalizer deletes the document it wrote (the
	// newcomer is not a writingClaimant, so there is nobody to defer to), and the newcomer -- now
	// the sole claimant -- writes its own at its next resync.
	for _, o := range ownerObjs {
		if err := testClient.Delete(testCtx, o); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(owner), &observabilityv1alpha1.Tenant{}))
	})
	waitCondition(t, newcomer, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if am := srv.Alertmanager("1"); am == nil || !strings.Contains(am.Config, "default/fin-cp-amx-b") {
		t.Fatalf("the sole surviving claimant must write its own document, got %+v", am)
	}

	for _, o := range newcomerObjs {
		_ = testClient.Delete(testCtx, o)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(newcomer), &observabilityv1alpha1.Tenant{}))
	})
}
