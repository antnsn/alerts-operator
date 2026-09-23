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

// TestTenantAlertmanagerRefusesWriteOnOwnershipCollision is the steady-state counterpart to
// TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes: the finalizer has always known the
// Alertmanager document's ownership key is (tenantId, address) with no prefix component, but
// syncAlertmanager had no such check and simply POSTed over whatever it found.
func TestTenantAlertmanagerRefusesWriteOnOwnershipCollision(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	mine := amPeer("am-conflict-a", "1", s.URL, "team-a")
	mine.Generation = 1
	peer := amPeer("am-conflict-b", "1", s.URL, "team-b")
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

// TestTenantAlertmanagerConflictLeavesRulesSyncing is the whole-reconcile view of the same finding,
// against the live envtest manager: the guard must be scoped to the one target that is genuinely
// shared. Two Tenants with different rulesNamespacePrefix values own disjoint rule namespaces and
// must go on syncing them normally -- it is only the single Alertmanager document at
// (tenantId, address) that neither may write.
func TestTenantAlertmanagerConflictLeavesRulesSyncing(t *testing.T) {
	srv := fakebackend.New()
	defer srv.Close()

	type pair struct {
		tn  *observabilityv1alpha1.Tenant
		cp  *observabilityv1alpha1.ContactPoint
		pol *observabilityv1alpha1.NotificationPolicy
		arg *observabilityv1alpha1.AlertRuleGroup
	}
	var pairs []pair
	for _, p := range []struct{ name, prefix string }{{"amx-a", "amx-prefix-a"}, {"amx-b", "amx-prefix-b"}} {
		tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: p.name},
			Spec: observabilityv1alpha1.TenantSpec{TenantID: "1",
				Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: p.prefix,
				// A conflict is cleared by the peer's CR disappearing, and the survivor is not
				// enqueued by that deletion (bead alerts-operator-29k) -- only its own resync
				// re-evaluates ownership. The production default is 5m; this keeps the last leg of
				// the test inside a poll window without weakening what it asserts.
				ResyncInterval: &metav1.Duration{Duration: 2 * time.Second}}}
		cp, pol := writingContactPointAndPolicy(tn.Name, p.name)
		arg := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "amx-g-" + p.name, Namespace: "default"},
			Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir,
				Groups: ruleGroups("up == 0")}}
		if err := testClient.Create(testCtx, tn); err != nil {
			t.Fatal(err)
		}
		pairs = append(pairs, pair{tn, cp, pol, arg})
	}
	// Both Tenant CRs must be visible in the manager's cache -- which is what conflictingTenant
	// reads -- before either gets a NotificationPolicy. Creating each Tenant together with its
	// children instead would let the first one compile and legitimately write the document while
	// it is still the sole claimant, which is correct behaviour but not what this test is about.
	waitFor(t, func() bool {
		for _, p := range pairs {
			if err := testCacheClient.Get(testCtx, clientKey(p.tn), &observabilityv1alpha1.Tenant{}); err != nil {
				return false
			}
		}
		return true
	})
	for _, p := range pairs {
		for _, o := range []client.Object{p.cp, p.pol, p.arg} {
			if err := testClient.Create(testCtx, o); err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, p := range pairs {
		waitCondition(t, p.tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
		waitCondition(t, p.tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
		waitCondition(t, p.tn, observabilityv1alpha1.ConditionMimirRulesSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
		if p.tn.Status.AlertmanagerConfigHash != "" {
			t.Fatalf("%s recorded a hash for a document it never wrote: %q", p.tn.Name, p.tn.Status.AlertmanagerConfigHash)
		}
	}
	if am := srv.Alertmanager("1"); am != nil {
		t.Fatalf("neither Tenant may write the shared document, got %+v", am)
	}
	rules := srv.Rules("1")
	for _, ns := range []string{"amx-prefix-a/default/amx-g-amx-a", "amx-prefix-b/default/amx-g-amx-b"} {
		if len(rules[ns]) != 1 {
			t.Fatalf("rule sync must be unaffected by an Alertmanager ownership conflict: %+v", rules)
		}
	}

	// Removing one claimant clears the conflict for the other: the survivor writes the document it
	// is now the sole owner of. (It is enqueued by its own resync, not by the peer's deletion --
	// see bead alerts-operator-29k -- so this can take up to one resyncInterval in production; the
	// default here is short enough for the poll below.)
	for _, o := range []client.Object{pairs[1].cp, pairs[1].pol, pairs[1].arg, pairs[1].tn} {
		if err := testClient.Delete(testCtx, o); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(pairs[1].tn), &observabilityv1alpha1.Tenant{}))
	})
	waitCondition(t, pairs[0].tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if am := srv.Alertmanager("1"); am == nil || !strings.Contains(am.Config, "default/fin-cp-amx-a") {
		t.Fatalf("sole surviving claimant must write its own document, got %+v", am)
	}

	for _, o := range []client.Object{pairs[0].cp, pairs[0].pol, pairs[0].arg, pairs[0].tn} {
		_ = testClient.Delete(testCtx, o)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(pairs[0].tn), &observabilityv1alpha1.Tenant{}))
	})
}
