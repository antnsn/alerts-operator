# ArgoCD health checks

Without these, ArgoCD shows every alerts-operator CR as `Healthy` the moment it exists.
Add to `argocd-cm` (`resource.customizations.health.<group>_<Kind>`).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
  namespace: argocd
data:
  resource.customizations.health.observability.antnsn.dev_Tenant: |
    hs = { status = "Progressing", message = "waiting for Ready condition" }
    local gen = obj.metadata.generation
    if obj.status ~= nil and obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        -- A condition whose observedGeneration lags metadata.generation is from before the
        -- latest spec edit reconciled -- e.g. a just-edited Tenant still carrying a prior
        -- Ready=True until its own reconcile catches up. Leave it Progressing rather than
        -- reporting the stale verdict as current.
        if c.type == "Ready" and c.observedGeneration == gen then
          if c.status == "True" then
            hs.status = "Healthy"
            hs.message = c.reason
          elseif c.reason == "Deleting" then
            hs.status = "Progressing"
            hs.message = c.message
          else
            hs.status = "Degraded"
            hs.message = c.reason .. ": " .. (c.message or "")
          end
        end
      end
    end
    return hs
  resource.customizations.health.observability.antnsn.dev_ContactPoint: &child |
    hs = { status = "Progressing", message = "waiting for Accepted/Synced" }
    local gen = obj.metadata.generation
    if obj.status ~= nil and obj.status.conditions ~= nil then
      local accepted, synced = nil, nil
      for _, c in ipairs(obj.status.conditions) do
        -- Same staleness guard as the Tenant check above: only trust a condition reported for
        -- the current generation, otherwise leave hs at its Progressing default.
        if c.type == "Accepted" and c.observedGeneration == gen then accepted = c end
        if c.type == "Synced" and c.observedGeneration == gen then synced = c end
      end
      if accepted ~= nil and accepted.status == "False" then
        return { status = "Degraded", message = accepted.reason .. ": " .. (accepted.message or "") }
      end
      if synced ~= nil then
        if synced.status == "True" then
          return { status = "Healthy", message = synced.reason }
        end
        return { status = "Degraded", message = synced.reason .. ": " .. (synced.message or "") }
      end
    end
    return hs
  resource.customizations.health.observability.antnsn.dev_NotificationPolicy: *child
  resource.customizations.health.observability.antnsn.dev_AlertRuleGroup: *child
```

YAML anchors are resolved by the apiserver's YAML parser, so the three child kinds share one Lua body.
Verify: `argocd app get <app>` shows `Degraded` for a CR with `Accepted=False`.
