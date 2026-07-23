// Copyright 2026, Pulumi Corporation.
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

package deploy

import (
	"fmt"
	"maps"
	"reflect"

	pkgresource "github.com/pulumi/pulumi/pkg/v3/resource"
	sdkproviders "github.com/pulumi/pulumi/sdk/v3/go/common/providers"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
)

type stateMigrationTarget struct {
	custom bool
	id     resource.ID
}

// A migration transaction rewrites every state already materialized when it commits. Program and provider work can
// still produce state later in the update, and those values may contain predecessor references. stateMigrationRewrite
// is the small, immutable remainder of the committed transaction used to normalize that later-produced state. Only
// successor URNs and successor identity are needed for this.
type stateMigrationRewrite struct {
	rootURN       resource.URN
	successorURNs map[resource.URN]resource.URN
	targets       map[resource.URN]stateMigrationTarget
}

func stateMigrationTargets(states []*pkgresource.State) map[resource.URN]stateMigrationTarget {
	targets := make(map[resource.URN]stateMigrationTarget, len(states))
	for _, state := range states {
		targets[state.URN] = stateMigrationTarget{custom: state.Custom, id: state.ID}
	}
	return targets
}

func newStateMigrationRewrite(
	rootURN resource.URN,
	successors map[resource.URN]resource.URN,
	targets []*pkgresource.State,
) *stateMigrationRewrite {
	successorURNs := make(map[resource.URN]resource.URN, len(successors))
	maps.Copy(successorURNs, successors)
	return &stateMigrationRewrite{
		rootURN:       rootURN,
		successorURNs: successorURNs,
		targets:       stateMigrationTargets(targets),
	}
}

func (rewrite *stateMigrationRewrite) rewriteResources(
	states []*pkgresource.State,
) ([]*pkgresource.State, error) {
	return rewriteStateMigrationReferencesWithTargets(states, rewrite.targets, rewrite.successorURNs)
}

// RewriteResources rewrites references in states to the transaction's canonical successors. Snapshot managers use
// this while preparing exact patches for states produced earlier in the update.
func (plan *StateMigrationPlan) RewriteResources(states []*pkgresource.State) ([]*pkgresource.State, error) {
	return rewriteStateMigrationReferencesWithTargets(
		states, stateMigrationTargets(plan.MigratedResources), plan.SuccessorURNs)
}

// RewriteResourcesInPlace rewrites states while preserving their pointer and lock identities.
func (plan *StateMigrationPlan) RewriteResourcesInPlace(states []*pkgresource.State) error {
	rewritten, err := plan.RewriteResources(states)
	if err != nil {
		return err
	}
	for i, state := range states {
		if rewritten[i] == state {
			continue
		}
		state.Lock.Lock()
		applyStateMigrationReferenceRewrite(state, rewritten[i])
		state.Lock.Unlock()
	}
	return nil
}

// applyStateMigrationReferenceRewrite copies precisely the fields changed by reference rewriting. The caller must
// synchronize access to state.
func applyStateMigrationReferenceRewrite(state, fixed *pkgresource.State) {
	state.Parent = fixed.Parent
	state.Dependencies = fixed.Dependencies
	state.PropertyDependencies = fixed.PropertyDependencies
	state.DeletedWith = fixed.DeletedWith
	state.ReplaceWith = fixed.ReplaceWith
	state.ViewOf = fixed.ViewOf
	state.Provider = fixed.Provider
	state.Inputs = fixed.Inputs
	state.Outputs = fixed.Outputs
	state.ReplacementTrigger = fixed.ReplacementTrigger
}

// rewriteStateMigrationResources normalizes states through every migration committed so far in this deployment.
func (d *Deployment) rewriteStateMigrationResources(
	states []*pkgresource.State,
) ([]*pkgresource.State, error) {
	d.stateMigrationsM.RLock()
	defer d.stateMigrationsM.RUnlock()

	rewritten := states
	var err error
	for _, rewrite := range d.stateMigrations {
		rewritten, err = rewrite.rewriteResources(rewritten)
		if err != nil {
			return nil, fmt.Errorf("rewriting state for migration of %s: %w", rewrite.rootURN, err)
		}
	}
	return rewritten, nil
}

// rewriteStateMigrationURN resolves a structural URN through every migration committed so far. Callers use this
// before the URN affects identity decisions such as validating a parent or deriving a child's qualified URN.
func (d *Deployment) rewriteStateMigrationURN(urn resource.URN) resource.URN {
	if urn == "" {
		return ""
	}

	d.stateMigrationsM.RLock()
	defer d.stateMigrationsM.RUnlock()

	rewritten := urn
	for _, migration := range d.stateMigrations {
		rewritten = rewriteStateMigrationSuccessor(rewritten, migration.successorURNs)
	}
	return rewritten
}

// rewriteStateMigrationState normalizes a live state in place while preserving its pointer and lock identity.
func (d *Deployment) rewriteStateMigrationState(state *pkgresource.State) error {
	if state == nil {
		return nil
	}
	state.Lock.Lock()
	defer state.Lock.Unlock()
	return d.rewriteStateMigrationStateLocked(state)
}

// rewriteStateMigrationStateLocked is rewriteStateMigrationState for callers that already hold state.Lock.
func (d *Deployment) rewriteStateMigrationStateLocked(state *pkgresource.State) error {
	rewritten, err := d.rewriteStateMigrationResources([]*pkgresource.State{state})
	if err != nil {
		return err
	}
	if rewritten[0] == state {
		return nil
	}

	applyStateMigrationReferenceRewrite(state, rewritten[0])
	return nil
}

func (d *Deployment) rewriteStateMigrationStep(step Step) error {
	if old := step.Old(); old != nil {
		if err := d.rewriteStateMigrationState(old); err != nil {
			return fmt.Errorf("normalizing old state for %s after state migration: %w", step.URN(), err)
		}
	}
	if newState := step.New(); newState != nil && newState != step.Old() {
		if err := d.rewriteStateMigrationState(newState); err != nil {
			return fmt.Errorf("normalizing new state for %s after state migration: %w", step.URN(), err)
		}
	}
	return nil
}

// rewriteStateMigrationReferences returns independent copies of states with every reference to a removed URN
// rewritten to its final successor. This includes structural dependencies and resource references nested in property
// values. Multiple sources may resolve to the same target; dependency lists are deduplicated in that case.
func rewriteStateMigrationReferences(
	states []*pkgresource.State, successors map[resource.URN]resource.URN,
) ([]*pkgresource.State, error) {
	return rewriteStateMigrationReferencesWithTargets(states, stateMigrationTargets(states), successors)
}

func rewriteStateMigrationReferencesWithTargets(
	states []*pkgresource.State,
	targets map[resource.URN]stateMigrationTarget,
	successors map[resource.URN]resource.URN,
) ([]*pkgresource.State, error) {
	if len(successors) == 0 {
		return states, nil
	}

	fixURN := func(urn resource.URN) resource.URN {
		return rewriteStateMigrationSuccessor(urn, successors)
	}
	rewriteURNs := func(urns []resource.URN) []resource.URN {
		if len(urns) == 0 {
			return urns
		}
		result := make([]resource.URN, 0, len(urns))
		seen := make(map[resource.URN]bool, len(urns))
		for _, urn := range urns {
			fixed := fixURN(urn)
			if !seen[fixed] {
				seen[fixed] = true
				result = append(result, fixed)
			}
		}
		return result
	}

	var rewritePropertyValue func(resource.PropertyValue) resource.PropertyValue
	rewritePropertyMap := func(properties resource.PropertyMap) resource.PropertyMap {
		if properties == nil {
			return nil
		}
		result := make(resource.PropertyMap, len(properties))
		for key, value := range properties {
			result[key] = rewritePropertyValue(value)
		}
		return result
	}
	rewritePropertyValue = func(value resource.PropertyValue) resource.PropertyValue {
		switch {
		case value.IsArray():
			array := value.ArrayValue()
			result := make([]resource.PropertyValue, len(array))
			for i, element := range array {
				result[i] = rewritePropertyValue(element)
			}
			return resource.NewProperty(result)
		case value.IsObject():
			return resource.NewProperty(rewritePropertyMap(value.ObjectValue()))
		case value.IsComputed():
			return resource.MakeComputed(rewritePropertyValue(value.Input().Element))
		case value.IsOutput():
			output := value.OutputValue()
			output.Element = rewritePropertyValue(output.Element)
			output.Dependencies = rewriteURNs(output.Dependencies)
			return resource.NewProperty(output)
		case value.IsSecret():
			return resource.MakeSecret(rewritePropertyValue(value.SecretValue().Element))
		case value.IsResourceReference():
			ref := value.ResourceReferenceValue()
			fixed := fixURN(ref.URN)
			if fixed != ref.URN {
				ref.URN = fixed
				ref.Name = fixed.Name()
				ref.Type = string(fixed.Type())
				ref.PackageVersion = ""
				if target, ok := targets[fixed]; ok {
					if target.custom {
						ref.ID = resource.NewProperty(string(target.id))
					} else {
						ref.ID = resource.NewNullProperty()
					}
				}
			}
			return resource.NewProperty(ref)
		default:
			return value
		}
	}

	result := make([]*pkgresource.State, len(states))
	for i, state := range states {
		fixed := state.Copy()
		fixed.Parent = fixURN(fixed.Parent)
		fixed.Dependencies = rewriteURNs(fixed.Dependencies)
		if fixed.PropertyDependencies != nil {
			propertyDependencies := make(map[resource.PropertyKey][]resource.URN, len(fixed.PropertyDependencies))
			for key, dependencies := range fixed.PropertyDependencies {
				propertyDependencies[key] = rewriteURNs(dependencies)
			}
			fixed.PropertyDependencies = propertyDependencies
		}
		fixed.DeletedWith = fixURN(fixed.DeletedWith)
		fixed.ReplaceWith = rewriteURNs(fixed.ReplaceWith)
		fixed.ViewOf = fixURN(fixed.ViewOf)
		if fixed.Provider != "" {
			ref, err := sdkproviders.ParseReference(fixed.Provider)
			if err != nil {
				return nil, fmt.Errorf("parsing provider reference %q: %w", fixed.Provider, err)
			}
			originalProviderURN := ref.URN()
			providerURN := fixURN(originalProviderURN)
			providerID := ref.ID()
			if providerURN != originalProviderURN {
				if provider, ok := targets[providerURN]; ok {
					providerID = provider.id
				}
			}
			providerRef, err := sdkproviders.NewReference(providerURN, providerID)
			if err != nil {
				return nil, fmt.Errorf("rewriting provider reference %q: %w", fixed.Provider, err)
			}
			fixed.Provider = providerRef.String()
		}
		fixed.Inputs = rewritePropertyMap(fixed.Inputs)
		fixed.Outputs = rewritePropertyMap(fixed.Outputs)
		fixed.ReplacementTrigger = resource.FromResourcePropertyValue(
			rewritePropertyValue(resource.ToResourcePropertyValue(fixed.ReplacementTrigger)))
		if fixed.Parent == state.Parent &&
			reflect.DeepEqual(fixed.Dependencies, state.Dependencies) &&
			reflect.DeepEqual(fixed.PropertyDependencies, state.PropertyDependencies) &&
			fixed.DeletedWith == state.DeletedWith &&
			reflect.DeepEqual(fixed.ReplaceWith, state.ReplaceWith) &&
			fixed.ViewOf == state.ViewOf &&
			fixed.Provider == state.Provider &&
			fixed.Inputs.DeepEquals(state.Inputs) &&
			fixed.Outputs.DeepEquals(state.Outputs) &&
			fixed.ReplacementTrigger.Equals(state.ReplacementTrigger) {
			result[i] = state
		} else {
			result[i] = fixed
		}
	}
	return result, nil
}
