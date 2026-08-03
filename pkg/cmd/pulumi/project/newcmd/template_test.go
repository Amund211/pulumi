// Copyright 2024, Pulumi Corporation.
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

package newcmd

import (
	"bytes"
	"errors"
	"testing"

	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulumi/pulumi/pkg/v3/backend/display"
	cmdTemplates "github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/templates"
)

func TestSanitizeTemplate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{"https://user:pass@example.com/path?param=value", "https://example.com/path"},
		{"https://user:pass@example.com", "https://example.com"},
		{"https://example.com/path?param=value", "https://example.com/path"},
		{"ssh://user@hostname/project/repo", "ssh://hostname/project/repo"},
		{"typescript", "typescript"},
		{"aws-typescript", "aws-typescript"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			result := sanitizeTemplate(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChooseTemplateNonInteractiveReturnsNil(t *testing.T) {
	t.Parallel()

	got, err := ChooseTemplate(
		[]cmdTemplates.Template{fakeTemplate{name: "aws-typescript"}},
		display.Options{IsInteractive: false},
	)
	require.NoError(t, err)
	assert.Nil(t, got)
}

// fakeSource is a guidedTemplateSource with independently controllable fetches.
type fakeSource struct {
	project     []cmdTemplates.Template
	registry    []cmdTemplates.Template
	vcsOrgs     []string
	all         []cmdTemplates.Template
	projectErr  error
	registryErr error
	allErr      error
}

func (f fakeSource) ProjectTemplates() ([]cmdTemplates.Template, error) {
	return f.project, f.projectErr
}

func (f fakeSource) RegistryTemplates() ([]cmdTemplates.Template, error) {
	return f.registry, f.registryErr
}

func (f fakeSource) VcsTemplateSourceOrgs() []string             { return f.vcsOrgs }
func (f fakeSource) Templates() ([]cmdTemplates.Template, error) { return f.all, f.allErr }

// sourceOf builds a fakeSource whose project fetch carries every template, as the local checkout
// does when nothing has been published to the registry.
func sourceOf(templates ...cmdTemplates.Template) fakeSource {
	return fakeSource{project: templates, all: templates}
}

func TestUseGuidedFlowOnlyWhenNothingIsNamed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args newArgs
		want bool
	}{
		{"nothing named", newArgs{}, true},
		{"template named", newArgs{templateNameOrURL: "aws-typescript"}, false},
		{"url named", newArgs{templateNameOrURL: "https://github.com/pulumi/examples"}, false},
		{"auto-accept", newArgs{yes: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.args.useGuidedFlow())
		})
	}
}

func TestGuidedChooserFallsBackToFlatWhenNothingIsCurated(t *testing.T) {
	t.Parallel()

	flatCalled := false
	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		flatCalled = true
		return fakeTemplate{name: "flat"}, nil
	}
	sel := func(string, []string, display.Options) (int, error) {
		t.Error("no prompt may be shown when the catalog is empty")
		return 0, nil
	}

	// A name the catalog can't decompose yields no providers, so guided must defer to the flat list.
	var notice bytes.Buffer
	got, err := guidedChooser(sel, flat)(
		sourceOf(fakeTemplate{name: "unparseable"}, fakeTemplate{name: "second"}),
		display.Options{IsInteractive: true, Stdout: &notice},
	)
	require.NoError(t, err)
	assert.True(t, flatCalled)
	assert.Equal(t, "flat", got.Name())
	assert.Contains(t, notice.String(), "Falling back to the full template list.")
}

func TestGuidedChooserFallbackHandlesTinyTemplateSets(t *testing.T) {
	t.Parallel()

	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		t.Error("the flat chooser must not run for zero or one template")
		return nil, nil
	}
	sel := func(string, []string, display.Options) (int, error) {
		t.Error("no prompt may be shown for zero or one template")
		return 0, nil
	}

	_, err := guidedChooser(sel, flat)(
		fakeSource{}, display.Options{IsInteractive: true},
	)
	assert.ErrorContains(t, err, "no templates")

	only := fakeTemplate{name: "unparseable"}
	got, err := guidedChooser(sel, flat)(
		sourceOf(only), display.Options{IsInteractive: true},
	)
	require.NoError(t, err)
	assert.Equal(t, "unparseable", got.Name(), "a single template is chosen without prompting")
}

func TestGuidedChooserBrowseAllListsInline(t *testing.T) {
	t.Parallel()

	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		t.Error("flat chooser must not run for Browse all; the guided flow lists templates itself")
		return nil, nil
	}
	sel, _ := scriptedSelect(t, optionBrowseAll, "aws-typescript    ")

	got, err := guidedChooser(sel, flat)(
		sourceOf(fakeTemplate{name: "aws-typescript"}),
		display.Options{IsInteractive: true},
	)
	require.NoError(t, err)
	assert.Equal(t, "aws-typescript", got.Name())
}

func TestGuidedChooserMapsInterruptToNoTemplateSelected(t *testing.T) {
	t.Parallel()

	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		t.Error("flat chooser must not run when the user cancels the guided flow")
		return nil, nil
	}
	sel := func(string, []string, display.Options) (int, error) {
		return 0, terminal.InterruptErr
	}

	got, err := guidedChooser(sel, flat)(
		sourceOf(fakeTemplate{name: "aws-typescript"}),
		display.Options{IsInteractive: true},
	)
	assert.Nil(t, got)
	assert.ErrorContains(t, err, "no template selected")
}

func TestGuidedChooserReturnsGuidedTemplateWithoutFlat(t *testing.T) {
	t.Parallel()

	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		t.Error("flat chooser must not run when guided resolves a template")
		return nil, nil
	}
	sel, _ := scriptedSelect(t, "AWS", "TypeScript")

	got, err := guidedChooser(sel, flat)(
		sourceOf(fakeTemplate{name: "aws-typescript"}),
		display.Options{IsInteractive: true},
	)
	require.NoError(t, err)
	assert.Equal(t, "aws-typescript", got.Name())
}

func TestGuidedChooserPropagatesErrors(t *testing.T) {
	t.Parallel()

	flat := func([]cmdTemplates.Template, display.Options) (cmdTemplates.Template, error) {
		t.Error("flat chooser must not run when guided errors")
		return nil, nil
	}
	sel := func(string, []string, display.Options) (int, error) {
		return 0, errors.New("boom")
	}

	_, err := guidedChooser(sel, flat)(
		sourceOf(fakeTemplate{name: "aws-typescript"}),
		display.Options{IsInteractive: true},
	)
	assert.ErrorContains(t, err, "boom")
}

func TestGuidedChooserNonInteractiveReturnsNil(t *testing.T) {
	t.Parallel()

	sel := func(string, []string, display.Options) (int, error) {
		t.Error("no prompt may be shown when non-interactive")
		return 0, nil
	}

	got, err := guidedChooser(sel, ChooseTemplate)(
		sourceOf(fakeTemplate{name: "aws-typescript"}),
		display.Options{IsInteractive: false},
	)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestSortedForDisplaySortsAndMarksBroken(t *testing.T) {
	t.Parallel()

	sorted := sortedForDisplay([]cmdTemplates.Template{
		fakeTemplate{name: "zeta"},
		fakeTemplate{name: "broken", err: errors.New("boom")},
		fakeTemplate{name: "alpha"},
	})
	require.Len(t, sorted, 3)
	assert.Equal(t, "alpha", sorted[0].Name())
	assert.Equal(t, "zeta", sorted[1].Name())
	assert.Equal(t, "broken", sorted[2].Name(), "broken templates sort to the end")
	assert.Contains(t, templateLabeler(sorted)(sorted[2]), BrokenTemplateDescription)
}
