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
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/AlecAivazis/survey/v2/terminal"

	"github.com/pulumi/pulumi/pkg/v3/backend/display"
	cmdTemplates "github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/templates"
	"github.com/pulumi/pulumi/sdk/v3/go/common/util/cmdutil"
)

const (
	BrokenTemplateDescription = "(This template is currently broken)"
)

var errNoTemplateSelected = errors.New("no template selected; please use `pulumi new` to choose one")

// ChooseTemplate will prompt the user to choose amongst the available templates.
func ChooseTemplate(templates []cmdTemplates.Template, opts display.Options) (cmdTemplates.Template, error) {
	if !opts.IsInteractive {
		return nil, nil
	}

	template, err := chooseTemplateFromList(sortedForDisplay(templates), opts, surveySelect)
	if err != nil {
		return nil, errNoTemplateSelected
	}
	return template, nil
}

// sortedForDisplay orders templates by display name, broken templates last.
func sortedForDisplay(templates []cmdTemplates.Template) []cmdTemplates.Template {
	sorted := slices.Clone(templates)
	slices.SortStableFunc(sorted, func(a, b cmdTemplates.Template) int {
		aBroken, bBroken := a.Error() != nil, b.Error() != nil
		if aBroken != bBroken {
			if aBroken {
				return 1
			}
			return -1
		}
		return strings.Compare(a.DisplayName(), b.DisplayName())
	})
	return sorted
}

// guidedTemplateSource is the subset of [cmdTemplates.Source] the guided flow needs: the fast
// fetches the first prompt can be built from without waiting on the upstream one, and the full
// set for the selections that require it.
type guidedTemplateSource interface {
	ProjectTemplates() ([]cmdTemplates.Template, error)
	RegistryTemplates() ([]cmdTemplates.Template, error)
	VcsTemplateSourceOrgs() []string
	Templates() ([]cmdTemplates.Template, error)
}

type chooseTemplateGuidedFunc func(
	src guidedTemplateSource, opts display.Options,
) (cmdTemplates.Template, error)

// pickFromSet chooses a template from the complete set. A lone template needs no prompt, `--yes`
// never prompts and takes no template rather than guessing among several, and anything else goes
// to the flat list.
func pickFromSet(
	templates []cmdTemplates.Template, yes bool, flat chooseTemplateFunc, opts display.Options,
) (cmdTemplates.Template, error) {
	switch {
	case len(templates) == 1:
		return templates[0], nil
	case yes:
		return nil, nil
	case len(templates) == 0:
		return nil, errors.New("no templates")
	}
	return flat(templates, opts)
}

// useGuidedFlow picks the guided provider/language flow only when the user named no template at
// all. Any named template or URL already narrows the choice, so those disambiguate against the
// flat list.
func (args newArgs) useGuidedFlow() bool {
	return args.templateNameOrURL == "" && !args.yes
}

// templateChooser is the flat list unless the guided flow applies and a guided chooser was wired
// up; callers that only supply chooseTemplate get the flat list everywhere.
func (args newArgs) templateChooser() chooseTemplateGuidedFunc {
	if args.useGuidedFlow() && args.chooseTemplateGuided != nil {
		return args.chooseTemplateGuided
	}
	return flatChooser(args.chooseTemplate, args.yes)
}

// flatChooser adapts the flat list to the guided signature, for the paths that never offer the
// guided prompts: the named-template and `--yes` invocations.
func flatChooser(flat chooseTemplateFunc, yes bool) chooseTemplateGuidedFunc {
	return func(src guidedTemplateSource, opts display.Options) (cmdTemplates.Template, error) {
		all, err := src.Templates()
		if err != nil {
			return nil, err
		}
		return pickFromSet(all, yes, flat, opts)
	}
}

func guidedChooser(sel selectFunc, flat chooseTemplateFunc) chooseTemplateGuidedFunc {
	return func(src guidedTemplateSource, opts display.Options) (cmdTemplates.Template, error) {
		if !opts.IsInteractive {
			return nil, nil
		}

		project, err := src.ProjectTemplates()
		if err != nil {
			return nil, err
		}
		registry, err := src.RegistryTemplates()
		if err != nil {
			return nil, err
		}
		// The fetch starts before the first prompt renders, so by the time a selection needs the
		// full set it has usually already finished and the spinner never draws a frame.
		fetchAll := sync.OnceValues(func() ([]cmdTemplates.Template, error) {
			spinner, ticker := cmdutil.NewSpinnerAndTicker(
				"Loading templates", nil, opts.Color, 8 /*timesPerSecond*/, !opts.IsInteractive,
			)
			defer cmdutil.SpinUntilStopped(spinner, ticker)()
			return src.Templates()
		})

		template, err := chooseGuided(guidedTemplates{
			project:  project,
			registry: registry,
			vcsOrgs:  src.VcsTemplateSourceOrgs(),
			fetchAll: fetchAll,
		}, opts, sel)
		switch {
		case errors.Is(err, errFallBackToFlatList):
			all, err := fetchAll()
			if err != nil {
				return nil, err
			}
			// Announce the switch to the flat list only when a prompt is actually about to
			// appear — pickFromSet skips the chooser for a lone template.
			noticed := func(
				templates []cmdTemplates.Template, opts display.Options,
			) (cmdTemplates.Template, error) {
				fmt.Fprintln(opts.StdoutOrDefault(), "Falling back to the full template list.")
				return flat(templates, opts)
			}
			return pickFromSet(all, false /*yes*/, noticed, opts)
		case errors.Is(err, terminal.InterruptErr):
			return nil, errNoTemplateSelected
		case err != nil:
			return nil, err
		}
		return template, nil
	}
}

func templateLabeler(templates []cmdTemplates.Template) func(cmdTemplates.Template) string {
	maxNameLength := 0
	for _, template := range templates {
		maxNameLength = max(maxNameLength, len(template.DisplayName()))
	}
	return func(template cmdTemplates.Template) string {
		desc := template.Description()
		if template.Error() != nil {
			desc = BrokenTemplateDescription
		}
		return fmt.Sprintf("%-*s    %s", maxNameLength, template.DisplayName(), desc)
	}
}

// sanitizeTemplate strips sensitive data such as credentials and query strings from a template URL.
func sanitizeTemplate(template string) string {
	// If it's a valid URL, strip any credentials and query strings.
	if parsedURL, err := url.Parse(template); err == nil {
		parsedURL.User = nil
		parsedURL.RawQuery = ""
		return parsedURL.String()
	}
	// Otherwise, return the original string.
	return template
}
