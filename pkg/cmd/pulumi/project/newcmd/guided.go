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

package newcmd

import (
	"errors"
	"fmt"
	"slices"

	survey "github.com/AlecAivazis/survey/v2"

	"github.com/pulumi/pulumi/pkg/v3/backend/display"
	"github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/cmd"
	"github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/project/newcmd/catalog"
	cmdTemplates "github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/templates"
	"github.com/pulumi/pulumi/pkg/v3/cmd/pulumi/ui"
)

const (
	optionOther     = "Other"
	optionBrowseAll = "Browse all templates"
)

type selectFunc func(message string, options []string, opts display.Options) (int, error)

var errFallBackToFlatList = errors.New("fall back to the flat template list")

// stepBack reports why the chosen row led nowhere and returns to the previous prompt.
func stepBack(opts display.Options, format string, a ...any) error {
	fmt.Fprintf(opts.StdoutOrDefault(), format+"\n", a...)
	return ui.ErrStepBack
}

func surveySelect(message string, options []string, opts display.Options) (int, error) {
	return ui.PromptUserIndexErr("\r"+message+"\n", options, opts.Color,
		survey.WithPageSize(cmd.OptimalPageSize(cmd.OptimalPageSizeOpts{Nopts: len(options)})))
}

// pick prompts for one of items. Duplicate display names are suffixed so identical rows stay
// visually distinct; selection is by index, so labels never round-trip back to items.
func pick[T any](
	sel selectFunc, message string, opts display.Options, items []T, name func(T) string,
) (T, error) {
	options := make([]string, len(items))
	counts := make(map[string]int, len(items))
	for i, item := range items {
		label := name(item)
		counts[label]++
		if n := counts[label]; n > 1 {
			label = fmt.Sprintf("%s (%d)", label, n)
		}
		options[i] = label
	}
	i, err := sel(message, options, opts)
	if err != nil {
		var zero T
		return zero, err
	}
	return items[i], nil
}

// fetchTemplatesFunc blocks until the full template set — including the VCS collections the
// service fetches upstream — is available.
type fetchTemplatesFunc func() ([]cmdTemplates.Template, error)

// guidedTemplates is what the guided prompts are built from. project and registry come from
// fetches that are fast enough to hold up the first prompt; fetchAll covers the rest and is only
// called for a row that cannot be answered without it.
type guidedTemplates struct {
	project  []cmdTemplates.Template
	registry []cmdTemplates.Template
	vcsOrgs  []string
	fetchAll fetchTemplatesFunc
}

func chooseGuided(
	t guidedTemplates, opts display.Options, sel selectFunc,
) (cmdTemplates.Template, error) {
	curated := make([]cmdTemplates.Template, 0, len(t.project))
	for _, template := range t.project {
		// A broken template can't back a provider/language row. Browse all still lists it, marked.
		if template.Error() != nil {
			continue
		}
		curated = append(curated, template)
	}

	cat := catalog.New(curated, cmdTemplates.Template.Name)
	orgs := t.orgRows()
	if cat.Empty() && len(orgs) == 0 {
		return nil, errFallBackToFlatList
	}

	rows := cloudRows(cat, orgs)
	var choice guidedChoice
	var language string
	if err := ui.SurveyStack(
		func() (err error) {
			choice, err = chooseCloud(rows, t, opts, sel)
			return err
		},
		func() (err error) {
			if choice.template != nil {
				return nil
			}
			l, err := pick(sel, "Which language would you like to use?", opts,
				choice.provider.Languages, func(l catalog.Language) string { return l.DisplayName })
			language = l.ID
			return err
		},
	); err != nil {
		return nil, err
	}
	if choice.template != nil {
		return choice.template, nil
	}

	// The prompts only offer values the catalog can resolve, so a miss here is a broken invariant.
	template, ok := cat.Resolve(choice.provider.ID, language)
	if !ok {
		return nil, fmt.Errorf("no template for provider %q and language %q", choice.provider.ID, language)
	}
	return template, nil
}

// guidedChoice is either a provider that still needs a language, or a registry template chosen
// directly.
type guidedChoice struct {
	provider catalog.Provider
	template cmdTemplates.Template
}

type rowKind int

const (
	rowProvider rowKind = iota
	rowOther
	rowOrg
	rowBrowseAll
)

type cloudRow struct {
	kind      rowKind
	label     string
	provider  catalog.Provider   // rowProvider
	providers []catalog.Provider // rowOther
	org       string             // rowOrg
}

// registryPublisher returns the organization that published a usable registry template.
func registryPublisher(t cmdTemplates.Template) (string, bool) {
	if t.Error() != nil {
		return "", false
	}
	publisher := t.Publisher()
	return publisher, publisher != ""
}

// orgRows names the organizations worth offering a row for: those that published templates to the
// registry, and those the service reports as having VCS collections, whose templates nothing has
// fetched yet. Both signals come from the database, so an organization can turn out to have
// nothing to show.
func (t guidedTemplates) orgRows() []string {
	names := slices.Clone(t.vcsOrgs)
	for _, template := range t.registry {
		if publisher, ok := registryPublisher(template); ok {
			names = append(names, publisher)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func cloudRows(cat *catalog.Catalog[cmdTemplates.Template], orgs []string) []cloudRow {
	featured := cat.Featured()
	rows := make([]cloudRow, 0, len(featured)+len(orgs)+3)
	for _, p := range featured {
		rows = append(rows, cloudRow{kind: rowProvider, label: p.DisplayName, provider: p})
	}
	if others := cat.Others(); len(others) > 0 {
		rows = append(rows, cloudRow{kind: rowOther, label: optionOther, providers: others})
	}
	if none, ok := cat.None(); ok {
		rows = append(rows, cloudRow{kind: rowProvider, label: none.DisplayName, provider: none})
	}
	for _, org := range orgs {
		rows = append(rows, cloudRow{kind: rowOrg, label: org + " templates", org: org})
	}
	rows = append(rows, cloudRow{kind: rowBrowseAll, label: optionBrowseAll})
	return rows
}

// chooseCloud runs the cloud prompt and its dispatch in a SurveyStack of their own, nested inside
// chooseGuided's outer stack: an interrupt in a dispatch sub-prompt (a template list, the Other
// providers) steps back to the cloud prompt here, while an interrupt at the language step lands on
// this whole function, skipping over the non-prompting dispatch step that would otherwise bounce
// the interrupt around.
func chooseCloud(
	rows []cloudRow, t guidedTemplates, opts display.Options, sel selectFunc,
) (guidedChoice, error) {
	var row cloudRow
	var choice guidedChoice
	err := ui.SurveyStack(
		func() (err error) {
			row, err = pick(sel, "Which cloud would you like to use?", opts, rows,
				func(r cloudRow) string { return r.label })
			return err
		},
		func() (err error) {
			choice = guidedChoice{}
			switch row.kind {
			case rowProvider:
				choice.provider = row.provider
			case rowOther:
				choice.provider, err = pick(sel, "Which provider would you like to use?", opts, row.providers,
					func(p catalog.Provider) string { return p.DisplayName })
			case rowOrg:
				choice.template, err = chooseOrgTemplates(row.org, t, opts, sel)
			case rowBrowseAll:
				choice.template, err = chooseAllTemplates(t.fetchAll, opts, sel)
			}
			return err
		},
	)
	return choice, err
}

// chooseAllTemplates and chooseOrgTemplates are the only rows that need the full template set, so
// they are also the only ones that can discover it is unavailable. Both step back to the cloud
// prompt rather than abandoning the flow: the provider and language rows still work without it.
func chooseAllTemplates(
	fetchAll fetchTemplatesFunc, opts display.Options, sel selectFunc,
) (cmdTemplates.Template, error) {
	all, err := fetchAll()
	if err != nil {
		return nil, stepBack(opts, "Could not load the full template list: %v", err)
	}
	return chooseTemplateFromList(sortedForDisplay(all), opts, sel)
}

// chooseOrgTemplates offers an organization's templates. An organization with no VCS collections
// is answered from the registry fetch, which has already finished, so its row costs nothing to
// open; one with collections has to wait for them to be fetched.
func chooseOrgTemplates(
	org string, t guidedTemplates, opts display.Options, sel selectFunc,
) (cmdTemplates.Template, error) {
	available := t.registry
	if slices.Contains(t.vcsOrgs, org) {
		all, err := t.fetchAll()
		if err != nil {
			return nil, stepBack(opts, "Could not load templates for organization %q: %v", org, err)
		}
		available = all
	}

	var orgTemplates []cmdTemplates.Template
	for _, template := range available {
		if publisher, ok := registryPublisher(template); ok && publisher == org {
			orgTemplates = append(orgTemplates, template)
		}
	}
	if len(orgTemplates) == 0 {
		return nil, stepBack(opts, "No templates found for organization %q.", org)
	}
	return chooseTemplateFromList(orgTemplates, opts, sel)
}

func chooseTemplateFromList(
	templates []cmdTemplates.Template, opts display.Options, sel selectFunc,
) (cmdTemplates.Template, error) {
	message := fmt.Sprintf("Please choose a template (%d total):", len(templates))
	return pick(sel, message, opts, templates, templateLabeler(templates))
}
