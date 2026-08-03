// Copyright 2016, Pulumi Corporation.
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

// Package templates adds an abstraction for project templates that may be local or
// remote.
//
// All templates are convertible into [ProjectTemplate].
package templates

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/pulumi/pulumi/pkg/v3/registry"
	"github.com/pulumi/pulumi/sdk/v3/go/common/apitype"
	"github.com/pulumi/pulumi/sdk/v3/go/common/env"
	"github.com/pulumi/pulumi/sdk/v3/go/common/util/contract"
)

// fetch is one independent template fetch. Each fetch owns the templates and errors it produced
// so that a caller can join one fetch without waiting on the others.
type fetch struct {
	wg sync.WaitGroup

	// m guards templates and errs: a fetch may fan out into goroutines of its own that report
	// results concurrently.
	m         sync.Mutex
	templates []Template
	errs      []error
}

func (f *fetch) addTemplate(t Template) {
	contract.Assertf(t != nil, "We should never return nil templates")
	f.m.Lock()
	f.templates = append(f.templates, t)
	f.m.Unlock()
}

func (f *fetch) addError(err error) {
	f.m.Lock()
	f.errs = append(f.errs, err)
	f.m.Unlock()
}

// join waits for the fetch to finish, then returns its templates, or its errors if it produced
// any.
func (f *fetch) join() ([]Template, error) {
	f.wg.Wait()

	f.m.Lock()
	defer f.m.Unlock()
	if err := errors.Join(f.errs...); err != nil {
		return nil, err
	}
	return slices.Clone(f.templates), nil
}

// Source provides access to a set of project templates, any set of which may be present on
// disk.
//
// Source is responsible for cleaning up old templates, and should always be [Close]d when
// created.
type Source struct {
	// The fetches, fastest first. project is the local disk and curated template set. registry is
	// the templates the service holds in its database, which it answers from without leaving the
	// building. vcs is the collections an organization configured against a version control
	// provider, which the service fetches upstream on every request and so is far slower than
	// the other two. Tracking them apart lets a caller join one without waiting on the rest.
	project  fetch
	registry fetch
	vcs      fetch

	// vcsSourceOrgs names the organizations that have VCS-backed template collections
	// configured, as reported by registry listings. It covers organizations whose templates
	// those listings do not themselves carry.
	vcsSourceOrgs []string

	errorOnEmpty []error

	// cancel holds the function to cancel the context passed into the [New] that created the source.
	cancel context.CancelFunc
	// closers holds a list of functions to be invoked when the Source is closed.
	closers []func() error
	closed  bool

	// m should be held whenever Source is mutated.
	m sync.Mutex
}

// fetches lists the fetches, fastest first. Anything that spans all of them — waiting, joining
// errors — should range over this so a new fetch only needs to be added here.
func (s *Source) fetches() []*fetch {
	return []*fetch{&s.project, &s.registry, &s.vcs}
}

// waitAll blocks until every fetch has finished.
func (s *Source) waitAll() {
	for _, w := range s.fetches() {
		w.wg.Wait()
	}
}

// Templates lists the templates available to the [Source].
//
// Templates *does not* produce a sorted list; it groups the fetches in the order they are declared
// on [Source]. If templates need to be sorted, then the caller is responsible for sorting them.
func (s *Source) Templates() ([]Template, error) {
	// Wait to ensure that all templates have been fetched before returning the template list.
	s.waitAll()

	s.lockOpen("read templates")
	defer s.m.Unlock()
	var errs []error
	for _, w := range s.fetches() {
		errs = append(errs, w.errs...)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	// A registry template already produced by an earlier fetch is dropped. A service that does
	// not honor the backing filter answers each fetch with everything it has, which would
	// otherwise list every cloud template twice.
	seen := map[string]bool{}
	var all []Template
	for _, w := range s.fetches() {
		for _, t := range w.templates {
			if m, ok := t.(TemplateMatchable); ok {
				id := registryIdentity(m)
				if seen[id] {
					continue
				}
				seen[id] = true
			}
			all = append(all, t)
		}
	}
	if len(all) == 0 {
		return nil, errors.Join(s.errorOnEmpty...)
	}
	return all, nil
}

// registryIdentity is the source/publisher/name triple that names a template in the registry.
func registryIdentity(t TemplateMatchable) string {
	return t.GetSource() + "/" + t.GetPublisher() + "/" + t.GetRegistryName()
}

// ProjectTemplates lists only the templates found by the project fetcher (local disk and the
// curated template set), without waiting for the slower cloud fetches, which keep running in the
// background. Use [Source.Templates] for the complete set.
//
// Unlike [Source.Templates], an empty result is not an error here: a fetch that is still running
// may yet produce templates, so only the complete set can conclude that there are none.
func (s *Source) ProjectTemplates() ([]Template, error) {
	return s.project.join()
}

// RegistryTemplates lists only the templates the service holds in its database, without waiting
// for the VCS collections it would have to fetch upstream. As with [Source.ProjectTemplates], an
// empty result is not an error.
func (s *Source) RegistryTemplates() ([]Template, error) {
	return s.registry.join()
}

// VcsTemplateSourceOrgs names the organizations that have VCS-backed template collections
// configured. Their templates are only in [Source.Templates], never in
// [Source.RegistryTemplates].
//
// The result is empty when the service does not report the organizations, so an organization
// listed here has collections but one absent from here may still have them.
func (s *Source) VcsTemplateSourceOrgs() []string {
	s.registry.wg.Wait()

	s.lockOpen("read vcs template source orgs")
	defer s.m.Unlock()
	return slices.Clone(s.vcsSourceOrgs)
}

func (s *Source) observeVcsTemplateSources(resp apitype.ListTemplatesResponse) {
	s.lockOpen("add vcs template source orgs")
	for _, total := range resp.VcsTemplateSourceTotals {
		if !slices.Contains(s.vcsSourceOrgs, total.OrgLogin) {
			s.vcsSourceOrgs = append(s.vcsSourceOrgs, total.OrgLogin)
		}
	}
	s.m.Unlock()
}

func (s *Source) addCloser(f func() error) {
	s.lockOpen("add closer")
	s.closers = append(s.closers, f)
	s.m.Unlock()
}

func (s *Source) addErrorOnEmpty(err error) {
	s.lockOpen("add error on empty")
	s.errorOnEmpty = append(s.errorOnEmpty, err)
	s.m.Unlock()
}

func (s *Source) lockOpen(action string) {
	s.m.Lock()
	contract.Assertf(!s.closed, "%s", "Attempted to act on closed source: "+action)
}

// Close cleans up the [Source] and any associated templates.
//
// Close should always be called when [Source] is dropped.
func (s *Source) Close() error {
	s.cancel()

	// Wait to ensure that all templates have been fetched so all closers are visible.
	s.waitAll()

	s.lockOpen("close")
	defer s.m.Unlock()
	s.closed = true
	errs := make([]error, len(s.closers))
	for i, f := range s.closers {
		errs[i] = f()
	}
	return errors.Join(errs...)
}

// A template entry to show in the chooser.
type Template interface {
	Name() string
	DisplayName() string
	Description() string
	Error() error
	// Publisher returns the organization that published this template to the registry. It is
	// empty for templates without one, such as the curated pulumi/templates set.
	Publisher() string
	// Download the template and return an instantiable [ProjectTemplate] for this template.
	Download(ctx context.Context) (ProjectTemplate, error)
}

// SearchScope dictates where [New] will search for templates.
type SearchScope struct{ kind string }

var (
	// ScopeAll searches for templates in all available locations.
	ScopeAll = SearchScope{}
	// ScopeLocal searches for templates only locally (on disk).
	ScopeLocal = SearchScope{"local"}
)

// Create a new [Template] [Source] associated with a given [SearchScope].
func New(
	ctx context.Context, templateNamePathOrURL string, scope SearchScope,
	templateKind TemplateKind, e env.Env,
) *Source {
	return newImpl(
		ctx, templateNamePathOrURL, scope,
		templateKind,
		RetrieveTemplates,
		e,
	)
}

// The impl for [New].
//
// having a separate impl function allows mocking out getProjectTemplates.
func newImpl(
	ctx context.Context, templateNamePathOrURL string, scope SearchScope,
	templateKind TemplateKind,
	getProjectTemplates getProjectTemplateFunc,
	e env.Env,
) *Source {
	var source Source
	ctx, cancel := context.WithCancel(ctx)
	source.cancel = cancel

	if scope == ScopeAll || scope == ScopeLocal {
		source.project.wg.Go(func() {
			source.getProjectTemplates(
				ctx, &source.project, templateNamePathOrURL, scope, templateKind, getProjectTemplates,
			)
		})
	}

	if scope == ScopeAll && templateKind == TemplateKindPulumiProject && isTemplateName(templateNamePathOrURL) {
		switch {
		case e.GetBool(env.DisableRegistryResolve):
			// Use the old org templates based API.
			//
			// This path can be removed when we are confident in registry resolution. We will
			// always need to maintain a way to access templates without the service, but we
			// should only need to maintain one way to access templates through the service.
			//
			// It has no notion of a backing and everything it returns is VCS-backed, so it
			// runs as the vcs fetch.
			source.vcs.wg.Go(func() {
				source.getOrgTemplates(ctx, &source.vcs, templateNamePathOrURL, e)
			})
		case templateNamePathOrURL == "":
			// Browsing splits the cloud fetch into separate requests so that the fast one can
			// be joined without the slow one. Neither asks for
			// [apitype.TemplateBackingPulumi]: those are the curated templates, which the
			// project fetch already has from its own checkout.
			r := defaultRegistry(ctx, e)
			source.registry.wg.Go(func() {
				source.getRegistryTemplates(ctx, &source.registry, r, "", registry.ListTemplatesOptions{
					Backing: []apitype.TemplateBacking{apitype.TemplateBackingRegistry},
				})
			})
			source.vcs.wg.Go(func() {
				source.getRegistryTemplates(ctx, &source.vcs, r, "", registry.ListTemplatesOptions{
					Backing: []apitype.TemplateBacking{apitype.TemplateBackingVcs},
				})
			})
		default:
			// Resolving a name has no prompt to render early, so it takes one unfiltered
			// lookup rather than paying for two.
			source.vcs.wg.Go(func() {
				source.getRegistryTemplates(
					ctx, &source.vcs, defaultRegistry(ctx, e), templateNamePathOrURL,
					registry.ListTemplatesOptions{},
				)
			})
		}
	}

	return &source
}

func isTemplateName(templateNamePathOrURL string) bool {
	return !IsGitRepoTemplateURL(templateNamePathOrURL) &&
		!isTemplatePath(templateNamePathOrURL)
}

func isTemplatePath(query string) bool {
	_, err := os.Stat(query)
	if errors.Is(err, fs.ErrNotExist) {
		if looksLikePath(query) {
			const msg = "%q looks like a file path, but no file exists. Assuming to be a template name"
			slog.Warn(fmt.Sprintf(msg, query))
		}
		return false
	} else if err != nil {
		slog.Warn("unable to stat", "query", query, "err", err.Error())
		return false
	}

	// query does point to a local file.

	if !looksLikePath(query) {
		const msg = `Assuming %[1]q is a file path, use "./%[1]s" to be unambiguous`
		slog.Warn(fmt.Sprintf(msg, query))
	}
	return err == nil
}

func looksLikePath(query string) bool {
	return strings.HasPrefix(query, "./") || strings.HasPrefix(query, "/")
}
