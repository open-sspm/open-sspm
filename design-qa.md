# Askbar Design QA

- Source visual truth: `/Users/sardo/.codex/generated_images/019f7188-762d-7c80-a1e6-026bcd92275e/exec-4a8ac784-273c-4054-b6c4-0c26f554d7b8.png`
- Implementation screenshot: `/Users/sardo/.codex/visualizations/2026/07/17/019f7188-762d-7c80-a1e6-026bcd92275e/askbar-implementation-open.png`
- Mobile implementation screenshot: `/Users/sardo/.codex/visualizations/2026/07/17/019f7188-762d-7c80-a1e6-026bcd92275e/askbar-implementation-mobile-open.png`
- Full-view comparison: `/Users/sardo/.codex/visualizations/2026/07/17/019f7188-762d-7c80-a1e6-026bcd92275e/askbar-side-by-side.png`
- Focused component comparison: `/Users/sardo/.codex/visualizations/2026/07/17/019f7188-762d-7c80-a1e6-026bcd92275e/askbar-focused-side-by-side.png`
- Viewport: 1440 x 1024 desktop; 390 x 844 responsive check
- State: light theme, active Type/Privileged/State chips, grouped filter chooser open

## Findings

No actionable P0, P1, or P2 differences remain within the Askbar implementation scope.

- Fonts and typography: the implementation uses the product's existing UI font stack, weights, and compact type scale. Search, chips, hint tokens, category labels, and previews preserve the source hierarchy without clipping.
- Spacing and layout rhythm: the two-tier composer, chip row, hint rail, 32rem chooser, 48px category rows, radii, and light elevation closely match the selected design. Desktop and 390px layouts remain usable without horizontal overflow.
- Colors and visual tokens: background, border, muted text, focus ring, amber warning chip, and neutral chip colors use existing semantic tokens and preserve light/dark compatibility.
- Image quality and assets: the target contains no new raster imagery. Existing brand imagery is preserved, and all interface icons come from the product's existing icon component set; no placeholder, CSS-drawn, emoji, or ad hoc SVG assets were introduced.
- Copy and content: the implementation keeps the selected search placeholder, `Try` grammar examples, and guided filter labels. Current-product fields replace demo-only fields where the live query model differs.
- Accessibility and behavior: native inputs/buttons remain labeled; the chooser exposes dialog/listbox/option semantics; selected state is announced; Escape, arrows, Home/End, Enter, Tab, close/back actions, outside click, chip removal, and `/` search focus are supported. Touch targets expand at mobile widths.

## Accepted Deviations

- The selected mock shows `⌘ K` inside the Askbar. Open SSPM already reserves `⌘ K` / `Ctrl+K` for its global command palette, so the implemented Askbar truthfully advertises `/` for page-search focus instead of creating a conflicting shortcut.
- The screenshot's surrounding stats and populated demo table are not part of the Askbar change. Their existing project placement and live data behavior were preserved. Component comparison was therefore the acceptance target; the empty QA database only affects content below the component.
- Filter value search is revealed after choosing a category. This keeps the initial chooser faithful to the compact mock while retaining fast server-backed search for long value lists.

## Comparison History

1. Initial comparison found one P2 fidelity issue: the always-visible filter search field made the chooser materially taller than the selected compact category menu.
2. Fix: hide value search in the category view, focus the first category for immediate keyboard use, and reveal the search field only after a category is selected. Escape returns to categories before closing.
3. Post-fix evidence: `askbar-focused-side-by-side.png` shows matching compact menu height, row rhythm, hierarchy, and anchoring. No P0/P1/P2 findings remain.

## Primary Interactions Tested

- Open/close and toggle the filter chooser.
- Navigate categories with arrow keys and select with Enter.
- Load category values from the server, search a value, and apply it with Enter.
- Confirm canonical hidden inputs, HTMX refresh, pushed URL state, and browser-history restoration.
- Remove chips and clear free text while preserving focus.
- Focus page search with `/` without stealing keystrokes from text-entry controls.
- Verify desktop and mobile open/closed states.
- Browser console checked: no warnings or errors.

## Follow-up Polish

- P3: category icons intentionally reuse the closest icons already present in the product set; a future icon-library expansion could add a clock and lock closer to the concept art.

## Branch Review — October 11, 2026

- Disabled browser autocomplete on the main Askbar search input; the filter value input also disables autocomplete.
- Fixed suggestion request races and cancellation when closing or returning to categories, keyboard focus, and active option announcements.
- Kept hint links tied to the applied query and refreshed their links and selected state after HTMX result swaps.
- Fixed Askbar initialization after cached browser history restoration. Back/Forward, Filter, Clear, Enter submission, category navigation, and value selection were checked in the browser with no console warnings or errors.
- Corrected identity hints to the supported `privileged` and `seen:stale` grammar.
- Verification passed: Go tests without optional database variables, `go vet`, all 139 JavaScript tests across 18 files, CSS build, documentation build, Helm lint/template, and `git diff --check`. Templates were regenerated with `just templ`.
- The optional database-enabled suite has existing failures: a clean snapshot of branch HEAD reproduced 56 failing top-level tests across 10 packages. The final HTTP handler suite has the same 37 failing tests/subtests as HEAD and no new failures; both updated hint-state integration tests pass.
- Browser checks used a disposable local database. No developer database was modified.
- Current review screenshot: `/Users/sardo/.codex/visualizations/2026/10/11/01a12a2d-4abe-7692-bd7a-7e784a6fd193/askbar-review.jpg`.

Askbar review passed; the existing database suite failures remain outside this change.
