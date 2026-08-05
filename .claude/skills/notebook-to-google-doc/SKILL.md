---
name: notebook-to-google-doc
description: Turn a finished analysis notebook into a Google Doc write-up with the standard Summary / Background / Findings format. Use when the user asks to write up a notebook, create a doc from an analysis, share findings as a Google Doc, or turn notebook results into a document for a wider audience.
---

# Notebook → Google Doc write-up

Converts a `recidiviz/research/notebooks/` analysis into a Google Doc. The notebook is the source
of truth; this skill is formatting and transport, not new analysis.

Reference output: [Release notes: who has one, and when](https://docs.google.com/document/d/11oEywtcpV4ro4zqDLMIXjJtiM9PT5zv3lj-kX4zFu8M/edit)

## Format

```
# <Title — the subject, not a finding>

## Summary
  One paragraph. The conclusion and the two or three numbers that carry it.

## Background
  What the signal/metric actually is, and what one row represents.

## Findings
  ### 1. <The finding, stated with its numbers>
     Lead paragraph — the numbers, key figure in bold.
     Optional second paragraph — what it means, caveats.
     Optional italic caption + table.
     Optional plot.
  ### 2. …
```

### Summary

One paragraph, no table, no bullets. State the conclusion first, then the two or three numbers
that carry it, then the "so what". A reader who stops here should have the answer.

### Background

Only what a reader needs to interpret the findings: what the signal or metric is (including the
raw fields or contact modes it comes from, in bold), and what one row represents. Two or three
sentences.

Do **not** add a separate scope or methodology paragraph. Scope belongs inline where it is used —
"released … during 2025" in the finding text, `*Snapshot 2026-07-30*` as a table caption. State
the population restriction in the Summary or the relevant finding, not as its own section.

### Findings

- One `###` per finding, numbered from 1, under a single `## Findings` header.
- **The heading is the finding**, stated with its numbers — not a topic label. `### 1. Only a
  third of people released from Idaho prisons in 2025 had a release district note`, not
  `### 1. Coverage`.
- Lead paragraph carries the numbers, with the headline figure in `<b>`. Add a second paragraph
  only when the interpretation or a caveat is worth the space — several findings need just one.
- Tables get an italic caption above them naming the population and period.
- Order findings so they build: the headline rate, then whether it moved, then why, then the
  current-population consequence.

Notebook `### 2.x` finding headings map to doc `### N.` findings, renumbered from 1. They will
often be reworded — the notebook heading is terse and self-contained, the doc heading reads as
prose.

## Sourcing the content

**Every number comes from the notebook's stored cell outputs. Never recompute and never carry a
number over from an earlier run of the notebook.**

Read them out of the `.ipynb` JSON:

```python
import json
nb = json.load(open(NB_PATH))
def out(i):
    return "".join(
        "".join(o.get("text") or (o.get("data") or {}).get("text/plain") or "")
        for o in nb["cells"][i].get("outputs", [])
    )
```

- Confirm the notebook has been executed under **one** analysis window, and that its own headings
  agree with its outputs. If the window changed and the headings are stale, fix the notebook
  first — a doc built from a half-updated notebook will contradict itself.
- Read `ANALYSIS_START_DATE` / `ANALYSIS_END_DATE_EXCLUSIVE` out of the setup cell and check every
  period you write against them.
- If the analysis mixes a date-range cohort with a current snapshot, say which is which where each
  appears; they are on different clocks.

## Figures

Notebooks don't write PNGs to disk — the rendered figure lives only in the cell's stored output.
Pull it from there, so the image matches the notebook exactly, hand-edited titles included, with
nothing re-executed. Identify each figure by its `plot_settings(title=...)`, since that is what the
doc's paste marker has to name:

```python
import base64, json, re
nb = json.load(open(NB_PATH))
for i, cell in enumerate(nb["cells"]):
    pngs = [o["data"]["image/png"] for o in cell.get("outputs", [])
            if "image/png" in (o.get("data") or {})]
    if not pngs:
        continue
    title = re.search(r'title="([^"]+)"', "".join(cell["source"])).group(1)
    print(i, title)                       # map cell -> doc section, then:
    open(f"{OUT_DIR}/{name}.png", "wb").write(base64.b64decode(pngs[0]))
```

Write them **outside the repo**, to a scratch dir or `~/Downloads/<analysis>_figures/`, named for
the doc section they belong to (`section2_monthly_trend.png`), and hand them over with
`SendUserFile`. These files exist only to be pasted into the doc — they are not an artifact of the
notebook and should not be committed.

**Default to a text-only doc with paste markers** and let the user drop the images in:

```html
<p style="border:1px dashed #999;padding:10px;color:#666;background-color:#f5f5f5">
[ paste plot here &mdash; <b>Monthly prison releases with a release note</b>
&nbsp;&middot;&nbsp; section2_monthly_trend.png &nbsp;&middot;&nbsp; covers calendar 2025 ]</p>
```

Base64 `data:image/png` URIs *do* survive Drive's HTML→Doc conversion, so images can be embedded —
but the payload has to pass through the tool call inline, which for four charts is tens of
thousands of tokens. Only embed if the user asks for a doc that needs no manual step.

## Creating the doc

Build the HTML in the scratchpad with a small script, then publish. Do not hand-write a long HTML
string — generate it so the markup is consistent and checkable.

```python
mcp__claude_ai_Google_Drive__create_file(
    title="<doc title>",
    contentMimeType="text/html",
    textContent=html,   # Drive converts HTML -> a native Google Doc
)
```

Markup that converts correctly:

| want | use |
|---|---|
| headings | `<h1>`, `<h2>`, `<h3>` |
| paragraph | `<p>…</p>` — every paragraph, no bare text |
| bold / italic | `<b>`, `<i>` (works inside table cells too) |
| dashes, quotes | `&mdash;`, `&ndash;`, `&ldquo;`, `&rsquo;`, `&hellip;` |
| table | `<table style="border-collapse:collapse">` with **inline** per-cell styles |

CSS classes and `<style>` blocks are dropped, so borders must be inline on every cell:

```python
TH = ' style="border:1px solid #999;padding:4px 8px;background-color:#eeeeee"'
TD = ' style="border:1px solid #999;padding:4px 8px"'
```

Assert the markup is balanced before publishing — an unclosed `<p>` silently swallows a paragraph:

```python
assert len(re.findall(r"<p[ >]", html)) == html.count("</p>"), "unbalanced <p> tags"
```

## Verify, then report

- Read the doc back with `mcp__claude_ai_Google_Drive__read_file_content` and check the headings,
  every table and every paste marker survived. For a bold-inside-table check, `download_file_content`
  with `exportMimeType="text/plain"` is more faithful than the markdown-ish read view.
- Report the doc link, and state the window and population the numbers came from so the user can
  sanity-check the framing.
- Flag anything the notebook can't support at its current window rather than quietly narrowing a
  claim — e.g. a single-year trend cannot establish that nothing changed historically.

## Gotchas

- **There is no Drive delete tool.** Every `create_file` is permanent from this session's point of
  view, so don't create throwaway test docs — and each revision means a new doc plus a stale one
  the user has to clean up. Get the content right before publishing.
- **`create_file` cannot update an existing doc.** Revising means publishing a new one; say so and
  give both links.
- Don't put the notebook path in the doc body unless asked — it means nothing to most readers.
