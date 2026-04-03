# Reprocessing a Country

Full reprocess: wipe git history + re-fetch + re-commit. Use when the pipeline has changed in ways that affect output (frontmatter fields, markdown rendering, metadata parsing, rank mappings, status logic).

## Prerequisites

- The country's data source must be accessible (API online, XML dump available)
- You must be in the `engine/` directory (for `config.yaml`)
- `legalize` CLI installed: `python3 -m pip install -e ".[dev]"`
- Prevent system sleep if leaving unattended

## Step 1: Preserve repo files

Before wiping git history, save any non-generated files (README, LICENSE, .gitignore) that live in the country repo. The git reset will delete them.

```bash
cd ../countries/{code}
cp README.md LICENSE .gitignore /tmp/legalize-backup-{code}/
```

## Step 2: Change default branch (if needed)

If GitHub's default branch is `master` instead of `main`, fix it first:

```bash
gh api repos/legalize-dev/legalize-{code} -X PATCH -f default_branch=main
git push origin --delete master  # only if master exists
```

## Step 3: Wipe remote and local git history

```bash
cd ../countries/{code}

# Save remote URL
git remote -v

# Wipe local
rm -rf .git
git init -b main
git remote add origin git@github.com:legalize-dev/legalize-{code}.git

# Create empty root and force push
git commit --allow-empty -m "reset: prepare for full reprocess"
git push --force origin main
```

**What is preserved on GitHub:** issues, PRs, stars, settings, webhooks.
**What is lost:** all commit history, tags, releases (verify with `git tag -l` and `gh release list` before proceeding).

## Step 4: Re-fetch all data

This re-downloads metadata + text from the source API and regenerates all JSON files in `data-{code}/json/`. Required when the parser has changed (new fields, bug fixes).

```bash
cd ~/autonomo/legalize/engine
legalize fetch -c {code} --catalog --force 2>&1 | tee fetch-{code}-reprocess.log
```

**Spain**: ~8,600 norms, ~3.5 hours at 2 req/s.
**France**: uses local LEGI dump, add `--legi-dir /path`.
**Sweden**: `legalize fetch -c se --all --force`.

If the parser has NOT changed and you only need to regenerate commits (e.g., frontmatter rendering change), skip this step — the existing JSONs are fine.

## Step 5: Generate commits

```bash
legalize commit -c {code} --all 2>&1 | tee commit-{code}-reprocess.log
```

This creates one commit per reform per law, with historical dates. Spain generates ~42k commits.

## Step 6: Restore repo files and push

```bash
cd ../countries/{code}

# Restore saved files
cp /tmp/legalize-backup-{code}/README.md .
cp /tmp/legalize-backup-{code}/LICENSE .
cp /tmp/legalize-backup-{code}/.gitignore .

# Commit and push everything
git add README.md LICENSE .gitignore
git commit -m "chore: add README, LICENSE, and .gitignore"
git push origin main
```

## One-liner (unattended)

After steps 1-3, run fetch + commit + catchup + push in sequence:

```bash
cd ~/autonomo/legalize/engine && \
  legalize fetch -c {code} --catalog --force 2>&1 | tee fetch-{code}-reprocess.log && \
  legalize commit -c {code} --all 2>&1 | tee commit-{code}-reprocess.log && \
  while legalize daily -c {code} 2>&1 | tee -a daily-{code}-catchup.log | grep -qv "Nothing to process"; do :; done && \
  cd ../countries/{code} && \
  cp /tmp/legalize-backup-{code}/README.md /tmp/legalize-backup-{code}/LICENSE /tmp/legalize-backup-{code}/.gitignore . && \
  git add README.md LICENSE .gitignore && \
  git commit -m "chore: add README, LICENSE, and .gitignore" && \
  git push origin main
```

## When to reprocess

| Change | Re-fetch needed? | Reprocess needed? |
|---|---|---|
| Frontmatter fields added/changed | No | Yes |
| Markdown rendering changed | No | Yes |
| Metadata parser changed (ranks, status, new fields) | Yes | Yes |
| Commit message format changed | No | Yes |
| Bug in fetcher/parser fixed | Yes | Yes |
| New norms added to source | No (use `daily` instead) | No |

## Step 7: Catch up to today

The bootstrap processes data up to the dump/fetch date. Run the daily incrementally to cover the gap between that date and today.

Set the state to the dump date (so the daily knows where to start), then loop until caught up:

```bash
cd ~/autonomo/legalize/engine

# Set state to the dump/fetch date
python3 -c "
from legalize.state.store import StateStore
from datetime import date
import os
cc_state = '../countries/data-{code}/state/state.json'
os.makedirs(os.path.dirname(cc_state), exist_ok=True)
state = StateStore(cc_state)
state.load()
state.last_summary_date = date(YYYY, M, D)  # dump/fetch date
state.save()
"

# Run daily in a loop (safety cap = 10 days per iteration)
while true; do
  output=$(legalize daily -c {code} 2>&1)
  echo "$output" | tail -3
  if echo "$output" | grep -q "Nothing to process"; then
    break
  fi
done
```

For France, the dump date is the Freemium tar.gz date. For API-based countries (ES, AT, SE), it's the date the fetch completed.

## Step 8: Push final state

```bash
cd ../countries/{code}
git push origin main
```

## Verify

After reprocessing:

```bash
cd ../countries/{code}
git log --oneline | wc -l          # expected commit count
git log --oneline | head -5        # latest commits look right
git log --oneline | tail -5        # oldest commits look right
legalize health -c {code}          # repo health check
```
