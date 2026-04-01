# QuantWhisper

QuantWhisper is a GitHub Pages dashboard for the EXP-0004 virtual portfolio.

## What it includes
- Daily virtual portfolio dashboard
- Net value vs benchmark chart (extended to the latest available date)
- Monthly returns table
- Latest holdings snapshot
- Latest rebalance Top 10 with price/volume/amount
- Comparison metrics summary
- GitHub Actions deployment to `gh-pages`
- Optional Telegram topic report
- Latest行情 snapshot with AkShare primary + repo fallback snapshot

## Live site
- Pages: https://lzq1206.github.io/QuantWhisper/
- Repo: https://github.com/lzq1206/QuantWhisper

## How the automation works
The workflow in `.github/workflows/deploy.yml` does four things:
1. Optionally syncs a source snapshot if `QUANTWHISPER_SOURCE_DIR` is configured as a secret.
2. Fetches the latest market snapshot using AkShare first, then falls back to a committed repo snapshot.
3. Rebuilds the static dashboard into `site/`.
4. Deploys the result to the `gh-pages` branch and, if Telegram secrets are present, posts a daily summary.

## Telegram configuration
Set these GitHub repository secrets if you want auto-posting into Telegram:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_THREAD_ID` (for topic 137, use `137`)

## Source snapshot
The current dashboard is built from the prepared outputs under:
- `project/reports/EXP-0004`

If you later want the workflow to refresh from a different source, set:
- `QUANTWHISPER_SOURCE_DIR` for local/manual sync runs
- or extend the workflow to download a remote artifact before building

## Local build
```bash
python scripts/sync_source_snapshot.py
python scripts/prepare_site.py
cp index.html site/index.html
```

## Notes
This repo is a static dashboard. It is now wired for scheduled rebuilds, and can be extended to pull live market data before rendering.
