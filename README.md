# QuantWhisper

QuantWhisper is a GitHub Pages dashboard for the EXP-0004 virtual portfolio.

## What it includes
- Daily virtual portfolio dashboard
- Net value vs benchmark chart
- Monthly returns table
- Latest holdings snapshot
- Comparison metrics summary
- GitHub Actions deployment to `gh-pages`

## Data source
The current site is built from the prepared outputs under:

- `project/reports/EXP-0004`

## Local build
```bash
python scripts/prepare_site.py
cp index.html site/index.html
```

## Deployment flow
- Push to `main`
- GitHub Actions builds `site/`
- Pages are deployed to `gh-pages`

## Notes
This repo is a static dashboard only. If you want it to fetch market data and run a fresh backtest every day, the workflow can be extended with data-fetch steps and a simulation script.
