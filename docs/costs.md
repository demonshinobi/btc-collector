# Costs (Render) — btc-collector

As of 2026-02-08, the minimal always-on setup for Phase 1 looks like:

- Web service: Render **Starter**: **$7/month**
- Database: Render Postgres **Basic-256mb**: **$6/month**
- Database storage: **$0.30/GB-month** (choose a size you’re comfortable keeping, because it can’t be decreased)

Approx formula:
```
monthly ~= 7 + 6 + (0.30 * storage_gb)
```

Examples:
- 1GB: ~$13.30/mo
- 5GB: ~$14.50/mo
- 10GB: ~$16.00/mo

Other considerations:
- Outbound bandwidth is included up to the plan’s allowance; expect overage costs if you start exporting large datasets frequently.
- Keeping Gunicorn at `--workers 1` is both a correctness constraint (single collector) and a cost constraint.

Prices/plans change. Treat this as a working estimate and re-check Render pricing before committing long-term.

