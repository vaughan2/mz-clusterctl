# GitHub Actions Setup

## Periodic mz-clusterctl Execution

The `periodic-clusterctl.yml` workflow runs mz-clusterctl against your
Materialize Cloud environment on a schedule.

### Required Secrets

Set these secrets in your GitHub repository settings:

- `MATERIALIZE_DATABASE_URL`: PostgreSQL connection string for your Materialize Cloud instance
  - Format: `postgresql://user:password@host:port/database`
  - Example: `postgresql://mz_user:password@cluster.materialize.cloud:6875/materialize`

### Configuration

- **Schedule**: Currently set to run every 5 minutes (`*/5 * * * *`)
- **Manual Trigger**: Can be triggered manually via GitHub Actions UI
- **Execution**: Runs both dry-run and apply modes sequentially

### Customization

You can modify the workflow to:
- Change the schedule frequency
- Add environment-specific configurations
- Include additional validation steps
- Add notifications on failure

### Security Considerations

- Database credentials are stored as GitHub secrets
- No persistent state is maintained between runs
