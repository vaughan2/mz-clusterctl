# GitHub Actions Setup

## Periodic mz-clusterctl Execution

We provide an example `periodic-clusterctl.yml` workflow that runs
mz-clusterctl against your Materialize Cloud environment on a schedule. It uses
our Composite GitHub Action.

### Setup

To set up the GitHub Actions workflow place the `periodic-clusterctl.yml` file
in the `.github/workflows/` directory of your GitHub repository.

For example:
```bash
mkdir -p .github/workflows
cp periodic-clusterctl.yml .github/workflows/
```

### Required Secrets

Set these secrets in your GitHub repository settings:

- `MATERIALIZE_DATABASE_URL`: PostgreSQL connection string for your Materialize Cloud instance
  - Format: `postgresql://user:password@host:port/database`
  - Example: `postgresql://mz_user:password@cluster.materialize.cloud:6875/materialize`

### Configuration

- **Schedule**: Currently set to run every 5 minutes (`*/5 * * * *`)
- **Manual Trigger**: Can be triggered manually via GitHub Actions UI

### Security Considerations

- Database credentials are stored as GitHub secrets
- No persistent state is maintained between runs
