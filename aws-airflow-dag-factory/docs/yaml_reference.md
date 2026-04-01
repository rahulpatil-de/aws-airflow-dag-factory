# YAML DAG Config — Full Reference

This document describes every field supported in the DAG YAML configuration files.

---

## Top-Level Fields

| Field | Required | Description |
|---|---|---|
| `dag_name` | ✅ | Unique DAG ID (must be a valid Python identifier) |
| `description` | ✅ | Short description shown on the Airflow UI |
| `timezone` | ✅ | Timezone string e.g. `"US/Eastern"`, `"UTC"` |
| `dag_config` | ✅ | Runtime settings (see below) |
| `default_args` | ✅ | Standard Airflow default_args (see below) |
| `schedule_interval` | ✅ | Cron string or `null` for manual trigger |
| `tasks` | ✅ | List of task definitions (see Task Types) |
| `dependencies` | ✅ | Execution order — list of task_ids or nested lists for parallel branches |
| `assets` | ❌ | Airflow 2.9+ Data Asset definitions |
| `asset_schedule` | ❌ | Schedule DAG on asset availability |

---

## `dag_config`

```yaml
dag_config:
  execution_timeout_minutes: 120   # Max DAG run time before timeout
  max_active_runs: 1               # Concurrent DAG runs allowed
  doc_md: |                        # Markdown shown on DAG detail page
    ## My DAG
    Description here.
```

---

## `default_args`

```yaml
default_args:
  owner: "airflow"
  depends_on_past: false
  start_date: "2024-01-01"
  email_on_failure: true
  email_on_retry: false
  retries: 1
  retry_delay_minutes: 5
  tags:
    - data_engineering
    - my_team
```

---

## Dependencies — Linear vs Parallel

```yaml
# Linear chain — each task waits for the previous
dependencies:
  - start
  - task_a
  - task_b
  - task_c

# Parallel fan-out — task_b and task_c run simultaneously
dependencies:
  - start
  - task_a
  - [task_b, task_c]
  - task_d

# Multiple parallel groups
dependencies:
  - start
  - [task_a, task_b]
  - [task_c, task_d]
  - end_task
```

---

## Task Types

### `empty`
Lightweight start/end marker. No computation.
```yaml
- task_id: "start"
  type: "empty"
```

### `job_step`
Executes a named job via `job_runner.py`. Connects to Redshift internally.
```yaml
- task_id: "js_load_data"
  type: "job_step"
  job_name: "YOUR_JOB_NAME"    # Must match a registered job in job_runner
  verbose: 0                   # 0=INFO | 1=DEBUG | 2=TRACE
```

### `call_crawler`
Starts an AWS Glue Crawler and optionally waits for completion.
```yaml
- task_id: "refresh_catalog"
  type: "call_crawler"
  crawler_name: "your_crawler_name"
  wait_for_completion: true      # optional, default: true
  aws_conn_id: "aws_default"     # optional
```

### `s3_actions`
Perform file operations on S3.
```yaml
- task_id: "move_files_to_archive"
  type: "s3_actions"
  sub_type: "move_files"           # move_files | copy_files | remove_files
  source_bucket: "your-bucket"
  source_key: "raw/data/"
  file_pattern: ".csv"             # string match against object key
  destination_bucket: "your-bucket"
  destination_key: "archive/data/"
  include_timestamp: true          # optional, appends _YYYYMMDD_HHMMSS
  delete_source: true              # optional, default true for move_files
```

Dynamic date in keys: use `eff_date=current_date` — it will be replaced with today's date at runtime.

### `create_s3_trigger_file`
Creates an empty trigger/marker file at a given S3 path.
```yaml
- task_id: "create_trigger"
  type: "create_s3_trigger_file"
  bucket: "your-bucket"
  key: "triggers/eff_date=current_date/"
  file_name: "trigger.done"
```

### `check_validation_view`
Runs a validation query against a Redshift view and fails the task if results indicate errors.
```yaml
- task_id: "validate_loaded_data"
  type: "check_validation_view"
  view_name: "vw_your_validation_view"
  xcom_prefix: "validation_result"
  date_column: "load_date"           # optional
```

### `asset_producer`
Produces an Airflow 2.9+ Data Asset, allowing downstream DAGs to trigger on it.
```yaml
- task_id: "produce_asset"
  type: "asset_producer"
  asset_uri: "s3://your-bucket/assets/my_asset.done"
  outlets:
    - my_asset_variable_name
```
Pair with a top-level `assets:` block defining the Asset objects.

### `remote_command` / `remote_sh_script` / `remote_py_script`
Execute a command or script on a remote server via SSH.
```yaml
- task_id: "run_remote_script"
  type: "remote_sh_script"
  command: "/path/to/your/script.sh --param value"
```

### `final_email`
Sends an HTML summary email after all upstream tasks complete (uses `trigger_rule='all_done'`).
Always place this as the last task in `dependencies`.
```yaml
- task_id: "send_email_notification"
  type: "final_email"
  subject_prefix: "PIPELINE STATUS"
  from_email: "pipeline@yourcompany.com"
  success_recipients: "team@yourcompany.com"
  failure_recipients: "team@yourcompany.com"
  include_validation_results: false
  validation_xcom_keys: []           # optional: list of XCom keys to include
  custom_message: |
    <p>Pipeline completed successfully.</p>
  show_custom_only_on_success: true  # custom_message only shown on success
```

---

## Asset Scheduling (Airflow 2.9+)

Trigger a DAG when one or more Data Assets are updated:

```yaml
# AND condition — trigger when ALL listed assets are updated
asset_schedule:
  condition: "AND"
  assets:
    - asset_one
    - asset_two

# OR condition — trigger when ANY asset is updated
asset_schedule:
  condition: "OR"
  assets:
    - asset_one
    - asset_two
```

Must also define the assets at the top level:

```yaml
assets:
  - name: asset_one
    uri: "s3://your-bucket/assets/asset_one.done"
  - name: asset_two
    uri: "s3://your-bucket/assets/asset_two.done"
```
