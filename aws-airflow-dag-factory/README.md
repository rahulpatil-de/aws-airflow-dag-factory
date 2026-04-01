# ⚙️ AWS Airflow DAG Factory

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange?logo=amazonaws)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apacheairflow)
![AWS MWAA](https://img.shields.io/badge/AWS-MWAA-orange?logo=amazonaws)
![AWS S3](https://img.shields.io/badge/AWS-S3-569A31?logo=amazons3)

> **Create production-ready Apache Airflow DAGs by dropping a single YAML file into S3 — no Python coding required.**

---

## 📌 What This Project Does

Managing hundreds of Airflow DAGs is a maintenance nightmare when each one is hand-coded.  
This project solves that with a **DAG Factory** pattern:

1. You describe a DAG in a simple **YAML file** (tasks, dependencies, schedule, email config)
2. You upload the YAML to a designated **S3 prefix**
3. An **AWS Lambda** function is triggered automatically
4. Lambda renders a full **Airflow DAG Python file** using a Jinja2 template
5. The rendered `.py` file is written to the **MWAA dags/ folder in S3**
6. **AWS MWAA** picks up the new DAG within **2–5 minutes** — no deployment needed

This approach was used to automate the creation of **500+ production DAGs** across DEV, TEST, and PROD environments.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Developer / Data Engineer                     │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  1. Upload sample_dag.yml
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          Amazon S3                                   │
│                                                                      │
│   s3://<bucket>/dag_configs/sample_dag.yml   ◄── YAML dropped here  │
│   s3://<bucket>/dags/sample_dag.py           ◄── Generated DAG here │
│   s3://<bucket>/modules/utils.py             ◄── Shared utilities   │
│   s3://<bucket>/modules/job_runner.py        ◄── Redshift job exec  │
│   s3://<bucket>/modules/aws_actions.py       ◄── S3 / Glue actions  │
└───────────┬─────────────────────────────────────────────────────────┘
            │  2. S3 PUT Event triggers Lambda
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       AWS Lambda                                     │
│                    (dag_generator.py)                                │
│                                                                      │
│   • Reads YAML config from S3                                        │
│   • Renders Jinja2 DAG template                                      │
│   • Writes .py DAG file back to S3 dags/ prefix                     │
└───────────┬─────────────────────────────────────────────────────────┘
            │  3. DAG .py written to S3 dags/ prefix
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Amazon MWAA (Managed Airflow)                     │
│                                                                      │
│   • Polls S3 dags/ folder every ~30 seconds                         │
│   • New DAG appears on Airflow UI within 2–5 minutes                │
│   • DAG runs use modules dynamically loaded from S3 at runtime      │
└─────────────────────────────────────────────────────────────────────┘
            │
            │  At DAG runtime, tasks use:
            ├──► AWS Glue Crawler    (catalog refresh)
            ├──► Amazon Redshift     (job_runner.py via job_step tasks)
            ├──► Amazon SNS / SES    (email notifications)
            └──► Amazon S3           (file operations via aws_actions.py)
```

---

## 📁 Repository Structure

```
aws-airflow-dag-factory/
│
├── lambda/
│   └── dag_generator.py        # Lambda handler — reads YAML, renders & uploads DAG
│
├── airflow/
│   ├── utils.py                # Shared utilities: email, task tracking, S3 loading
│   ├── job_runner.py           # Redshift job execution engine (JobRunner class)
│   └── aws_actions.py          # S3 file operations + Glue crawler trigger
│
├── dag_configs/
│   └── sample_vendor_dag.yml   # Sample YAML — copy & customize for each new DAG
│
├── docs/
│   └── yaml_reference.md       # Full YAML schema with all task types documented
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- AWS account with MWAA environment running
- S3 bucket configured as the MWAA dags source
- Lambda execution role with `s3:GetObject` and `s3:PutObject` permissions
- The following Airflow Variables set in MWAA:

| Variable | Description |
|---|---|
| `S3_BUCKET` | Shared S3 bucket for configs, dags, and modules |
| `S3_UTILS_KEY` | S3 key for `utils.py` (e.g. `modules/utils.py`) |
| `S3_JOB_RUNNER_KEY` | S3 key for `job_runner.py` |
| `S3_AWS_ACTIONS_KEY` | S3 key for `aws_actions.py` |
| `REDSHIFT_CONN_ID` | Airflow connection ID for Redshift |
| `SSH_CONN_ID` | Airflow connection ID for remote SSH server |
| `AWS_CONN_ID` | Airflow AWS connection ID (default: `aws_default`) |
| `ENVIRONMENT` | Deployment environment (`Dev`, `Test`, `Prod`) |
| `REDSHIFT_CLUSTER` | Redshift cluster identifier |
| `AIRFLOW_WEBSERVER_BASE_URL` | MWAA webserver URL (for log links in emails) |

### Step 1 — Deploy the Lambda

```bash
cd lambda/
zip dag_generator.zip dag_generator.py
aws lambda create-function \
  --function-name airflow-dag-generator \
  --runtime python3.11 \
  --handler dag_generator.lambda_handler \
  --zip-file fileb://dag_generator.zip \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<LAMBDA_ROLE>
```

### Step 2 — Add S3 Trigger to Lambda

Configure the Lambda trigger in the AWS Console:
- **Event type:** `PUT`
- **Prefix:** `dag_configs/`
- **Suffix:** `.yml`

### Step 3 — Upload Utility Modules to S3

```bash
aws s3 cp airflow/utils.py       s3://<YOUR_BUCKET>/modules/utils.py
aws s3 cp airflow/job_runner.py  s3://<YOUR_BUCKET>/modules/job_runner.py
aws s3 cp airflow/aws_actions.py s3://<YOUR_BUCKET>/modules/aws_actions.py
```

### Step 4 — Create Your First DAG

Copy the sample YAML, edit it, and upload:

```bash
cp dag_configs/sample_vendor_dag.yml dag_configs/my_new_dag.yml
# Edit dag_name, tasks, schedule, email recipients...

aws s3 cp dag_configs/my_new_dag.yml \
  s3://<YOUR_BUCKET>/dag_configs/my_new_dag.yml
```

Your DAG will appear on the MWAA Airflow UI within **2–5 minutes**. ✅

---

## 📝 YAML Schema — Supported Task Types

### `empty` — Start/End marker task

```yaml
- task_id: "start"
  type: "empty"
```

### `job_step` — Execute a Redshift job via job_runner

```yaml
- task_id: "js_load_staging_data"
  type: "job_step"
  job_name: "PRE_STG_YOUR_JOB_NAME"
  verbose: 0   # 0=INFO, 1=DEBUG, 2=TRACE
```

### `call_crawler` — Trigger an AWS Glue Crawler

```yaml
- task_id: "refresh_data_catalog"
  type: "call_crawler"
  crawler_name: "your_glue_crawler_name"
  wait_for_completion: true   # optional, default true
```

### `s3_actions` — Move, copy, or delete S3 files

```yaml
- task_id: "archive_processed_files"
  type: "s3_actions"
  sub_type: "move_files"           # move_files | copy_files | remove_files
  source_bucket: "your-bucket"
  source_key: "incoming/data/"
  file_pattern: ".csv"
  destination_bucket: "your-bucket"
  destination_key: "archive/data/"
  include_timestamp: true
```

### `final_email` — HTML status email (always runs, success or fail)

```yaml
- task_id: "send_email_notification"
  type: "final_email"
  subject_prefix: "MY PIPELINE STATUS"
  from_email: "pipeline-status@yourcompany.com"
  success_recipients: "data-team@yourcompany.com"
  failure_recipients: "data-team@yourcompany.com"
  include_validation_results: false
  custom_message: |
    <p>Pipeline completed successfully.</p>
  show_custom_only_on_success: true
```

### `asset_producer` — Airflow 2.9+ Data Asset producer

```yaml
- task_id: "produce_daily_asset"
  type: "asset_producer"
  asset_uri: "s3://your-bucket/assets/daily_data.done"
  outlets:
    - daily_data_asset
```

### Dependencies — Linear or parallel

```yaml
# Linear chain
dependencies:
  - start
  - task_a
  - task_b
  - send_email_notification

# Parallel branch (task_b and task_c run simultaneously after task_a)
dependencies:
  - start
  - task_a
  - [task_b, task_c]   # parallel
  - task_d
  - send_email_notification
```

---

## 🔑 Key Design Decisions

| Decision | Reason |
|---|---|
| YAML-driven DAG creation | Allows non-Python users (analysts, ops) to create DAGs safely |
| Jinja2 template in Lambda | Single source of truth for DAG structure — update once, all future DAGs benefit |
| Modules loaded from S3 at runtime | Update `utils.py` or `job_runner.py` in S3 without redeploying any DAG |
| `trigger_rule='all_done'` on email task | Email always fires regardless of upstream success or failure |
| XCom-based task state collection | Works reliably across MWAA 2.x and 3.x versions |

---

## 🛠️ Technologies Used

| Layer | Technology |
|---|---|
| Workflow Orchestration | Apache Airflow (AWS MWAA) |
| DAG Generation | AWS Lambda + Jinja2 |
| Config Storage | Amazon S3 |
| Data Catalog | AWS Glue Crawler |
| Data Warehouse | Amazon Redshift |
| Notifications | Amazon SES / SMTP via Airflow |
| File Operations | Amazon S3 (boto3) |
| Language | Python 3.11 |

---

## 📄 License

MIT License — free to use, fork, and adapt.
