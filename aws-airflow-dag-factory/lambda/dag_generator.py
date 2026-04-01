"""
dag_generator.py — AWS Lambda Handler
======================================
Triggered by S3 PUT events when a YAML config file is uploaded to the
dag_configs/ prefix. Reads the YAML, renders a fully-formed Airflow DAG
Python file using the embedded Jinja2 template, and writes it to the
MWAA dags/ prefix in S3. MWAA picks up the new file within ~2-5 minutes.

Supported task types in YAML:
  empty | job_step | call_crawler | final_email | asset_producer |
  s3_actions | check_validation_view | remote_command | remote_sh_script

Environment / Airflow Variables required (set in MWAA):
  S3_BUCKET            — S3 bucket shared by configs, dags, and utility modules
  S3_UTILS_KEY         — S3 key for utils.py
  S3_JOB_RUNNER_KEY    — S3 key for job_runner.py
  S3_AWS_ACTIONS_KEY   — S3 key for aws_actions.py

S3 Layout expected:
  s3://<S3_BUCKET>/
    dag_configs/       ← drop YAML files here to trigger DAG creation
    dags/              ← generated DAG .py files land here (MWAA reads this)
    modules/           ← utils.py, job_runner.py, aws_actions.py
"""

import boto3
import yaml
from jinja2 import Template

# === Jinja2 DAG Template ===
DAG_TEMPLATE = """# ===== Related YAML File is {{ yaml_filename }} =====
import sys
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pytz import timezone
from airflow.exceptions import AirflowException
from airflow.models import Variable
import pendulum
from airflow import settings
from airflow.models import TaskInstance
from airflow.utils.session import create_session

# DAG-specific constants
EST = timezone('{{ timezone }}')

# Handle start_date with proper fallback
{% if start_date and start_date != '' %}
start_date_val = datetime.strptime('{{ start_date }}', '%Y-%m-%d').replace(tzinfo=EST)
{%- else -%}
start_date_val = datetime(2024, 1, 1, tzinfo=EST)  # Default fallback
{% endif %}

default_args = {
    'owner': '{{ default_args.owner }}',
    'depends_on_past': {{ default_args.depends_on_past }},
    'start_date': start_date_val,
    'email_on_failure': {{ default_args.email_on_failure }},
    'email_on_retry': {{ default_args.email_on_retry }},
    'retries': {{ default_args.retries }},
    'retry_delay': timedelta(minutes={{ default_args.retry_delay_minutes }}),
    'tags': {{ default_args.tags | tojson }}
}

{# ===== ASSET DEFINITIONS (MODULE LEVEL - FIXED) ===== #}
{% if assets is defined and assets %}
# Asset definitions - MODULE LEVEL (like working example)
{% for asset in assets %}
{{ asset.name }} = Asset(
    name="{{ asset.name }}",
    uri="{{ asset.uri }}"
)
{% endfor -%}
{% endif -%}

{# ===== ASSET SCHEDULE CONFIG ===== #}
{% if asset_schedule is defined and asset_schedule %}
# Asset scheduling configuration
{% if asset_schedule.condition == "AND" %}
# AND condition: DAG triggers when ALL assets are updated
asset_schedule_config = [
    {% for asset_name in asset_schedule.assets %}
    {{ asset_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
]
{% else %}
# OR condition: DAG triggers when ANY asset is updated  
asset_schedule_config = [
    {% for asset_name in asset_schedule.assets %}
    {{ asset_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
]
{% endif -%}
{% endif -%}

def load_module_from_s3(s3_path: str, module_name: str):
    \"\"\"Dynamically load module from S3 with enhanced error handling\"\"\"
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        # Parse S3 path
        s3_path = s3_path.replace("s3://", "")
        bucket_name = s3_path.split('/')[0]
        key = '/'.join(s3_path.split('/')[1:])
        
        logging.info(f"Loading module '{module_name}' from s3://{bucket_name}/{key}")
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        
        if not file_content:
            raise AirflowException(f"Empty file content for {s3_path}")
        
        # Create and execute module
        module = type(sys)(module_name)
        exec(file_content, module.__dict__)
        sys.modules[module_name] = module
        
        logging.info(f"✓ Successfully loaded module: {module_name}")
        return module
        
    except Exception as e:
        logging.error(f"Failed to load module {module_name} from {s3_path}: {str(e)}")
        raise AirflowException(f"Module loading failed: {str(e)}")

def load_utils_functions(utils_module):
    \"\"\"Extract commonly used utility functions from utils module\"\"\"
    utils_functions = {}
    available_functions = [
        'get_redshift_error_details',
        'execute_remote_job',
        'send_email_notification',
        'create_task_with_tracking',
        'execute_job_runner',
        'create_s3_trigger_file',
        'check_validation_view'
    ]
    
    for func_name in available_functions:
        if hasattr(utils_module, func_name):
            utils_functions[func_name] = getattr(utils_module, func_name)
            logging.info(f"✓ Loaded function: {func_name}")
        else:
            logging.warning(f"✗ Function not found: {func_name}")
    
    return utils_functions

# Simple and reliable task state collection using only XCom data
def get_task_states_from_xcom(dag_run, dag, ti, context_kwargs=None):
    try:
        task_states = []
        
        logging.info(f"📊 Collecting task states for DAG run: {dag_run.run_id}")
        
        # Use context_kwargs if provided
        if context_kwargs:
            task_instances_from_context = context_kwargs.get('task_instances', [])
            if task_instances_from_context:
                logging.info("✅ Using task instances from context")
                return task_instances_from_context
        
        # Method 1: Try to get task instances from Airflow database
        try:
            from airflow.utils.session import create_session
            from airflow.models import TaskInstance
            
            with create_session() as session:
                task_instances = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id
                ).all()
                
            logging.info(f"✅ Found {len(task_instances)} task instances from database")
            
            for task_instance in task_instances:
                if task_instance.task_id == 'send_email_notification':
                    continue
                    
                class TaskInfo:
                    def __init__(self, ti_instance, dag_run):
                        self.task_id = ti_instance.task_id
                        self.state = ti_instance.state
                        self.start_date = ti_instance.start_date
                        self.end_date = ti_instance.end_date
                        
                        # Build log URL
                        try:
                            from airflow.models import Variable
                            base_url = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                        except Exception:
                            base_url = ""
                        
                        if base_url:
                            self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={ti_instance.task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                        else:
                            self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={ti_instance.task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                
                task_info = TaskInfo(task_instance, dag_run)
                task_states.append(task_info)
                logging.info(f"📊 Task {task_instance.task_id} - DB State: {task_instance.state}")
            
            return task_states
            
        except Exception as e:
            logging.warning(f"⚠️ Could not get task instances from database: {str(e)}")
        
        # Method 2: Use XCom data (most reliable since we control this)
        logging.info("🔄 Using XCom data for task states")
        for task in dag.tasks:
            if task.task_id == 'send_email_notification':
                continue
                
            xcom_data = ti.xcom_pull(task_ids=task.task_id, key='task_state', include_prior_dates=False)
            
            class XComTaskInfo:
                def __init__(self, task_id, dag_run, xcom_data):
                    self.task_id = task_id
                    
                    if xcom_data:
                        self.state = xcom_data.get('state', 'UNKNOWN')
                        self.start_date = self._safe_parse_date(xcom_data.get('start_date'))
                        self.end_date = self._safe_parse_date(xcom_data.get('end_date'))
                        logging.info(f"📊 Task {task_id} - XCom State: {self.state}")
                    else:
                        self.state = 'UNKNOWN'
                        self.start_date = None
                        self.end_date = None
                        logging.info(f"📊 Task {task_id} - No XCom data")
                    
                    # Build log URL
                    try:
                        from airflow.models import Variable
                        base_url = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                    except Exception:
                        base_url = ""
                    
                    if base_url:
                        self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                    else:
                        self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                
                def _safe_parse_date(self, date_str):
                    if not date_str:
                        return None
                    try:
                        return pendulum.parse(date_str)
                    except Exception:
                        return None
            
            task_info = XComTaskInfo(task.task_id, dag_run, xcom_data)
            task_states.append(task_info)
        
        return task_states
        
    except Exception as e:
        logging.error(f"❌ Failed to get task states: {str(e)}")
        return []

# Load modules at DAG parse time
try:
    # Get S3 paths from Airflow Variables
    s3_bucket = Variable.get("S3_BUCKET")
    s3_utils_key = Variable.get("S3_UTILS_KEY") 
    s3_job_runner_key = Variable.get("S3_JOB_RUNNER_KEY")
    s3_aws_actions_key = Variable.get("S3_AWS_ACTIONS_KEY") 
    
    # Construct full S3 paths
    utils_s3_path = f"s3://{s3_bucket}/{s3_utils_key}"
    job_runner_s3_path = f"s3://{s3_bucket}/{s3_job_runner_key}"
    aws_actions_s3_path = f"s3://{s3_bucket}/{s3_aws_actions_key}"
    
    
    logging.info("=== LOADING MODULES FROM S3 ===")
    logging.info(f"Utils path: {utils_s3_path}")
    logging.info(f"Job Runner path: {job_runner_s3_path}")
    logging.info(f"AWS Actions path: {aws_actions_s3_path}")
    
    
    # Load modules
    utils_module = load_module_from_s3(utils_s3_path, "utils")
    job_runner_module = load_module_from_s3(job_runner_s3_path, "job_runner")
    aws_actions_module = load_module_from_s3(aws_actions_s3_path, "aws_actions")
    
    # VERIFY MODULES ARE USABLE
    logging.info("=== VERIFYING MODULE FUNCTIONALITY ===")
    if not hasattr(job_runner_module, 'main'):
        raise AirflowException("job_runner_module is missing required 'main' function")
    
    if not callable(job_runner_module.main):
        raise AirflowException("job_runner_module.main is not callable")
    
    # Extract utility functions
    utils_functions = load_utils_functions(utils_module)
    
    # VERIFY CRITICAL FUNCTIONS EXIST
    required_functions = ['execute_job_runner', 'create_task_with_tracking']
    for func_name in required_functions:
        if func_name not in utils_functions:
            raise AirflowException(f"Required function '{func_name}' not found in utils")
    
    logging.info("=== MODULES LOADED AND VERIFIED SUCCESSFULLY ===")
    logging.info(f"Available utils functions: {list(utils_functions.keys())}")
    
except Exception as e:
    logging.error(f"❌ DAG initialization failed: {str(e)}")
    # Log detailed error information
    import traceback
    logging.error(f"Initialization traceback: {traceback.format_exc()}")
    raise AirflowException(f"DAG initialization failed: {str(e)}")

@dag(
    dag_id='{{ dag_name }}',
    default_args=default_args,
    description='{{ description }}',
    {# Asset schedule logic - FIXED #}
    {% if asset_schedule is defined and asset_schedule %}
    {% if asset_schedule.condition == "AND" %}
    # AND condition: DAG triggers when ALL assets are updated
    schedule=[{% for asset_name in asset_schedule.assets %}{{ asset_name }}{% if not loop.last %}, {% endif %}{% endfor %}],
    {% else %}
    # OR condition: DAG triggers when ANY asset is updated
    schedule=({% for asset_name in asset_schedule.assets %}{{ asset_name }}{% if not loop.last %} | {% endif %}{% endfor %}),
    {% endif %}
    {%- else %}
    schedule={{ "None" if schedule_interval is none else "'" + schedule_interval + "'" }},
    {%- endif %}
    catchup=False,
    dagrun_timeout=timedelta(minutes={{ dag_config.execution_timeout_minutes }}),
    max_active_runs={{ dag_config.max_active_runs }},
    tags=default_args['tags'],
    doc_md='''{{ dag_config.doc_md }}'''
)
def {{ dag_name }}():
    {%- for task in tasks %}
    {%- if task.type == 'empty' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"Start task with proper timing\"\"\"
        return "{{ task.task_id }} completed successfully"
    {%- elif task.type == 'job_step' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        try:
            # PRE-FLIGHT CHECKS - Force early logging to create CloudWatch stream
            logging.info(f"🚀 Starting {{ task.job_name }} task")
            logging.info(f"📋 Verifying dependencies for {{ task.job_name }}...")
            
            # Test critical dependencies before proceeding
            if not job_runner_module:
                raise AirflowException("job_runner_module is None - module loading failed")
            
            if not hasattr(job_runner_module, 'main'):
                raise AirflowException("job_runner_module missing 'main' method")
            
            if 'execute_job_runner' not in utils_functions:
                raise AirflowException("execute_job_runner function not available")
            
            logging.info("✅ All dependencies verified successfully")
            
            # Now execute the actual job
            utils_functions['execute_job_runner'](
                '{{ task.job_name }}', 
                job_runner_module, 
                utils_functions, 
                {{ task.verbose }},
                task_type='job_step', 
                **kwargs
            )
            
            logging.info(f"✅ {{ task.job_name }} completed successfully")
            
        except Exception as e:
            logging.error(f"❌ {{ task.job_name }} failed: {str(e)}")
            # Force detailed logging to ensure CloudWatch stream is created
            import traceback
            logging.error(f"🔍 Stack trace: {traceback.format_exc()}")
            raise AirflowException(f"{{ task.job_name }} failed: {str(e)}")
    {% elif task.type == 'asset_producer' %}
    {# FIXED: Use the exact same variable names from asset definitions #}
    @task(task_id="{{ task.task_id }}", outlets=[{% for outlet in task.outlets %}{{ outlet }}{% if not loop.last %}, {% endif %}{% endfor %}])
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            import pendulum
            
            timestamp = pendulum.now().strftime("%Y%m%d_%H%M%S")
            
            # Parse S3 URI properly
            asset_uri = "{{ task.asset_uri }}"
            s3_path = asset_uri.replace("s3://", "")
            parts = s3_path.split('/', 1)  # Split into bucket and key
            bucket_name = parts[0]
            base_key = parts[1] if len(parts) > 1 else ""
            
            # Create timestamped historical copy
            historical_key = f"{base_key}_{timestamp}.done"
            historical_content = f"Asset produced at {pendulum.now().to_iso8601_string()}"
            
            # Create/update the main asset marker
            main_key = base_key
            main_content = f"Latest update at {pendulum.now().to_iso8601_string()}"
            
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            # Upload both files
            s3_hook.load_string(
                string_data=historical_content,
                key=historical_key,
                bucket_name=bucket_name,
                replace=True
            )
            logging.info(f"✅ Created historical asset marker: s3://{bucket_name}/{historical_key}")
            
            s3_hook.load_string(
                string_data=main_content,
                key=main_key,
                bucket_name=bucket_name,
                replace=True
            )
            logging.info(f"✅ Updated main asset marker: s3://{bucket_name}/{main_key}")
            
            # Get asset name from outlets for success message
            asset_names = {{ task.outlets | tojson }}
            return f"Asset {asset_names[0] if asset_names else 'unknown'} produced successfully"
            
        except Exception as e:
            # Get asset name from outlets for error message
            asset_names = {{ task.outlets | tojson }}
            logging.error(f"❌ Failed to produce asset {asset_names[0] if asset_names else 'unknown'}: {str(e)}")
            raise AirflowException(f"Asset production failed: {str(e)}")
    {% elif task.type == 'pc_job' or task.type == 'idmc_job' or task.type == 'remote_command' or task.type == 'remote_sh_script' or task.type == 'remote_py_script' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"{{ task.command }} task\"\"\"
        try:
            # Store a generic job name for error tracking
            job_name = "{{ task.task_id }}"
            kwargs['ti'].xcom_push(key='job_name', value=job_name)
            
            utils_functions['execute_remote_job'](
                command='{{ task.command }}',
                **kwargs
            )
            
        except Exception as e:
            logging.error(f"❌ {{ task.task_id }} failed: {str(e)}")
            # For non-job_step tasks, store basic error details
            error_details = {
                'job_name': '{{ task.task_id }}',
                'status': 'ERROR',
                'sql_code': -1,
                'timestamp': pendulum.now('UTC').isoformat(),
                'error_messages': f"Remote job failed: {str(e)}",
                'ddl': None
            }
            kwargs['ti'].xcom_push(key=f'error_details_{{ task.task_id }}', value=error_details)
            raise AirflowException(f"{{ task.task_id }} failed: {str(e)}")
    {% elif task.type == 'create_s3_trigger_file' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"Create S3 trigger file task\"\"\"
        try:
            # Get current date for dynamic path
            import pendulum
            current_date = pendulum.now().strftime("%Y-%m-%d")
            
            # Construct the full S3 key with dynamic date
            base_key = "{{ task.key }}".replace("eff_date=current_date", f"eff_date={current_date}")
            full_key = f"{base_key}{{ task.file_name }}"
            
            logging.info(f"Creating S3 trigger file at: s3://{{ task.bucket }}/{full_key}")
            
            # Call the create_s3_trigger_file function - let it use default AWS_CONN_ID from utils.py
            utils_functions['create_s3_trigger_file'](
                bucket_name='{{ task.bucket }}',
                key=full_key,
                **kwargs)
            
            logging.info(f"✅ S3 trigger file created successfully: s3://{{ task.bucket }}/{full_key}")
            return f"S3 trigger file created: s3://{{ task.bucket }}/{full_key}"
            
        except Exception as e:
            logging.error(f"❌ Failed to create S3 trigger file: {str(e)}")
            raise AirflowException(f"S3 trigger file creation failed: {str(e)}")
    {% elif task.type == 'check_validation_view' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"Check validation view: {{ task.view_name }}\"\"\"
        try:
            # Add task-specific parameters to kwargs
            kwargs['view_name'] = '{{ task.view_name }}'
            kwargs['xcom_prefix'] = '{{ task.xcom_prefix }}'
            {% if task.date_column is defined %}
            kwargs['date_column'] = '{{ task.date_column }}'
            {% endif %}
            
            logging.info(f"🔍 Running validation check for view: {{ task.view_name }}")
            logging.info(f"📊 XCom prefix: {{ task.xcom_prefix }}")
            {% if task.date_column is defined %}
            logging.info(f"Date column: {{ task.date_column }}")
            {% endif %}
            
            # Execute the validation check
            result = utils_functions['check_validation_view'](**kwargs)
            
            if result:
                logging.info(f"✅ Validation check completed for {{ task.view_name }}")
                return f"Validation check completed for {{ task.view_name }}"
            else:
                raise AirflowException(f"Validation check failed for {{ task.view_name }}")
                
        except Exception as e:
            logging.error(f"❌ Validation check task failed for {{ task.view_name }}: {str(e)}")
            raise AirflowException(f"Validation check task failed: {str(e)}")
    
    {% elif task.type == 's3_actions' %}
    {% if task.sub_type is not defined %}
        {% set error_msg = "sub_type is required for s3_actions task" %}
        {{ raise(error_msg) }}
    {% endif %}
    {% if task.source_bucket is not defined %}
        {% set error_msg = "source_bucket is required for s3_actions task" %}
        {{ raise(error_msg) }}
    {% endif %}
    {% if task.source_key is not defined %}
        {% set error_msg = "source_key is required for s3_actions task" %}
        {{ raise(error_msg) }}
    {% endif %}
    {% if task.file_pattern is not defined %}
        {% set error_msg = "file_pattern is required for s3_actions task" %}
        {{ raise(error_msg) }}
    {% endif %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"S3 Actions task: {{ task.sub_type }}\"\"\"
        try:
            from aws_actions import s3_actions
            
            # Build parameters dictionary from YAML
            params = {
                'sub_type': '{{ task.sub_type }}',
                'source_bucket': '{{ task.source_bucket }}',
                'source_key': '{{ task.source_key }}',
                'file_pattern': '{{ task.file_pattern }}',
                'ti': kwargs['ti']
            }
            
            # Add optional parameters if they exist
            {% if task.destination_bucket is defined and task.destination_bucket %}
            params['destination_bucket'] = '{{ task.destination_bucket }}'
            {% endif %}
            
            {% if task.destination_key is defined and task.destination_key %}
            params['destination_key'] = '{{ task.destination_key }}'
            {% endif %}
            
            {% if task.include_timestamp is defined %}
            params['include_timestamp'] = {{ task.include_timestamp|lower }}
            {% endif %}
            
            {% if task.delete_source is defined %}
            params['delete_source'] = {{ task.delete_source|lower }}
            {% endif %}
            
            # Execute S3 action
            result = s3_actions(**params)
            
            # Log result
            logging.info(f"S3 Action {params['sub_type']} completed: {result['message']}")
            
            # Return result for XCom
            return result
            
        except Exception as e:
            logging.error(f"S3 Action failed: {str(e)}")
            raise AirflowException(f"S3 Action failed: {str(e)}")
            
    {% elif task.type == 'call_crawler' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"Call AWS Glue Crawler: {{ task.crawler_name }}\"\"\"
        try:
            from aws_actions import start_crawler
            
            logging.info(f"🚀 Preparing to start crawler: {{ task.crawler_name }}")
            
            # Execute crawler with parameters from YAML
            result = start_crawler(
                crawler_name='{{ task.crawler_name }}',
                {% if task.wait_for_completion is defined %}
                wait_for_completion={{ task.wait_for_completion|lower }},
                {% endif %}
                {% if task.aws_conn_id is defined %}
                aws_conn_id='{{ task.aws_conn_id }}',
                {% endif %}
                **kwargs  # This contains 'ti' and any other context
            )
            
            # Push crawler result to XCom
            if 'ti' in kwargs:
                kwargs['ti'].xcom_push(key='crawler_result', value=result)
                kwargs['ti'].xcom_push(key='crawler_name', value='{{ task.crawler_name }}')
                # Since result is a string, just push a simple status
                kwargs['ti'].xcom_push(key='crawler_status', value='completed')
            
            logging.info(f"✅ Crawler {{ task.crawler_name }} operation completed")
            return result
            
        except Exception as e:
            logging.error(f"❌ Crawler {{ task.crawler_name }} failed: {str(e)}")
            import traceback
            logging.error(f"🔍 Stack trace: {traceback.format_exc()}")
            raise AirflowException(f"Crawler {{ task.crawler_name }} failed: {str(e)}")
            
    {% elif task.type == 'call_crawler' %}
    @task(task_id="{{ task.task_id }}")
    @utils_functions['create_task_with_tracking']
    def {{ task.task_id }}(**kwargs):
        \"\"\"Call AWS Glue Crawler: {{ task.crawler_name }}\"\"\"
        try:
            from aws_actions import start_crawler
            
            logging.info(f"🚀 Preparing to start crawler: {{ task.crawler_name }}")
            
            # Execute crawler with parameters from YAML
            result = start_crawler(
                crawler_name='{{ task.crawler_name }}',
                {% if task.wait_for_completion is defined %}
                wait_for_completion={{ task.wait_for_completion|lower }},
                {% endif %}
                {% if task.aws_conn_id is defined %}
                aws_conn_id='{{ task.aws_conn_id }}',
                {% endif %}
                ti=kwargs['ti'],
                **kwargs
            )
            
            logging.info(f"✅ Crawler {{ task.crawler_name }} operation completed")
            return result
            
        except Exception as e:
            logging.error(f"❌ Crawler {{ task.crawler_name }} failed: {str(e)}")
            import traceback
            logging.error(f"🔍 Stack trace: {traceback.format_exc()}")
            raise AirflowException(f"Crawler {{ task.crawler_name }} failed: {str(e)}")
        
    {% elif task.type == 'final_email' %}
    @task(task_id="{{ task.task_id }}", trigger_rule='all_done')
    def {{ task.task_id }}(**kwargs):
        try:
            ti = kwargs['ti']
            dag_run = kwargs['dag_run']
            dag = kwargs['dag']
            
            # Get task states first
            all_task_states = get_task_states_from_xcom(dag_run, dag, ti, context_kwargs=kwargs)
            
            # If we got no task states, create minimal ones
            if not all_task_states:
                logging.warning("No task states found, creating minimal task info")
                for task_obj in dag.tasks:
                    if task_obj.task_id == 'send_email_notification':
                        continue
                    class MinimalTaskInfo:
                        def __init__(self, task_id, dag_run):
                            self.task_id = task_id
                            self.state = 'UNKNOWN'
                            self.start_date = None
                            self.end_date = None
                            try:
                                from airflow.models import Variable
                                base_url = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                            except Exception:
                                base_url = ""
                            if base_url:
                                self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                            else:
                                self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                    task_info = MinimalTaskInfo(task_obj.task_id, dag_run)
                    all_task_states.append(task_info)
            
            # Count failed tasks from the states we have
            failed_tasks = [ti for ti in all_task_states if getattr(ti, 'state', '').upper() in ['FAILED']]
            success_tasks = [ti for ti in all_task_states if getattr(ti, 'state', '').upper() in ['SUCCESS']]
            unknown_tasks = [ti for ti in all_task_states if getattr(ti, 'state', '').upper() in ['UNKNOWN']]
            
            # DETERMINE DAG SUCCESS BASED ON TASK STATES
            # We rely on the actual task states rather than DAG run state
            # because the email task executes while DAG might still be RUNNING
            dag_succeeded = len(failed_tasks) == 0
            
            logging.info(f"📊 DAG Status Summary:")
            logging.info(f"   - Total tasks: {len(all_task_states)}")
            logging.info(f"   - Success tasks: {len(success_tasks)}")
            logging.info(f"   - Failed tasks: {len(failed_tasks)}")
            logging.info(f"   - Unknown tasks: {len(unknown_tasks)}")
            logging.info(f"   - DAG succeeded: {dag_succeeded}")
            
            # Add task_instances to kwargs for the email function
            kwargs['task_instances'] = all_task_states
            
            # Add this information to kwargs for the email function
            kwargs['_manual_dag_succeeded'] = dag_succeeded
            kwargs['_manual_failed_tasks'] = failed_tasks
            
            # SAFE EMAIL EXECUTION - WRAP IN TRY-CATCH
            try:
                # Call the original email function
                utils_functions["send_email_notification"](
                    subject_prefix='{{ task.subject_prefix }}',
                    from_email='{{ task.from_email }}',
                    success_recipients='{{ task.success_recipients }}',
                    failure_recipients='{{ task.failure_recipients }}',
                    include_validation_results={{ task.include_validation_results }},
                    {% if task.validation_xcom_keys is defined and task.validation_xcom_keys %}
                    validation_xcom_keys={{ task.validation_xcom_keys | tojson }},
                    {% else %}
                    validation_xcom_keys=[],
                    {% endif %}
                    custom_message='''{{ task.custom_message }}''',
                    show_custom_only_on_success={{ task.show_custom_only_on_success }},
                    **kwargs
                )
                logging.info("✅ Email notification sent successfully")
            except Exception as email_error:
                logging.error(f"Email function failed but DAG won't crash: {str(email_error)}")
                # Don't re-raise - email failures shouldn't fail the DAG
                return "Email completed with errors but DAG succeeded"
                
        except Exception as e:
            logging.error(f"Failed in email notification setup: {str(e)}")
            # Don't raise the exception - we don't want email failures to fail the DAG
            return "Email notification completed with errors"
    {%- endif %}
    {%- endfor %}

    # Instantiate tasks
    {%- for task in tasks %}
    {{ task.task_id }}_task = {{ task.task_id }}()
    {%- endfor %}

    # ===== Dependencies =====
    {% for i in range(dependencies|length - 1) %}
    {%- if dependencies[i] is string and dependencies[i+1] is string -%}
    {{ dependencies[i] }}_task >> {{ dependencies[i+1] }}_task
    {%- elif dependencies[i] is string and dependencies[i+1] is sequence -%}
    {{ dependencies[i] }}_task >> [{{ dependencies[i+1]|join('_task, ') }}_task]
    {%- elif dependencies[i] is sequence and dependencies[i+1] is string -%}
    [{{ dependencies[i]|join('_task, ') }}_task] >> {{ dependencies[i+1] }}_task
    {%- elif dependencies[i] is sequence and dependencies[i+1] is sequence -%}
    [{{ dependencies[i]|join('_task, ') }}_task] >> [{{ dependencies[i+1]|join('_task, ') }}_task]
    {%- endif %}
    {% endfor %}

# Instantiate the DAG
{{ dag_name }}_instance = {{ dag_name }}()
"""

def lambda_handler(event, context):
    print("Received event:", event)

    # Extract S3 details from event
    record = event['Records'][0]
    config_bucket = record['s3']['bucket']['name']
    config_key = record['s3']['object']['key']

    # Download YAML config
    s3 = boto3.client("s3")  # ✅ s3 client defined ONLY in Lambda context
    response = s3.get_object(Bucket=config_bucket, Key=config_key)
    config = yaml.safe_load(response['Body'].read())

    # Render DAG
    dag_code = Template(DAG_TEMPLATE).render(**config, yaml_filename=config_key.split('/')[-1])
    dag_file = f"{config['dag_name']}.py"

    # Upload DAG to MWAA bucket
    MWAA_BUCKET = config_bucket
    MWAA_DAG_PREFIX = "dev/airflow_mwaa/dags"
    mwaa_key = f"{MWAA_DAG_PREFIX}/{dag_file}"

    s3.put_object(
        Bucket=MWAA_BUCKET,
        Key=mwaa_key,
        Body=dag_code.encode("utf-8")
    )

    print(f"✅ DAG {config['dag_name']} created at s3://{MWAA_BUCKET}/{mwaa_key}")
    return {"status": "success", "dag": config['dag_name']}

