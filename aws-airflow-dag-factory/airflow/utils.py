# ##########################
# ######## utils.py ########
# ##########################

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import functools
import logging
import sys
import contextlib
import pendulum
from pytz import timezone
import psycopg2
import boto3
import pandas as pd
from datetime import datetime
from pytz import timezone

# Shared Constants (can be overridden in individual DAGs)
# REDSHIFT_CONN_ID = "redshift_default"
# SSH_CONN_ID = "ssh_remote_server"
# AWS_CONN_ID = "aws_default"
# # INFA_LBIN = "//app/etl/lbin"
# # JS_ENV = "ETL_ENV"
# EST = timezone('US/Eastern')
# ENVIRONMENT = "Non-Prod"

REDSHIFT_CONN_ID = Variable.get("REDSHIFT_CONN_ID")
SSH_CONN_ID = Variable.get("SSH_CONN_ID")
AWS_CONN_ID = Variable.get("AWS_CONN_ID") 
ENVIRONMENT = Variable.get("ENVIRONMENT")
REDSHIFT_CLUSTER = Variable.get("REDSHIFT_CLUSTER")
EST = timezone('US/Eastern')

# Module cache to avoid reloading
_MODULE_CACHE = {}

def load_module_from_s3(module_name, s3_bucket_var, s3_key_var, aws_conn_id=AWS_CONN_ID):
    """Dynamically load Python module from S3 with caching"""
    cache_key = f"{module_name}_{s3_bucket_var}_{s3_key_var}"
    
    if cache_key in _MODULE_CACHE:
        logging.info(f"Using cached module: {module_name}")
        return _MODULE_CACHE[cache_key]
    
    try:
        s3_bucket = Variable.get(s3_bucket_var)
        s3_key = Variable.get(s3_key_var)
        
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        file_content = s3_hook.read_key(key=s3_key, bucket_name=s3_bucket)
        
        # Create a module object
        module = type(sys)(module_name)
        exec(file_content, module.__dict__)
        sys.modules[module_name] = module
        
        _MODULE_CACHE[cache_key] = module
        logging.info(f"Successfully loaded {module_name} from S3")
        return module
        
    except Exception as e:
        logging.error(f"Failed to load {module_name} from S3: {str(e)}")
        raise AirflowException(f"Failed to load {module_name}: {str(e)}")

def load_utils_from_s3(s3_bucket_var="S3_BUCKET", s3_key_var="S3_UTILS_KEY", aws_conn_id=AWS_CONN_ID):
    """Load utils module and extract commonly used functions"""
    utils_module = load_module_from_s3('utils', s3_bucket_var, s3_key_var, aws_conn_id)
    
    # Extract relevant utility functions
    utils_functions = {}
    available_functions = [
        'get_redshift_error_details',
        'send_email_notification'
    ]
    
    for func_name in available_functions:
        if hasattr(utils_module, func_name):
            utils_functions[func_name] = getattr(utils_module, func_name)
    
    return utils_functions

def load_job_runner_from_s3(s3_bucket_var="S3_BUCKET", s3_key_var="S3_JOB_RUNNER_KEY", aws_conn_id=AWS_CONN_ID):
    """Load job_runner module from S3"""
    return load_module_from_s3('job_runner', s3_bucket_var, s3_key_var, aws_conn_id)

@contextlib.contextmanager
def redirect_stderr_to_stdout():
    """
    Context manager to redirect stderr to stdout
    """
    original_stderr = sys.stderr
    sys.stderr = sys.stdout
    try:
        yield
    finally:
        sys.stderr = original_stderr

def execute_job_runner(job_name, job_runner_module, utils, verbose=0, task_type='job_step', **kwargs):
    """Wrapper function to execute job_runner with proper arguments & error tracking"""
    
    ti = kwargs.get('ti')
    
    # Store job name in XCom for error tracking
    if ti:
        ti.xcom_push(key='job_name', value=job_name)
        logging.info(f"📝 Stored job_name in XCom: {job_name}")
    
    original_argv = sys.argv.copy()
    
    try:
        # Force immediate logging to create CloudWatch stream
        logging.info(f"🎯 Starting {task_type} execution: {job_name}")
        
        with redirect_stderr_to_stdout():
            # Set the expected command line arguments for job_runner
            sys.argv = ['job_runner.py', job_name, str(verbose)]
            logging.info(f"📋 Executing {task_type} with args: {sys.argv}")
            
            exit_code = job_runner_module.main()
            logging.info(f"📊 {task_type} {job_name} completed with exit code: {exit_code}")
            
            # Check the exit code and raise exception if failed
            if exit_code != 0:
                logging.error(f"❌ {task_type} {job_name} failed with exit code {exit_code}")
                error_details = _handle_job_failure(job_name, exit_code, utils, task_type, **kwargs)
                raise AirflowException(f"{task_type} {job_name} failed with exit code {exit_code}")
            else:
                logging.info(f"✅ {task_type} {job_name} executed successfully")
                
    except SystemExit as e:
        # FIX: Only treat as error if exit code is not 0
        if e.code != 0:
            logging.error(f"🛑 {task_type} {job_name} called sys.exit({e.code})")
            error_details = _handle_job_failure(job_name, e.code, utils, task_type, **kwargs)
            raise AirflowException(f"{task_type} {job_name} failed with exit code {e.code}")
        else:
            logging.info(f"✅ {task_type} {job_name} completed successfully via sys.exit(0)")
    except Exception as e:
        logging.error(f"💥 {task_type} {job_name} execution failed with exception: {str(e)}")
        # For non-Redshift errors, store the exception message
        error_details = {
            'job_name': job_name,
            'status': 'ERROR',
            'sql_code': -1,
            'timestamp': pendulum.now('UTC').isoformat(),
            'error_messages': str(e),
            'ddl': None
        }
        if ti:
            ti.xcom_push(key=f'error_details_{job_name}', value=error_details)
        raise AirflowException(f"{task_type} {job_name} execution failed: {str(e)}")
    finally:
        # Restore original arguments
        sys.argv = original_argv
        logging.info(f"🔚 {task_type} {job_name} execution finalized")

def _handle_job_failure(job_name, exit_code, utils, task_type='job_step', **kwargs):
    """Handle job failure with different logic based on task type"""
    logging.info(f"=== JOB FAILED: {job_name} with exit code {exit_code} ===")
    
    # Initialize error details with basic info
    error_details = {
        'job_name': job_name,
        'status': 'ERROR', 
        'sql_code': exit_code,
        'timestamp': pendulum.now('UTC').isoformat(),
        'error_messages': f"Job failed with exit code {exit_code}",
        'ddl': None
    }
    
    # Different error handling based on task type
    if task_type == 'job_step':
        # For job_step tasks, try to get Redshift error details
        error_details = _get_redshift_error_details(job_name, exit_code, utils, error_details, **kwargs)
    else:
        # For all other task types, use Airflow log errors
        error_details = _get_airflow_error_details(job_name, exit_code, error_details, **kwargs)
    
    # Store the detailed error in XCom for email
    logging.info(f"Storing error_details in XCom: {error_details}")
    kwargs['ti'].xcom_push(key=f'error_details_{job_name}', value=error_details)
    logging.info(f"✓ Stored error details in XCom for {job_name}")
    
    return error_details

def _get_redshift_error_details(job_name, exit_code, utils, error_details, **kwargs):
    """Get Redshift error details for job_step tasks"""
    try:
        logging.info(f"Checking if 'get_redshift_error_details' exists in utils")
        
        if 'get_redshift_error_details' in utils:
            logging.info(f"Calling get_redshift_error_details for: {job_name}")
            redshift_errors = utils["get_redshift_error_details"](job_name)
            logging.info(f"get_redshift_error_details returned: {redshift_errors is not None}")
            
            if redshift_errors:
                logging.info(f"✓ SUCCESS: Retrieved Redshift errors for {job_name}")
                
                # Map the response to our expected format
                error_details.update({
                    'job_name': redshift_errors.get('job_name', job_name),
                    'status': redshift_errors.get('disposition', 'ERROR'),
                    'sql_code': redshift_errors.get('sql_code', exit_code),
                    'timestamp': redshift_errors.get('start_time', pendulum.now('UTC').isoformat()),
                    'error_messages': redshift_errors.get('message', f"Job failed with exit code {exit_code}"),
                    'ddl': redshift_errors.get('ddl')
                })
                logging.info(f"✓ Updated error_details with Redshift data")
            else:
                logging.warning(f"✗ get_redshift_error_details returned None for: {job_name}")
        else:
            logging.error("✗ get_redshift_error_details function NOT found in utils")
    except Exception as redshift_error:
        logging.error(f"❌ Exception in get_redshift_error_details: {str(redshift_error)}")
        import traceback
        logging.error(f"❌ Traceback: {traceback.format_exc()}")
        error_details['error_messages'] = f"Job failed with exit code {exit_code}. Error retrieving Redshift details: {str(redshift_error)}"
    
    return error_details

def _get_airflow_error_details(job_name, exit_code, error_details, **kwargs):
    """Get Airflow log errors for non-job_step tasks"""
    try:
        ti = kwargs.get('ti')
        if ti:
            # For non-Redshift tasks, capture the exception message from logs
            # This would be enhanced to capture actual log snippets in production
            task_id = ti.task_id
            error_details['error_messages'] = f"Task {task_id} failed. Check Airflow task logs for detailed error information. Exit code: {exit_code}"
            
            # You could enhance this to capture the last few lines of logs
            # from Airflow's log storage if needed
            logging.info(f"📝 Captured Airflow error details for task: {task_id}")
            
    except Exception as airflow_error:
        logging.error(f"❌ Error capturing Airflow error details: {str(airflow_error)}")
        error_details['error_messages'] = f"Task failed with exit code {exit_code}. Error capturing details: {str(airflow_error)}"
    
    return error_details

def track_task_state(task_id, dag_run, ti, state='RUNNING', **kwargs):
    """Track task state for email notification with standardized format"""
    task_info = {
        'task_id': task_id,
        'state': state,
        'start_date': pendulum.now('UTC').isoformat(),
        'dag_run_id': dag_run.run_id,
        'dag_id': dag_run.dag_id
    }
    
    # Add end_date for completed tasks
    if state in ['SUCCESS', 'FAILED']:
        task_info['end_date'] = pendulum.now('UTC').isoformat()
    
    ti.xcom_push(key='task_state', value=task_info)
    logging.info(f"Task state set to: {state} for task: {task_id}")

def create_task_with_tracking(task_func):
    """Decorator to add automatic state tracking to tasks with proper error handling"""
    @functools.wraps(task_func)
    def wrapper(**kwargs):
        ti = kwargs.get('ti')
        dag_run = kwargs.get('dag_run')
        
        # Force early logging to ensure CloudWatch stream creation
        logging.info(f"🚀 Starting task: {ti.task_id if ti else 'unknown'}")
        logging.info(f"📋 Task {ti.task_id if ti else 'unknown'} - Initializing...")
        
        if ti and dag_run:
            # Track task state as RUNNING
            task_info = {
                'task_id': ti.task_id,
                'state': 'RUNNING',
                'start_date': pendulum.now('UTC').isoformat(),
                'dag_run_id': dag_run.run_id,
                'dag_id': dag_run.dag_id
            }
            ti.xcom_push(key='task_state', value=task_info)
            logging.info(f"✅ Task {ti.task_id} state set to RUNNING")
        
        try:
            # Execute the actual task function
            result = task_func(**kwargs)
            
            if ti and dag_run:
                # Update task state to SUCCESS
                task_info = {
                    'task_id': ti.task_id,
                    'state': 'SUCCESS',
                    'start_date': ti.xcom_pull(key='task_state', task_ids=ti.task_id).get('start_date'),
                    'end_date': pendulum.now('UTC').isoformat(),
                    'dag_run_id': dag_run.run_id,
                    'dag_id': dag_run.dag_id
                }
                ti.xcom_push(key='task_state', value=task_info)
                logging.info(f"✅ Task {ti.task_id} completed successfully")
            
            return result
            
        except Exception as e:
            logging.error(f"❌ Task {ti.task_id if ti else 'unknown'} failed: {str(e)}")
            
            if ti and dag_run:
                # CRITICAL FIX: Update task state to FAILED with proper error handling
                try:
                    # Get the original start date from XCom
                    existing_state = ti.xcom_pull(key='task_state', task_ids=ti.task_id)
                    start_date = existing_state.get('start_date') if existing_state else pendulum.now('UTC').isoformat()
                    
                    # Update task state to FAILED
                    task_info = {
                        'task_id': ti.task_id,
                        'state': 'FAILED',
                        'start_date': start_date,
                        'end_date': pendulum.now('UTC').isoformat(),
                        'dag_run_id': dag_run.run_id,
                        'dag_id': dag_run.dag_id,
                        'error_message': str(e)
                    }
                    ti.xcom_push(key='task_state', value=task_info)
                    logging.info(f"✅ Task {ti.task_id} state set to FAILED")
                    
                except Exception as xcom_error:
                    logging.error(f"❌ Failed to update task state in XCom: {str(xcom_error)}")
                    # Last resort: try to set minimal FAILED state
                    try:
                        task_info = {
                            'task_id': ti.task_id,
                            'state': 'FAILED',
                            'start_date': pendulum.now('UTC').isoformat(),
                            'end_date': pendulum.now('UTC').isoformat(),
                            'dag_run_id': dag_run.run_id,
                            'dag_id': dag_run.dag_id,
                            'error_message': str(e)
                        }
                        ti.xcom_push(key='task_state', value=task_info)
                        logging.info(f"✅ Task {ti.task_id} state set to FAILED (fallback)")
                    except Exception as fallback_error:
                        logging.error(f"❌ Complete failure to update task state: {str(fallback_error)}")
            
            # Re-raise the exception to maintain Airflow's normal failure behavior
            raise AirflowException(f"Task {ti.task_id if ti else 'unknown'} failed: {str(e)}")
            
        finally:
            # Final log to ensure CloudWatch stream has content
            logging.info(f"🏁 Task {ti.task_id if ti else 'unknown'} - Execution completed")
    
    return wrapper

def _get_actual_task_states_from_airflow(dag_run, dag, ti):
    """Get actual task states from Airflow metadata using public APIs"""
    try:
        from airflow.models import TaskInstance
        from airflow.utils.state import TaskInstanceState
        
        task_states = []
        
        for task in dag.tasks:
            if task.task_id == 'send_email_notification':
                continue
                
            try:
                # Use Airflow's public API to get task instance
                task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
                task_instance.refresh_from_db()
                
                # Create task info object
                class ActualTaskInfo:
                    def __init__(self, ti_instance, dag_run):
                        self.task_id = ti_instance.task_id
                        self.state = ti_instance.state
                        self.start_date = ti_instance.start_date
                        self.end_date = ti_instance.end_date
                        
                        # Build log URL using Airflow's public method
                        try:
                            from airflow.models import Variable
                            base_url = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                        except Exception:
                            base_url = ""
                        
                        if base_url:
                            self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={ti_instance.task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                        else:
                            self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={ti_instance.task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                
                task_info = ActualTaskInfo(task_instance, dag_run)
                task_states.append(task_info)
                logging.info(f"📊 Task {task.task_id} - Actual state: {task_instance.state}")
                
            except Exception as task_error:
                logging.warning(f"⚠️ Could not get state for {task.task_id} from Airflow API: {str(task_error)}")
                # Fallback to XCom data - FIXED: Remove self.
                _add_fallback_task_info(task, dag_run, ti, task_states)  # REMOVED self.
        
        return task_states
        
    except Exception as e:
        logging.error(f"❌ Failed to get task states from Airflow API: {str(e)}")
        # Return empty list as fallback - will use XCom data instead
        return []

def _add_fallback_task_info(self, task, dag_run, ti, task_states):
    """Fallback to XCom data when Airflow API fails"""
    try:
        xcom_data = ti.xcom_pull(task_ids=task.task_id, key='task_state', include_prior_dates=False)
        if xcom_data:
            class XComTaskInfo:
                def __init__(self, data, dag_run):
                    self.task_id = data['task_id']
                    self.state = data['state']
                    self.start_date = self._safe_parse_date(data.get('start_date'))
                    self.end_date = self._safe_parse_date(data.get('end_date'))
                    
                    try:
                        from airflow.models import Variable
                        base_url = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                    except Exception:
                        base_url = ""
                    
                    if base_url:
                        self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={data['task_id']}&map_index=-1&dag_run_id={dag_run.run_id}"
                    else:
                        self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={data['task_id']}&map_index=-1&dag_run_id={dag_run.run_id}"
                
                def _safe_parse_date(self, date_str):
                    if not date_str:
                        return None
                    try:
                        return pendulum.parse(date_str)
                    except Exception:
                        return None
            
            task_info = XComTaskInfo(xcom_data, dag_run)
            task_states.append(task_info)
        else:
            # Last resort - minimal task info
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
            
            task_info = MinimalTaskInfo(task.task_id, dag_run)
            task_states.append(task_info)
            
    except Exception as fallback_error:
        logging.error(f"❌ Fallback also failed for {task.task_id}: {str(fallback_error)}")
        
# Keep all your existing functions below (they remain unchanged)
def get_redshift_error_details(job_name, redshift_conn_id=REDSHIFT_CONN_ID):
    """Fetch Redshift error details from EDW.JOB_LOGS using IAM authentication"""
    try:
        logging.info(f"=== Starting Redshift error lookup for: {job_name} ===")
        
        # Get connection details from Airflow
        # from airflow.hooks.base import BaseHook
        
        redshift_conn = BaseHook.get_connection(redshift_conn_id)
        
        # Use IAM authentication (same as job_runner.py)
        client = boto3.client('redshift')
        response = client.get_cluster_credentials(
            DbUser=redshift_conn.login,
            ClusterIdentifier=REDSHIFT_CLUSTER,  # Same as job_runner.py
            AutoCreate=False,
            DurationSeconds=3600
        )
        
        # Connect using IAM credentials
        conn = psycopg2.connect(
            host=redshift_conn.host,
            port=redshift_conn.port,
            dbname=redshift_conn.schema,
            user=response['DbUser'],
            password=response['DbPassword'],
            connect_timeout=30,
            sslmode='require'
        )
        
        cursor = conn.cursor()
        
        query = """
        SELECT job_name, job_disposition, job_sql_code, job_message, 
               job_ddl, job_start_time
        FROM EDW.JOB_LOGS 
        WHERE UPPER(JOB_NAME) = %s
        ORDER BY JOB_START_TIME DESC
        LIMIT 1
        """
        
        logging.info(f"Executing query for job: {job_name}")
        cursor.execute(query, (job_name.upper(),))
        result = cursor.fetchone()
        
        if result:
            job_name, disposition, sql_code, message, ddl, timestamp = result
            
            # Format timestamp
            if timestamp:
                if hasattr(timestamp, 'microsecond') and timestamp.microsecond > 0:
                    formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                else:
                    formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_time = "N/A"
            
            logging.info(f"✓ Successfully retrieved job details: {job_name}")
            
            return {
                'job_name': job_name,
                'disposition': disposition,
                'sql_code': sql_code,
                'message': message,
                'ddl': ddl,
                'start_time': formatted_time
            }
        
        logging.warning(f"✗ No job found in EDW.JOB_LOGS for: {job_name}")
        return None
        
    except Exception as e:
        logging.error(f"❌ Failed to fetch Redshift errors: {str(e)}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def _debug_task_dates(task_instances, est):
    """Debug function to identify problematic date values"""
    try:
        logging.info("=== DEBUG: Checking all task date values ===")
        for i, task_instance in enumerate(task_instances):
            task_id = getattr(task_instance, 'task_id', 'UNKNOWN')
            start_date = getattr(task_instance, 'start_date', 'MISSING')
            end_date = getattr(task_instance, 'end_date', 'MISSING')
            
            logging.info(f"Task {i}: {task_id}")
            logging.info(f"  start_date: {start_date} (type: {type(start_date)})")
            logging.info(f"  end_date: {end_date} (type: {type(end_date)})")
            
            # Test the formatting with extra safety
            try:
                test_start = _safe_format_date(start_date, est)
                logging.info(f"  formatted_start: {test_start}")
            except Exception as e:
                logging.error(f"  ERROR formatting start_date: {str(e)}")
            
            try:
                test_end = _safe_format_date(end_date, est)
                logging.info(f"  formatted_end: {test_end}")
            except Exception as e:
                logging.error(f"  ERROR formatting end_date: {str(e)}")

        logging.info("=== END DEBUG ===")
    except Exception as debug_error:
        logging.error(f"DEBUG function failed: {str(debug_error)}")

def _safe_format_date(date_obj, timezone):
    """Safely format date objects to prevent 'NoneType' errors"""
    try:
        # Explicit None check first - this is the most important check
        if date_obj is None:
            return 'N/A'
        
        # Handle empty strings or falsey values
        if not date_obj:
            return 'N/A'
        
        # Handle string dates - parse them first
        if isinstance(date_obj, str):
            try:
                # Parse the string to a datetime object
                parsed_date = pendulum.parse(date_obj)
                if parsed_date is None:
                    return 'N/A'
                # Convert to target timezone and format
                return parsed_date.in_timezone(timezone).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as parse_error:
                logging.warning(f"Failed to parse date string '{date_obj}': {str(parse_error)}")
                return 'N/A'
        
        # Handle pendulum DateTime objects
        if hasattr(date_obj, 'in_timezone') and callable(getattr(date_obj, 'in_timezone')):
            try:
                # Additional safety check for None
                if date_obj is None:
                    return 'N/A'
                return date_obj.in_timezone(timezone).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as pendulum_error:
                logging.warning(f"Failed to format pendulum object {date_obj}: {str(pendulum_error)}")
                return 'N/A'
        
        # Handle Python datetime objects - FIXED: Add explicit None check
        if (date_obj is not None and 
            hasattr(date_obj, 'astimezone') and 
            callable(getattr(date_obj, 'astimezone'))):
            try:
                return date_obj.astimezone(timezone).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as tz_error:
                logging.warning(f"Failed to convert timezone for {date_obj}: {str(tz_error)}")
                # Fallback: try string representation
                try:
                    return date_obj.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    return 'N/A'
        
        # Final fallback - try to convert to string representation
        try:
            return str(date_obj)
        except Exception:
            return 'N/A'
            
    except Exception as e:
        logging.warning(f"Date formatting failed for value {date_obj} (type: {type(date_obj)}): {str(e)}")
        return 'N/A'
    
def send_email_notification(
    subject_prefix,
    from_email,
    success_recipients,
    failure_recipients,
    ssh_conn_id=SSH_CONN_ID,
    include_validation_results=True,
    validation_xcom_keys=None,
    custom_message=None,
    show_custom_only_on_success=True,
    **kwargs
):
    """
    Universal function to send comprehensive email notifications - Airflow 3.0 compatible
    """
    try:
        ti = kwargs.get('ti')
        dag_run = kwargs.get('dag_run')
        dag = kwargs.get('dag')
        
        if dag_run is None or dag is None:
            raise ValueError("DAG context not available")
            
        est = pendulum.timezone('US/Eastern')
        
        # FIX: Safe handling of data_interval_start which might be None
        data_interval_start = getattr(dag_run, 'data_interval_start', None)
        if data_interval_start is not None:
            exec_date_est = pendulum.instance(data_interval_start).in_timezone(est)
            formatted_date = exec_date_est.format('YYYY-MM-DD HH:mm:ss ZZZ')
        else:
            # Fallback to current time if data_interval_start is None
            formatted_date = pendulum.now(est).format('YYYY-MM-DD HH:mm:ss ZZZ')
            logging.warning("data_interval_start is None, using current time as fallback")
        
        # Get task instances from kwargs (passed by TaskFlow API)
        task_instances = kwargs.get('task_instances', [])
        
        # MANUAL DAG SUCCESS CHECK - Use the manually passed value or calculate
        if '_manual_dag_succeeded' in kwargs:
            dag_succeeded = kwargs['_manual_dag_succeeded']
            logging.info(f"Using manual DAG success status: {dag_succeeded}")
            
            # When using manual status, we still need to populate filtered_tasks and failed_tasks
            # for the email content generation
            if not task_instances:
                logging.warning("No task instances provided in context, creating minimal task info")
                for task in dag.tasks:
                    if task.task_id != 'send_email_notification':
                        class MinimalTaskInfo:
                            def __init__(self, task_id):
                                self.task_id = task_id
                                self.state = 'UNKNOWN'
                                self.start_date = None
                                self.end_date = None
                                # Build log URL - handle Variable import properly
                                try:
                                    from airflow.sdk.variable import Variable as SDKVariable
                                    base_url = SDKVariable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                                except ImportError:
                                    from airflow.models import Variable as SDKVariable
                                    base_url = SDKVariable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                                
                                if base_url:
                                    self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                                else:
                                    self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                        task_instances.append(MinimalTaskInfo(task.task_id))
            
            # Filter out email task
            filtered_tasks = [ti for ti in task_instances if getattr(ti, 'task_id', '') != 'send_email_notification']
            failed_tasks = [ti for ti in filtered_tasks if getattr(ti, 'state', '') in ['FAILED', 'failed']]
            
        else:
            # Fallback to original logic if manual status not provided
            # If no task instances provided, create minimal ones from DAG structure
            if not task_instances:
                logging.warning("No task instances provided in context, creating minimal task info")
                for task in dag.tasks:
                    if task.task_id != 'send_email_notification':
                        class MinimalTaskInfo:
                            def __init__(self, task_id):
                                self.task_id = task_id
                                self.state = 'UNKNOWN'
                                self.start_date = None
                                self.end_date = None
                                # Build log URL - handle Variable import properly
                                try:
                                    from airflow.sdk.variable import Variable as SDKVariable
                                    base_url = SDKVariable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                                except ImportError:
                                    from airflow.models import Variable as SDKVariable
                                    base_url = SDKVariable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var="")
                                
                                if base_url:
                                    self.log_url = f"{base_url}/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                                else:
                                    self.log_url = f"/log?dag_id={dag_run.dag_id}&task_id={task_id}&map_index=-1&dag_run_id={dag_run.run_id}"
                        task_instances.append(MinimalTaskInfo(task.task_id))
            
            # Filter out email task and find failed tasks
            filtered_tasks = [ti for ti in task_instances if getattr(ti, 'task_id', '') != 'send_email_notification']
            
            # If we have actual task instances from the DAG run, use their states instead of XCom
            if task_instances and hasattr(task_instances[0], 'state') and task_instances[0].state in ['success', 'failed', 'running']:
                # Use the actual task states from the DAG run
                for task_instance in task_instances:
                    task_id = getattr(task_instance, 'task_id', '')
                    actual_state = getattr(task_instance, 'state', '').upper()
                    
                    # Update the state in our filtered_tasks list
                    for filtered_task in filtered_tasks:
                        if getattr(filtered_task, 'task_id', '') == task_id:
                            filtered_task.state = actual_state
                            break
            
            # Calculate DAG success based on failed tasks
            failed_tasks = [ti for ti in filtered_tasks if getattr(ti, 'state', '') in ['FAILED', 'failed']]
            dag_succeeded = not bool(failed_tasks)
            logging.info(f"Calculated DAG success status: {dag_succeeded} (failed tasks: {len(failed_tasks)})")
        
        # DETERMINE RECIPIENTS BASED ON SUCCESS/FAILURE
        if dag_succeeded:
            final_recipients = success_recipients
            logging.info(f"Using success recipients: {final_recipients}")
        else:
            final_recipients = failure_recipients
            logging.info(f"Using failure recipients: {final_recipients}")
        
        # DEBUG: Check all date values before proceeding
        _debug_task_dates(filtered_tasks, est)        
        
        # Only include validation results if DAG succeeded
        if not dag_succeeded:
            include_validation_results = False
        
        # Get validation results if requested and DAG succeeded
        validation_sections = ""
        if include_validation_results and validation_xcom_keys:
            logging.info(f"Looking for validation results with keys: {validation_xcom_keys}")
            
            for key in validation_xcom_keys:
                results = None
                
                # Search across ALL tasks for the XCom key (most reliable approach)
                for task in dag.tasks:
                    if task.task_id != 'send_email_notification':  # Skip email task itself
                        # Try to pull XCom from each task
                        temp_results = ti.xcom_pull(task_ids=task.task_id, key=f'{key}_results_html')
                        if temp_results:
                            results = temp_results
                            logging.info(f"Found validation results for '{key}' in task: {task.task_id}")
                            break
                
                if results:
                    validation_sections += f"<h3>Post-Validation Data:</h3>{results}"
                    logging.info(f"Added validation results for {key}")
                else:
                    logging.warning(f"No validation results found for key: {key} in any task")
    
        # Get error details for failed tasks - USE JOB NAME FROM XCOM
        error_details = {}

        for filtered_task in filtered_tasks:
            task_id = getattr(filtered_task, 'task_id', '')
            task_state = getattr(filtered_task, 'state', '')
            
            # Only get error details for actually failed tasks
            if task_state in ['FAILED', 'failed']:
                # Get the exact job name from XCom (stored by execute_job_runner)
                actual_job_name = ti.xcom_pull(task_ids=task_id, key='job_name')
                
                if actual_job_name:
                    error_key = f'error_details_{actual_job_name}'
                    
                    # Try to get error details from XCom using the exact job name
                    error_data = ti.xcom_pull(task_ids=task_id, key=error_key, include_prior_dates=False)
                    
                    if error_data:
                        error_details[task_id] = error_data
                        logging.info(f"Found error details for {task_id}")
                    else:
                        # No error details found
                        error_details[task_id] = {
                            'job_name': actual_job_name,
                            'status': 'ERROR',
                            'sql_code': -1,
                            'timestamp': 'N/A',
                            'error_messages': f"No error details available for task {task_id}. Check Airflow task logs for more information.",
                            'ddl': None
                        }
                        logging.warning(f"No error details found in XCom for task {task_id} with job: {actual_job_name}")
                else:
                    # Job name not found in XCom
                    error_details[task_id] = {
                        'job_name': 'UNKNOWN',
                        'status': 'ERROR',
                        'sql_code': -1,
                        'timestamp': 'N/A',
                        'error_messages': f"Job name not found in XCom for task {task_id}. Cannot retrieve error details.",
                        'ddl': None
                    }
                    logging.error(f"Job name not found in XCom for task {task_id}")
        
        # Build email subject with status indicator
        status = '❌FAILED' if not dag_succeeded else '✅SUCCESS'
        email_subject = f"{status} [{ENVIRONMENT}] - {subject_prefix}"
        
        # HTML email template
        email_content = f"""<html>
<head>
<style>
    body {{ font-family: Arial, sans-serif; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
    th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; }}
    th {{ background-color: #f2f2f2; font-weight: bold; }}
    .error {{ color: #d9534f; }}
    .success {{ color: #5cb85c; }}
    .warning {{ color: #f0ad4e; }}
    pre {{ 
        background-color: #f8f9fa; 
        padding: 10px; 
        border-radius: 4px; 
        border: 1px solid #ddd;
        overflow-x: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
    }}
    .task-details {{ 
        margin-bottom: 20px; 
        padding: 15px;
        background-color: #f8f9fa;
        border-left: 4px solid #d9534f;
        border-radius: 4px;
    }}
    .summary-table {{ width: auto; }}
</style>
</head>
<body>
<h2>Complete Execution Report</h2>
<p><strong>Execution Time:</strong> {formatted_date}</p>
<p><strong>Environment:</strong> {ENVIRONMENT}</p>
<p><strong>Recipients:</strong> {final_recipients}</p>
"""
        if custom_message and (not show_custom_only_on_success or dag_succeeded):
            email_content += f"""
        <div style="background-color: #f8f9fa; 
                    padding: 15px; 
                    border-left: 4px solid #5bc0de;
                    margin-bottom: 20px;
                    border-radius: 4px;">
            {custom_message}
        </div>
        """
        
        # Add failed task details if any
        if failed_tasks:
            email_content += "<h3 class='error'>DAG Execution Failed!</h3>"
            email_content += "<p>The following tasks failed:</p>"
            
            for failed_ti in failed_tasks:
                task_id = getattr(failed_ti, 'task_id', '')
                log_url = getattr(failed_ti, 'log_url', '#')
                
                email_content += f"""
                <div class='task-details'>
                    <h4>Task: {task_id}</h4>
                    <p><strong>Logs:</strong> <a href="{log_url}">View Logs</a></p>
                """
                
                if task_id in error_details:
                    error_info = error_details[task_id]
                    email_content += f"""
                    <h5>Redshift Error Details:</h5>
                    <table>
                        <tr><th>Job Name</th><td>{error_info.get('job_name', 'N/A')}</td></tr>
                        <tr><th>Status</th><td>{error_info.get('status', 'N/A')}</td></tr>
                        <tr><th>SQL Code</th><td>{error_info.get('sql_code', 'N/A')}</td></tr>
                        <tr><th>Timestamp</th><td>{error_info.get('timestamp', 'N/A')}</td></tr>
                    </table>
                    <h5>Error Messages:</h5>
                    <pre>{error_info.get('error_messages', 'No error messages available')}</pre>
                    """
                    
                    if error_info.get('ddl'):
                        email_content += f"""
                        <h5>Related DDL:</h5>
                        <pre>{error_info['ddl']}</pre>
                        """
                else:
                    email_content += f"""
                    <h5>Error Details:</h5>
                    <p><em>No error details available for this task.</em></p>
                    """
                
                email_content += "</div>"
        else:
            email_content += "<h3 class='success'>Execution Succeeded!</h3>"

        # Add validation results if DAG succeeded
        if include_validation_results and validation_sections:
            email_content += validation_sections

        # Task execution summary (automatically excludes this email task)
        email_content += """
        <h3>Task Execution Summary</h3>
        <table class="summary-table">
        <tr><th>Task ID</th><th>Status</th><th>Start Time</th><th>End Time</th></tr>
        """

        # Sort tasks by start_date if available
        sortable_tasks = [ti for ti in filtered_tasks if getattr(ti, 'start_date', None)]
        unsortable_tasks = [ti for ti in filtered_tasks if not getattr(ti, 'start_date', None)]

        sorted_tasks = sorted(sortable_tasks, key=lambda x: x.start_date) + unsortable_tasks

        # In the email template section, replace the date formatting part with:
        for task_instance in sorted_tasks:
            task_id = getattr(task_instance, 'task_id', '')
            state = getattr(task_instance, 'state', 'UNKNOWN')
            
            # FIX: Add explicit None checks for date attributes
            start_date = getattr(task_instance, 'start_date', None)
            end_date = getattr(task_instance, 'end_date', None)
            
            status_class = "error" if state in ['FAILED', 'failed'] else "success" if state in ['SUCCESS', 'success'] else ""
            
            # Use the fixed _safe_format_date function with extra safety
            try:
                start_time = _safe_format_date(start_date, est) 
                end_time = _safe_format_date(end_date, est)
            except Exception as date_error:
                logging.error(f"Failed to format dates for task {task_id}: {str(date_error)}")
                start_time = 'N/A'
                end_time = 'N/A'
            
            email_content += f"""
        <tr>
            <td>{task_id}</td>
            <td class="{status_class}">{state}</td>
            <td>{start_time}</td>
            <td>{end_time}</td>
        </tr>
        """

        email_content += """
        </table>
        <div style="margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-left: 4px solid #6c757d;">
            <p style="margin: 0; font-style: italic; color: #6c757d;">
                <strong>NOTE:</strong> This is an auto-generated email. Please do not reply.
            </p>
        </div>
        """

        # Send email
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        with ssh_hook.get_conn() as ssh_client:
            temp_file = f"/tmp/email_{dag_run.run_id}.html"
            
            email_message = f"""From: {from_email}
To: {final_recipients}
Subject: {email_subject}
MIME-Version: 1.0
Content-Type: text/html; charset=UTF-8
Content-Transfer-Encoding: 7bit

{email_content}
"""
            
            put_command = f'cat > {temp_file} << "EOF"\n{email_message}\nEOF'
            stdin, stdout, stderr = ssh_client.exec_command(put_command)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error = stderr.read().decode()
                raise Exception(f"Failed to create email file: {error}")
            
            send_command = f"/usr/sbin/sendmail -t < {temp_file} && rm {temp_file}"
            stdin, stdout, stderr = ssh_client.exec_command(send_command)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error = stderr.read().decode()
                raise Exception(f"Failed to send email: {error}")
            
            logging.info(f"Email notification sent successfully to: {final_recipients}")
            
    except Exception as e:
        logging.error(f"Failed to send comprehensive email: {str(e)}")
        raise

def execute_remote_job(command, **kwargs):
    """Execute remote job with simplified error handling"""
    try:
        ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        with ssh_hook.get_conn() as ssh_client:
            # Execute the exact command as provided
            full_command = f"{command}; echo $? > /tmp/exit_code"
            
            stdin, stdout, stderr = ssh_client.exec_command(full_command)
            stdout_output = stdout.read().decode().strip()
            stderr_output = stderr.read().decode().strip()

            # Get exit code
            stdin, stdout, stderr = ssh_client.exec_command("cat /tmp/exit_code")
            exit_code = int(stdout.read().decode().strip())
            
            logging.info(f"STDOUT: {stdout_output}")
            if stderr_output:
                logging.error(f"STDERR: {stderr_output}")
            
            if exit_code != 0:
                error_msg = f"Command failed with exit code {exit_code}: {stderr_output}"
                logging.error(error_msg)
                raise AirflowException(error_msg)

            return stdout_output
            
    except Exception as e:
        logging.error(f"SSH Connection Failed: {str(e)}")
        raise AirflowException(f"Task failed: {str(e)}")

def create_s3_trigger_file(bucket_name, key, aws_conn_id=AWS_CONN_ID, **kwargs):
    """
    Create an S3 trigger file with optional content
    
    Args:
        bucket_name (str): S3 bucket name
        key (str): S3 key/path for the trigger file
        aws_conn_id (str): AWS connection ID
        content (str, optional): Content to put in the file. Defaults to empty string.
        **kwargs: Additional keyword arguments including task instance
    """
    try:
        content = kwargs.get('content', '')
        
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_string(
            string_data=content,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(f"Created trigger at s3://{bucket_name}/{key}")
        
        trigger_status = f"<p style='color: green; font-weight: bold;'>Trigger file successfully created at s3://{bucket_name}/{key}</p>"
        
        # Push to XCom if task instance is available
        if 'ti' in kwargs:
            kwargs['ti'].xcom_push(key='trigger_status', value=trigger_status)
            kwargs['ti'].xcom_push(key='trigger_location', value=f"s3://{bucket_name}/{key}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to create trigger: {str(e)}")
        error_msg = f"<p style='color: red; font-weight: bold;'>Failed to create trigger file: {str(e)}</p>"
        
        # Push error to XCom if task instance is available
        if 'ti' in kwargs:
            kwargs['ti'].xcom_push(key='trigger_status', value=error_msg)
        
        raise AirflowException(f"Failed to create S3 trigger: {str(e)}")

def check_validation_view(**kwargs):
    """
    Generic function to check any validation view and push results to XCom
    Uses IAM authentication same as get_redshift_error_details
    """
    try:    
        # Get parameters from task
        view_name = kwargs['view_name']
        date_column = kwargs.get('date_column', 'dwlodate')
        xcom_prefix = kwargs.get('xcom_prefix', view_name.lower())
        
        current_date = datetime.now(timezone('US/Eastern')).strftime('%Y-%m-%d')
        
        logging.info(f"🔍 Starting validation check for view: {view_name}")
        logging.info(f"📅 Checking date: {current_date} using column: {date_column}")
        
        # Use IAM authentication (same as get_redshift_error_details)
        redshift_conn = BaseHook.get_connection(REDSHIFT_CONN_ID)
        
        # Get IAM credentials
        client = boto3.client('redshift')
        response = client.get_cluster_credentials(
            DbUser=redshift_conn.login,
            ClusterIdentifier=REDSHIFT_CLUSTER,
            AutoCreate=False,
            DurationSeconds=3600
        )
        
        # Connect using IAM credentials
        conn = psycopg2.connect(
            host=redshift_conn.host,
            port=redshift_conn.port,
            dbname=redshift_conn.schema,
            user=response['DbUser'],
            password=response['DbPassword'],
            connect_timeout=30,
            sslmode='require'
        )
        
        query = f"""
        SELECT * FROM edw.{view_name}
        WHERE TRIM(TO_CHAR({date_column}, 'YYYY-MM-DD')) = '{current_date}'
        """
        
        logging.info(f"📊 Executing query: {query}")
        
        # Use pandas to read the results
        df = pd.read_sql(query, conn)
        
        # Convert any datetime columns to EST timezone
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    # First try converting from naive (UTC) to EST
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
                except TypeError:
                    # If already timezone-aware, just convert
                    df[col] = pd.to_datetime(df[col]).dt.tz_convert('US/Eastern')
                df[col] = df[col].astype(str)  # Convert to string for HTML
        
        results_html = df.to_html(index=False) if not df.empty else f"No results available for {view_name} on {current_date}"
        
        # Push to XCom with dynamic keys
        kwargs['ti'].xcom_push(key=f'{xcom_prefix}_current_date', value=current_date)
        kwargs['ti'].xcom_push(key=f'{xcom_prefix}_results_html', value=results_html)
        
        logging.info(f"✅ Pre-validation results for {view_name}: {len(df)} rows found")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Validation check failed for {view_name}: {str(e)}")
        error_msg = f"Validation check failed for {view_name} - {str(e)}"
        kwargs['ti'].xcom_push(key=f'{xcom_prefix}_current_date', value=current_date)
        kwargs['ti'].xcom_push(key=f'{xcom_prefix}_results_html', value=error_msg)
        return True
        
    finally:
        if 'conn' in locals():
            conn.close()