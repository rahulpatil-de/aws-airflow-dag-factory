import json
import psycopg2
from psycopg2 import sql
import boto3
from botocore.exceptions import ClientError
import logging
import threading
from typing import Dict, List, Any, Tuple, Optional
import time
import os
from datetime import datetime
import subprocess
import io
import sys
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import redshift_connector

REDSHIFT_CLUSTER = Variable.get("REDSHIFT_CLUSTER")

# BEST PRACTICE: Standard Python logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobRunner:
    def __init__(self, job_name: str, verbose: int = 0, airflow_conn_id: str = "redshift_default"):
        self.job_name = job_name
        self.verbose = verbose
        self.airflow_conn_id = airflow_conn_id
        self.backup_running = self.check_backup_running()
        self.steps = []
        
        # Adjust log level based on verbosity
        if verbose > 1:
            logger.setLevel(logging.DEBUG)
        
        if self.backup_running:
            logger.info("Backup is running - will skip certain operations")


    def check_backup_running(self) -> bool:
        """Check if backup is running (placeholder implementation)"""
        return os.path.exists('/tmp/NZBACKUP_RUNNING')

    def get_redshift_connection(self, autocommit: bool = False):
        """Smart Redshift connection - works in both MWAA 2.10.1 and 3.0.6"""
        try:
            # First try IAM authentication (for MWAA 3.0.6)
            return self._get_iam_connection(autocommit)
        except Exception as iam_error:
            # Fallback to password authentication (for MWAA 2.10.1)
            logger.warning(f"IAM auth failed, falling back to password: {iam_error}")
            return self._get_password_connection(autocommit)

    def _get_iam_connection(self, autocommit: bool = False):
        """IAM authentication for MWAA 3.0.6"""
        redshift_conn = BaseHook.get_connection(self.airflow_conn_id)
        
        logger.info(f"Connecting to Redshift at {redshift_conn.host} using IAM")
        
        # Get temporary IAM credentials
        client = boto3.client('redshift')
        response = client.get_cluster_credentials(
            DbUser=redshift_conn.login,
            ClusterIdentifier=REDSHIFT_CLUSTER,
            AutoCreate=False,
            DurationSeconds=3600
        )
        
        # Use psycopg2 with temporary credentials
        conn = psycopg2.connect(
            host=redshift_conn.host,
            port=redshift_conn.port,
            dbname=redshift_conn.schema,
            user=response['DbUser'],
            password=response['DbPassword'],
            connect_timeout=30,
            sslmode='require'
        )
        
        conn.autocommit = autocommit
        logger.info("Successfully connected using IAM authentication")
        return conn

    def _get_password_connection(self, autocommit: bool = False):
        """Password authentication for MWAA 2.10.1"""
        redshift_conn = BaseHook.get_connection(self.airflow_conn_id)
        
        logger.info(f"Connecting to Redshift at {redshift_conn.host} using password")
        
        conn = psycopg2.connect(
            host=redshift_conn.host,
            port=redshift_conn.port,
            dbname=redshift_conn.schema,
            user=redshift_conn.login,
            password=redshift_conn.password,
            connect_timeout=30
        )
        
        conn.autocommit = autocommit
        logger.info("Successfully connected using password authentication")
        return conn

    def adjust_ddl_for_backup(self, ddl: str) -> Tuple[str, bool]:
        """Adjust DDL commands if backup is running"""
        ddl_upper = ddl.upper()
        skip = False
        
        if self.backup_running:
            if ddl_upper.startswith('TRUNCATE TABLE '):
                ddl = f"/* deltrunc */ DELETE FROM {ddl[15:]}"
            elif ddl_upper.startswith('TRUNCATE '):
                ddl = f"/* deltrunc */ DELETE FROM {ddl[9:]}"
            elif ddl_upper.startswith(('GROOM ', 'GENERATE ')):
                ddl = f"-- backup running, skipped: {ddl}"
                skip = True
                
        return ddl, skip

    def run_shell_command(self, cmd: str) -> Tuple[int, str]:
        """Execute shell command and return result"""
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            return result.returncode, result.stdout + result.stderr
        except Exception as e:
            return -1, f"Error executing command: {e}"

    def execute_and_capture_output(self, conn, sql: str, params: Tuple = None) -> Tuple[bool, Any, str]:
        """Execute SQL and capture all output including NOTICE and INFO messages"""
        output_messages = []
        
        # Store original notice processor if it exists
        original_notice_processor = None
        if hasattr(conn, 'notices'):
            original_notice_processor = conn.notices
            conn.notices = []  # Clear existing notices
        
        # Custom notice processor to capture messages
        def notice_processor(msg):
            output_messages.append(str(msg))
        
        try:
            # Set the notice processor
            if hasattr(conn, 'set_notice_processor'):
                conn.set_notice_processor(notice_processor)
            
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                # Get results if it's a query
                sql_upper = sql.strip().upper()
                result = None
                if sql_upper.startswith(('SELECT', 'WITH', 'SHOW', 'CALL')):
                    try:
                        result = cursor.fetchall()
                    except psycopg2.ProgrammingError:
                        # Some statements don't return results
                        result = cursor.rowcount
                else:
                    result = cursor.rowcount
                
                # Commit if not in autocommit mode
                if not conn.autocommit:
                    conn.commit()
                
                # Also capture any notices stored in the connection
                if hasattr(conn, 'notices') and conn.notices:
                    for notice in conn.notices:
                        output_messages.append(str(notice))
                
                # Combine all captured messages
                output = "\n".join(output_messages)
                return True, result, output
                
        except Exception as e:
            # Capture any notices that might have been generated before the error
            if hasattr(conn, 'notices') and conn.notices:
                for notice in conn.notices:
                    output_messages.append(str(notice))
            
            error_output = "\n".join(output_messages) + f"\nERROR: {str(e)}"
            if not conn.autocommit:
                try:
                    conn.rollback()
                except:
                    pass
            return False, str(e), error_output
        finally:
            # Restore original notice processor if it existed
            if hasattr(conn, 'set_notice_processor') and original_notice_processor is not None:
                conn.set_notice_processor(original_notice_processor)
            elif hasattr(conn, 'notices') and original_notice_processor is not None:
                conn.notices = original_notice_processor

    def execute_statement(self, stmt: str, conn) -> Tuple[int, str]:
        """Execute SQL statement or shell command and capture output"""
        stmt = stmt.strip()
        
        # Handle shell commands
        if stmt.upper().startswith('SH '):
            return self.run_shell_command(stmt[3:])
        
        # Handle SQL statements with output capture
        success, result, output = self.execute_and_capture_output(conn, stmt)
        
        if success:
            # Always return the captured output, even if empty
            return 0, output
        else:
            return 1, output if output.strip() else str(result)

    def update_job_step_status(self, conn, jobid: int, jobseq: int, active: str) -> bool:
        """Update job step status if needed"""
        if active in ['S', 'O']:
            new_status = 'Y' if active == 'S' else 'N'
            update_sql = """
                UPDATE job_steps 
                SET active_indicator = %s 
                WHERE jobid = %s AND jobseq = %s
            """
            
            try:
                success, _, _ = self.execute_and_capture_output(conn, update_sql, (new_status, jobid, jobseq))
                return success
            except Exception as e:
                logger.error(f"Error updating job status: {e}")
                
        return False

    def log_job_start(self, conn, jrseq: int, jrgseq: int, jobid: int, jobseq: int, 
                     ddl: str, status: str) -> bool:
        """Log job start to job_logs table"""
        start_log_sql = """
        INSERT INTO edw.job_logs 
        (jobrunseq, jobrungrpseq, jobid, jobseq, job_name, job_disposition, 
         job_sql_code, job_message, job_ddl, job_start_time)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, %s, GETDATE())
        """
        
        try:
            success, _, _ = self.execute_and_capture_output(conn, start_log_sql, 
                                                          (jrseq, jrgseq, jobid, jobseq, self.job_name, status, ddl))
            return success
        except Exception as e:
            logger.error(f"Error logging job start: {e}")
            return False

    def log_job_result(self, conn, jrseq: int, status: str, error_code: int, error_msg: str) -> bool:
        """Log job result to job_logs table"""
        # Truncate error message to avoid column length issues
        truncated_msg = str(error_msg)[:10000] if error_msg else ""
        
        update_sql = """
        UPDATE edw.job_logs 
        SET job_end_time = GETDATE(), 
            job_disposition = %s,
            job_sql_code = %s,
            job_message = %s
        WHERE jobrunseq = %s
        """
        
        try:
            success, _, _ = self.execute_and_capture_output(conn, update_sql, (status, error_code, truncated_msg, jrseq))
            return success
        except Exception as e:
            logger.error(f"Error logging job result: {e}")
            return False

    def execute_job_step(self, main_conn, jobid: int, jobseq: int, job_ddl: str, 
               active: str, retries: int, jrgseq: int) -> bool:
        """Execute a single job step with proper connection handling"""
        jrseq = None
        
        try:
            # Adjust DDL for backup if needed
            adjusted_ddl, skip = self.adjust_ddl_for_backup(job_ddl)
            
            # Update job status if needed
            self.update_job_step_status(main_conn, jobid, jobseq, active)
            
            # Get sequence using separate autocommit connection ← FIXED
            seq_conn = self.get_redshift_connection(autocommit=True)
            success, result, _ = self.execute_and_capture_output(seq_conn, "CALL get_next_seqval('SQ_JOBRUN')")
            seq_conn.close()
            
            if not success:
                logger.error(f"Failed to get next sequence: {result}")
                return False
                
            jrseq = result[0][0] if result and isinstance(result, list) and len(result) > 0 else 1
            
            # Log start
            start_status = 'SKIPPED' if skip else 'STARTED'
            if not self.log_job_start(main_conn, jrseq, jrgseq, jobid, jobseq, adjusted_ddl, start_status):
                logger.error("Failed to log job start")
                return False
            
            if skip:
                result_code, result_msg = 0, "Skipped due to backup"
                step_success = True
            else:
                # Execute with retry logic
                attempt = 0
                step_success = False
                result_code, result_msg = 1, "No execution attempted"
                
                while attempt <= retries and not step_success:
                    # Dynamic detection of statements requiring autocommit
                    sql_upper = adjusted_ddl.upper()
                    
                    # Check for statements that need autocommit
                    needs_autocommit = (
                        'COMMIT' in sql_upper or
                        'TRUNCATE' in sql_upper or
                        'VACUUM' in sql_upper or
                        'ALTER TABLE' in sql_upper or
                        sql_upper.strip().startswith('CALL')
                    )
                    
                    # Use appropriate connection
                    if needs_autocommit:
                        # Create a separate autocommit connection
                        proc_conn = self.get_redshift_connection(autocommit=True)
                        result_code, result_msg = self.execute_statement(adjusted_ddl, proc_conn)
                        proc_conn.close()
                    else:
                        result_code, result_msg = self.execute_statement(adjusted_ddl, main_conn)
                    
                    step_success = result_code == 0
                    
                    if not step_success:
                        if attempt < retries:
                            sleep_time = 5 + (attempt * 60)
                            logger.info(f"Sleeping (retry {attempt + 1} of {retries}, {sleep_time} secs)")
                            time.sleep(sleep_time)
                            attempt += 1
                        else:
                            # AFTER ALL RETRIES FAILED - BREAK OUT OF LOOP
                            logger.error(f"Job step {jobid}-{jobseq} failed after {retries} retries")
                            step_success = False
                            break
            
            # Log result - CRITICAL FOR ERROR LOGGING
            final_status = 'SUCCESS' if step_success else 'ERROR'
            if not self.log_job_result(main_conn, jrseq, final_status, result_code, result_msg):
                logger.error("Failed to log job result")
            
            if step_success:
                # Update last run time for successful execution
                update_step_sql = """
                UPDATE job_steps 
                SET lst_run_dt = GETDATE() 
                WHERE jobid = %s AND jobseq = %s
                """
                self.execute_and_capture_output(main_conn, update_step_sql, (jobid, jobseq))
                logger.info(f"Job step {jobid}-{jobseq} completed successfully")
                
                # Log the actual output for debugging
                if result_msg and result_msg != "Success":
                    logger.info(f"Job step output: {result_msg[:500]}...")  # Log first 500 chars
            else:
                logger.error(f"Job step {jobid}-{jobseq} failed after {retries} retries: {result_msg}")
            
            return step_success
            
        except Exception as e:
            logger.error(f"Unexpected error in job step {jobid}-{jobseq}: {str(e)}")
            # Try to log the error even if execution failed
            if jrseq:
                try:
                    self.log_job_result(main_conn, jrseq, 'ERROR', 1, f"Unexpected error: {str(e)}")
                except:
                    pass
            return False

    def run(self) -> int:
        """Main execution method"""
        main_conn = None
        try:
            main_conn = self.get_redshift_connection(autocommit=True)
            
            # Use a separate autocommit connection for sequence calls
            seq_conn = self.get_redshift_connection(autocommit=True)
            success, result, _ = self.execute_and_capture_output(seq_conn, "CALL get_next_seqval('SQ_JOBRUNGRP')")
            seq_conn.close()
            
            if not success:
                logger.error(f"Failed to get group sequence: {result}")
                return 1
                
            jrgseq = result[0][0] if result and isinstance(result, list) and len(result) > 0 else 1
                
            # Get all job steps (including inactive ones for status updates)
            get_steps_sql = """
            SELECT jobid, jobseq, active_indicator, retries, job_ddl 
            FROM edw.job_steps 
            WHERE job_name = %s
            ORDER BY jobid, jobseq
            """
            
            success, steps, _ = self.execute_and_capture_output(main_conn, get_steps_sql, (self.job_name,))
            if not success:
                logger.error(f'Failed to get job steps: {steps}')
                return 1
            
            if not steps:
                logger.error(f'No job steps found for: {self.job_name}')
                return 1
            
            # Store steps for later use in update_job_step_status_after_completion
            self.steps = steps
            
            logger.info(f"Found {len(steps)} steps for job: {self.job_name}")
            
            # Print all the result of get_steps_sql
            logger.info("Job steps details:")
            for i, step in enumerate(steps, 1):
                jobid, jobseq, active_ind, retries, job_ddl = step
                logger.info(f"Step {i}: jobid={jobid}, jobseq={jobseq}, active_indicator={active_ind}, "
                        f"retries={retries}, job_ddl_length={len(job_ddl) if job_ddl else 0}")
            
            exit_code = 0
            all_success = True
            
            # Execute steps sequentially, stop on first failure
            for step in steps:
                jobid, jobseq, active_ind, retries, job_ddl = step
                
                if active_ind in ['Y', 'O']:
                    logger.info(f"Processing step: {job_ddl} --> {jobid}-{jobseq}")
                    
                    step_success = self.execute_job_step(main_conn, jobid, jobseq, job_ddl, 
                                                    active_ind, retries, jrgseq)
                    
                    if not step_success:
                        all_success = False
                        exit_code = 1
                        logger.error(f"Job step {jobid}-{jobseq} failed. Stopping further execution.")
                        break  # THIS STOPS THE DAG EXECUTION ON FIRST FAILURE
            
            # Run update_job_step_status after completion of all steps
            if all_success:
                self.update_job_step_status_after_completion(main_conn)
            
            return exit_code  # Returns 0 for success, 1 for failure
            
        except Exception as e:
            logger.error(f"Job execution failed: {str(e)}", exc_info=True)
            return 1
        finally:
            if main_conn:
                try:
                    main_conn.close()
                    logger.info("Main connection closed")
                except Exception as e:
                    logger.warning(f"Error closing main connection: {str(e)}")

    def update_job_step_status_after_completion(self, conn) -> None:
        """Update job step status after all steps complete successfully"""
        if not self.steps:
            logger.info("No steps to update after completion")
            return
        
        logger.info("Updating job step status after successful completion of all steps")
        
        for step in self.steps:
            jobid, jobseq, active_ind, retries, job_ddl = step
            
            # Only update steps that have active_indicator 'O' (one-time execution)
            if active_ind == 'S':
                success = self.update_job_step_status(conn, jobid, jobseq, active_ind)
                if success:
                    logger.info(f"Updated step for: {job_ddl} {jobid}-{jobseq} from 'S' to 'Y' after successful completion")
                else:
                    logger.warning(f"Failed to update step {jobid}-{jobseq} status after completion")

def main():
    """Main function for command-line execution"""
    if len(sys.argv) < 2:
        print("Usage: python job_runner.py <job_name> [verbose]")
        print("Example: python job_runner.py your_job_name 1")
        sys.exit(1)
    
    job_name = sys.argv[1]
    verbose = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    
    try:
        logger.info(f"\n{' JOB EXECUTION STARTED '.center(80, '=')}")
        logger.info(f"Processing job: {job_name}")
        
        runner = JobRunner(job_name, verbose)
        exit_code = runner.run()
        
        if exit_code == 0:
            message = f'Job {job_name} completed successfully'
            logger.info(message)
        else:
            message = f'Job {job_name} failed with exit code {exit_code}'
            logger.error(message)
        
        logger.info(f"\n{' JOB EXECUTION COMPLETED '.center(80, '=')}")
        sys.exit(exit_code)
            
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()