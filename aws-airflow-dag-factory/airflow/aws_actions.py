"""
S3 Actions Module for Dynamic File Operations
Supports: Move files, Remove files, Copy files between S3 locations, Calling Crawlers
Called directly from Airflow tasks with parameters
"""

import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime
import re
from typing import List, Dict, Optional, Any

# Configure logging
logger = logging.getLogger(__name__)

def s3_actions(**kwargs) -> Dict[str, Any]:
    """
    Main S3 actions function that routes to specific operations based on sub_type.
    
    Expected parameters:
        - sub_type: 'remove_files', 'move_files', 'copy_files' (required)
        - source_bucket: Source S3 bucket name (required)
        - source_key: Source key/path (required, can include eff_date=current_date)
        - file_pattern: Pattern to match files (e.g., '.csv', 'data_') (required)
        - destination_bucket: Target bucket (optional, defaults to source_bucket)
        - destination_key: Target key/path (required for move/copy)
        - include_timestamp: Add timestamp to target filename (default: False)
        - delete_source: Whether to delete source after copy (default: True for move, False for copy)
        - ti: Airflow task instance for XCom pushing (optional)
    """
    try:
        # Extract required parameters
        sub_type = kwargs.get('sub_type', '').lower()
        source_bucket = kwargs.get('source_bucket')
        source_key = kwargs.get('source_key', '')
        file_pattern = kwargs.get('file_pattern', '')
        
        # Validate required parameters
        if not sub_type:
            raise ValueError("sub_type is required (remove_files, move_files, copy_files)")
        if not source_bucket:
            raise ValueError("source_bucket is required")
        if not file_pattern:
            raise ValueError("file_pattern is required")
        
        # Extract optional parameters with defaults
        destination_bucket = kwargs.get('destination_bucket', source_bucket)
        destination_key = kwargs.get('destination_key', '')
        include_timestamp = kwargs.get('include_timestamp', False)
        timestamp_format = kwargs.get('timestamp_format', '%Y%m%d_%H%M%S')
        delete_source = kwargs.get('delete_source', True if sub_type == 'move_files' else False)
        ti = kwargs.get('ti')
        
        # Validate move/copy have destination_key
        if sub_type in ['move_files', 'copy_files'] and not destination_key:
            raise ValueError(f"destination_key is required for {sub_type}")
        
        # Process dynamic date in keys
        source_key = _process_dynamic_date(source_key)
        if destination_key:
            destination_key = _process_dynamic_date(destination_key)
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Log operation details
        logger.info(f"=== S3 {sub_type.upper()} OPERATION ===")
        logger.info(f"Source: s3://{source_bucket}/{source_key}")
        if sub_type in ['move_files', 'copy_files']:
            logger.info(f"Destination: s3://{destination_bucket}/{destination_key}")
        logger.info(f"File Pattern: '{file_pattern}'")
        
        # Route to appropriate operation
        result = None
        if sub_type == 'remove_files':
            result = _remove_files(s3_client, source_bucket, source_key, file_pattern)
        elif sub_type == 'move_files':
            result = _move_files(s3_client, source_bucket, source_key, 
                                destination_bucket, destination_key, file_pattern, 
                                delete_source, include_timestamp, timestamp_format)
        elif sub_type == 'copy_files':
            result = _copy_files(s3_client, source_bucket, source_key,
                               destination_bucket, destination_key, file_pattern,
                               include_timestamp, timestamp_format)
        else:
            raise ValueError(f"Unsupported sub_type: {sub_type}. Use: remove_files, move_files, copy_files")
        
        # Add operation metadata to result
        result['operation'] = sub_type
        result['source_bucket'] = source_bucket
        result['source_key'] = source_key
        result['file_pattern'] = file_pattern
        
        if sub_type in ['move_files', 'copy_files']:
            result['destination_bucket'] = destination_bucket
            result['destination_key'] = destination_key
        
        # Push to XCom if ti is available
        if ti:
            ti.xcom_push(key=f's3_action_result', value=result)
            ti.xcom_push(key=f's3_action_status', value=result['status'])
            ti.xcom_push(key=f's3_action_message', value=result['message'])
            ti.xcom_push(key=f's3_action_files_processed', value=result['files_processed'])
        
        logger.info(f"✅ Operation completed: {result['message']}")
        return result
        
    except Exception as e:
        error_msg = f"S3 operation failed: {str(e)}"
        logger.error(error_msg)
        
        # Push error to XCom if ti is available
        if 'ti' in kwargs:
            kwargs['ti'].xcom_push(key='s3_action_error', value=error_msg)
            kwargs['ti'].xcom_push(key='s3_action_status', value='error')
        
        # Re-raise for Airflow to handle
        raise

def _process_dynamic_date(key_path: str) -> str:
    """Replace 'eff_date=current_date' with actual current date."""
    if 'eff_date=current_date' in key_path:
        current_date = datetime.now().strftime('%Y-%m-%d')
        return key_path.replace('eff_date=current_date', f'eff_date={current_date}')
    return key_path

def _list_matching_files(s3_client, bucket: str, prefix: str, pattern: str) -> List[Dict]:
    """List files in S3 bucket matching the pattern."""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Skip directory markers
                    if key.endswith('/'):
                        continue
                    
                    # Check if pattern is in the key (simple contains)
                    if pattern in key:
                        files.append({
                            'key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
        
        logger.info(f"Found {len(files)} files matching pattern '{pattern}' in s3://{bucket}/{prefix}")
        return files
        
    except ClientError as e:
        logger.error(f"Error listing files: {str(e)}")
        return []

def _remove_files(s3_client, bucket: str, prefix: str, pattern: str) -> Dict:
    """Remove files from S3 bucket."""
    files = _list_matching_files(s3_client, bucket, prefix, pattern)
    
    if not files:
        return {
            'status': 'success',
            'message': f'No files found matching pattern "{pattern}" to remove',
            'files_processed': 0,
            'deleted_count': 0,
            'deleted_files': []
        }
    
    deleted_files = []
    failed_files = []
    
    for file_info in files:
        try:
            s3_client.delete_object(Bucket=bucket, Key=file_info['key'])
            deleted_files.append(file_info['key'])
            logger.info(f"Deleted: s3://{bucket}/{file_info['key']}")
        except ClientError as e:
            failed_files.append({'key': file_info['key'], 'error': str(e)})
            logger.error(f"Failed to delete {file_info['key']}: {str(e)}")
    
    return {
        'status': 'success' if not failed_files else 'partial',
        'message': f"Deleted {len(deleted_files)} of {len(files)} files",
        'files_processed': len(files),
        'deleted_count': len(deleted_files),
        'failed_count': len(failed_files),
        'deleted_files': deleted_files,
        'failed_files': failed_files if failed_files else None
    }

def _move_files(s3_client, src_bucket: str, src_prefix: str, 
                tgt_bucket: str, tgt_prefix: str, pattern: str,
                delete_source: bool, include_timestamp: bool,
                timestamp_format: str) -> Dict:
    """Move files from source to target S3 location."""
    files = _list_matching_files(s3_client, src_bucket, src_prefix, pattern)
    
    if not files:
        return {
            'status': 'success',
            'message': f'No files found matching pattern "{pattern}" to move',
            'files_processed': 0,
            'moved_count': 0,
            'moved_files': []
        }
    
    timestamp = datetime.now().strftime(timestamp_format) if include_timestamp else None
    
    moved_files = []
    failed_files = []
    
    for file_info in files:
        source_key = file_info['key']
        
        # Determine target key
        if tgt_prefix.endswith('/'):
            filename = source_key.split('/')[-1]
            
            if include_timestamp and timestamp:
                name_parts = filename.rsplit('.', 1)
                if len(name_parts) == 2:
                    filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
                else:
                    filename = f"{filename}_{timestamp}"
            
            target_key = f"{tgt_prefix}{filename}"
        else:
            target_key = tgt_prefix
            
            if include_timestamp and timestamp:
                name_parts = target_key.rsplit('.', 1)
                if len(name_parts) == 2:
                    target_key = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
                else:
                    target_key = f"{target_key}_{timestamp}"
        
        try:
            # Copy to target
            copy_source = {'Bucket': src_bucket, 'Key': source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=tgt_bucket,
                Key=target_key
            )
            logger.info(f"Copied: s3://{src_bucket}/{source_key} -> s3://{tgt_bucket}/{target_key}")
            
            # Delete source if requested
            if delete_source:
                s3_client.delete_object(Bucket=src_bucket, Key=source_key)
                logger.info(f"Deleted source: s3://{src_bucket}/{source_key}")
            
            moved_files.append({
                'source': f"s3://{src_bucket}/{source_key}",
                'target': f"s3://{tgt_bucket}/{target_key}"
            })
            
        except ClientError as e:
            failed_files.append({
                'source': f"s3://{src_bucket}/{source_key}",
                'error': str(e)
            })
            logger.error(f"Failed to move {source_key}: {str(e)}")
    
    return {
        'status': 'success' if not failed_files else 'partial',
        'message': f"Moved {len(moved_files)} of {len(files)} files",
        'files_processed': len(files),
        'moved_count': len(moved_files),
        'failed_count': len(failed_files),
        'moved_files': moved_files,
        'failed_files': failed_files if failed_files else None
    }

def _copy_files(s3_client, src_bucket: str, src_prefix: str,
                tgt_bucket: str, tgt_prefix: str, pattern: str,
                include_timestamp: bool, timestamp_format: str) -> Dict:
    """Copy files without deleting source."""
    return _move_files(s3_client, src_bucket, src_prefix, tgt_bucket, tgt_prefix,
                      pattern, delete_source=False, include_timestamp=include_timestamp,
                      timestamp_format=timestamp_format)


def start_crawler(crawler_name, aws_conn_id='aws_default', wait_for_completion=True, **kwargs):
    """
    Start an AWS Glue crawler and optionally wait for completion.
    
    Args:
        crawler_name (str): Name of the Glue crawler to start
        aws_conn_id (str): AWS connection ID (default: 'aws_default')
        wait_for_completion (bool): Whether to wait for crawler to complete (default: True)
        **kwargs: Additional arguments including task instance
    """
    try:
        from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
        
        logging.info(f"🚀 Starting crawler: {crawler_name}")
        
        crawler_hook = GlueCrawlerHook(aws_conn_id=aws_conn_id)
        
        # Start the crawler
        crawler_hook.start_crawler(crawler_name)
        
        # Wait for crawler to complete if requested
        if wait_for_completion:
            crawler_hook.wait_for_crawler_completion(crawler_name)
            logging.info(f"✅ Crawler {crawler_name} completed successfully")
        else:
            logging.info(f"✅ Crawler {crawler_name} started successfully")
        
        # Push to XCom if task instance is available
        if 'ti' in kwargs:
            kwargs['ti'].xcom_push(key='crawler_name', value=crawler_name)
            kwargs['ti'].xcom_push(key='crawler_status', value='completed' if wait_for_completion else 'started')
        
        return f"Crawler {crawler_name} {'completed' if wait_for_completion else 'started'}"
        
    except Exception as e:
        logging.error(f"❌ Crawler execution failed: {str(e)}")
        raise