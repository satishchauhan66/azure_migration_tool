# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Azure Data Factory (ADF) Client
Handles authentication, pipeline triggering, and status monitoring.
"""

import time
from typing import Dict, Optional, Callable, List
from datetime import datetime

try:
    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import PipelineRun, CreateRunResponse
    AZURE_SDK_AVAILABLE = True
except ImportError:
    AZURE_SDK_AVAILABLE = False


class ADFClient:
    """Client for interacting with Azure Data Factory."""
    
    def __init__(self, factory_name: str, resource_group: str, subscription_id: Optional[str] = None, 
                 credentials=None, logger: Optional[Callable] = None):
        """
        Initialize ADF client.
        
        Args:
            factory_name: Name of the ADF factory
            resource_group: Resource group containing the factory
            subscription_id: Azure subscription ID (optional, can be auto-detected)
            credentials: Azure credentials (optional, will use DefaultAzureCredential if None)
            logger: Optional logging callback function(message: str)
        """
        if not AZURE_SDK_AVAILABLE:
            raise ImportError(
                "Azure SDK packages not installed. Install with: "
                "pip install azure-identity azure-mgmt-datafactory"
            )
        
        self.factory_name = factory_name
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        self.logger = logger or (lambda msg: None)
        
        # Initialize credentials
        if credentials is None:
            # Try interactive browser credential first (supports MFA)
            try:
                self.credentials = InteractiveBrowserCredential()
                self.logger("Using Interactive Browser Credential (supports MFA)")
            except Exception:
                # Fall back to DefaultAzureCredential
                self.credentials = DefaultAzureCredential()
                self.logger("Using Default Azure Credential")
        else:
            self.credentials = credentials
        
        # Initialize Data Factory client
        if subscription_id:
            self.client = DataFactoryManagementClient(self.credentials, subscription_id)
        else:
            # Try to get subscription from credentials
            try:
                from azure.mgmt.resource import ResourceManagementClient
                resource_client = ResourceManagementClient(self.credentials, "")
                subscriptions = list(resource_client.subscriptions.list())
                if subscriptions:
                    self.subscription_id = subscriptions[0].subscription_id
                    self.logger(f"Auto-detected subscription: {self.subscription_id}")
                    self.client = DataFactoryManagementClient(self.credentials, self.subscription_id)
                else:
                    raise ValueError("No Azure subscriptions found. Please provide subscription_id.")
            except Exception as e:
                raise ValueError(f"Could not determine subscription ID: {e}. Please provide subscription_id parameter.")
    
    def list_pipelines(self) -> List[str]:
        """
        List all pipelines in the factory.
        
        Returns:
            List of pipeline names
        """
        try:
            pipelines = self.client.pipelines.list_by_factory(self.resource_group, self.factory_name)
            return [p.name for p in pipelines]
        except Exception as e:
            self.logger(f"Error listing pipelines: {e}")
            raise
    
    def pipeline_exists(self, pipeline_name: str) -> bool:
        """
        Check if a pipeline exists in the factory.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            True if pipeline exists, False otherwise
        """
        try:
            self.client.pipelines.get(self.resource_group, self.factory_name, pipeline_name)
            return True
        except Exception as e:
            if "NotFound" in str(e) or "404" in str(e):
                return False
            raise
    
    def get_pipeline(self, pipeline_name: str):
        """
        Get pipeline definition.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Pipeline object
        """
        return self.client.pipelines.get(self.resource_group, self.factory_name, pipeline_name)
    
    def trigger_pipeline(self, pipeline_name: str, parameters: Optional[Dict] = None) -> str:
        """
        Trigger a pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline to trigger
            parameters: Pipeline parameters dictionary
            
        Returns:
            Pipeline run ID
        """
        try:
            # Validate pipeline exists
            if not self.pipeline_exists(pipeline_name):
                raise ValueError(f"Pipeline '{pipeline_name}' not found in factory '{self.factory_name}'")
            
            # Trigger pipeline
            run_response = self.client.pipeline_runs.create_run(
                self.resource_group,
                self.factory_name,
                pipeline_name,
                parameters=parameters or {}
            )
            
            run_id = run_response.run_id
            self.logger(f"Pipeline '{pipeline_name}' triggered. Run ID: {run_id}")
            return run_id
            
        except Exception as e:
            self.logger(f"Error triggering pipeline: {e}")
            raise
    
    def get_pipeline_run_status(self, run_id: str) -> Dict:
        """
        Get current status of a pipeline run.
        
        Args:
            run_id: Pipeline run ID
            
        Returns:
            Dictionary with status information:
            {
                'status': 'Queued' | 'InProgress' | 'Succeeded' | 'Failed' | 'Cancelled',
                'run_start': datetime,
                'run_end': datetime (if completed),
                'duration_ms': int (if completed),
                'message': str (error message if failed)
            }
        """
        try:
            run = self.client.pipeline_runs.get(self.resource_group, self.factory_name, run_id)
            
            status_info = {
                'status': run.status,
                'run_start': run.run_start,
                'run_end': run.run_end,
                'duration_ms': run.duration_in_ms,
            }
            
            # Add error message if failed
            if run.status == 'Failed' and hasattr(run, 'message'):
                status_info['message'] = run.message
            elif run.status == 'Failed':
                # Try to get activity runs for more details
                try:
                    activity_runs = self.client.activity_runs.query_by_pipeline_run(
                        self.resource_group,
                        self.factory_name,
                        run_id,
                        run.run_start,
                        run.run_end or datetime.utcnow()
                    )
                    if activity_runs.value:
                        # Get error from first failed activity
                        for activity in activity_runs.value:
                            if activity.status == 'Failed':
                                status_info['message'] = activity.error.get('message', 'Unknown error') if activity.error else 'Unknown error'
                                break
                except:
                    pass
            
            return status_info
            
        except Exception as e:
            self.logger(f"Error getting pipeline run status: {e}")
            raise
    
    def wait_for_completion(self, run_id: str, timeout: Optional[int] = None, 
                           callback: Optional[Callable[[Dict], None]] = None,
                           poll_interval: int = 10) -> Dict:
        """
        Poll pipeline run until completion.
        
        Args:
            run_id: Pipeline run ID
            timeout: Maximum time to wait in seconds (None = no timeout)
            callback: Optional callback function(status_info: Dict) called on each poll
            poll_interval: Seconds between status checks (default: 10)
            
        Returns:
            Final status dictionary
            
        Raises:
            TimeoutError: If timeout is reached before completion
        """
        start_time = time.time()
        last_status = None
        
        while True:
            status_info = self.get_pipeline_run_status(run_id)
            current_status = status_info['status']
            
            # Call callback if provided
            if callback:
                callback(status_info)
            
            # Check if completed
            if current_status in ('Succeeded', 'Failed', 'Cancelled'):
                return status_info
            
            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Pipeline run {run_id} did not complete within {timeout} seconds")
            
            # Log status change
            if current_status != last_status:
                elapsed = int(time.time() - start_time)
                self.logger(f"Pipeline status: {current_status} (elapsed: {elapsed}s)")
                last_status = current_status
            
            # Wait before next poll
            time.sleep(poll_interval)
    
    def cancel_pipeline_run(self, run_id: str) -> bool:
        """
        Cancel a running pipeline.
        
        Args:
            run_id: Pipeline run ID
            
        Returns:
            True if cancellation was successful
        """
        try:
            # ADF doesn't have a direct cancel API, but we can check status
            status_info = self.get_pipeline_run_status(run_id)
            if status_info['status'] in ('Succeeded', 'Failed', 'Cancelled'):
                self.logger(f"Pipeline run {run_id} is already {status_info['status']}")
                return False
            
            # Note: ADF doesn't support programmatic cancellation via SDK
            # User would need to cancel via Azure Portal
            self.logger("Note: Pipeline cancellation must be done via Azure Portal")
            return False
            
        except Exception as e:
            self.logger(f"Error checking pipeline status for cancellation: {e}")
            return False
