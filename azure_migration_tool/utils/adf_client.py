# Author: Satish Ch@uhan

"""
Azure Data Factory (ADF) Client
Handles authentication, pipeline triggering, and status monitoring.
"""

import json
import time
from typing import Dict, Optional, Callable, List, Any, Tuple, Set
from datetime import datetime, timedelta, timezone

try:
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import (
        CreateRunResponse,
        PipelineRun,
        RunFilterParameters,
        RunQueryFilter,
    )
    AZURE_SDK_AVAILABLE = True
except ImportError:
    AZURE_SDK_AVAILABLE = False
    RunFilterParameters = None  # type: ignore
    RunQueryFilter = None  # type: ignore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return _utc_now()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _activity_output_as_dict(output: Any) -> Dict[str, Any]:
    if output is None:
        return {}
    if isinstance(output, dict):
        return output
    if hasattr(output, "as_dict"):
        try:
            return output.as_dict()
        except Exception:
            pass
    return {}


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
            credentials: Azure credentials (optional; shared interactive credential if None)
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
        
        # Initialize credentials (reuse one InteractiveBrowserCredential app-wide — see azure_shared_credential)
        if credentials is None:
            from azure_migration_tool.utils.azure_shared_credential import (
                get_shared_azure_credential,
            )

            self.credentials = get_shared_azure_credential(self.logger)
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
    
    def _query_activity_runs(
        self,
        pipeline_run_id: str,
        run_start: Optional[datetime],
        run_end: Optional[datetime],
        *,
        execute_pipeline_only: bool,
    ):
        """Query activity runs for a pipeline run (paginated when supported)."""
        start = _ensure_utc(run_start) - timedelta(minutes=5)
        end = _ensure_utc(run_end) + timedelta(minutes=5)
        if end <= start:
            end = start + timedelta(minutes=1)

        filters: List = []
        if execute_pipeline_only and RunQueryFilter is not None:
            filters = [
                RunQueryFilter(
                    operand="ActivityType",
                    operator="Equals",
                    values=["ExecutePipeline"],
                )
            ]

        def _filter_exec(rows: List[Any]) -> List[Any]:
            if not execute_pipeline_only:
                return rows
            out = []
            for row in rows:
                at = getattr(row, "activity_type", None)
                if at is None and isinstance(row, dict):
                    at = row.get("activityType")
                if at == "ExecutePipeline":
                    out.append(row)
            return out

        all_rows: List[Any] = []
        continuation = None
        while True:
            params = RunFilterParameters(
                last_updated_after=start,
                last_updated_before=end,
                continuation_token=continuation,
                filters=filters,
            )
            try:
                resp = self.client.activity_runs.query_by_pipeline_run(
                    self.resource_group,
                    self.factory_name,
                    pipeline_run_id,
                    params,
                )
            except TypeError:
                resp = self.client.activity_runs.query_by_pipeline_run(
                    self.resource_group,
                    self.factory_name,
                    pipeline_run_id,
                    start,
                    end,
                )
                chunk = getattr(resp, "value", None) or []
                chunk = _filter_exec(chunk)

                class _Legacy:
                    value = chunk

                return _Legacy()
            chunk = getattr(resp, "value", None) or []
            all_rows.extend(chunk)
            continuation = getattr(resp, "continuation_token", None)
            if not continuation:
                break

        class _Merged:
            value = all_rows

        return _Merged()

    def collect_child_pipeline_run_ids_from_master(
        self,
        master_run_id: str,
        master_run_start: Optional[datetime],
        query_end: Optional[datetime],
    ) -> Tuple[Set[str], Dict[str, str]]:
        """Return (child_run_ids, run_id -> pipeline name) from ExecutePipeline activity outputs.

        ``query_end`` should be *now* (or later) on each poll so activity outputs written after
        the master run finishes are included.
        """
        ids: Set[str] = set()
        names: Dict[str, str] = {}
        merged = self._query_activity_runs(
            master_run_id,
            master_run_start,
            query_end or _utc_now(),
            execute_pipeline_only=True,
        )
        for ar in getattr(merged, "value", None) or []:
            out = _activity_output_as_dict(getattr(ar, "output", None))
            cid = out.get("pipelineRunId") or out.get("pipeline_run_id")
            if not cid:
                continue
            cid = str(cid).strip()
            if not cid:
                continue
            ids.add(cid)
            pname = out.get("pipelineName") or out.get("pipeline_name") or "?"
            names[cid] = str(pname)
        return ids, names

    def wait_for_all_child_pipelines(
        self,
        master_run_id: str,
        master_run_start: Optional[datetime],
        poll_interval: int = 10,
        timeout_seconds: Optional[int] = None,
        callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        After the master pipeline run completes, discover child runs from ExecutePipeline
        activity outputs and poll until each child reaches a terminal status.

        Use when ``waitOnCompletion`` is false on ExecutePipeline: the master can show
        Succeeded while copies are still running; this waits for real child completion
        before post-processing (e.g. schema restore) runs.
        """
        log = callback or self.logger
        deadline = None if timeout_seconds is None else (time.time() + timeout_seconds)
        start_wall = time.time()
        last_ids: Set[str] = set()
        empty_rounds = 0
        max_empty_rounds = max(3, (30 + poll_interval - 1) // poll_interval)

        while True:
            if deadline and time.time() > deadline:
                return {
                    "ok": False,
                    "reason": "timeout waiting for child pipeline runs",
                    "children": [],
                    "child_run_ids": sorted(last_ids),
                }

            child_ids, id_to_name = self.collect_child_pipeline_run_ids_from_master(
                master_run_id, master_run_start, _utc_now()
            )

            if not child_ids:
                empty_rounds += 1
                if empty_rounds >= max_empty_rounds:
                    log(
                        "[INFO] No ExecutePipeline child runs found in activity outputs "
                        f"(after ~{empty_rounds * poll_interval}s). "
                        "If this master only uses inline activities, that is expected."
                    )
                    return {
                        "ok": True,
                        "children": [],
                        "child_run_ids": [],
                        "reason": "no_child_runs_found",
                    }
                log(
                    f"  (Waiting for ExecutePipeline activity outputs to list child runs… "
                    f"{empty_rounds}/{max_empty_rounds})"
                )
                time.sleep(poll_interval)
                continue

            empty_rounds = 0
            last_ids = set(child_ids)

            rows: List[Dict[str, Any]] = []
            all_terminal = True
            all_ok = True
            for cid in sorted(child_ids):
                try:
                    st = self.get_pipeline_run_status(cid)
                except Exception as exc:
                    all_terminal = False
                    rows.append(
                        {
                            "run_id": cid,
                            "pipeline_name": id_to_name.get(cid, "?"),
                            "status": "?",
                            "error": str(exc),
                        }
                    )
                    all_ok = False
                    continue
                status = st.get("status") or "?"
                if status not in ("Succeeded", "Failed", "Cancelled", "Canceling"):
                    all_terminal = False
                if status != "Succeeded":
                    all_ok = False
                row = {
                    "run_id": cid,
                    "pipeline_name": id_to_name.get(cid, "?"),
                    "status": status,
                    "run_start": st.get("run_start"),
                    "run_end": st.get("run_end"),
                    "duration_ms": st.get("duration_ms"),
                    "message": st.get("message"),
                }
                rows.append(row)

            lines = ["  Child pipeline runs (live from ADF):"]
            for r in rows:
                msg = r.get("message") or ""
                extra = f"  err={msg[:120]}" if msg else ""
                dur = r.get("duration_ms")
                ds = f"  duration_ms={dur}" if dur is not None else ""
                lines.append(
                    f"    • {r['pipeline_name']!r}  run={r['run_id'][:8]}…  "
                    f"status={r['status']}{ds}{extra}"
                )
            log("\n".join(lines))

            if all_terminal:
                return {
                    "ok": all_ok,
                    "children": rows,
                    "child_run_ids": sorted(child_ids),
                    "reason": None if all_ok else "one_or_more_child_pipelines_failed",
                }

            elapsed = int(time.time() - start_wall)
            log(f"  Child pipelines still running… (elapsed {elapsed}s, next poll in {poll_interval}s)")
            time.sleep(poll_interval)

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

    def get_declared_pipeline_parameters(self, pipeline_name: str) -> Dict[str, str]:
        """
        Read parameter names and default values from the pipeline definition.

        Trigger-time ``parameters`` only affect activities that reference
        ``@pipeline().parameters.<Name>``. Hardcoded activity values are unchanged.
        """
        out: Dict[str, str] = {}
        try:
            pr = self.get_pipeline(pipeline_name)
            props = getattr(pr, "properties", None)
            params = getattr(props, "parameters", None) if props is not None else None
            if not params:
                return out
            for name, spec in params.items():
                if spec is None:
                    out[name] = ""
                    continue
                dv = getattr(spec, "default_value", None)
                if dv is None and hasattr(spec, "as_dict"):
                    d = spec.as_dict()
                    dv = d.get("defaultValue")
                out[name] = "" if dv is None else str(dv)
        except Exception as e:
            self.logger(f"Could not read declared pipeline parameters: {e}")
        return out

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
            
            # Trigger pipeline (create_run is on PipelinesOperations, not PipelineRunsOperations)
            params = parameters or {}
            try:
                self.logger("create_run parameters:\n" + json.dumps(params, default=str, indent=2))
            except Exception:
                self.logger(f"create_run parameters (repr): {params!r}")

            run_response = self.client.pipelines.create_run(
                self.resource_group,
                self.factory_name,
                pipeline_name,
                parameters=params,
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
                try:
                    ar = self._query_activity_runs(
                        run_id,
                        run.run_start,
                        run.run_end or _utc_now(),
                        execute_pipeline_only=False,
                    )
                    if ar and ar.value:
                        for activity in ar.value:
                            if activity.status == 'Failed':
                                err = getattr(activity, "error", None)
                                if isinstance(err, dict):
                                    status_info['message'] = err.get('message', 'Unknown error')
                                elif err is not None and hasattr(err, "message"):
                                    status_info['message'] = getattr(err, "message", None) or str(err)
                                else:
                                    status_info['message'] = 'Unknown error'
                                break
                except Exception:
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
    
    def warn_if_execute_pipeline_does_not_wait_for_children(self, pipeline_name: str) -> None:
        """Log a clear warning when ExecutePipeline uses waitOnCompletion=false.

        In that mode the *master* run can show Succeeded as soon as all child runs are
        *queued*, while copies are still running. Phase 3 post-restore then applies
        indexes/constraints to the whole DB too early.
        """
        try:
            pl = self.get_pipeline(pipeline_name)
            if hasattr(pl, "as_dict"):
                root = pl.as_dict()
            else:
                return
        except Exception as e:
            self.logger(f"(Could not inspect pipeline for waitOnCompletion: {e})")
            return

        offenders: list = []

        def walk(node):
            if isinstance(node, dict):
                if node.get("type") == "ExecutePipeline":
                    tp = node.get("typeProperties") or {}
                    if tp.get("waitOnCompletion") is False:
                        offenders.append(node.get("name") or "ExecutePipeline")
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(root)
        if not offenders:
            return
        names = ", ".join(offenders)
        self.logger(
            "[INFO] ExecutePipeline with waitOnCompletion=false: "
            f"{names}. "
            "This tool will still wait for discovered child pipeline runs (from activity "
            "outputs) to finish before post-data restore."
        )

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
