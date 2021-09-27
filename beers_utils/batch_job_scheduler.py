import boto3
import os

from typing import Union
from .abstract_job_scheduler import AbstractJobScheduler


class BatchJobScheduler(AbstractJobScheduler):
    """
    Provides methods for submitting, monitoring, and terminating jobs using AWS Batch.
    """

    def __init__(
        self,
        default_num_processors: int = 1,
        default_memory_in_mb: int = 6000,
        queue: str = os.environ["JOB_QUEUE_ARN"],
        worker: str = os.environ["WORKER_JOB_DEFINITION_NAME"],
    ):
        super().__init__(default_num_processors, default_memory_in_mb)

        self.batch = boto3.client("batch")
        self.queue = queue
        self.worker = worker

    def check_job_status(
        self, job_id: str, additional_args: str = ""
    ) -> Union["PENDING", "RUNNING", "COMPLETED", "FAILED", "ERROR"]:
        try:
            job_status = self.batch.describe_jobs(jobs=[job_id])["jobs"][0]["status"]
        except:
            return "ERROR"

        if job_status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING"]:
            return "PENDING"

        if job_status == "RUNNING":
            return "RUNNING"

        if job_status == "SUCCEEDED":
            return "COMPLETED"

        if job_status == "FAILED":
            return "FAILED"

        return "ERROR"

    def submit_job(
        self,
        job_command: str,
        job_name: str,
        stdout_logfile: str = "",
        stderr_logfile: str = "",
        num_processors: Union[int, None] = None,
        memory_in_mb: Union[int, None] = None,
        additional_args: str = "",
    ) -> Union[str, "ERROR"]:
        """
        Returns the job ID of the submitted job or "ERROR" string indicating job submission failed.
        """

        try:
            return self.batch.submit_job(
                jobName=job_name,
                jobQueue=self.queue,
                jobDefinition=self.worker,
                containerOverrides={
                    "command": ["python3", "worker", job_command],
                    "environment": [
                        {"name": "STDOUT_LOG", "value": stdout_logfile},
                        {"name": "STDERR_LOG", "value": stderr_logfile},
                    ],
                    "resourceRequirements": [
                        {
                            "type": "MEMORY",
                            "value": str(memory_in_mb or self.default_memory_in_mb),
                        },
                        {
                            "type": "VCPU",
                            "value": str(num_processors or self.default_num_processors),
                        },
                    ],
                },
            )["jobId"]
        except:
            return "ERROR"

    def kill_job(self, job_id: str, additional_args: str = "") -> bool:
        try:
            self.batch.terminate_job(jobId=job_id, reason="N/A")
        except:
            return False  # termination request failed
        else:
            return True  # termination request executed successfully
