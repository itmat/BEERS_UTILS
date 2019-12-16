import subprocess
from datetime import datetime
from beers_utils.abstract_job_scheduler import AbstractJobScheduler

class SerialJobScheduler(AbstractJobScheduler):
    """
    Wrapper around running jobs serially on the local machine in a single thread.
    Provides methods for submitting and killing jobs, as well as mointoring job
    status. Note, the killing and status monitoring methods are placeholders to
    maintain compatilbility with the AbstractJobScheduler interface. The job is
    executed and completed entirely within the submission method, so there is no
    point to kill a job or check its status in another function.
    """

    def __init__(self, default_num_processors=1, default_memory_in_mb=6000):
        """
        Initialize serial scheduler. The default number of processors and amount
        of memory are ignored when submitting jobs. In some cases, these can be
        controlled by additional arguments provided to the individual pipeline
        steps.
        .

        Parameters
        ----------
        default_num_processors : int
            Default number of processors/cores to request when submitting jobs.
            This argument is ignored and retained solely for compatibility with
            the AbstractJobScheduler interface. Default: 1.
        default_memory_in_mb : int
            Default memory (in Mb) to request when submitting jobs. This argument
            is ignored and retained solely for compatibility with the
            AbstractJobScheduler interface. Default: 6000.

        """
        super().__init__(default_num_processors, default_memory_in_mb)
        # Job ID returned by submit_job() method upon successful execution of
        # the command. This also serves as a counter for the number of jobs that
        # have attempted execution
        self.serial_job_id = 0

    def check_job_status(self, job_id, additional_args=""):
        """
        Return status of given job in the serial queue. This method always reports
        a job as completed, since a job is only ever running inside the submit_job()
        method. Final verification of a step's completion status output is left
        to the class-specific is_output_valid() method.

        Parameters
        ----------
        job_id : string
            Job ID assigned by the scheduler. This argument is ignored and retained
            solely for compatibility with the AbstractJobScheduler interface.
        additional_args : string
            Additional arguments provided to the status-check command. This argument
            is ignored and retained solely for compatibility with the AbstractJobScheduler
            interface.

        Returns
        -------
        string
            COMPLETED - All serially run jobs are considered completed from the
                        scheduler's perspective, since they are executed entirely
                        within the submit_job() method.

        """

        job_status = "COMPLETED"
        return job_status

    def submit_job(self, job_command, job_name, stdout_logfile=None, stderr_logfile=None,
                   num_processors=None, memory_in_mb=None, additional_args=""):
        """
        Run a given job on the local machine.

        Parameters
        ----------
        job_command : string
            Full command to execute job when run from the command line. It should
            not contain any unix output redirection (i.e. using ">" or "2>"), but
            the method does not check this and will still run (there just won't
            be anything in stdout_logfile and stderr_logfile).
        job_name : string
            Name assigned to job by scheduler.
        stdout_logfile : string
            Full path to file where job stdout should be stored. The job's full
            command, start time, and stop time are listed here. Default: None.
        stderr_logfile : string
            Full path to file where job stderr should be stored. If a stderr log
            file isn't specified, all stderr output is redirected to the stdout
            log file (if it is also given). Default: None.
        num_processors : int
            Number of processor units to request for running the job. This argument
            is ignored and retained solely for compatibility with the
            AbstractJobScheduler interface.
        memory_in_mb : int
            Memory (in Mb) to request for running the job. This argument is ignored
            and retained solely for compatibility with the AbstractJobScheduler
            interface.
        additional_args : string
            This argument is ignored and retained solely for compatibility with
            the AbstractJobScheduler interface.
            Default: empty string.

        Returns
        -------
        string
            Unique identifier for the submitted job assigned by the serial scheduler.
            "ERROR" string indicates job submission failed.

        """
        job_id = self._get_next_job_id()
        job_start = datetime.now()

        # Assemble the command to execute
        exec_command = job_command

        # Prepare log files (if provided) and include as part of the job execution
        # command.
        if stdout_logfile:
            # exec_command += f" >> {stdout_logfile}"
            with open(stdout_logfile, 'w') as stdout_log:
                stdout_log.write(f"Job submission ID: {job_id}\n"
                                 f"Job command: {job_command}\n"
                                 f"Job start time: {job_start.strftime('%a %b %d %Y %H:%M:%S %Z')}\n")

        try:
            exec_result = subprocess.run(exec_command,
                                         shell=True, check=True, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, encoding="ascii")
        except subprocess.CalledProcessError as err:
            job_id = "ERROR"
            # Allows exeception's output to be printed to stdout and stderr
            # files below (if provided by method call).
            exec_result = err

        job_end = job_start = datetime.now()

        if stdout_logfile:
            with open(stdout_logfile, 'a') as stdout_log:

                stdout_log.write(f"Job end time: {job_end.strftime('%a %b %d %Y %H:%M:%S %Z')}\n")

                if job_id == "ERROR":
                    stdout_log.write(f"\nFAILURE - Exit code {exec_result.returncode}.\n")
                else:
                    stdout_log.write(f"\nSuccessfully completed.\n")

                if stderr_logfile:
                    stdout_log.write(f"\nFor stderr see {stderr_logfile}\n")

                stdout_log.write("Output (if any) follows:\n")
                stdout_log.write("\n------------STDOUT------------\n")
                stdout_log.write(exec_result.stdout if exec_result.stdout else "")
                if not stderr_logfile:
                    stdout_log.write("\n------------STDERR------------\n")
                    stdout_log.write(exec_result.stderr if exec_result.stderr else "")

        if stderr_logfile:
            with open(stderr_logfile, 'w') as stderr_log:
                stderr_log.write(exec_result.stderr if exec_result.stderr else "")

        return job_id

    def kill_job(self, job_id, additional_args=""):
        """
        Kill local job. This method always reports a job as successfully killed,
        since a job is only ever running inside the submit_job() method.

        Parameters
        ----------
        job_id : string
            Job ID assigned by the scheduler. This argument is ignored and retained
            solely for compatibility with the AbstractJobScheduler interface.
        additional_args : string
            Additional arguments provided to the kill command. This argument is
            ignored and retained solely for compatibility with the AbstractJobScheduler
            interface.

        Returns
        -------
        boolean
            True - All serially run jobs are killed successfully from the
                   scheduler's perspective, since they are executed entirely
                   within the submit_job() method.

        """
        kill_status = True
        return kill_status

    def _get_next_job_id(self):
        """
        Private method for incrementing and retrieving the unique job IDs tracked
        by the serial scheduler. NOTE: This is not an accessor method, as it always
        increments the current job ID before returning it. This method should only
        be used internally by the SerialJobScheduler.

        Returns
        -------
        int
            A new job ID number unique to this instance of the serial job scheduler.
        """
        self.serial_job_id += 1
        return self.serial_job_id
