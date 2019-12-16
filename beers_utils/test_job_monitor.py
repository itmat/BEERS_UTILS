import unittest

from beers_utils.job_monitor import JobMonitor, Job, JobMonitorException
from beers_utils.sample import Sample
from beers_utils.abstract_pipeline_step import AbstractPipelineStep

class TestJobMonitor(unittest.TestCase):
    """Unit tests for assorted job monitor functionality. Note, none of these
    tests actually submit jobs or require a job monitoring system to be installed
    on the system where these tests are running.

    Run the following command from the main BEERS_UTILS directory:
    python -m unittest -v beers_utils/test_job_monitor.py

    """

    def setUp(self):
        self.test_monitor = JobMonitor("./", "serial")
        # Replacing the existing scheduler means I don't need to register this
        # testing scheduler with job_scheduler_provider.
        self.test_monitor.scheduler_name = "TestingScheduler"
        self.test_monitor.job_scheduler = TestingScheduler()
        self.testing_step_classname = "TestingStep"
        """
            def test_Monitor_invalid_scheduler(self):
                with self.assertRaisesRegex(JobMonitorException, "ERROR: .* is not a supported scheduler."):
                    new_test_monitor = JobMonitor("./", "NotaScheduler")
        """

    def test_Monitor_add_valid_pipeline_step(self):
        test_monitor = self.test_monitor
        valid_testing_step = TestingStep
        test_monitor.add_pipeline_step(self.testing_step_classname, valid_testing_step)
        self.assertTrue(test_monitor.pipeline_steps[self.testing_step_classname] == valid_testing_step)

    def test_Monitor_add_invalid_pipeline_step(self):
        test_monitor = self.test_monitor
        invalid_testing_step = dict
        with self.assertRaisesRegex(JobMonitorException, "could not be added to pipeline"):
            test_monitor.add_pipeline_step(self.testing_step_classname, invalid_testing_step)

    def test_Monitor_get_valid_pipeline_step(self):
        test_monitor = self.test_monitor
        testing_step = TestingStep()
        test_monitor.pipeline_steps[self.testing_step_classname] = testing_step
        retreived_step = test_monitor.get_pipeline_step(self.testing_step_classname)
        self.assertTrue(retreived_step == testing_step)

    def test_Monitor_get_invalid_pipeline_step(self):
        test_monitor = self.test_monitor
        testing_step = TestingStep()
        test_monitor.pipeline_steps[self.testing_step_classname] = testing_step
        step_not_in_pipeline = "Not" + self.testing_step_classname
        retreived_step = test_monitor.get_pipeline_step(step_not_in_pipeline)
        self.assertTrue(retreived_step is None)

    def test_Monitor_has_pipeline_step(self):
        test_monitor = self.test_monitor
        testing_step = TestingStep()
        test_monitor.pipeline_steps[self.testing_step_classname] = testing_step
        self.assertTrue(test_monitor.has_pipeline_step(self.testing_step_classname))

    def test_Monitor_does_not_have_pipeline_step(self):
        test_monitor = self.test_monitor
        testing_step = TestingStep()
        test_monitor.pipeline_steps[self.testing_step_classname] = testing_step
        step_not_in_pipeline = "Not" + self.testing_step_classname
        self.assertFalse(test_monitor.has_pipeline_step(step_not_in_pipeline))

    def test_Job_check_job_status_completed(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_step_object.execute(will_pass=True)
        test_job_status = "COMPLETED"
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "COMPLETED")

    # Mimics a job that completed it's execution according the the scheduler, but
    # ultimately fails because it's output is not valid by the step_class.is_output_valid()
    # output.
    def test_Job_check_job_status_fail_by_invalid_output(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_step_object.execute(will_pass=False)
        test_job_status = "COMPLETED"
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "FAILED")

    # Mimics a job that fails according to the job scheduler.
    def test_Job_check_job_status_fail_by_scheduler(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_step_object.execute(will_pass=True)
        test_job_status = "FAILED"
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "FAILED")

    def test_Job_check_job_status_waiting(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_job_status = None
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None", validation_attributes="None",
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "WAITING_FOR_DEPENDENCY")

    # Mimic job that has a submitted status because scheduler reports job is pending.
    def test_Job_check_job_status_submitted_pending_status(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_job_status = "PENDING"
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None", validation_attributes="None",
                       output_directory_path="",
                       system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "SUBMITTED")

    # Mimic job that has a submitted status because scheduler reports job is running.
    def test_Job_check_job_status_submitted_running_status(self):
        test_monitor = self.test_monitor
        test_step_object = TestingStep()
        test_job_status = "RUNNING"
        test_job = Job(job_id="1", job_command="", sample_id="1",
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None", validation_attributes="None",
                       output_directory_path="",
                       system_id=test_job_status,
                       dependency_list=[])
        self.assertTrue(test_job.check_job_status(pipeline_step=test_step_object,
                                                  scheduler=test_monitor.job_scheduler) == "SUBMITTED")


    def test_Monitor_submit_new_job(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.submit_new_job(job_id=test_job.job_id,
                                    job_command=test_job.job_command,
                                    sample=test_sample,
                                    step_name=test_job.step_name,
                                    scheduler_arguments=test_job.scheduler_arguments,
                                    validation_attributes=test_job.validation_attributes,
                                    output_directory_path=test_job.output_directory,
                                    system_id=test_job.system_id)
        # Comparing the reprs is a quick way to test for equality across all of
        # the job objects' attributes.
        self.assertTrue(repr(test_monitor.pending_list[test_job.job_id]) == repr(test_job))

    # The step name associated with the submitted job is not in the list of
    # pipeline steps tracked by the job monitor
    def test_Monitor_submit_new_job_step_not_in_pipline(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        step_not_in_pipeline = "Not" + self.testing_step_classname
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=step_not_in_pipeline, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        with self.assertRaisesRegex(JobMonitorException,
                                    "Could not add job .* to the scheduler because "
                                    "its associated pipeline step (.*) is not "
                                    "currently tracked by the job monitor"):
            test_monitor.submit_new_job(job_id=test_job.job_id,
                                        job_command=test_job.job_command,
                                        sample=test_sample,
                                        step_name=test_job.step_name,
                                        scheduler_arguments=test_job.scheduler_arguments,
                                        validation_attributes=test_job.validation_attributes,
                                        output_directory_path=test_job.output_directory,
                                        system_id=test_job.system_id)

    def test_Monitor_submit_pending_job(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        test_monitor.submit_pending_job(test_job.job_id)
        # Comparing the reprs is a quick way to test for equality across all of
        # the job objects' attributes.
        self.assertTrue(repr(test_monitor.running_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.pending_list)

    # Job ID of pending job marked for submission is not in the pending queue
    def test_Monitor_submit_pending_job_not_in_pending_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        job_id_not_in_pending = "Not" + str(test_job.job_id)
        with self.assertRaisesRegex(JobMonitorException, "Job missing from the list of pending jobs"):
            test_monitor.submit_pending_job(job_id_not_in_pending)

    def test_Monitor_submit_pending_already_in_running_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.running_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Job is already in the list of running jobs "
                                    "or jobs marked for resubmission."):
            test_monitor.submit_pending_job(test_job.job_id)

    def test_Monitor_submit_pending_already_in_resubmission_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.resubmission_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Job is already in the list of running jobs "
                                    "or jobs marked for resubmission."):
            test_monitor.submit_pending_job(test_job.job_id)

    # The job scheduler reported an error when submitting the job
    def test_Monitor_submit_pending_scheduler_failed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname,
                       # The TestingScheduler class will mimic a submission error
                       # if the 'additional_args' parameter of the submtted Job
                       # object is set to "ERROR".
                       scheduler_arguments={'additional_args' : "ERROR"},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Job submission failed for .*. See log file "
                                    "for full details."):
            test_monitor.submit_pending_job(test_job.job_id)

    def test_Monitor_resubmit_job(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.resubmission_list[test_job.job_id] = test_job
        test_monitor.resubmit_job(test_job.job_id)
        # Comparing the reprs is a quick way to test for equality across all of
        # the job objects' attributes.
        self.assertTrue(repr(test_monitor.running_list[test_job.job_id]) == repr(test_job) and \
                        test_monitor.running_list[test_job.job_id].resubmission_counter == 1 and \
                        test_job.job_id not in test_monitor.resubmission_list)

    def test_Monitor_resubmit_job_resubmission_limit_reached(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_job.resubmission_counter = test_monitor.max_resub_limit
        test_monitor.resubmission_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "exceeded the maximum resubmission limit of"):
            test_monitor.resubmit_job(test_job.job_id)

    # Job ID marked for resubmission is not in the resubmittion queue
    def test_Monitor_resubmit_job_not_in_pending_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.resubmission_list[test_job.job_id] = test_job
        job_id_not_in_resub = "Not" + str(test_job.job_id)
        with self.assertRaisesRegex(JobMonitorException,
                                    "Resubmitted job missing from the list of jobs "
                                    "marked for resubmission"):
            test_monitor.resubmit_job(job_id_not_in_resub)

    def test_Monitor_resubmit_job_already_in_running_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.running_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Resubmitted job is already in the list of "
                                    "running or pending jobs."):
            test_monitor.resubmit_job(test_job.job_id)

    def test_Monitor_resubmit_job_already_in_pending_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Resubmitted job is already in the list of "
                                    "running or pending jobs."):
            test_monitor.resubmit_job(test_job.job_id)

    # The job scheduler reported an error when submitting the job
    def test_Monitor_resubmit_job_scheduler_failed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname,
                       # The TestingScheduler class will mimic a submission error
                       # if the 'additional_args' parameter of the submtted Job
                       # object is set to "ERROR".
                       scheduler_arguments={'additional_args' : "ERROR"},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.resubmission_list[test_job.job_id] = test_job
        with self.assertRaisesRegex(JobMonitorException,
                                    "Job submission failed for .*. See log file "
                                    "for full details."):
            test_monitor.resubmit_job(test_job.job_id)

    def test_Monitor_mark_job_completed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.running_list[test_job.job_id] = test_job
        test_monitor.mark_job_completed(test_job.job_id)
        # Comparing the reprs is a quick way to test for equality across all of
        # the job objects' attributes.
        self.assertTrue(repr(test_monitor.completed_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.running_list)

    def test_Monitor_mark_job_for_resubmission(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.running_list[test_job.job_id] = test_job
        test_monitor.mark_job_for_resubmission(test_job.job_id)
        # Comparing the reprs is a quick way to test for equality across all of
        # the job objects' attributes.
        self.assertTrue(repr(test_monitor.resubmission_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.running_list)

    def test_Monitor_is_processing_complete_no_jobs(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        self.assertTrue(test_monitor.is_processing_complete())

    def test_Monitor_is_processing_complete_jobs_in_completed_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the submitted job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.completed_list[test_job.job_id] = test_job
        self.assertTrue(test_monitor.is_processing_complete())

    def test_Monitor_is_processing_complete_completed_job_in_running_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        test_step_object = TestingStep()
        test_step_object.execute(will_pass=True)
        test_job_status = "COMPLETED"
        # Contruct what the completed job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        test_monitor.running_list[test_job.job_id] = test_job
        self.assertTrue(test_monitor.is_processing_complete() and \
                        repr(test_monitor.completed_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.running_list)

    def test_Monitor_is_processing_complete_failed_job_in_running_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        test_step_object = TestingStep()
        test_job_status = "FAILED"
        # Contruct what the completed job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        test_monitor.running_list[test_job.job_id] = test_job
        self.assertTrue(test_monitor.is_processing_complete() is False and \
                        repr(test_monitor.resubmission_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.running_list)

    def test_Monitor_is_processing_complete_running_job_in_running_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        test_step_object = TestingStep()
        test_job_status = "RUNNING"
        # Contruct what the completed job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname,
                       scheduler_arguments="None",
                       validation_attributes=test_step_object.get_validation_attributes(),
                       output_directory_path="", system_id=test_job_status,
                       dependency_list=[])
        test_monitor.running_list[test_job.job_id] = test_job
        self.assertTrue(test_monitor.is_processing_complete() is False and \
                        repr(test_monitor.running_list[test_job.job_id]) == repr(test_job) and \
                        test_job.job_id not in test_monitor.resubmission_list)

    def test_Monitor_is_processing_complete_job_in_pending_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        self.assertFalse(test_monitor.is_processing_complete())

    def test_Monitor_is_processing_complete_job_in_resubmission_queue(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.resubmission_list[test_job.job_id] = test_job
        self.assertFalse(test_monitor.is_processing_complete())

    def test_Monitor_are_dependencies_satisfied_no_dependencies(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=None)
        test_monitor.pending_list[test_job.job_id] = test_job
        self.assertTrue(test_monitor.are_dependencies_satisfied(test_job.job_id))

    def test_Monitor_are_dependencies_satisfied_all_completed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Construct two dependencies
        dep_job_1 = Job(job_id="Dependency_1", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        dep_job_2 = Job(job_id="Dependency_2", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=[dep_job_1.job_id, dep_job_2.job_id])
        test_monitor.pending_list[test_job.job_id] = test_job
        test_monitor.completed_list[dep_job_1.job_id] = dep_job_1
        test_monitor.completed_list[dep_job_2.job_id] = dep_job_2
        self.assertTrue(test_monitor.are_dependencies_satisfied(test_job.job_id))

    def test_Monitor_are_dependencies_satisfied_some_completed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Construct two dependencies
        dep_job_1 = Job(job_id="Dependency_1", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        dep_job_2 = Job(job_id="Dependency_2", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=[dep_job_1.job_id, dep_job_2.job_id])
        test_monitor.pending_list[test_job.job_id] = test_job
        test_monitor.completed_list[dep_job_1.job_id] = dep_job_1
        test_monitor.running_list[dep_job_2.job_id] = dep_job_2
        self.assertFalse(test_monitor.are_dependencies_satisfied(test_job.job_id))

    def test_Monitor_are_dependencies_satisfied_none_completed(self):
        test_monitor = self.test_monitor
        test_monitor.pipeline_steps[self.testing_step_classname] = TestingStep()
        test_sample = Sample(sample_id=1, sample_name="1", fastq_file_paths="1",
                             adapter_sequences="1", pooled=False)
        # Construct two dependencies
        dep_job_1 = Job(job_id="Dependency_1", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        dep_job_2 = Job(job_id="Dependency_2", job_command="", sample_id=test_sample.sample_id,
                        step_name=self.testing_step_classname, scheduler_arguments={},
                        validation_attributes=None, output_directory_path="",
                        system_id=None, dependency_list=None)
        # Contruct what the pending job should look like:
        test_job = Job(job_id=1, job_command="", sample_id=test_sample.sample_id,
                       step_name=self.testing_step_classname, scheduler_arguments={},
                       validation_attributes=None, output_directory_path="",
                       system_id=None, dependency_list=[dep_job_1.job_id, dep_job_2.job_id])
        test_monitor.pending_list[test_job.job_id] = test_job
        test_monitor.pending_list[dep_job_1.job_id] = dep_job_1
        test_monitor.running_list[dep_job_2.job_id] = dep_job_2
        self.assertFalse(test_monitor.are_dependencies_satisfied(test_job.job_id))

if __name__ == '__main__':
    unittest.main()





class TestingStep(AbstractPipelineStep):
    """Dummy pipeline step class for testing purposes.

    Attributes
    ----------
    will_pass : boolean
        Determines output of is_output_valid() method.

    """

    def __init__(self):
        self.will_pass = False

    def execute(self, will_pass=False):
        self.will_pass = will_pass

    def validate(self):
        return True

    def get_validation_attributes(self):
        validation_attributes = {}
        validation_attributes["Passes"] = self.will_pass
        return validation_attributes

    @staticmethod
    def is_output_valid(validation_attributes):
        return validation_attributes.get("Passes")


class TestingScheduler:
    """Dummy scheduler class for testing purposes.

    """

    def __init__(self, default_num_processors=1, default_memory_in_mb=1):
        self.default_memory_in_mb = default_memory_in_mb
        self.default_num_processors = default_num_processors

    def check_job_status(self, job_id, additional_args=""):
        """job_id determines output status

        Returns
        -------
        string
            One of the following:
                RUNNING - according to scheduler and the job is actively running.
                PENDING - according to scheduler and the job is pending.
                FAILED - according to scheduler the job finished with error status.
                COMPLETED - according to scheduler the job finished without error status.
                ERROR - could not retrieve job status from scheduler.

        """
        job_status = "COMPLETED"

        if job_id == "RUNNING":
            job_status = "RUNNING"
        elif job_id == "PENDING":
            job_status = "PENDING"
        elif job_id == "FAILED":
            job_status = "FAILED"
        elif job_id == "ERROR":
            job_status = "ERROR"

        return job_status

    def submit_job(self, job_command="", job_name="", stdout_logfile="", stderr_logfile="",
                   num_processors=1, memory_in_mb=1, additional_args=""):
        if additional_args == "ERROR":
            return "ERROR"
        return "COMPLETED"

    def kill_job(self, job_id, additional_args=True):
        return additional_args
