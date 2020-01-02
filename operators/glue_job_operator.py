import time

from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueJobOperator(BaseOperator):
    """This operator allows Airflow to trigger Glue jobs and polls for their completion.

    The GlueJobOperator will error when the Glue Job failed completion.

    Args:
        job_name (string): Name of the AWS Glue job to run.
        aws_conn_id (string): AWS connection used. Defaults to None.
        region_name (string): Region in which to run the Glue Job. Defaults to None.
        arguments (dict): Dictionary of arguments to pass to the Glue Job. Defaults to None.
        polling interval (int): Number of seconds between glue job polls. Defaults to 60.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments. See BaseOperator for more info.
    """

    @apply_defaults
    def __init__(
        self,
        job_name,
        aws_conn_id=None,
        region_name=None,
        arguments=None,
        polling_interval=60,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.job_name = job_name
        self.arguments = arguments
        self.polling_interval = polling_interval

    def get_hook(self):
        """get_hook returns a connection to AWS Glue.

        Returns:
            AWSGlueCatalogHook: A connection object to AWS Glue.
        """
        return AwsGlueCatalogHook(
            aws_conn_id=self.aws_conn_id, region_name=self.region_name
        )

    def execute(self, context):
        """Starts a glue job and polls for its completion.

        Args:
            context: unused, needed to implement BaseOperator interface.
        """
        glue_hook = self.get_hook()
        conn = glue_hook.get_conn()

        self.log.info("Starting job: {}".format(self.job_name))
        response = conn.start_job_run(JobName=self.job_name, Arguments=self.arguments)

        job_run_id = response["JobRunId"]
        self.log.info(
            "started job: {}, job_run id: {}".format(self.job_name, job_run_id)
        )

        self.__poll_job_completion(conn, job_run_id)

    def __poll_job_completion(self, conn, job_run_id):
        """Poll AWS glue for completion of the given run.

        Args:
            conn (AwsGlueCatalogHook): The AWS Glue connection object.
            job_run_id (str): Id of the job to poll.

        Raises:
            AirflowException: Errors when the job did not succeed.
        """
        self.log.info("start polling glue job completion")
        state = conn.get_job_run(JobName=self.job_name, RunId=job_run_id)
        status = state["JobRun"]["JobRunState"]
        while status != "STOPPED" and status != "SUCCEEDED":
            self.log.info("Current job state: {}".format(status))
            time.sleep(self.polling_interval)
            state = conn.get_job_run(JobName=self.job_name, RunId=job_run_id)
            status = state["JobRun"]["JobRunState"]

        if status == "STOPPED":
            msg = state["JobRun"]["ErrorMessage"]
            raise AirflowException("Glue threw an error: {}".format(msg))

        self.log.info("Glue job run {} succeeded".format(job_run_id))
