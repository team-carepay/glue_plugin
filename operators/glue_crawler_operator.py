import time

from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueCrawlerOperator(BaseOperator):
    """This operator allows Airflow to trigger Glue crawlers and polls for their completion.

    The GlueCrawlerOperator will error when the Glue crawler failed completion.

    Args:
        crawler_name (string). Name of the AWS Glue crawler to run.
        aws_conn_id (string). AWS connection used. Defaults to None.
        region_name (string). Region in which to run the Glue crawler. Defaults to None.
        polling interval (int). Number of seconds between glue crawler polls. Defaults to 60.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments. See BaseOperator for more info.
    """

    @apply_defaults
    def __init__(
        self,
        crawler_name,
        aws_conn_id=None,
        region_name=None,
        polling_interval=60,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.crawler_name = crawler_name
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
        """Starts a glue crawler and polls for its completion.

        Args:
            context: unused, needed to implement BaseOperator interface.
        """
        glue_hook = self.get_hook()
        conn = glue_hook.get_conn()

        self.log.info("Starting crawler: {}".format(self.crawler_name))
        conn.start_crawler(Name=self.crawler_name)

        self.log.info("started crawler: {}".format(self.crawler_name))

        self.__poll_crawler_completion(conn)

    def __poll_crawler_completion(self, conn):
        """Poll AWS glue for completion of the given crawler.

        Args:
            conn (AwsGlueCatalogHook): The AWS Glue connection object.

        Raises:
            AirflowException: Errors when the crawler did not succeed.
        """
        self.log.info("start polling glue crawler completion")
        state = conn.get_crawler(Name=self.crawler_name)
        status = state["Crawler"]["State"]
        while status != "READY":
            self.log.info("Current crawler state: {}".format(status))
            time.sleep(self.polling_interval)
            state = conn.get_crawler(Name=self.crawler_name)
            status = state["Crawler"]["State"]

        last_crawl_state = state["Crawler"]["LastCrawl"]["Status"]
        if last_crawl_state == "FAILED":
            msg = state["Crawler"]["LastCrawl"]["ErrorMessage"]
            raise AirflowException("Glue threw an error: {}".format(msg))
        elif last_crawl_state == "CANCELLED":
            raise AirflowException("Somebody is messing with the Glue crawlers")

        self.log.info("Glue crawler {} succeeded".format(self.crawler_name))
