from airflow.plugins_manager import AirflowPlugin
from glue_plugin.operators.glue_job_operator import GlueJobOperator
from glue_plugin.operators.glue_crawler_operator import GlueCrawlerOperator


class GluePlugin(AirflowPlugin):
    name = 'glue_plugin'
    hooks = []
    operators = [GlueJobOperator, GlueCrawlerOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []