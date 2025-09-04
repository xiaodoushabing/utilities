from pyspark.sql import SparkSession
from ..._aux import retry_args  # Uncomment and fix import if needed

class EDWDataManager:
    def __init__(
        self,
        spark_edw_credential: dict,
        spark_edw_config: dict,
        spark_session,
    ):
        self.user = spark_edw_credential['user']
        self.pwd = spark_edw_credential['password']
        self.driver = spark_edw_config['driver']
        self.url = spark_edw_config['URL']
        self.spark_session = spark_session

    # @retry_args
    def query(self, query: str):
        '''Internal method. Queries data from a EDW table.'''
        data = self.spark_session.read.format('jdbc') \
            .option('driver', self.driver) \
            .option('url', self.url) \
            .option('dbtable', f'( {query} ) AS TBL') \
            .option('user', self.user) \
            .option('password', self.pwd) \
            .load()
        return data

    # @retry_args
    def write(
        self,
        data,
        table: str,
        database: str,
        mode: str,
        n_partitions: int = 10
    ):
        '''Internal method. Writes data to a EDW table.'''
        table_name = f'{database}.{table}'
        if n_partitions > 0:
            data = data.repartition(n_partitions)
        data.write.format('jdbc') \
            .mode(mode.lower()) \
            .option('driver', self.driver) \
            .option('url', self.url) \
            .option('user', self.user) \
            .option('password', self.pwd) \
            .option('dbtable', table_name) \
            .save()