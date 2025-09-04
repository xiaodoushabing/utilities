from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from ..._aux import retry_args


class HiveDataManager:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def query(
            self,
            query: str
        ) -> DataFrame:
        """Internal method. Queries data from a Hive table."""
        data = self.spark_session.sql(query)
        return data

    @retry_args
    def write(
        self,
        data: DataFrame,
        table: str,
        database: str,
        mode: str,
        n_partitions: Optional[int] = None,
        partition_col: Optional[str] = None
    ) -> None:
        """Internal method. Writes data to a Hive table."""
        table_name = f"{database}.{table}"
        if partition_col is not None:
            data.write.mode(mode.lower()).format('hive').insertInto(table_name)
        elif n_partitions is not None:
            data = data.repartition(int(n_partitions))
            data.write.mode(mode.lower()).format('hive').insertInto(table_name)

    def create(self, table: str, schema: dict, partition: dict = {'businessdate': 'STRING'}) -> None:
        """
        Create the given Hive table.
        Args:
            table (str): Name of the table to create. The format should be: <DB_NAME.TBL_NAME>.
            schema (dict): Dictionary of column types. Expected format of keys and values are strings.
            partition (dict): Partition column and type. Defaults to {businessdate: STRING}
        """
        columns = ", ".join([f"{k} {v}" for k, v in schema.items()])
        partition_cols = ', '.join([f"{k} {v}" for k, v in partition.items()])
        create_sql = f"""CREATE TABLE IF NOT EXISTS {table} ({columns}) PARTITIONED BY ({partition_cols}) STORED AS PARQUET"""
        
        self.spark_session.sql(create_sql)
        
        

    def create_from_dataframe(self, table: str, spark_df: DataFrame, partition: dict = {'businessdate': 'STRING'}) -> None:
        """
        Create a Hive table from a Spark DataFrame by automatically extracting the schema.
        
        Args:
            table (str): Name of the table to create. The format should be: <DB_NAME.TBL_NAME>.
            spark_df (DataFrame): Spark DataFrame to extract schema from.
            partition (dict): Partition column and type. Defaults to {businessdate: STRING}
        """
        if not isinstance(spark_df, DataFrame):
            raise TypeError(f"spark_df must be a Spark DataFrame, got {type(spark_df)}")
        
        schema = self.extract_schema_from_spark_df(spark_df)
        self.create(table, schema, partition)

    def check_hive_connectivity(self, cleanup=True) -> bool:
        """
        Check if Hive is properly configured and accessible.
        
        Returns:
            bool: True if Hive is accessible, False otherwise
            db_names (list or None): List of database names if accessible, None otherwise
        """
        try:
            # Check catalog implementation
            catalog_impl = self.spark_session.conf.get("spark.sql.catalogImplementation")
            print(f"Catalog implementation: {catalog_impl}")
            
            if catalog_impl != "hive":
                print(f"Warning: Catalog implementation is '{catalog_impl}', not 'hive'")
                return False, None
            
            # Check if Hive support is enabled
            try:
                hive_support = self.spark_session.conf.get("spark.sql.hive.metastore.jars", None)
                print(f"Hive metastore jars config: {hive_support}")
            except Exception as e:
                print(f"Could not check Hive metastore configuration: {e}")
            
            # Check if we can access databases
            databases_df = self.spark_session.sql("SHOW DATABASES")
            databases = databases_df.collect()
            
            # Handle different column names across Spark versions
            if len(databases) > 0:
                first_row = databases[0]
                if hasattr(first_row, 'databaseName'):
                    db_names = [row.databaseName for row in databases]
                elif hasattr(first_row, 'database_name'):
                    db_names = [row.database_name for row in databases]
                elif hasattr(first_row, 'namespace'):
                    db_names = [row.namespace for row in databases]
                else:
                    # Fallback: get the first column value
                    db_names = [row[0] for row in databases]
                print(f"Available databases: {db_names}")
            else:
                print("No databases found")
            
            # Try to create a simple test table with a more basic approach
            test_table = "test_hive_connectivity_temp"
            
            # First try with a simpler CREATE TABLE statement
            try:
                simple_test_sql = f"""
                CREATE TABLE IF NOT EXISTS {test_table} (
                    test_col STRING
                )
                """
                if self.spark_session.sql(simple_test_sql):
                    print("Simple test table creation successful")

                    if cleanup:  # Clean up
                        self.spark_session.sql(f"DROP TABLE IF EXISTS {test_table}")
                    print("Hive connectivity check passed")
                return True, db_names
                
            except Exception as create_error:
                print(f"Failed to create simple table: {create_error}")
                
        except Exception as e:
            print(f"Hive connectivity issue: {e}")
            print("This error suggests that Hive support is not properly enabled.")
            print("Make sure the SparkSession was created with .enableHiveSupport()")
            return False, None

    @retry_args
    def delete(self, table: str) -> None:
        """
        Delete data from the given Hive table.
        Args:
            table (str): Name of the table to delete. The format should be: <DB_NAME.TBL_NAME>.
        """
        self.spark_session.sql(f"DROP TABLE IF EXISTS {table} PURGE")