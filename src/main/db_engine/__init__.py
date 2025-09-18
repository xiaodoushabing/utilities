# from .edw_engine import EDWEngine
# from .hive_engine import HiveEngine
# from .impala_engine import ImpalaEngine
# from .mongodb_engine import MongoDBEngine
# from .trino_engine import TrinoEngine
# from .yugabyte_engine import YugabyteEngine
# SparkEngine will only be imported if necessary

from ..auth.credentials import CredentialsSafeBox, get_credentials
from ..aux import retry_args, iter_update_dict

import pandas as pd
import yaml
from types import SimpleNamespace
from typing import Union, Optional

db_engine_mapping = {
	# "edw": EDWEngine,
	# "hive": HiveEngine,
	# "impala": ImpalaEngine,
	# "mongodb": MongoDBEngine,
	# "trino": TrinoEngine,
	# "yugabyte": YugabyteEngine,
	"spark": ""  # placeholder. SparkEngine will only be imported if necessary
}

supported_base = list(db_engine_mapping.keys())  # supports all engines
supported_delete = ["mongodb", "spark"]
supported_get_schema = ["spark"]
supported_to_pandas = ["spark"]
supported_to_spark = ["spark"]
supported_create = ["spark"]

class DatabaseEngine:
    """A class to manage multiple database engine connections and operations.

    This class reads connection configurations and credentials from a YAML file,
    instantiates clients for various database engines, and provides methods to
    interact with these engines.

    Attributes:
        conf (dict): Configuration dictionary loaded from the YAML file.
        user (str, optional): Username for database authentication.
        password (str, optional): Password for database authentication.
        credential_path (str, optional): Path to credential file for password extraction.
        env (str, optional): Working environment (e.g., 'dev', 'prod').
        _config_credentials (dict): Database credentials configuration.
        _config_endpoint (dict): Database endpoint configuration.
        _credentials_safe_box (CredentialsSafeBox): Instance to manage credentials securely.
        credentials (dict): Extracted credentials for database engines.
        engines (SimpleNamespace): Namespace containing instantiated database engine clients.
    """
    def __init__(
        self,
        config: str = "",
        user: Optional[str] = None,
        password: Optional[str] = None,
        credential_path: Optional[str] = None,
        env: Optional[str] = None,
        *args,
        **kwargs
    ):
        """Initialize the DatabaseEngine instance.
        Loads connection configurations and credentials from a YAML file,
        sets user credentials, initializes the credentials safe box,
        and instantiates database engine clients.
        
        Args:
            config (str): Path to the YAML configuration file.
            user (str, optional): Username for database authentication. Defaults to None.
            password (str, optional): Password for database authentication. Defaults to None.
            credential_path (str, optional): Path to credential file for password extraction. Defaults to None.
            env (str, optional): Working environment (e.g., 'dev', 'prod'). If None, attempts to auto-detect. Defaults to None.
            
        Raises:
            ValueError: If both password and credential_path are provided, or if config file is not provided.
        """
        if not config:
            raise ValueError("config file must be provided.")
            
        # Validate that only one of password or credential_path is provided
        if password is not None and credential_path is not None:
            raise ValueError("Only one of 'password' or 'credential_path' should be provided, not both.")
        
        with open(config, "r") as f:
            self.conf = yaml.safe_load(f)
            
        self.user = user
        self.password = password
        self.credential_path = credential_path
        self.ENV = env
        
        if not self.ENV:
            try:
                print("'env' is set to none. Detecting working environment ... ")
                from hydra.utils.common import check_cml_env
                self.ENV = check_cml_env()
            except Exception as e:
                raise ValueError(f"Failed to detect working environment: {e}")
            
        # grab retry config if available
        # these instance attributes will be used by the retry_args decorator
        # if keys are missing, don't assign, use default values
        self._retry_conf = self.conf.get("retry", {})
        if self._retry_conf:
            if "max_attempts" in self._retry_conf:
                self.max_attempts = self._retry_conf.get("max_attempts", None)
            if "wait" in self._retry_conf:
                self.wait = self._retry_conf.get("wait", None)

        # grab endpoints
        self._config_endpoint = self.conf["database"]["default"]
        iter_update_dict(self._config_endpoint, self.conf["database"].get(self.ENV, {}))
        
        # grab credentials
        self._config_credentials = self.conf["db_credentials"]
        self.credentials_safe_box = CredentialsSafeBox(config=self._config_credentials[self.ENV])
        self.credentials = self.credentials_safe_box._credentials

        # Handle user-provided credentials (password or credential_path)
        if self.user and (self.password or self.credential_path):
            self._handle_user_credentials()
        
        # Handle special case for yugabyte
        self._handle_yugabyte_credentials()

        self.engines = SimpleNamespace()
        self._instantiate_engines()
        
    def _handle_user_credentials(self):
        """Handle user-provided credentials (password or credential_path).
        
        Uses provided credentials for engines defined in config that have missing credentials.
        """
        password_to_use = self.password
        
        # If credential_path is provided, extract password using CredentialsSafeBox
        if self.credential_path:
            password_to_use = get_credentials(self.credential_path)
            
        # Apply to engines with missing credentials
        for engine, credentials in self.credentials.items():
            if not credentials and engine != "spark":
                print(f"Using provided user and {'credential_path' if self.credential_path else 'password'} for '{engine}' engine.")
                self.credentials[engine] = {
                    "user": self.user,
                    "password": password_to_use
                }
    
    def _handle_yugabyte_credentials(self):
        """Handle special case for yugabyte credentials.
        
        If yugabyte is commented out in config, skip entirely.
        If yugabyte has user in config but no password/credential_path, use user input.
        Otherwise, let _handle_user_credentials handle it.
        """
        # If yugabyte is commented out in credentials config, skip entirely
        if "yugabyte" not in self.credentials:
            return
            
        yugabyte_creds = self.credentials.get("yugabyte", {})
        
        # Check if yugabyte has user in config but no password/credential_path
        if yugabyte_creds.get("user") and not yugabyte_creds.get("password"):
            # If user provided credentials, use password or credential_path for yugabyte
            if self.user and (self.password or self.credential_path):
                password_to_use = self.password
                
                # If credential_path is provided, extract password using CredentialsSafeBox
                if self.credential_path:
                    password_to_use = get_credentials(self.credential_path)
                
                print(f"Using user from config and user-provided {'credential_path' if self.credential_path else 'password'} for yugabyte engine.")
                self.credentials["yugabyte"]["password"] = password_to_use
        
    def _instantiate_engines(self):
        """Instantiate database engines based on the db_engine_mapping.
        
        Skips engines if credentials are not found in the config file and
        user/password are not provided to overwrite.
        """
        # import traceback
        for engine in db_engine_mapping:
            if engine not in self.credentials:
                print(f"Warning: '{engine}' engine not found in config file. Skipping instantiation of '{engine}' engine.")
                continue
            try:
                engine_instance = self._get_engine_instance(engine)
                setattr(self.engines, engine, engine_instance)
            except Exception as e:
                print(f"Error instantiating engine '{engine}': {e}")
                # traceback.print_exc()
                
    def _get_engine_instance(self, db_engine: str):
        """Create an instance of a database engine client.
        
        Args:
            db_engine (str): The name of the database engine to instantiate.

        Returns:
            An instance of the specified database engine.
        """
        if db_engine == 'spark':
            try:
                from .spark_engine import SparkEngine
                db_engine_mapping[db_engine] = SparkEngine
            except ImportError as e:
                raise ImportError(f"{e}")
            # For Spark, pass all credentials since it needs sub-engine credentials
            return SparkEngine(
                engine_config=self._config_endpoint.get(db_engine, {}),
                all_credentials=self.credentials,  # Pass all credentials for sub-engines
            )
        # If SparkEngine is imported successfully, it is added to the mapping
        return db_engine_mapping[db_engine](
            engine_config=self._config_endpoint.get(db_engine, {}),
            credentials=self.credentials[db_engine]
        )

    def list_engines(self) -> list[str]:
        """List the names of all instantiated database engines.
        
        Returns:
            list[str]: A list of database engine names available in `self.engines`.
        
        """
        return list(vars(self.engines).keys())

    ## ==========================================================================================
    ## Database Operations
    ## ==========================================================================================

    @retry_args
    def query(
        self,
        query: str,
        engine: str,
        **kwargs,
    ) -> pd.DataFrame:
        """Execute a SQL query and return the results as a pandas DataFrame.
        
        Args:
            query (str): The SQL query to execute.
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_base:
            raise KeyError(f"Query operation is not supported for {engine}. Supported engines: {supported_base}")
        
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.query(query, **kwargs)

    @retry_args
    def execute(
        self,
        query: str,
        engine: str,
        **kwargs
    ) -> None:
        """Execute a SQL command and return the results.
        
        Args:
            query (str): The SQL command to execute.
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_base:
            raise KeyError(f"Execute operation is not supported for {engine}. Supported engines: {supported_base}")
        
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.execute(query, **kwargs)

    @retry_args
    def write(
        self,
        data: pd.DataFrame,
        engine: str,
        **kwargs
    ) -> None:
        """Write a pandas DataFrame to a database table.
        
        Args:
            data (pd.DataFrame): The DataFrame to write to the database.
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_base:
            raise KeyError(f"Write operation is not supported for {engine}. Supported engines: {supported_base}")
        
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.write(data, **kwargs)

    @retry_args
    def delete(
        self,
        engine: str,
        **kwargs
    ) -> None:
        """Drop a collection from the database. Only supported for MongoDB engine.
        
        Args:
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_delete:
            raise KeyError(f"Delete operation is not supported for {engine}. Supported engines: {supported_delete}")

        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.delete(**kwargs)

    @retry_args
    def create(
        self,
        engine: str,
        **kwargs
    ) -> None:
        """Create a table in the database.
        
        Args:
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_create:
            raise KeyError(f"Create operation is not supported for {engine}. Supported engines: {supported_create}")

        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.create(**kwargs)

    @retry_args
    def get_table_schema(
        self,
        table: str,
        database: str,
        engine: str,
        sub_engine: str,
        **kwargs
    ):
        """Get the schema of a table from the database.
        
        Args:
            table (str): The name of the table.
            database (str): The name of the database.
            engine (str): The name of the database engine to use.
            sub_engine (str): The name of the sub-engine.
        """
        engine = engine.lower()
        if engine not in supported_get_schema:
            raise KeyError(f"get_table_schema operation is not supported for {engine}. Supported engines: {supported_get_schema}")
        
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.get_table_schema(table=table, database=database, sub_engine=sub_engine, **kwargs)
    
    @retry_args
    def to_pandas(
        self,
        data,
        engine: str,
        **kwargs
    ):
        """ Convert data to a pandas DataFrame.

        Args:
            data: The data to be converted.
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_to_pandas:
            raise KeyError(f"to_pandas operation is not supported for {engine}. Supported engines: {supported_to_pandas}")
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.to_pandas(data, **kwargs)

    @retry_args
    def to_spark(
        self,
        data,
        engine: str,
        **kwargs
    ):
        """ Convert data to a Spark DataFrame.

        Args:
            data: The data to be converted.
            engine (str): The name of the database engine to use.
        """
        engine = engine.lower()
        if engine not in supported_to_spark:
            raise KeyError(f"to_spark operation is not supported for {engine}. Supported engines: {supported_to_spark}")
        if not hasattr(self.engines, engine):
            raise KeyError(f"Engine '{engine}' is not instantiated or does not exist.")
        engine_instance = getattr(self.engines, engine)
        return engine_instance.to_spark(data, **kwargs)