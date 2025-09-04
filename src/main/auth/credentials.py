import fsspec
import base64
import copy
# from 1dap3 import Connection, NTLM
from .._aux import retry_args

@retry_args
def get_credentials(credential_path: str):
	"""Get decrypted password for credential.
	Args:
		credential_path (str): path to the hdfs file containing the encrypted credentials
	Raises:
		ValueError: File not found in HDFS or not reachable by current user
	Returns:
		str: decrypted password
	"""
	fs = fsspec.filesystem("hdfs")
	with fs.open(credential_path, "rb") as f:
		return base64.b64decode(f.read()).decode('ascii')


class CredentialsSafeBox:
	def __init__(self, config: dict, auto_parse: bool = True):
		self._credentials = copy.deepcopy(config)
		if auto_parse:
			self.update(self._credentials)

	def update(self, config: dict):
		assert isinstance(config, dict), "Credentials config should be a dict."
		for engine, asset in config.items():
			if not asset:
				print(f"Warning: Credentials not found for '{engine}' engine in config file.")
				self._credentials[engine] = {}
				continue
			if asset.get("password"):
				self._credentials[engine]["password"] = asset["password"]
			elif asset.get("credential_path"):
				self._credentials[engine]["password"] = get_credentials(asset["credential_path"])
