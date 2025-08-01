import yaml
from io import BytesIO
from upath import UPath

class YamlFileIO:
    """
    Class for reading and writing YAML files.
    """

    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> object:
        """
        Read a YAML file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing YAML data.

        Returns:
            object: Parsed YAML data.
        """
        return yaml.safe_load(b)

    @staticmethod
    def _write(upath_obj: UPath, data: object, mode: str='w', *args, **kwargs):
        """
        Write data to a YAML file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (object): Data to write to the YAML file.
            mode (str): Mode to open the file, default is 'w'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            yaml.dump(data, f, *args, **kwargs)