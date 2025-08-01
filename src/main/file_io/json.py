import json
from io import BytesIO
from upath import UPath

class JsonFileIO:
    """
    Class for reading and writing JSON files.
    """

    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> object:
        """
        Read a JSON file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing JSON data.

        Returns:
            object: Parsed JSON data.
        """
        return json.load(b, *args, **kwargs)

    @staticmethod
    def _write(upath_obj: UPath, data: object, mode: str='w', *args, **kwargs):
        """
        Write data to a JSON file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (object): Data to write to the JSON file.
            mode (str): Mode to open the file, default is 'w'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            json.dump(data, f, *args, **kwargs)