import pickle
from io import BytesIO
from upath import UPath

class PickleFileIO:
    """
    Class for reading and writing pickle files.
    """

    @staticmethod
    def _read(b: BytesIO, *args, **kwargs) -> object:
        """
        Read a pickle file from a BytesIO object.
        
        Args:
            b (BytesIO): BytesIO object containing pickle data.

        Returns:
            object: Unpickled data.
        """
        return pickle.load(b)

    @staticmethod
    def _write(upath_obj: UPath, data: object, mode: str='wb', *args, **kwargs):
        """
        Write data to a pickle file at the specified UPath location.
        
        Args:
            upath_obj (UPath): UPath object representing the file path.
            data (object): Data to write to the pickle file.
            mode (str): Mode to open the file, default is 'wb'.
        """
        with upath_obj.fs.open(upath_obj.path, mode) as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL, *args, **kwargs)