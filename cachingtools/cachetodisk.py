import os
import shutil
from functools import wraps
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Callable, Optional, ParamSpec, TypeVar

import joblib

P = ParamSpec("P")
T = TypeVar("T")


class CacheToDisk:
    """A decorator class that caches function results to disk.

    Args:
        directory (Path): The directory where the cache files will be stored.
        compress (bool, optional): Whether to compress the cache files.
            Defaults to True.
        capacity (int, optional): The maximum size of the cache directory in bytes.
            Defaults to None. If None, the cache directory will not be shrunk.
    """

    def __init__(
        self,
        directory: Path,
        compress: bool = True,
        capacity: Optional[int] = None,
    ) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.compress = compress
        self.capacity = capacity

    def __repr__(self) -> str:
        return f"CacheToDisk(directory={self.directory}, compress={self.compress}, \
capacity={self.capacity})"

    @property
    def size(self) -> int:
        """Return the size of the cache directory in bytes."""
        return sum(f.stat().st_size for f in self.directory.glob("**/*") if f.is_file())

    def purge(self) -> None:
        """Delete all cache files in the cache directory."""
        shutil.rmtree(self.directory)

    def shrink(self, size: int) -> None:
        """Delete the least recently accessed cache files until the cache directory is
            below the specified size.

        Args:
            size (int): The maximum size of the cache directory in bytes.
        """
        current_size = self.size
        excess_size = current_size - size
        if excess_size > 0:
            files = sorted(self.directory.rglob("*"), key=lambda x: x.stat().st_atime)
            files = [f for f in files if f.is_file()]
            while excess_size > 0:
                print("Deleting file")
                file = files.pop(0)
                excess_size -= file.stat().st_size
                file.unlink()

    def __call__(self, name: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """Return a decorator that caches the results of a function to disk.

        Args:
            name (str): The name of subdirectory in the cache directory where the cache
                files will be stored.

        Returns:
            Callable[[Callable[P, T]], Callable[P, T]]: A decorator that caches the
                results of a function to disk.
        """

        def decorator(function: Callable[P, T]) -> Callable[P, T]:
            directory = self.directory / name
            directory.mkdir(parents=True, exist_ok=True)

            @wraps(function)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                to_serialize = {"args": args, "kwargs": kwargs}
                buffer = BytesIO()
                joblib.dump(to_serialize, buffer, compress=False)
                buffer.seek(0)
                bytes_string = buffer.read()
                hash_value = sha256(bytes_string).hexdigest()
                file_path = directory / hash_value

                if not file_path.exists():
                    directory.mkdir(parents=True, exist_ok=True)
                    result = function(*args, **kwargs)
                    joblib.dump(result, file_path, compress=self.compress)
                else:
                    result = joblib.load(file_path)

                os.utime(file_path, None)

                if self.capacity is not None:
                    self.shrink(self.capacity)

                return result

            return wrapper

        return decorator

    def __enter__(self) -> "CacheToDisk":
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.purge()
