import sys

from executorlib_cache.backend import execute_hdf5_file


if __name__ == "__main__":
    execute_hdf5_file(file_name=sys.argv[1])
