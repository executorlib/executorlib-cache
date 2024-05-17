import cloudpickle
import h5io
import h5py
import numpy as np


def dump(file_name: str, data_dict: dict):
    """
    Dump data dictionary into HDF5 file

    Args:
        file_name (str): file name of the HDF5 file as absolute path
        data_dict (dict): dictionary containing the python function to be executed {"fn": ..., "args": (), "kwargs": {}}
    """
    with h5py.File(file_name, "a") as fname:
        for data_key, data_value in data_dict.items():
            if data_key == "fn":
                h5io.write_hdf5(
                    fname=fname,
                    data=np.void(data_value),
                    overwrite="update",
                    title="function",
                )
            elif data_key == "args":
                h5io.write_hdf5(
                    fname=fname,
                    data=np.void(cloudpickle.dumps(data_value)),
                    overwrite="update",
                    title="input_args",
                )
            elif data_key == "kwargs":
                h5io.write_hdf5(
                    fname=fname,
                    data=np.void(cloudpickle.dumps(data_value)),
                    overwrite="update",
                    title="input_kwargs",
                )
            elif data_key == "output":
                h5io.write_hdf5(
                    fname=fname,
                    data=np.void(cloudpickle.dumps(data_value)),
                    overwrite="update",
                    title="output",
                )


def load(file_name: str) -> dict:
    """
    Load data dictionary from HDF5 file

    Args:
        file_name (str): file name of the HDF5 file as absolute path

    Returns:
        dict: dictionary containing the python function to be executed {"fn": ..., "args": (), "kwargs": {}}
    """
    with h5py.File(file_name, "r") as hdf:
        if "input_args" in hdf and "input_kwargs" in hdf:
            return {
                "fn": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                ),
                "args": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="input_args", slash="ignore")
                ),
                "kwargs": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="input_kwargs", slash="ignore")
                ),
            }
        elif "input_args" in hdf:
            return {
                "fn": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                ),
                "args": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="input_args", slash="ignore")
                ),
                "kwargs": {},
            }
        elif "input_kwargs" in hdf:
            return {
                "fn": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                ),
                "args": [],
                "kwargs": cloudpickle.loads(
                    h5io.read_hdf5(fname=hdf, title="input_kwargs", slash="ignore")
                ),
            }
        else:
            raise TypeError


def check_output(file_name: str):
    """
    Check if output is available in the HDF5 file

    Args:
        file_name (str): file name of the HDF5 file as absolute path

    Returns:
        (bool, ...): boolean flag if output is available and the output itself
    """
    with h5py.File(file_name, "r") as hdf:
        if "output" in hdf:
            return True, cloudpickle.loads(
                h5io.read_hdf5(fname=hdf, title="output", slash="ignore")
            )
        else:
            return False, None
