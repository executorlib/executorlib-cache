from typing import Tuple

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
    group_dict = {
        "fn": "function",
        "args": "input_args",
        "kwargs": "input_kwargs",
        "output": "output",
    }
    with h5py.File(file_name, "a") as fname:
        for data_key, data_value in data_dict.items():
            if data_key == "fn":
                h5io.write_hdf5(
                    fname=fname,
                    data=np.void(cloudpickle.dumps(data_value)),
                    overwrite="update",
                    title=group_dict[data_key],
                )
            elif data_key == "args":
                for k, v in enumerate(data_value):
                    h5io.write_hdf5(
                        fname=fname,
                        data=v,
                        overwrite="update",
                        title=group_dict[data_key] + "/" + str(k),
                        use_state=True,
                    )
            elif data_key == "kwargs":
                for k, v in data_value.items():
                    h5io.write_hdf5(
                        fname=fname,
                        data=v,
                        overwrite="update",
                        title=group_dict[data_key] + "/" + k,
                        use_state=True,
                    )
            elif data_key == "output":
                h5io.write_hdf5(
                    fname=fname,
                    data=data_value,
                    overwrite="update",
                    title=group_dict[data_key],
                    use_state=True,
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
        data_dict = {}
        if "function" in hdf:
            data_dict["fn"] = cloudpickle.loads(
                h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
            )
        else:
            raise TypeError
        if "input_args" in hdf:
            data_dict["args"] = [
                h5io.read_hdf5(fname=hdf, title="input_args/" + str(k), slash="ignore")
                for k in sorted([int(k) for k in hdf["input_args"].keys()])
            ]
        else:
            data_dict["args"] = ()
        if "input_kwargs" in hdf:
            data_dict["kwargs"] = {
                k: h5io.read_hdf5(fname=hdf, title="input_kwargs/" + k, slash="ignore")
                for k in hdf["input_kwargs"].keys()
            }
        else:
            data_dict["kwargs"] = {}
        return data_dict


def get_output(file_name: str) -> Tuple[bool, object]:
    """
    Check if output is available in the HDF5 file

    Args:
        file_name (str): file name of the HDF5 file as absolute path

    Returns:
        (bool, object): boolean flag if output is available and the output object itself
    """
    with h5py.File(file_name, "r") as hdf:
        if "output" in hdf:
            return True, h5io.read_hdf5(fname=hdf, title="output", slash="ignore")
        else:
            return False, None
