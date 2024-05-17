import h5io
import h5py
import numpy as np
import subprocess


def write_to_h5_file(file_name, data_dict):
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
                    data=data_value,
                    overwrite="update",
                    title="input_args",
                    slash="ignore",
                )
            elif data_key == "kwargs":
                for k, v in data_value.items():
                    h5io.write_hdf5(
                        fname=fname,
                        data=v,
                        overwrite="update",
                        title="input_kwargs/" + k,
                        slash="ignore",
                    )
            elif data_key == "output":
                h5io.write_hdf5(
                    fname=fname,
                    data=data_value,
                    overwrite="update",
                    title="output",
                    slash="ignore",
                )


def get_execute_command(file_name):
    return ["python", "-m", "executorlib_cache", file_name]


def execute_in_subprocess(command):
    subprocess.Popen(command, universal_newlines=True)
