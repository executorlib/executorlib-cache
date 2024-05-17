import os

import cloudpickle
import h5io
import h5py

from executorlib_cache.shared import write_to_h5_file


def apply_funct(apply_dict):
    return apply_dict["fn"].__call__(*apply_dict["args"], **apply_dict["kwargs"])


def execute_hdf5_file(file_name):
    with h5py.File(file_name, "r") as hdf:
        if "input_args" in hdf and "input_kwargs" in hdf:
            result = apply_funct(
                apply_dict={
                    "fn": cloudpickle.loads(
                        h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                    ),
                    "args": h5io.read_hdf5(
                        fname=hdf, title="input_args", slash="ignore"
                    ),
                    "kwargs": h5io.read_hdf5(
                        fname=hdf, title="input_kwargs", slash="ignore"
                    ),
                }
            )
        elif "input_args" in hdf:
            result = apply_funct(
                apply_dict={
                    "fn": cloudpickle.loads(
                        h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                    ),
                    "args": h5io.read_hdf5(
                        fname=hdf, title="input_args", slash="ignore"
                    ),
                    "kwargs": {},
                }
            )
        elif "input_kwargs" in hdf:
            result = apply_funct(
                apply_dict={
                    "fn": cloudpickle.loads(
                        h5io.read_hdf5(fname=hdf, title="function", slash="ignore")
                    ),
                    "args": [],
                    "kwargs": h5io.read_hdf5(
                        fname=hdf, title="input_kwargs", slash="ignore"
                    ),
                }
            )
        else:
            raise TypeError
    file_name_out = os.path.splitext(file_name)[0]
    os.rename(file_name, file_name_out + ".h5ready")
    write_to_h5_file(file_name=file_name_out + ".h5ready", data_dict={"output": result})
    os.rename(file_name_out + ".h5ready", file_name_out + ".h5out")
