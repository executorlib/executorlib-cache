import hashlib
import os
import queue
import re

import cloudpickle
import h5io
import h5py

from executorlib_core.base import ExecutorBase
from executorlib_core.thread import RaisingThread
from executorlib_cache.shared import (
    write_to_h5_file,
    execute_in_subprocess,
    get_execute_command,
)


def get_hash(binary):
    # Remove specification of jupyter kernel from hash to be deterministic
    binary_no_ipykernel = re.sub(b"(?<=/ipykernel_)(.*)(?=/)", b"", binary)
    return str(hashlib.md5(binary_no_ipykernel).hexdigest())


def serialize_funct_h5(fn, *args, **kwargs):
    binary_funct = cloudpickle.dumps(fn)
    binary_all = cloudpickle.dumps({"fn": fn, "args": args, "kwargs": kwargs})
    task_key = fn.__name__ + get_hash(binary=binary_all)
    data = {"fn": binary_funct, "args": args, "kwargs": kwargs}
    return task_key, data


def check_output(task_key, future_obj, cache_directory):
    file_name = os.path.join(cache_directory, task_key + ".h5out")
    if not os.path.exists(file_name):
        return future_obj
    with h5py.File(file_name, "r") as hdf:
        if "output" in hdf:
            future_obj.set_result(
                h5io.read_hdf5(fname=hdf, title="output", slash="ignore")
            )
    return future_obj


def execute_tasks_h5(future_queue, cache_directory, execute_function):
    memory_dict = {}
    while True:
        task_dict = None
        try:
            task_dict = future_queue.get_nowait()
        except queue.Empty:
            pass
        if (
            task_dict is not None
            and "shutdown" in task_dict.keys()
            and task_dict["shutdown"]
        ):
            future_queue.task_done()
            future_queue.join()
            break
        elif task_dict is not None:
            task_key, data_dict = serialize_funct_h5(
                task_dict["fn"], *task_dict["args"], **task_dict["kwargs"]
            )
            if task_key not in memory_dict.keys():
                if task_key + ".h5out" not in os.listdir(cache_directory):
                    file_name = os.path.join(cache_directory, task_key + ".h5in")
                    write_to_h5_file(file_name=file_name, data_dict=data_dict)
                    execute_function(command=get_execute_command(file_name=file_name))
                memory_dict[task_key] = task_dict["future"]
            future_queue.task_done()
        else:
            memory_dict = {
                key: check_output(
                    task_key=key, future_obj=value, cache_directory=cache_directory
                )
                for key, value in memory_dict.items()
                if not value.done()
            }


class FileExecutor(ExecutorBase):
    def __init__(self, cache_directory="cache", execute_function=execute_in_subprocess):
        super().__init__()
        cache_directory_path = os.path.abspath(cache_directory)
        os.makedirs(cache_directory_path, exist_ok=True)
        self._set_process(
            RaisingThread(
                target=execute_tasks_h5,
                kwargs={
                    "future_queue": self._future_queue,
                    "execute_function": execute_function,
                    "cache_directory": cache_directory_path,
                },
            )
        )
