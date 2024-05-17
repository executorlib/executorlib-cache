import os

from executorlib_cache.hdf import dump, load


def execute_hdf5_file(file_name):
    apply_dict = load(file_name=file_name)
    result = apply_dict["fn"].__call__(*apply_dict["args"], **apply_dict["kwargs"])
    file_name_out = os.path.splitext(file_name)[0]
    os.rename(file_name, file_name_out + ".h5ready")
    dump(file_name=file_name_out + ".h5ready", data_dict={"output": result})
    os.rename(file_name_out + ".h5ready", file_name_out + ".h5out")
