from concurrent.futures import Future
import os
import shutil
from unittest import TestCase
from executorlib_cache.shared import _check_task_output, _serialize_funct_h5, dump
from executorlib_cache.backend import execute_task_in_file


def my_funct(a, b):
    return a+b


class TestSharedFunctions(TestCase):
    def test_execute_function_mixed(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        task_key, data_dict = _serialize_funct_h5(
            my_funct, 1, b=2,
        )
        file_name = os.path.join(cache_directory, task_key + ".h5in")
        dump(file_name=file_name, data_dict=data_dict)
        execute_task_in_file(file_name=file_name)
        future_obj = Future()
        _check_task_output(
            task_key=task_key, future_obj=future_obj, cache_directory=cache_directory
        )
        self.assertTrue(future_obj.done())
        self.assertEqual(future_obj.result(), 3)

    def test_execute_function_args(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        task_key, data_dict = _serialize_funct_h5(
            my_funct, 1, 2,
        )
        file_name = os.path.join(cache_directory, task_key + ".h5in")
        dump(file_name=file_name, data_dict=data_dict)
        execute_task_in_file(file_name=file_name)
        future_obj = Future()
        _check_task_output(
            task_key=task_key, future_obj=future_obj, cache_directory=cache_directory
        )
        self.assertTrue(future_obj.done())
        self.assertEqual(future_obj.result(), 3)

    def test_execute_function_kwargs(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        task_key, data_dict = _serialize_funct_h5(
            my_funct, a=1, b=2,
        )
        file_name = os.path.join(cache_directory, task_key + ".h5in")
        dump(file_name=file_name, data_dict=data_dict)
        execute_task_in_file(file_name=file_name)
        future_obj = Future()
        _check_task_output(
            task_key=task_key, future_obj=future_obj, cache_directory=cache_directory
        )
        self.assertTrue(future_obj.done())
        self.assertEqual(future_obj.result(), 3)

    def tearDown(self):
        if os.path.exists("cache"):
            shutil.rmtree("cache")
