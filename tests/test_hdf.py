import os
import shutil
from unittest import TestCase

import cloudpickle

from executorlib_cache.hdf import dump, load


def my_funct(a, b):
    return a+b


class TestSharedFunctions(TestCase):
    def test_hdf_mixed(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        file_name = os.path.join(cache_directory, "test.h5")
        a = 1
        b = 2
        dump(
            file_name=file_name,
            data_dict={"fn": my_funct, "args": [a], "kwargs": {"b": b}}
        )
        data_dict = load(file_name=file_name)
        self.assertTrue("fn" in data_dict.keys())
        self.assertEqual(data_dict["args"], [a])
        self.assertEqual(data_dict["kwargs"], {"b": b})

    def test_hdf_args(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        file_name = os.path.join(cache_directory, "test.h5")
        a = 1
        b = 2
        dump(
            file_name=file_name,
            data_dict={"fn": my_funct, "args": [a, b]}
        )
        data_dict = load(file_name=file_name)
        self.assertTrue("fn" in data_dict.keys())
        self.assertEqual(data_dict["args"], [a, b])
        self.assertEqual(data_dict["kwargs"], {})

    def test_hdf_kwargs(self):
        cache_directory = os.path.abspath("cache")
        os.makedirs(cache_directory, exist_ok=True)
        file_name = os.path.join(cache_directory, "test.h5")
        a = 1
        b = 2
        dump(
            file_name=file_name,
            data_dict={"fn": my_funct, "args": (), "kwargs": {"a": a, "b": b}}
        )
        data_dict = load(file_name=file_name)
        self.assertTrue("fn" in data_dict.keys())
        self.assertEqual(data_dict["args"], ())
        self.assertEqual(data_dict["kwargs"], {"a": a, "b": b})

    def tearDown(self):
        if os.path.exists("cache"):
            shutil.rmtree("cache")