from concurrent.futures import Future
import os
from queue import Queue
import shutil
from unittest import TestCase
from executorlib_core.thread import RaisingThread
from executorlib_cache import FileExecutor
from executorlib_cache.shared import execute_tasks_h5, execute_in_subprocess


def my_funct(a, b):
    return a+b


class TestFileExecutor(TestCase):
    def test_executor_mixed(self):
        with FileExecutor() as exe:
            fs1 = exe.submit(my_funct, 1, b=2)
            self.assertFalse(fs1.done())
            self.assertEqual(fs1.result(), 3)
            self.assertTrue(fs1.done())

    def test_executor_function(self):
        fs1 = Future()
        q = Queue()
        q.put({"fn": my_funct, "args": (), "kwargs": {"a": 1, "b": 2}, "future": fs1})
        cache_dir = os.path.abspath("cache")
        os.makedirs(cache_dir, exist_ok=True)
        process = RaisingThread(
            target=execute_tasks_h5,
            kwargs={
                "future_queue":q,
                "cache_directory": cache_dir,
                "execute_function": execute_in_subprocess,
            }
        )
        process.start()
        self.assertFalse(fs1.done())
        self.assertEqual(fs1.result(), 3)
        self.assertTrue(fs1.done())
        q.put({"shutdown": True, "wait": True})

    def test_executor_function_dependence(self):
        fs1 = Future()
        fs2 = Future()
        q = Queue()
        q.put({"fn": my_funct, "args": (), "kwargs": {"a": 1, "b": 2}, "future": fs1})
        q.put({"fn": my_funct, "args": (), "kwargs": {"a": 1, "b": fs1}, "future": fs2})
        cache_dir = os.path.abspath("cache")
        os.makedirs(cache_dir, exist_ok=True)
        process = RaisingThread(
            target=execute_tasks_h5,
            kwargs={
                "future_queue": q,
                "cache_directory": cache_dir,
                "execute_function": execute_in_subprocess,
            }
        )
        process.start()
        self.assertFalse(fs2.done())
        self.assertEqual(fs2.result(), 4)
        self.assertTrue(fs2.done())
        q.put({"shutdown": True, "wait": True})

    def tearDown(self):
        if os.path.exists("cache"):
            shutil.rmtree("cache")
