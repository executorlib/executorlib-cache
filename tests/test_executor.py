from unittest import TestCase
from executorlib_cache import FileExecutor


def my_funct(a, b):
    return a+b


class TestFileExecutor(TestCase):
    def test_executor(self):
        with FileExecutor() as exe:
            fs1 = exe.submit(my_funct, 1, 2)
            self.assertFalse(fs1.done())
            self.assertEqual(fs1.result(), 3)
            self.assertTrue(fs1.done())
