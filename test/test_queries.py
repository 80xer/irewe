from unittest import TestCase
from src.const import Const
from src.db import queries

class TestQueries(TestCase):
    def test_getItems(self):
        const = Const(False)
        qr = queries(const)
        self.assertRaises(Exception, qr.getITemsFromDV, 'abc')


