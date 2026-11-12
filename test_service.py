import unittest
from decimal import Decimal
from service import notice_key

class ProcurementDeltaTests(unittest.TestCase):
    def test_identity_includes_host(self):
        self.assertNotEqual(notice_key('a.example', '7'), notice_key('b.example', '7'))

    def test_normalizes_host(self):
        self.assertEqual(notice_key('A.EXAMPLE/', ' 7 '), 'a.example:7')


if __name__ == '__main__':
    unittest.main()
