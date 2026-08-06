'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from freezegun import freeze_time

from keboola.component.exceptions import UserException

from component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestDeltaLookback(unittest.TestCase):

    @staticmethod
    def _component(state):
        comp = Component.__new__(Component)
        comp.state = state
        return comp

    def test_shift_full_timestamp(self):
        self.assertEqual(Component._shift_delta_pointer(20260806021626, 10), 20260727021626)

    def test_shift_date_only_keeps_type(self):
        self.assertEqual(Component._shift_delta_pointer("20260301", 1), "20260228")

    def test_unsupported_pointer_returned_as_is(self):
        self.assertEqual(Component._shift_delta_pointer(12345, 10), 12345)

    def test_negative_lookback_raises(self):
        with self.assertRaises(UserException):
            Component._shift_delta_pointer(20260806021626, -1)

    def test_init_delta_applies_lookback(self):
        comp = self._component({"RES": {"delta_max": 20260806021626}})
        self.assertEqual(comp._init_delta("incremental_sync", "RES", 10), 20260727021626)

    def test_init_delta_without_lookback(self):
        comp = self._component({"RES": {"delta_max": 20260806021626}})
        self.assertEqual(comp._init_delta("incremental_sync", "RES"), 20260806021626)

    def test_init_delta_full_sync_ignores_lookback(self):
        comp = self._component({"RES": {"delta_max": 20260806021626}})
        self.assertIsNone(comp._init_delta("full_sync", "RES", 10))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
