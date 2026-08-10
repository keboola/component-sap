'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from datetime import datetime
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
    """Covers `source.delta_lookback_days` (SUPPORT-17281).

    NOW is the run time; STORED is a pointer the previous run left behind, one day old.
    """

    NOW = datetime(2026, 8, 6, 2, 16, 26)
    STORED = 20260805021626

    def _shift(self, pointer, lookback_days, now=None):
        return Component._apply_delta_lookback(pointer, lookback_days, now or self.NOW)

    # --- the pointer sent to SAP ------------------------------------------------

    def test_sent_pointer_is_now_minus_lookback(self):
        """The customer's ask: 10 days back from today, not from the stored pointer."""
        self.assertEqual(20260727021626, self._shift(self.STORED, 10))

    def test_sent_pointer_falls_back_to_stored_when_schedule_is_behind(self):
        """A run that is behind must resume from the stored pointer, never skip the gap."""
        stored = 20260601000000
        self.assertEqual(stored, self._shift(stored, 10))

    def test_sent_pointer_is_never_newer_than_stored(self):
        for lookback_days in (1, 5, 10, 365):
            with self.subTest(lookback_days=lookback_days):
                self.assertLessEqual(self._shift(self.STORED, lookback_days), self.STORED)

    def test_date_only_pointer_keeps_its_format(self):
        self.assertEqual(20260727, self._shift(20260805, 10))

    def test_string_pointer_stays_a_string(self):
        shifted = self._shift("20260805021626", 10)
        self.assertEqual("20260727021626", shifted)
        self.assertIsInstance(shifted, str)

    def test_month_boundary(self):
        now = datetime(2026, 3, 1)
        self.assertEqual(20260228000000, self._shift(20260301000000, 1, now))

    def test_leap_day(self):
        now = datetime(2028, 3, 1)
        self.assertEqual(20280229000000, self._shift(20280301000000, 1, now))

    # --- pointers a lookback cannot be expressed on -----------------------------

    def test_sequential_id_pointer_raises(self):
        with self.assertRaises(UserException):
            self._shift(12345, 10)

    def test_date_like_sequential_id_raises_instead_of_being_corrupted(self):
        """`10000101` parses as year 1000; shifting it would jump the id back by millions."""
        for pointer in (10000101, 12340101, 20260101021626000):
            with self.subTest(pointer=pointer):
                with self.assertRaises(UserException):
                    self._shift(pointer, 10)

    def test_non_numeric_pointer_raises(self):
        with self.assertRaises(UserException):
            self._shift("2026-08-05", 10)

    # --- validation --------------------------------------------------------------

    def test_invalid_lookback_values_raise(self):
        for value in (-1, 3651, True, "10"):
            with self.subTest(value=value):
                with self.assertRaises(UserException):
                    Component._validate_delta_lookback_days(value)

    def test_valid_lookback_values_accepted(self):
        for value in (0, 10, 3650):
            with self.subTest(value=value):
                self.assertEqual(value, Component._validate_delta_lookback_days(value))

    # --- what gets written back to state -----------------------------------------

    def test_state_does_not_regress_when_sap_returns_no_pointer(self):
        """Reproduces the original failure: the lookback window walking backwards every run.

        On a run where nothing changed, SAP returns no delta pointer of its own, so the client's
        maximum is just the (shifted) pointer it was handed. Persisting that would make the next
        run shift an already-shifted pointer, and the window would creep back a further N days
        every run, without limit and without any error.
        """
        stored = self.STORED
        now = self.NOW

        for run in range(5):
            sent = self._shift(stored, 10, now)
            # SAP reports nothing changed -> the client's max is the pointer it was given.
            persisted = Component._persisted_delta_pointer(sent, stored)

            self.assertEqual(stored, persisted, f"pointer regressed on run {run + 1}")
            stored = persisted

    def test_state_advances_when_sap_returns_a_newer_pointer(self):
        persisted = Component._persisted_delta_pointer(20260806021626, self.STORED)
        self.assertEqual(20260806021626, persisted)

    def test_persisted_pointer_without_stored_value_is_the_clients_max(self):
        """First run: nothing stored yet, so there is no floor to apply."""
        self.assertEqual(20260806021626, Component._persisted_delta_pointer(20260806021626, False))
        self.assertIsNone(Component._persisted_delta_pointer(None, False))

    # --- unchanged behaviour when the option is not set ---------------------------

    def test_unset_lookback_leaves_the_stored_pointer_untouched(self):
        """With the option off, the pointer sent and the pointer persisted are both the stored one."""
        state = {"ACC_DOC_HEADER": {"delta_max": self.STORED}}
        component = Component.__new__(Component)
        component.state = state

        stored = component._init_delta("incremental_sync", "ACC_DOC_HEADER")

        self.assertEqual(self.STORED, stored)
        self.assertEqual(self.STORED, Component._persisted_delta_pointer(stored, stored))

    def test_full_sync_ignores_the_stored_pointer(self):
        component = Component.__new__(Component)
        component.state = {"ACC_DOC_HEADER": {"delta_max": self.STORED}}

        self.assertIsNone(component._init_delta("full_sync", "ACC_DOC_HEADER"))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
