'''
Created on 12. 11. 2018

@author: esner
'''
import json
import shutil
import tempfile
import unittest
import mock
import os
from freezegun import freeze_time

from keboola.component.exceptions import UserException

from component import Component
from sap_client.client import SAPClient

# Chosen at midday so the local-timezone shift dateparser applies to relative dates
# ("10 days ago") never crosses a day boundary, keeping date-level assertions stable in any timezone.
FROZEN_NOW = "2026-08-06 12:00:00"


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


@freeze_time(FROZEN_NOW)
class TestDateFrom(unittest.TestCase):
    """Covers `source.date_from` - the Date Start window (SUPPORT-17281).

    STORED is a 14-digit pointer a previous run left behind, one day before the frozen now.
    """

    STORED = 20260805021626

    # --- the pointer sent to SAP ------------------------------------------------

    def test_absolute_date_from_moves_pointer_back(self):
        """The customer's ask: fetch from a chosen date, not from the stored pointer."""
        self.assertEqual(20260727021626, Component._apply_date_from(self.STORED, "2026-07-27 02:16:26"))

    def test_relative_date_from_is_now_minus_window(self):
        """`10 days ago` from the frozen now (Aug 6) is Jul 27; assert the date, not the tz-shifted time."""
        sent = Component._apply_date_from(self.STORED, "10 days ago")
        self.assertEqual(20260727, sent // 1_000_000)
        self.assertLessEqual(sent, self.STORED)

    def test_date_from_falls_back_to_stored_when_schedule_is_behind(self):
        """A run whose stored pointer is already older than Date Start must not skip the gap."""
        stored = 20260601000000
        self.assertEqual(stored, Component._apply_date_from(stored, "2026-07-27"))

    def test_date_only_pointer_keeps_its_format(self):
        self.assertEqual(20260727, Component._apply_date_from(20260805, "2026-07-27"))

    def test_string_pointer_stays_a_string(self):
        sent = Component._apply_date_from("20260805021626", "2026-07-27")
        self.assertEqual("20260727000000", sent)
        self.assertIsInstance(sent, str)

    def test_sent_pointer_is_never_newer_than_stored(self):
        for date_from in ("2026-07-27", "2026-08-01", "1 day ago", "2 weeks ago"):
            with self.subTest(date_from=date_from):
                self.assertLessEqual(Component._apply_date_from(self.STORED, date_from), self.STORED)

    # --- pointers a Date Start cannot be expressed on ---------------------------

    def test_sequential_id_pointer_raises(self):
        with self.assertRaises(UserException):
            Component._apply_date_from(12345, "10 days ago")

    def test_date_like_sequential_id_raises_instead_of_being_corrupted(self):
        """`10000101` parses as year 1000; treating it as a date would jump the id back by millions."""
        for pointer in (10000101, 12340101, 20260101021626000):
            with self.subTest(pointer=pointer):
                with self.assertRaises(UserException):
                    Component._apply_date_from(pointer, "10 days ago")

    def test_non_numeric_pointer_raises(self):
        with self.assertRaises(UserException):
            Component._apply_date_from("2026-08-05", "10 days ago")

    # --- Date Start string validation --------------------------------------------

    def test_unparseable_date_from_raises(self):
        for value in ("garbage", "not a date", "next bluesday"):
            with self.subTest(value=value):
                with self.assertRaises(UserException):
                    Component._validate_date_from(value)

    def test_valid_date_from_accepted(self):
        for value in ("2026-01-01", "10 days ago", "today", "2 weeks ago"):
            with self.subTest(value=value):
                Component._validate_date_from(value)  # must not raise

    # --- what gets written back to state -----------------------------------------

    def test_state_does_not_regress_when_sap_returns_no_pointer(self):
        """On a quiet run SAP returns no delta pointer, and the stored one must survive intact.

        The client seeds its own maximum with the pointer it was handed, so without the floor the
        moved-back pointer is what gets persisted: state drops to the start of the window and stays
        there, and the window then widens by a day for every day that passes.
        """
        stored = self.STORED

        for run in range(5):
            sent = Component._apply_date_from(stored, "10 days ago")
            # SAP reports nothing changed -> the client's max is the pointer it was given.
            persisted = Component._persisted_delta_pointer(sent, stored)

            self.assertEqual(stored, persisted, f"pointer regressed on run {run + 1}")
            stored = persisted

    def test_persisted_pointer_holds_when_sap_returns_an_older_pointer(self):
        self.assertEqual(self.STORED, Component._persisted_delta_pointer(20260101000000, self.STORED))

    def test_state_advances_when_sap_returns_a_newer_pointer(self):
        persisted = Component._persisted_delta_pointer(20260806021626, self.STORED)
        self.assertEqual(20260806021626, persisted)

    def test_persisted_pointer_without_stored_value_is_the_clients_max(self):
        """First run: nothing stored yet, so there is no floor to apply."""
        self.assertEqual(20260806021626, Component._persisted_delta_pointer(20260806021626, False))
        self.assertIsNone(Component._persisted_delta_pointer(None, False))

    # --- unchanged behaviour when Date Start is not set ---------------------------

    def test_unset_date_from_leaves_the_stored_pointer_untouched(self):
        """With Date Start empty, the pointer sent and the pointer persisted are both the stored one."""
        component = Component.__new__(Component)
        component.state = {"ACC_DOC_HEADER": {"delta_max": self.STORED}}

        stored = component._init_delta("incremental_sync", "ACC_DOC_HEADER")

        self.assertEqual(self.STORED, stored)
        self.assertEqual(self.STORED, Component._persisted_delta_pointer(stored, stored))

    def test_full_sync_ignores_the_stored_pointer(self):
        component = Component.__new__(Component)
        component.state = {"ACC_DOC_HEADER": {"delta_max": self.STORED}}

        self.assertIsNone(component._init_delta("full_sync", "ACC_DOC_HEADER"))


class FakeSapClient(SAPClient):
    """A SAPClient that talks to nothing.

    Subclasses the real client so that the delta pointer bookkeeping under test - seeding
    `delta_values` with the inbound pointer, and reducing it with `max_timestamp_or_id` - is the
    real implementation rather than a restatement of it.
    """

    sent_pointers = []
    returned_pointer = None
    metadata = {"ACC_NUMBER": {"TYPE": "CHAR", "LENGTH": 10, "KEY": True}}
    rows = []

    def __init__(self, **kwargs):
        # Deliberately not calling super().__init__: it would build a real HTTP client.
        self.delta = kwargs.get("delta")
        self.destination = kwargs.get("destination")
        self.metadata = FakeSapClient.metadata
        self.delta_values = []
        if self.delta:
            self.delta_values.append(self.delta)
        FakeSapClient.sent_pointers.append(self.delta)

    async def fetch(self, resource_alias, paging_method):
        if FakeSapClient.rows:
            with open(os.path.join(self.destination, f"{resource_alias}_0.json"), "w") as f:
                json.dump(FakeSapClient.rows, f)
        if FakeSapClient.returned_pointer:
            self.delta_values.append(FakeSapClient.returned_pointer)


@freeze_time(FROZEN_NOW)
class TestDateFromRun(unittest.TestCase):
    """Drives the real `Component.run()` so the Date Start wiring itself is covered, not just the helpers."""

    ALIAS = "ACC_DOC_HEADER"
    STORED = 20260805021626

    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.data_dir, "in"))
        os.makedirs(os.path.join(self.data_dir, "out", "tables"))
        FakeSapClient.sent_pointers = []
        FakeSapClient.returned_pointer = None
        FakeSapClient.metadata = {"ACC_NUMBER": {"TYPE": "CHAR", "LENGTH": 10, "KEY": True}}
        FakeSapClient.rows = []

    def tearDown(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _write_config(self, date_from="", sync_type="incremental_sync", load_type="incremental_load"):
        source = {
            "resource_alias": self.ALIAS,
            "sync_type": sync_type,
            "paging_method": "key",
        }
        if date_from:
            source["date_from"] = date_from
        config = {
            "parameters": {
                "authentication": {"server_url": "https://sap.example", "username": "u", "#password": "p"},
                "source": source,
                "destination": {"output_table_name": "", "load_type": load_type},
            }
        }
        with open(os.path.join(self.data_dir, "config.json"), "w") as f:
            json.dump(config, f)

    def _write_state(self, delta_max):
        with open(os.path.join(self.data_dir, "in", "state.json"), "w") as f:
            json.dump({self.ALIAS: {"delta_max": delta_max}}, f)

    def _read_state(self):
        with open(os.path.join(self.data_dir, "out", "state.json")) as f:
            return json.load(f)

    def _run(self):
        with mock.patch.dict(os.environ, {"KBC_DATADIR": self.data_dir}):
            with mock.patch("component.SAPClient", FakeSapClient):
                Component().run()

    def test_run_sends_date_from_pointer_but_persists_the_stored_one(self):
        """The failure this guards against: a quiet run persisting the moved-back pointer.

        Without the floor at the state write, `delta_max` drops to the start of the window and the
        window silently widens from then on.
        """
        self._write_config(date_from="2026-07-27 02:16:26")
        self._write_state(self.STORED)

        self._run()

        self.assertEqual([20260727021626], FakeSapClient.sent_pointers)
        self.assertEqual(self.STORED, self._read_state()[self.ALIAS]["delta_max"])

    def test_run_without_date_from_is_unchanged(self):
        self._write_config(date_from="")
        self._write_state(self.STORED)

        self._run()

        self.assertEqual([self.STORED], FakeSapClient.sent_pointers)
        self.assertEqual(self.STORED, self._read_state()[self.ALIAS]["delta_max"])

    def test_run_persists_a_newer_pointer_returned_by_sap(self):
        self._write_config(date_from="2026-07-27")
        self._write_state(self.STORED)
        FakeSapClient.returned_pointer = 20260806021626

        self._run()

        self.assertEqual(20260806021626, self._read_state()[self.ALIAS]["delta_max"])

    def test_run_writes_the_table_with_date_from(self):
        self._write_config(date_from="2026-07-27")
        self._write_state(self.STORED)
        FakeSapClient.rows = [{"ACC_NUMBER": "1"}, {"ACC_NUMBER": "2"}]

        self._run()

        table_path = os.path.join(self.data_dir, "out", "tables", self.ALIAS)
        self.assertTrue(os.path.exists(table_path))
        self.assertTrue(os.path.exists(table_path + ".manifest"))
        self.assertEqual(["ACC_NUMBER"], self._read_state()[self.ALIAS]["columns"])

    def test_run_fails_when_date_from_source_has_no_primary_key(self):
        """The guard must run before anything is written, and regardless of how many rows came back."""
        for rows in ([], [{"ACC_NUMBER": "1"}]):
            with self.subTest(rows=len(rows)):
                self.tearDown()
                self.setUp()
                FakeSapClient.metadata = {"ACC_NUMBER": {"TYPE": "CHAR", "LENGTH": 10}}
                FakeSapClient.rows = rows
                self._write_config(date_from="2026-07-27")
                self._write_state(self.STORED)

                with self.assertRaises(UserException):
                    self._run()

                # Nothing written: no manifest, and the stored pointer is left alone.
                table_path = os.path.join(self.data_dir, "out", "tables", self.ALIAS)
                self.assertFalse(os.path.exists(table_path + ".manifest"))
                self.assertFalse(os.path.exists(os.path.join(self.data_dir, "out", "state.json")))

    def test_run_fails_fast_on_unparseable_date_from(self):
        """A malformed Date Start fails before any fetch - no request is built."""
        self._write_config(date_from="garbage")
        self._write_state(self.STORED)

        with self.assertRaises(UserException):
            self._run()

        self.assertEqual([], FakeSapClient.sent_pointers)

    def test_full_sync_row_with_a_leftover_date_from_is_unaffected(self):
        """The field is hidden outside incremental sync, so a stale value must not fail the run."""
        self._write_config(date_from="2026-07-27", sync_type="full_sync")
        self._write_state(self.STORED)
        FakeSapClient.metadata = {"ACC_NUMBER": {"TYPE": "CHAR", "LENGTH": 10}}

        self._run()

        self.assertEqual([None], FakeSapClient.sent_pointers)
        self.assertEqual(self.STORED, self._read_state()[self.ALIAS]["delta_max"])

    def test_first_run_without_a_stored_pointer_full_syncs(self):
        self._write_config(date_from="10 days ago")

        self._run()

        self.assertEqual([False], FakeSapClient.sent_pointers)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
