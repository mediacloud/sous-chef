"""Tests for sous_chef.runtime recording and timeline artifact."""

import json
import unittest

from sous_chef.runtime import mark_step, runtime_session


class TestRuntimeSession(unittest.TestCase):
    def test_mark_step_no_session_is_noop(self) -> None:
        mark_step("should not raise")

    def test_recipe_bounds_and_mark_step(self) -> None:
        with runtime_session(recipe_name="test_recipe", run_id="run-1") as rec:
            mark_step("step_a", meta={"k": 1})
            mark_step("step_b")

        art = rec.to_timeline_artifact()
        self.assertEqual(art.recipe_name, "test_recipe")
        self.assertEqual(art.run_id, "run-1")
        self.assertEqual(art.final_status, "ok")
        self.assertIsNotNone(art.total_duration_ms)
        kinds = [r["event_kind"] for r in art.rows]
        self.assertEqual(
            kinds,
            ["recipe_start", "mark_step", "mark_step", "recipe_end"],
        )
        self.assertEqual(art.rows[1]["name"], "step_a")
        meta = json.loads(art.rows[1]["meta_json"] or "{}")
        self.assertEqual(meta, {"k": 1})

    def test_error_records_recipe_end(self) -> None:
        rec = None
        with self.assertRaises(ValueError):
            with runtime_session(recipe_name="bad") as rec:
                mark_step("before")
                raise ValueError("boom")
        assert rec is not None
        art = rec.to_timeline_artifact()
        self.assertEqual(art.final_status, "error")
        self.assertTrue(any(r["event_kind"] == "recipe_end" for r in art.rows))
        end_rows = [r for r in art.rows if r["event_kind"] == "recipe_end"]
        self.assertEqual(len(end_rows), 1)

    def test_to_table_adds_artifact_type_per_row(self) -> None:
        with runtime_session(recipe_name="x") as rec:
            pass
        art = rec.to_timeline_artifact()
        table = art.to_table()
        self.assertEqual(len(table), 2)
        for row in table:
            self.assertEqual(row["_artifact_type"], "runtime_timeline")


if __name__ == "__main__":
    unittest.main()
