"""Unit tests for the conformance kit's matcher engine + scenario
validator. Pure Python, no subprocess. Run: `python -m unittest
conformance/test_lib.py` (from the repo root) or `python conformance/test_lib.py`."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from conformance import lib  # noqa: E402
from conformance.lib import match_value, substitute_captures, load_scenario  # noqa: E402


def m(pattern, actual, captures=None):
    return match_value(pattern, actual, captures if captures is not None else {})


class MatcherTests(unittest.TestCase):
    def test_scalars_and_equality(self):
        self.assertTrue(m(1, 1)[0])
        self.assertFalse(m(1, 2)[0])
        self.assertTrue(m("x", "x")[0])
        self.assertFalse(m("x", "y")[0])
        self.assertTrue(m(True, True)[0])
        self.assertFalse(m(True, 1)[0])  # bool vs int distinct

    def test_ignore_and_any_types(self):
        self.assertTrue(m("$IGNORE", {"anything": [1, 2]})[0])
        self.assertTrue(m("$ANY_STRING", "hi")[0])
        self.assertFalse(m("$ANY_STRING", 1)[0])
        self.assertTrue(m("$ANY_INT", 5)[0])
        self.assertFalse(m("$ANY_INT", 5.0)[0])
        self.assertFalse(m("$ANY_INT", True)[0])
        self.assertTrue(m("$ANY_NUMBER", 5)[0])
        self.assertTrue(m("$ANY_NUMBER", 5.5)[0])
        self.assertTrue(m("$ANY_BOOL", False)[0])
        self.assertTrue(m("$ANY_OBJECT", {})[0])
        self.assertFalse(m("$ANY_OBJECT", [])[0])
        self.assertTrue(m("$ANY_ARRAY", [1])[0])

    def test_objects_exact_keyset(self):
        self.assertTrue(m({"a": 1, "b": "$ANY_STRING"}, {"a": 1, "b": "x"})[0])
        self.assertFalse(m({"a": 1}, {"a": 1, "b": 2})[0])  # extra key
        self.assertFalse(m({"a": 1, "b": 2}, {"a": 1})[0])  # missing key

    def test_extra_keys_allow(self):
        ok, _ = m({"a": 1, "$extra_keys": "allow"}, {"a": 1, "manifest": {"big": True}})
        self.assertTrue(ok)

    def test_absent_value(self):
        self.assertTrue(m({"a": 1, "err": "$ABSENT"}, {"a": 1})[0])
        self.assertFalse(m({"a": 1, "err": "$ABSENT"}, {"a": 1, "err": "x"})[0])

    def test_optional_value(self):
        pat = {"data": {"$optional": {"details": "$ANY_OBJECT"}}}
        self.assertTrue(m(pat, {})[0])
        self.assertTrue(m(pat, {"data": {"details": {"f": 1}}})[0])
        self.assertFalse(m(pat, {"data": {"details": "notobj"}})[0])

    def test_one_of(self):
        pat = {"$one_of": ["a", "$ANY_INT"]}
        self.assertTrue(m(pat, "a")[0])
        self.assertTrue(m(pat, 7)[0])
        self.assertFalse(m(pat, "b")[0])

    def test_arrays(self):
        self.assertTrue(m([1, "$ANY_STRING"], [1, "x"])[0])
        self.assertFalse(m([1, 2], [1])[0])
        self.assertFalse(m([1, 2], [1, 3])[0])

    def test_capture_bind_and_equality(self):
        caps = {}
        ok, _ = m({"id": "$capture:rid", "x": 1}, {"id": 42, "x": 1}, caps)
        self.assertTrue(ok)
        self.assertEqual(caps["rid"], 42)
        # re-capture with same value → ok
        ok, _ = m({"id": "$capture:rid"}, {"id": 42}, caps)
        self.assertTrue(ok)
        # re-capture with different value → mismatch
        ok, why = m({"id": "$capture:rid"}, {"id": 99}, caps)
        self.assertFalse(ok)
        self.assertIn("rid", why)

    def test_substitute_captures(self):
        caps = {"rid": 42}
        out = substitute_captures({"id": "$capture:rid", "result": {"ok": True}}, caps)
        self.assertEqual(out, {"id": 42, "result": {"ok": True}})
        with self.assertRaises(KeyError):
            substitute_captures({"id": "$capture:nope"}, caps)


class ScenarioValidatorTests(unittest.TestCase):
    def _write(self, obj) -> Path:
        d = Path(tempfile.mkdtemp())
        p = d / "99_tmp.json"
        p.write_text(json.dumps(obj))
        return p

    def test_good_scenario_loads(self):
        p = self._write({
            "name": "ok", "manifest": "[plugin]\nid=\"p\"\nversion=\"0.1.0\"\nname=\"P\"\ndescription=\"d\"\nmin_nexo_version=\">=0.1.0\"\n",
            "fixture_config": {},
            "steps": [{"send": {"method": "initialize", "id": 1}}, {"expect": {"id": 1, "result": "$ANY_OBJECT"}}],
        })
        scn = load_scenario(p)
        self.assertEqual(scn.name, "ok")

    def test_declared_tools_not_in_manifest_rejected(self):
        p = self._write({
            "name": "bad", "manifest": "[plugin]\nid=\"p\"\nversion=\"0.1.0\"\nname=\"P\"\ndescription=\"d\"\nmin_nexo_version=\">=0.1.0\"\n",
            "fixture_config": {"declare_tools": ["p_foo"]},
            "steps": [{"send": {"method": "initialize", "id": 1}}],
        })
        with self.assertRaises(ValueError):
            load_scenario(p)

    def test_capture_before_bind_rejected(self):
        p = self._write({
            "name": "bad2", "manifest": "[plugin]\nid=\"p\"\nversion=\"0.1.0\"\nname=\"P\"\ndescription=\"d\"\nmin_nexo_version=\">=0.1.0\"\n",
            "fixture_config": {},
            "steps": [{"send": {"id": "$capture:rid"}}],
        })
        with self.assertRaises(ValueError):
            load_scenario(p)

    def test_expect_exit_must_be_last(self):
        p = self._write({
            "name": "bad3", "manifest": "[plugin]\nid=\"p\"\nversion=\"0.1.0\"\nname=\"P\"\ndescription=\"d\"\nmin_nexo_version=\">=0.1.0\"\n",
            "fixture_config": {},
            "steps": [{"expect_exit": 0}, {"send": {"method": "x"}}],
        })
        with self.assertRaises(ValueError):
            load_scenario(p)

    def test_manifest_extends_tools_subset_ok(self):
        manifest = ("[plugin]\nid=\"p\"\nversion=\"0.1.0\"\nname=\"P\"\ndescription=\"d\"\nmin_nexo_version=\">=0.1.0\"\n"
                    "\n[plugin.extends]\ntools = [\"p_a\", \"p_b\"]\n")
        p = self._write({
            "name": "ok2", "manifest": manifest,
            "fixture_config": {"declare_tools": ["p_a"]},
            "steps": [{"send": {"method": "initialize", "id": 1}}],
        })
        scn = load_scenario(p)
        self.assertEqual(scn.declared_tools, ["p_a"])


if __name__ == "__main__":
    unittest.main()
