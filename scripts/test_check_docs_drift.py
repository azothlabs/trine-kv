import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check_docs_drift.py")
SPEC = importlib.util.spec_from_file_location("check_docs_drift", SCRIPT)
assert SPEC and SPEC.loader
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class DocumentationDriftTests(unittest.TestCase):
    def test_repository_release_contract_is_aligned(self) -> None:
        self.assertEqual(CHECK.check_dependency_versions(), [])
        self.assertEqual(CHECK.check_release_contract(), [])
        self.assertEqual(CHECK.check_local_markdown_links("README.md"), [])


if __name__ == "__main__":
    unittest.main()
