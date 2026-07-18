import importlib.util
import tempfile
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

    def test_legacy_checkout_runtime_is_rejected(self) -> None:
        original_root = CHECK.ROOT
        with tempfile.TemporaryDirectory() as directory:
            CHECK.ROOT = Path(directory)
            workflow_dir = CHECK.ROOT / ".github/workflows"
            workflow_dir.mkdir(parents=True)
            workflow = workflow_dir / "ci.yaml"
            workflow.write_text("uses: actions/checkout@v4\n", encoding="utf-8")
            try:
                self.assertEqual(
                    CHECK.check_checkout_action_versions(),
                    [
                        ".github/workflows/ci.yaml: actions/checkout uses v4; "
                        "expected v7 for the Node.js 24 runtime"
                    ],
                )
                workflow.write_text("uses: actions/checkout@v7\n", encoding="utf-8")
                self.assertEqual(CHECK.check_checkout_action_versions(), [])
            finally:
                CHECK.ROOT = original_root


if __name__ == "__main__":
    unittest.main()
