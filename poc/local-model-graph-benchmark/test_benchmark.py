import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from benchmark import build_context, resolve_cases


class BenchmarkContextTest(unittest.TestCase):
    def test_free_form_prompt_creates_one_case(self) -> None:
        args = Namespace(prompt="Trace statement execution", case_id=None, case_limit=0)

        cases = resolve_cases(args, Path(__file__).resolve().parent)

        self.assertEqual(
            [
                {
                    "id": "free-form",
                    "prompt": "Trace statement execution",
                    "retrieval_terms": [],
                    "criteria": [],
                    "references": [],
                }
            ],
            cases,
        )

    def test_free_form_prompt_rejects_case_selection(self) -> None:
        args = Namespace(
            prompt="Trace statement execution", issue_url=None,
            case_id=["prepared-statement-execution"], case_limit=0
        )

        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            resolve_cases(args, Path(__file__).resolve().parent)

    @patch("benchmark._fetch_github_issue")
    def test_issue_url_creates_case_with_issue_and_instruction(self, fetch_issue) -> None:
        fetch_issue.return_value = {
            "number": 2957,
            "html_url": "https://github.com/microsoft/mssql-jdbc/issues/2957",
            "title": "Enclave queries bypass the CEK cache",
            "body": "Repeated queries call the key provider every time.",
        }
        args = Namespace(
            prompt="Find the root cause and fix.",
            issue_url="https://github.com/microsoft/mssql-jdbc/issues/2957",
            case_id=None,
            case_limit=0,
        )

        cases = resolve_cases(args, Path(__file__).resolve().parent)

        self.assertEqual("github-issue-2957", cases[0]["id"])
        self.assertIn("Find the root cause and fix.", cases[0]["prompt"])
        self.assertIn("Enclave queries bypass the CEK cache", cases[0]["prompt"])
        self.assertIn("Repeated queries call the key provider every time.", cases[0]["prompt"])
        self.assertEqual(["Enclave queries bypass the CEK cache", "Repeated queries call the key provider every time."],
                         cases[0]["retrieval_terms"])

    def test_graph_condition_adds_sanitized_graph_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            source_file = repo_root / "src" / "Example.java"
            source_file.parent.mkdir(parents=True)
            source_file.write_text("class Example extends BaseExample {}\n", encoding="utf-8")

            graph_file = repo_root / "target" / "knowledge-graph" / "knowledge-graph.json"
            graph_file.parent.mkdir(parents=True)
            graph_file.write_text(
                json.dumps(
                    {
                        "metadata": {"path": str(repo_root)},
                        "nodes": [
                            {"id": "class:Example", "type": "class", "name": "Example", "path": str(source_file)},
                            {"id": "class:BaseExample", "type": "class", "name": "BaseExample"},
                        ],
                        "edges": [{"from": "class:Example", "to": "class:BaseExample", "type": "extends"}],
                    }
                ),
                encoding="utf-8",
            )

            source_context = build_context(repo_root, graph_file, "How does Example extend BaseExample?", False, 4_000)
            graph_context = build_context(repo_root, graph_file, "How does Example extend BaseExample?", True, 4_000)

            self.assertNotIn("GRAPH RELATIONSHIPS", source_context)
            self.assertTrue(graph_context.startswith(source_context))
            self.assertIn("GRAPH RELATIONSHIPS", graph_context)
            self.assertIn("class:Example --extends--> class:BaseExample", graph_context)
            self.assertNotIn(str(repo_root), graph_context)
            self.assertLessEqual(len(graph_context), 4_000)


if __name__ == "__main__":
    unittest.main()