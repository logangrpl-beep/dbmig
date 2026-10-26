"""Repo-level rules from CLAUDE.md / test_spec §3.9."""
import ast
import re

from tests.conftest import REPO

CODE_DIRS = ["src", "scripts", "notebooks"]


def _py_files():
    for d in CODE_DIRS:
        yield from sorted((REPO / d).rglob("*.py"))


def test_no_hardcoded_catalog():
    # X2: the catalog name must never appear in code as an identifier prefix.
    pattern = re.compile(r"\bdbmig\s*\.|`dbmig`")
    hits = [f"{p.relative_to(REPO)}:{i}" for p in _py_files()
            for i, line in enumerate(p.read_text().splitlines(), 1) if pattern.search(line)]
    assert hits == []


def test_no_per_table_code():
    # Table behaviour comes from tables.yaml: no table names as string literals in src/.
    names = {"customers", "products", "orders"}
    hits = []
    for p in sorted((REPO / "src").rglob("*.py")):
        for node in ast.walk(ast.parse(p.read_text())):
            if isinstance(node, ast.Constant) and node.value in names:
                hits.append(f"{p.relative_to(REPO)}:{node.lineno}")
    assert hits == []


def test_every_src_function_has_a_test():
    tests_text = "\n".join(p.read_text() for p in (REPO / "tests").glob("test_*.py"))
    missing = []
    for p in sorted((REPO / "src").rglob("*.py")):
        for node in ast.parse(p.read_text()).body:
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and not node.name.startswith("_"):
                if not re.search(rf"\b{node.name}\b", tests_text):
                    missing.append(f"{p.stem}.{node.name}")
    assert missing == []


def test_notebook_compiles():
    src = (REPO / "notebooks" / "run_pipeline.py").read_text()
    assert src.startswith("# Databricks notebook source")
    compile(src, "run_pipeline.py", "exec")
