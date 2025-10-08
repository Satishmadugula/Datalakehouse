from pathlib import Path

import ast

ROOT = Path(__file__).resolve().parents[1]


def test_schema_evolution_script_has_functions():
    tree = ast.parse((ROOT / "docker" / "spark" / "jobs" / "schema_evolution_demo.py").read_text())
    func_names = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    for expected in {"run_version_1", "promote_new_fields", "run_version_2", "rename_field"}:
        assert expected in func_names
