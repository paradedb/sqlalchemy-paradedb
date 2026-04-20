"""Validate that api.json5 matches the ORM wrapper surface."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import json5

ROOT = Path(__file__).resolve().parents[1]
API_JSON = ROOT / "api.json5"
APIIGNORE_JSON = ROOT / "apiignore.json5"
PDB_SYMBOL_RE = re.compile(r"\bpdb\.[A-Za-z_][A-Za-z0-9_]*\b")


def load_json(path: Path) -> object:
    try:
        return json5.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{path} not found") from exc
    except ValueError as exc:
        raise ValueError(f"invalid JSON5 in {path}: {exc}") from exc


def flatten_ignore(section: object, *, kind: str) -> set[str]:
    if section is None:
        return set()
    if isinstance(section, list):
        return {str(item) for item in section}
    if isinstance(section, dict):
        flattened: set[str] = set()
        for values in section.values():
            if not isinstance(values, list):
                raise ValueError(f"apiignore {kind} section values must be arrays when grouped.")
            flattened.update(str(item) for item in values)
        return flattened
    raise ValueError(f"apiignore {kind} section must be an array or object of arrays.")


def source_paths() -> list[Path]:
    return sorted(path for path in ROOT.glob("paradedb/**/*.py") if path.name != "api.py")


def parse_module(path: Path) -> ast.AST:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        raise SyntaxError(f"failed to parse {path}: {exc}") from exc


def _attribute_chain(node: ast.AST) -> str | None:
    parts: list[str] = []
    current: ast.AST | None = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _string_literal(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_name(node: ast.Call) -> str | None:
    return _attribute_chain(node.func)


class APIReferenceCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.pdb_symbols: set[str] = set()
        self.operator_symbols: set[str] = set()

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self.pdb_symbols.update(PDB_SYMBOL_RE.findall(node.value))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        chain = _attribute_chain(node)
        if chain and chain.startswith("func.pdb."):
            self.pdb_symbols.add("pdb." + chain.removeprefix("func.pdb."))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        call_name = _call_name(node)

        if call_name == "PDBFunctionWithNamedArgs" and node.args:
            function_name = _string_literal(node.args[0])
            if function_name is not None:
                self.pdb_symbols.add(f"pdb.{function_name}")

        elif call_name == "PDBCast" and len(node.args) >= 2:
            type_name = _string_literal(node.args[1])
            if type_name is not None:
                self.pdb_symbols.add(f"pdb.{type_name}")

        elif call_name == "_build_spec" and node.args:
            tokenizer_name = _string_literal(node.args[0])
            if tokenizer_name is not None:
                self.pdb_symbols.add(f"pdb.{tokenizer_name}")

        elif call_name == "TokenizerSpec":
            tokenizer_name: str | None = None
            if node.args:
                tokenizer_name = _string_literal(node.args[0])
            if tokenizer_name is None:
                for keyword in node.keywords:
                    if keyword.arg == "name":
                        tokenizer_name = _string_literal(keyword.value)
                        break
            if tokenizer_name is not None:
                self.pdb_symbols.add(f"pdb.{tokenizer_name}")

        elif call_name == "operators.custom_op" and node.args:
            operator_symbol = _string_literal(node.args[0])
            if operator_symbol is not None:
                self.operator_symbols.add(operator_symbol)

        self.generic_visit(node)


def collect_api_references(module: ast.AST) -> tuple[set[str], set[str]]:
    collector = APIReferenceCollector()
    collector.visit(module)
    return collector.pdb_symbols, collector.operator_symbols


def main() -> int:
    try:
        api = load_json(API_JSON)
        apiignore = load_json(APIIGNORE_JSON) if APIIGNORE_JSON.is_file() else {}
    except (FileNotFoundError, ValueError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    if not isinstance(api, dict):
        print("❌ api.json5 must contain a JSON object.", file=sys.stderr)
        return 1
    if not isinstance(apiignore, dict):
        print("❌ apiignore.json5 must contain a JSON object.", file=sys.stderr)
        return 1

    try:
        operators = api["operators"]
        functions = api["functions"]
        types = api["types"]
    except KeyError as exc:
        print(f"❌ api.json5 missing required section: {exc}", file=sys.stderr)
        return 1

    if not all(isinstance(section, dict) for section in (operators, functions, types)):
        print(
            "❌ api.json5 sections operators/functions/types must all be objects.",
            file=sys.stderr,
        )
        return 1

    expected_operator_symbols = {str(value) for value in operators.values()}
    expected_pdb_symbols = {
        *(str(value) for value in functions.values()),
        *(str(value) for value in types.values()),
    }

    try:
        ignored_functions = flatten_ignore(apiignore.get("functions"), kind="functions")
        ignored_types = flatten_ignore(apiignore.get("types"), kind="types")
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    referenced_pdb_symbols: set[str] = set()
    referenced_operator_symbols: set[str] = set()

    try:
        paths = source_paths()
        for path in paths:
            module = parse_module(path)
            file_pdb_symbols, file_operator_symbols = collect_api_references(module)
            referenced_pdb_symbols.update(file_pdb_symbols)
            referenced_operator_symbols.update(file_operator_symbols)
    except (SyntaxError, OSError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    missing_pdb_symbols = sorted(expected_pdb_symbols - referenced_pdb_symbols)
    missing_operator_symbols = sorted(expected_operator_symbols - referenced_operator_symbols)

    allowed_symbols = {
        *expected_pdb_symbols,
        *ignored_functions,
        *ignored_types,
    }
    untracked_symbols = sorted(referenced_pdb_symbols - allowed_symbols)

    issues: list[str] = []
    if missing_pdb_symbols:
        issues.append("api.json5 pdb.* symbols not referenced by ORM wrappers: " + ", ".join(missing_pdb_symbols))
    if missing_operator_symbols:
        issues.append("api.json5 operators not referenced by ORM wrappers: " + ", ".join(missing_operator_symbols))
    if untracked_symbols:
        issues.append(
            "pdb.* symbols used in package source but missing from api.json5/apiignore.json5: "
            + ", ".join(untracked_symbols)
        )

    if issues:
        print("❌ API coverage check failed:", file=sys.stderr)
        for issue in issues:
            print(f"   - {issue}", file=sys.stderr)
        print(
            "\nUpdate api.json5, apiignore.json5, or the ORM wrappers so they stay in sync.",
            file=sys.stderr,
        )
        return 1

    print("✅ API coverage check passed.")
    covered_total = len(expected_pdb_symbols & referenced_pdb_symbols) + len(
        expected_operator_symbols & referenced_operator_symbols
    )
    expected_total = len(expected_pdb_symbols) + len(expected_operator_symbols)
    print(f"   api symbols referenced: {covered_total}/{expected_total}")
    print(
        f"   source files: {len(paths)}, concrete API references checked: "
        f"{len(referenced_pdb_symbols) + len(referenced_operator_symbols)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
