#!/usr/bin/env python3
"""
Phase 0 Verification Script
Verifies that all Phase 0 hygiene tasks have been completed.
"""

import sys
from pathlib import Path


class Phase0Verifier:
    def __init__(self):
        self.repo_root = Path.cwd()
        self.issues = []
        self.warnings = []
        self.passes = []

    def check_gitignore(self):
        """Verify .gitignore exists and has required entries."""
        gitignore_path = self.repo_root / ".gitignore"

        if not gitignore_path.exists():
            self.issues.append("❌ .gitignore file not found")
            return

        required_entries = [
            "__pycache__",
            "*.log",
            ".venv",
            ".pytest_cache",
            ".ruff_cache",
            "*.egg-info",
            ".coverage",
            "htmlcov",
        ]

        with open(gitignore_path) as f:
            content = f.read()

        # Check for pyc files (can be *.pyc or *.py[cod])
        has_pyc = "*.pyc" in content or "*.py[cod]" in content

        missing = [entry for entry in required_entries if entry not in content]

        if missing or not has_pyc:
            all_missing = missing + (["*.pyc"] if not has_pyc else [])
            self.issues.append(
                f"❌ .gitignore missing entries: {', '.join(all_missing)}"
            )
        else:
            self.passes.append("✅ .gitignore has all required entries")

    def check_requirements_files(self):
        """Verify requirements.txt and requirements-dev.txt exist."""
        req_path = self.repo_root / "requirements.txt"
        dev_req_path = self.repo_root / "requirements-dev.txt"

        if not req_path.exists():
            self.issues.append("❌ requirements.txt not found")
        else:
            with open(req_path) as f:
                content = f.read()
                if "Flask" in content and "pyzmq" in content and "requests" in content:
                    self.passes.append("✅ requirements.txt has core dependencies")
                else:
                    self.issues.append("❌ requirements.txt missing core dependencies")

        if not dev_req_path.exists():
            self.issues.append("❌ requirements-dev.txt not found")
        else:
            with open(dev_req_path) as f:
                content = f.read()
                if (
                    "pytest" in content
                    and "ruff" in content
                    and "mypy" in content
                ):
                    self.passes.append("✅ requirements-dev.txt has dev tools")
                else:
                    self.issues.append(
                        "❌ requirements-dev.txt missing dev tools"
                    )

    def check_pyproject_toml(self):
        """Verify pyproject.toml exists with required sections."""
        pyproject_path = self.repo_root / "pyproject.toml"

        if not pyproject_path.exists():
            self.issues.append("❌ pyproject.toml not found")
            return

        required_sections = [
            "[build-system]",
            "[project]",
            "[tool.ruff]",
            "[tool.mypy]",
            "[tool.pytest.ini_options]",
        ]

        with open(pyproject_path) as f:
            content = f.read()

        missing = [section for section in required_sections if section not in content]
        if missing:
            self.issues.append(
                f"❌ pyproject.toml missing sections: {', '.join(missing)}"
            )
        else:
            self.passes.append("✅ pyproject.toml has all required sections")

    def check_python2_compat(self):
        """Check that Python 2 compat imports are removed."""
        python_files = [
            p for p in self.repo_root.glob("**/*.py")
            if "verify-phases" not in str(p)  # Skip verification scripts
            and "client" not in p.parts  # Skip client test files (can be examples)
        ]

        python2_imports = [
            "from __future__ import",
            "from builtins import",
            "from past.utils import",
        ]

        violations = []
        for py_file in python_files:
            with open(py_file) as f:
                content = f.read()

            for import_stmt in python2_imports:
                if import_stmt in content:
                    violations.append(
                        f"  {py_file.relative_to(self.repo_root)}: {import_stmt}"
                    )

        if violations:
            self.issues.append(
                f"❌ Python 2 compatibility imports found:\n{chr(10).join(violations)}"
            )
        else:
            self.passes.append("✅ No Python 2 compatibility imports found")

    def check_indentation(self):
        """Check that no tabs exist in core files."""
        core_files = [
            "raft/protocol.py",
            "raft/raft.py",
            "raft/interface.py",
        ]

        tab_violations = []
        for file_path in core_files:
            full_path = self.repo_root / file_path
            if full_path.exists():
                with open(full_path, "rb") as f:
                    content = f.read()

                if b"\t" in content:
                    tab_violations.append(f"  {file_path}")

        if tab_violations:
            self.issues.append(
                f"❌ Tab characters found in:\n{chr(10).join(tab_violations)}"
            )
        else:
            self.passes.append("✅ No tabs found in core files (using spaces)")

    def check_dead_code_removed(self):
        """Verify dead code files have been removed."""
        dead_files = [
            "raft/start.py",
            "raft/modified_raft.py",
            "server/server.py",
        ]

        remaining = []
        for file_path in dead_files:
            full_path = self.repo_root / file_path
            if full_path.exists():
                remaining.append(file_path)

        if remaining:
            self.warnings.append(
                f"⚠️  Dead code files still present:\n  {chr(10).join(f'  {f}' for f in remaining)}"
            )
        else:
            self.passes.append("✅ Dead code files removed")

    def check_flask_server(self):
        """Verify flask_http_server.py exists and has required content."""
        flask_path = self.repo_root / "flask_http_server.py"

        if not flask_path.exists():
            self.issues.append("❌ flask_http_server.py not found")
            return

        with open(flask_path) as f:
            content = f.read()

        if "Flask" in content and "def " in content:
            self.passes.append("✅ flask_http_server.py present and has endpoints")
        else:
            self.issues.append("❌ flask_http_server.py appears incomplete")

    def run_all_checks(self):
        """Run all verification checks."""
        print("\n🔍 Running Phase 0 Verification Checks...\n")

        self.check_gitignore()
        self.check_requirements_files()
        self.check_pyproject_toml()
        self.check_python2_compat()
        self.check_indentation()
        self.check_dead_code_removed()
        self.check_flask_server()

        return self.print_results()

    def print_results(self):
        """Print verification results."""
        print("\n" + "=" * 60)
        print("PHASE 0 VERIFICATION RESULTS")
        print("=" * 60 + "\n")

        if self.passes:
            print("✅ PASSING CHECKS:")
            for check in self.passes:
                print(f"  {check}")
            print()

        if self.warnings:
            print("⚠️  WARNINGS:")
            for warning in self.warnings:
                print(f"  {warning}")
            print()

        if self.issues:
            print("❌ ISSUES FOUND:")
            for issue in self.issues:
                print(f"  {issue}")
            print()

        print("=" * 60)

        if self.issues:
            print(f"❌ Phase 0 INCOMPLETE - {len(self.issues)} issues to fix")
            return False
        elif self.warnings:
            print(
                f"⚠️  Phase 0 READY - {len(self.warnings)} warnings (review before merge)"
            )
            return True
        else:
            print(f"✅ Phase 0 COMPLETE - All {len(self.passes)} checks passed!")
            return True


def main():
    verifier = Phase0Verifier()
    success = verifier.run_all_checks()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
