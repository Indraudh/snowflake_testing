"""
validate_snowsql.py

For each changed .snowsql file:
  1. Substitute ${TOKEN_NAME} placeholders using GitHub environment variables
  2. Write the substituted content to a temp .sql file
  3. Run sqlfluff parse with --dialect snowflake
  4. Collect errors and write to /tmp/snowsql_errors.txt for the email step
  5. Exit 1 if any errors found — triggers the email notification in the workflow
"""

import os
import re
import sys
import subprocess
import tempfile

ERROR_LOG = "/tmp/snowsql_errors.txt"


# ── Token substitution ────────────────────────────────────────────────────────

def substitute_tokens(content: str) -> tuple[str, list[str]]:
    """Replace ${TOKEN_NAME} with values from environment variables."""
    missing = []

    def replacer(match):
        token = match.group(1)
        value = os.environ.get(token)
        if value is None:
            missing.append(token)
            # Replace with a dummy valid SQL identifier so sqlfluff
            # doesn't fail on the placeholder itself
            return f"'__MISSING_{token}__'"
        return value

    substituted = re.sub(r'\$\{([^}]+)\}', replacer, content)
    return substituted, missing


# ── Validation ────────────────────────────────────────────────────────────────

def validate_file(filepath: str) -> tuple[bool, str, list[str]]:
    """
    Substitute tokens and validate a .snowsql file using sqlfluff
    with the Snowflake dialect.
    Returns (passed, error_output, missing_tokens).
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    substituted, missing_tokens = substitute_tokens(content)

    # Write substituted content to a temp .sql file
    # sqlfluff needs a .sql extension to apply dialect rules correctly
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.sql', delete=False, encoding='utf-8'
    ) as tmp:
        tmp.write(substituted)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            [
                'sqlfluff', 'parse',
                '--dialect', 'snowflake',   # Snowflake dialect inline — no .sqlfluff file needed
                '--disable-progress-bar',
                '--nofail',          # Don't exit non-zero for warnings, only real parse errors
                tmp_path
            ],
            capture_output=True,
            text=True,
            timeout=120
        )

        output = result.stdout + result.stderr

        # Detect actual parse failures
        has_error = (
            '[UNPARSABLE]' in output or
            'FATAL' in output or
            result.returncode not in (0, 1)  # sqlfluff exits 1 for lint warnings, 0 for clean
        )

        return not has_error, output, missing_tokens

    except subprocess.TimeoutExpired:
        return False, f"Validation timed out after 120 seconds.", missing_tokens
    finally:
        os.unlink(tmp_path)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    files_env = os.environ.get('CHANGED_FILES', '').strip()

    if not files_env:
        print("No changed .snowsql files detected. Skipping validation.")
        sys.exit(0)

    files = [f for f in files_env.split() if f.endswith('.snowsql')]

    if not files:
        print("No .snowsql files in changed set. Skipping.")
        sys.exit(0)

    print(f"\n{'═'*60}")
    print(f"  SnowSQL Syntax Validator  (sqlfluff · dialect: snowflake)")
    print(f"  Files to validate: {len(files)}")
    print(f"{'═'*60}\n")

    all_errors = []
    failed_files = []

    for filepath in files:
        print(f"▶ Validating: {filepath}")

        if not os.path.exists(filepath):
            print(f"  ⚠️  File not found (possibly deleted in this commit). Skipping.\n")
            continue

        passed, output, missing_tokens = validate_file(filepath)

        if missing_tokens:
            print(f"  ⚠️  Missing GitHub env vars for tokens: {missing_tokens}")
            print(f"     Add them in: Settings → Environments → snowflake-validation")

        if passed:
            print(f"  ✅ PASSED\n")
        else:
            print(f"  ❌ FAILED")
            for line in output.strip().splitlines():
                print(f"    {line}")
            print()
            failed_files.append(filepath)
            all_errors.append({
                'file': filepath,
                'output': output.strip()
            })

    # Write error report for the email step
    if all_errors:
        with open(ERROR_LOG, 'w') as f:
            for err in all_errors:
                f.write(f"File: {err['file']}\n")
                f.write("-" * 60 + "\n")
                f.write(err['output'] + "\n\n")

    print(f"{'═'*60}")
    if failed_files:
        print(f"  RESULT: {len(failed_files)}/{len(files)} file(s) failed validation.")
        print(f"  Failed: {', '.join(failed_files)}")
        print(f"{'═'*60}\n")
        sys.exit(1)
    else:
        print(f"  RESULT: All {len(files)} file(s) passed. ✅")
        print(f"{'═'*60}\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
