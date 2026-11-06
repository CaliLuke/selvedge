"""Exercise the public check script with isolated command fixtures."""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.name == "posix", "quality hooks require a POSIX shell")
class OrchestrationTests(unittest.TestCase):
    def test_signal_removes_owned_database_container(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts/quality").mkdir(parents=True)
            shutil.copy(ROOT / "scripts/check.sh", root / "scripts/check.sh")
            shutil.copy(ROOT / "scripts/quality/tools.env", root / "scripts/quality/tools.env")
            binaries = root / "bin"
            binaries.mkdir()
            commands = {
                "golangci-lint": 'if [ "$1" = version ]; then echo "golangci-lint has version 2.15.0"; fi\n',
                "python3": "exit 0\n",
                "go": '''case "$1" in
  test) case "$*" in *-json*) touch "$FIXTURE_DIR/entered"; sleep 60;; esac;;
  run) echo '{}';;
esac
''',
                "docker": '''case "$1" in
  run) touch "$FIXTURE_DIR/container"; echo container-id;;
  container) test -f "$FIXTURE_DIR/container";;
  rm) rm "$FIXTURE_DIR/container"; touch "$FIXTURE_DIR/removed";;
  port) echo 127.0.0.1:5432;;
esac
''',
            }
            for name, body in commands.items():
                executable = binaries / name
                executable.write_text("#!/bin/sh\n" + body)
                executable.chmod(0o755)
            environment = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                               FIXTURE_DIR=str(root), GOLANGCI_LINT=str(binaries / "golangci-lint"))
            environment.pop("SELVEDGE_TEST_POSTGRES_URL", None)
            process = subprocess.Popen(["bash", "scripts/check.sh", "full"], cwd=root, env=environment,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                       start_new_session=True)
            try:
                deadline = time.monotonic() + 10
                while not (root / "entered").exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue((root / "entered").exists(), "full gate did not reach the owned-container test stage")
                os.killpg(process.pid, signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(143, process.returncode, stdout + stderr)
                self.assertTrue((root / "removed").exists(), stdout + stderr)
                self.assertFalse((root / "container").exists())
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
