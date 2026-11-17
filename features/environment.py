"""behave setup: the repository root on the import path, and a temporary folder for pending drafts."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def before_scenario(context, scenario):
    context.tmp = tempfile.TemporaryDirectory()


def after_scenario(context, scenario):
    context.tmp.cleanup()
