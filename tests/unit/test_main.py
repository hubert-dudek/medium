from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from videobundle.main import build_message


def test_build_message_formats_output() -> None:
    assert build_message("hello") == "template-demo:hello"
