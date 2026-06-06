from __future__ import annotations

import argparse


def build_message(message: str) -> str:
    return f"template-demo:{message}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--message", required=True)
    args = parser.parse_args()

    print(build_message(f"{args.catalog}:{args.message}"))


if __name__ == "__main__":
    main()
