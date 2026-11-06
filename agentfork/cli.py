"""Small entry point for diagnostics, benchmark runs, and the local viewer."""

import argparse
import json
import runpy
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("command", choices=["doctor", "audit", "run", "view", "inventory"])
    if len(sys.argv) == 1 or sys.argv[1] in {"-h", "--help"}:
        parser.print_help()
        return
    args, rest = parser.parse_known_args()
    if args.command in {"run", "view"}:
        module = "agentfork.campaign" if args.command == "run" else "agentfork.viewer.server"
        sys.argv = [module, *rest]
        runpy.run_module(module, run_name="__main__")
    elif args.command == "doctor":
        from agentfork.doctor import main as doctor

        doctor(rest)
    elif args.command == "audit":
        from agentfork.checkpoints import main as audit

        audit(rest)
    else:
        from agentfork.config import task_inventory

        print(json.dumps({"tasks": task_inventory(rest[0] if rest else None)}, indent=2))


if __name__ == "__main__":
    main()
