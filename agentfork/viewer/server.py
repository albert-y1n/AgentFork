"""Read-only local explorer for saved Claude Code / AgentFork experiment traces."""

from __future__ import annotations
from agentfork.config import task_root

import argparse
import asyncio
import gzip
import hashlib
import json
import re
import threading
import time
from collections import OrderedDict
from pathlib import Path

from aiohttp import web

from agentfork.config import ROOT as WORKSPACE

MANAGED_NATIVE_ROOT = WORKSPACE / "storage/btrfs-criu-tb40/retained-native-transcripts"
STATIC = Path(__file__).parent
ATTEMPT = re.compile(r"(?:root-\d+|branch-r\d+-\d+)\Z")
REQUEST = re.compile(r"\d{5}\.json\.gz\Z")
SECRET_KEYS = {
    "api_key",
    "authorization",
    "x-api-key",
    "anthropic_api_key",
    "anthropic_auth_token",
    "tb40_api_key",
    "openai_api_key",
    "access_token",
}


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def read_gzip(path: Path):
    try:
        with gzip.open(path, "rt") as stream:
            return json.load(stream)
    except (OSError, EOFError, ValueError):
        return None


def events(path: Path):
    result = []
    try:
        for line in path.read_text().splitlines():
            try:
                result.append(json.loads(line))
            except ValueError:
                continue
    except OSError:
        pass
    return result


def redact(value):
    if isinstance(value, dict):
        return {k: "[REDACTED]" if k.lower() in SECRET_KEYS else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        value = re.sub(
            r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b", "[REDACTED]", value
        )
        value = re.sub(r"\bsk-[A-Za-z0-9_-]{12,}\b", "[REDACTED]", value)
        value = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~-]{12,}", r"\1[REDACTED]", value)
        return re.sub(
            r"(?im)((?:ANTHROPIC_AUTH_TOKEN|ANTHROPIC_API_KEY|TB40_API_KEY|OPENAI_API_KEY)\s*=\s*)[^\s]+",
            r"\1[REDACTED]",
            value,
        )
    return value


def text_content(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(text_content(item) for item in content)
    if isinstance(content, dict):
        if content.get("type") in ("text", "thinking"):
            return content.get("text", content.get("thinking", ""))
        if "content" in content:
            return text_content(content["content"])
        return json.dumps(content, ensure_ascii=False)
    return str(content or "")


def parse_response(path: Path):
    """Read only completed, persisted provider responses, never the live stream."""
    try:
        with gzip.open(path, "rt") as stream:
            lines = stream.read().splitlines()
    except (OSError, EOFError):
        return None
    blocks, partial, complete = {}, {}, False
    stream_error = False
    sse_type = ""
    for line in lines:
        if not line:
            sse_type = ""
        if line.startswith("event:"):
            sse_type = line[6:].strip()
        if not line.startswith("data:"):
            continue
        try:
            event = json.loads(line[5:].strip())
        except ValueError:
            continue
        kind = event.get("type")
        if sse_type == "error" or kind == "error" or ("error" in event and not kind):
            stream_error = True
        if kind == "content_block_start":
            blocks[event["index"]] = event["content_block"]
        elif kind == "content_block_delta":
            index, delta = event["index"], event["delta"]
            block = blocks.setdefault(index, {})
            if delta.get("type") == "text_delta":
                block["text"] = block.get("text", "") + delta["text"]
            elif delta.get("type") == "thinking_delta":
                block["thinking"] = block.get("thinking", "") + delta["thinking"]
            elif delta.get("type") == "input_json_delta":
                partial[index] = partial.get(index, "") + delta["partial_json"]
        elif kind == "message_stop":
            complete = True
    if not complete or stream_error:
        return None
    for index, value in partial.items():
        try:
            blocks[index]["input"] = json.loads(value)
        except ValueError:
            blocks[index]["input"] = {"_partial_json": value}
    return {"role": "assistant", "content": [blocks[k] for k in sorted(blocks)]}


def usage(rows):
    answer = dict(requests=len(rows), input_tokens=0, output_tokens=0, cached_tokens=0)
    for row in rows:
        u = row.get("usage", {})
        cache = u.get("cache_read_input_tokens", 0)
        answer["input_tokens"] += u.get("input_tokens", 0) + cache + u.get("cache_creation_input_tokens", 0)
        answer["output_tokens"] += u.get("output_tokens", 0)
        answer["cached_tokens"] += cache
    return answer


def normalize(messages, points, origin):
    turns, calls = [], {}
    boundary = origin.get("message_count", 0) if origin else 0
    for index, message in enumerate(messages):
        content = message.get("content", [])
        if isinstance(content, str):
            content = [{"type": "text", "text": content}]
        if message.get("role") == "assistant":
            turn = {
                "id": len(turns),
                "message_index": index,
                "end_message_index": index,
                "inherited": index < boundary,
                "guided": bool(origin and index == boundary),
                "thinking": [],
                "text": [],
                "tools": [],
                "checkpoints": [],
            }
            for block in content:
                kind = block.get("type")
                if kind == "thinking":
                    turn["thinking"].append(block.get("thinking", ""))
                elif kind == "text":
                    turn["text"].append(block.get("text", ""))
                elif kind == "tool_use":
                    tool = {
                        "id": block.get("id"),
                        "name": block.get("name", "Tool"),
                        "input": block.get("input", {}),
                        "output": None,
                        "is_error": False,
                    }
                    turn["tools"].append(tool)
                    calls[tool["id"]] = (turn, tool)
            turns.append(turn)
        elif message.get("role") == "user":
            for block in content:
                if block.get("type") == "tool_result" and block.get("tool_use_id") in calls:
                    turn, tool = calls[block["tool_use_id"]]
                    tool["output"] = text_content(block.get("content", ""))
                    tool["is_error"] = bool(block.get("is_error"))
                    turn["end_message_index"] = max(index, turn["end_message_index"])
    for point in points:
        candidates = [t for t in turns if t["message_index"] < point["message_count"]]
        if candidates:
            candidates[-1]["checkpoints"].append(point["step"])
    return turns


def turn_summary(turn):
    tools = turn["tools"]
    first = tools[0] if tools else None
    inp = first["input"] if first and isinstance(first["input"], dict) else {}
    title = inp.get("description") or inp.get("command") or inp.get("file_path") or inp.get("pattern")
    title = str(title or " ".join(turn["text"]) or "模型思考").strip()
    return {k: turn[k] for k in ("id", "message_index", "inherited", "guided", "checkpoints")} | {
        "title": title[:210],
        "tools": [t["name"] for t in tools],
        "thinking_chars": sum(map(len, turn["thinking"])),
        "has_error": any(t["is_error"] for t in tools),
        "pending_tools": sum(t["output"] is None for t in tools),
        "preview": " ".join(str(t["input"]) for t in tools)[:1600],
    }


class Store:
    def __init__(self, campaign: Path):
        self.campaign = campaign.resolve()
        self.config = read_json(campaign / "configuration.json")
        if not self.config:
            raise ValueError("Campaign configuration.json is missing")
        self.tasks = set(self.config["task_names"])
        self.cache = OrderedDict()
        self.lock = threading.RLock()

    def task_path(self, task):
        if task not in self.tasks:
            raise web.HTTPNotFound(text="Unknown task")
        return self.campaign / task

    def attempt_path(self, task, attempt):
        if not ATTEMPT.fullmatch(attempt):
            raise web.HTTPNotFound(text="Unknown attempt")
        path = self.task_path(task) / "attempts" / attempt
        if not path.is_dir():
            raise web.HTTPNotFound(text="Attempt has not started")
        return path

    def overview(self):
        self.config = read_json(self.campaign / "configuration.json", self.config)
        progress = read_json(self.campaign / "progress.json", {})
        diagnostics = read_json(self.campaign / "debug-diagnostics.json", {})
        tasks = []
        for name in self.config["task_names"]:
            folder = self.campaign / name
            result = read_json(folder / "result.json")
            error = read_json(folder / "error.json")
            history = events(folder / "events.jsonl")
            recovery = read_json(folder / "recovery.json", {})
            status = (
                "complete"
                if result
                else "error"
                if error
                else "pending"
                if recovery.get("state") == "queued"
                else "running"
                if history
                else "pending"
            )
            if progress.get("state") == "paused" and status == "running":
                status = "paused"
            if progress.get("state") == "stopped_incomplete" and status == "running":
                status = "cancelled"
            tasks.append(
                {
                    "name": name,
                    "status": status,
                    "baseline_resolved": result.get("baseline_resolved") if result else None,
                    "agentfork_resolved": result.get("agentfork_resolved") if result else None,
                    "root_resolved": result.get("root_resolved") if result else None,
                    "last_event": history[-1] if history else None,
                    "attempts": len(list((folder / "attempts").glob("*"))),
                    "diagnosis": diagnostics.get("tasks", {}).get(name),
                    "retry": read_json(folder / "retry.json"),
                    "recovery": {"state": recovery.get("state")} if recovery else None,
                }
            )
        return {
            "campaign": self.campaign.name,
            "configuration": self.config,
            "progress": progress,
            "tasks": tasks,
            "resources": read_json(self.campaign / "resources.json", {}),
            "health": read_json(self.campaign / "health.json"),
            "storage_probe": read_json(WORKSPACE / "runs/tb40-preflight/storage-100-snapshots.json"),
            "server_time": time.time(),
            "source": "live_local_experiment",
        }

    def task(self, name):
        folder = self.task_path(name)
        paused = read_json(self.campaign / "progress.json", {}).get("state") == "paused"
        case_error = read_json(folder / "error.json")
        recovery = read_json(folder / "recovery.json", {})
        history = events(folder / "events.jsonl")
        reviews = []
        plans = {}
        for path in sorted((folder / "review").glob("round-*/plan.json")):
            plan = read_json(path, {})
            round_index = int(path.parent.name.split("-")[-1])
            reviews.append({"round": round_index, **plan})
            for i, branch in enumerate(plan.get("branches", [])):
                plans[f"branch-r{round_index}-{i}"] = branch
        attempts = []
        for path in sorted((folder / "attempts").glob("*")):
            if not path.is_dir() or not ATTEMPT.fullmatch(path.name):
                continue
            record = read_json(path / "outcome.json") or read_json(path / "agentfork.rollout.json", {})
            local_events = [e for e in history if e.get("attempt") == path.name]
            last = local_events[-1] if local_events else {}
            start = next((e for e in local_events if e["event"] == "rollout_started"), {})
            origin = record.get("origin") or start.get("origin")
            points = read_json(path / "checkpoints.json", [])
            rows = read_json(path / "api-usage.json", [])
            reward = record.get("reward")
            state = (
                "passed"
                if reward == 1
                else "failed"
                if reward is not None
                else "queued"
                if recovery.get("state") == "queued"
                else "cancelled"
                if (
                    record.get("error", "").startswith("CancelledError")
                    or case_error
                    and record.get("status") not in ("agent_error", "infrastructure_error", "restore_failed")
                )
                else "error"
                if record.get("error") or record.get("status") in ("agent_error", "infrastructure_error")
                else "grading"
                if last.get("event") == "grading"
                else "running"
            )
            if paused and state in ("running", "grading"):
                state = "paused"
            attempts.append(
                {
                    "name": path.name,
                    "state": state,
                    "reward": reward,
                    "origin": origin,
                    "started_at": start.get("time"),
                    "wall_seconds": record.get("wall_seconds"),
                    "checkpoints": len(points),
                    "usage": usage(rows),
                    "plan": plans.get(path.name),
                    "checkpoint_failures": record.get(
                        "checkpoint_failures",
                        read_json(path / "actor-restarts.json", {}).get("checkpoint_failures", []),
                    ),
                    "analysis": read_json(path / "analysis.json"),
                }
            )
        dataset = task_root(WORKSPACE) / name / "instruction.md"
        return {
            "name": name,
            "instruction": dataset.read_text() if dataset.exists() else "",
            "attempts": attempts,
            "reviews": reviews,
            "events": history,
            "gate": read_json(folder / "gate.json"),
            "result": read_json(folder / "result.json"),
            "error": case_error,
            "review_usage": read_json(folder / "review-usage.json", []),
            "diagnostics": read_json(self.campaign / "debug-diagnostics.json", {}).get("tasks", {}).get(name),
            "retry": read_json(folder / "retry.json"),
            "run_configuration": read_json(folder / "run-configuration.json", self.config),
            "recovery": {
                "state": recovery.get("state"),
                "resumed_attempts": list(recovery.get("resume_attempts", {})),
            }
            if recovery
            else None,
        }

    def trace(self, task, attempt):
        folder = self.attempt_path(task, attempt)
        with self.lock:
            requests = sorted(p for p in (folder / "requests").glob("*.json.gz") if REQUEST.fullmatch(p.name))
            files = requests[-2:] + [
                folder / "checkpoints.json",
                folder / "agentfork.rollout.json",
                folder / "outcome.json",
            ]
            if requests:
                files.append(requests[-1].with_name(requests[-1].name.replace(".json.gz", ".sse.gz")))
            signature = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in files if p.exists())
            key = (task, attempt)
            if key in self.cache and self.cache[key]["signature"] == signature:
                self.cache.move_to_end(key)
                return self.cache[key]
            body, source = {}, None
            for path in reversed(requests):
                body = read_gzip(path)
                if body:
                    source = path
                    break
            messages = list((body or {}).get("messages", []))
            response = (
                parse_response(source.with_name(source.name.replace(".json.gz", ".sse.gz")))
                if source
                else None
            )
            if response:
                messages.append(response)
            record = read_json(folder / "outcome.json") or read_json(folder / "agentfork.rollout.json", {})
            origin = record.get("origin")
            if not origin:
                origin_event = next(
                    (
                        e
                        for e in events(folder.parents[1] / "events.jsonl")
                        if e.get("attempt") == attempt and e["event"] == "rollout_started"
                    ),
                    {},
                )
                origin = origin_event.get("origin")
                if origin:
                    parent_points = read_json(folder.parent / origin["attempt"] / "checkpoints.json", [])
                    origin = next(
                        (p for p in parent_points if p["step"] == origin["step"]),
                        origin,
                    )
            points = read_json(folder / "checkpoints.json", [])
            turns = normalize(messages, points, origin)
            result = {
                "signature": signature,
                "turns": turns,
                "points": points,
                "origin": origin,
                "record": record,
                "request": source.name if source else None,
                "response_complete": bool(response),
            }
            self.cache[key] = result
            self.cache.move_to_end(key)
            while len(self.cache) > 8:
                self.cache.popitem(last=False)
            return result

    def attempt(self, task, attempt):
        trace = self.trace(task, attempt)
        return {
            "name": attempt,
            "turns": [turn_summary(t) for t in trace["turns"]],
            "points": trace["points"],
            "origin": trace["origin"],
            "latest_request": trace["request"],
            "response_complete": trace["response_complete"],
            "revision": hashlib.sha256(repr(trace["signature"]).encode()).hexdigest()[:16],
            "record": trace["record"],
        }

    def turn(self, task, attempt, index):
        trace = self.trace(task, attempt)
        if index < 0 or index >= len(trace["turns"]):
            raise web.HTTPNotFound(text="Unknown turn")
        turn = trace["turns"][index]
        return {
            **turn,
            "checkpoint_details": [p for p in trace["points"] if p["step"] in turn["checkpoints"]],
        }

    def audit(self, task, attempt):
        trace = self.trace(task, attempt)
        origin = trace["origin"]
        if not origin:
            return {"is_branch": False}
        folder = self.attempt_path(task, attempt)
        parent = self.attempt_path(task, origin["attempt"])
        points = read_json(parent / "checkpoints.json", [])
        point = next((p for p in points if p["step"] == origin["step"]), None)
        if not point:
            return {"is_branch": True, "available": False, "origin": origin}
        request_name = Path(point["request_path"]).name
        if not REQUEST.fullmatch(request_name):
            raise web.HTTPBadRequest(text="Invalid saved checkpoint")
        before = read_gzip(parent / "requests" / request_name)
        after = read_gzip(folder / "requests/00000.json.gz")
        provider_rows = read_json(folder / "api-usage.json", [])
        provider_row = next(
            (r for r in provider_rows if r.get("http_status") == 200 and r.get("complete")),
            None,
        )
        provider_name = f"{provider_row['request']:05d}.json.gz" if provider_row else None
        provider_request = read_gzip(folder / "requests" / provider_name) if provider_name else None
        native = Path(point.get("native_path", parent / "native.jsonl")).resolve()
        expected_native = parent / "checkpoint-artifacts" / f"{int(point['step']):05d}" / "native.jsonl"
        managed_native = (
            expected_native.is_symlink()
            and expected_native.resolve() == native
            and native.is_relative_to(MANAGED_NATIVE_ROOT.resolve())
        )
        if not native.is_relative_to(parent.resolve()) and not managed_native:
            location = read_json(self.task_path(task) / "archive-location.json", {})
            old = Path(location.get("original_directory", "/__no_archive__"))
            if native.is_relative_to(old):
                native = (
                    Path(location.get("archive_directory", "/__no_archive__")) / native.relative_to(old)
                ).resolve()
            if not native.is_relative_to(parent.resolve()):
                raise web.HTTPBadRequest(text="Invalid native prefix path")
        prefix = native.read_bytes()[: point["byte_length"]] if native.exists() else None
        snapshot = str(point.get("snapshot_id", ""))
        return {
            "is_branch": True,
            "available": bool(before and after and provider_request),
            "origin": point,
            "model_prefix_equal": before["messages"]
            == provider_request["messages"][: len(before["messages"])]
            if before and provider_request
            else None,
            "gateway_prefix_equal": before["messages"] == after["messages"] if before and after else None,
            "system_equal": before.get("system") == provider_request.get("system")
            if before and provider_request
            else None,
            "checked_provider_request": provider_name,
            "native_prefix_hash_valid": hashlib.sha256(prefix).hexdigest() == point["sha256"]
            if prefix is not None
            else None,
            "snapshot_retained": (
                Path(self.config.get("btrfs_store", WORKSPACE / "storage/btrfs")) / "snapshots" / snapshot
            ).is_dir()
            if snapshot and Path(snapshot).name == snapshot
            else False,
            "native_hash": point["sha256"],
            "snapshot_backend": point.get("snapshot_backend", "btrfs-disk"),
            "memory_archive_bytes": point.get("memory_checkpoint", {}).get("archive_bytes"),
            "native_bytes": point["byte_length"],
            "note": "已结束任务的磁盘快照会回收；保留的模型请求和原生会话前缀仍可用于核对。",
        }

    def grade(self, task, attempt):
        folder = self.attempt_path(task, attempt)
        record = read_json(folder / "outcome.json", {})
        logs = []
        for path in sorted((folder / "grade").glob("*/verifier/*.txt")):
            logs.append(
                {
                    "name": path.name,
                    "content": path.read_text(errors="replace")[:250000],
                }
            )
        return {"grade": record.get("grade"), "logs": logs}


def create_app(campaign: Path, campaigns=None):
    stores = (
        {"default": Store(campaign)}
        if not campaigns
        else {key: Store(Path(value["path"])) for key, value in campaigns.items()}
    )
    default_campaign = next(iter(stores))

    @web.middleware
    async def headers(request, handler):
        try:
            response = await handler(request)
        except web.HTTPException as error:
            response = web.json_response({"error": error.reason}, status=error.status)
        response.headers["Cache-Control"] = "no-store" if request.path.startswith("/api/") else "no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'"
        )
        return response

    app = web.Application(middlewares=[headers])

    async def api(request):
        selected = request.query.get("campaign", default_campaign)
        if selected not in stores:
            raise web.HTTPNotFound(text="Unknown campaign")
        store = stores[selected]
        action = request.match_info["action"]
        task, attempt = request.query.get("task", ""), request.query.get("attempt", "")
        methods = {
            "overview": lambda: store.overview(),
            "task": lambda: store.task(task),
            "attempt": lambda: store.attempt(task, attempt),
            "turn": lambda: store.turn(task, attempt, int(request.query.get("turn", "0"))),
            "audit": lambda: store.audit(task, attempt),
            "grade": lambda: store.grade(task, attempt),
        }
        if action not in methods:
            raise web.HTTPNotFound()
        try:
            data = await asyncio.to_thread(methods[action])
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="Invalid request parameters") from None
        if action == "overview":
            data["campaign_id"] = selected
            data["campaigns"] = [
                {"id": key, "label": campaigns[key].get("label", key) if campaigns else key} for key in stores
            ]
        response = web.json_response(redact(data), dumps=lambda d: json.dumps(d, ensure_ascii=False))
        response.enable_compression()
        return response

    async def static(request):
        name = request.match_info.get("name", "index.html")
        if name not in {"index.html", "app.js", "style.css", "health.css", "favicon.svg"}:
            raise web.HTTPNotFound()
        return web.FileResponse(STATIC / name)

    app.router.add_get("/api/{action}", api)
    app.router.add_get("/", static)
    app.router.add_get("/{name}", static)
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, default=WORKSPACE / "runs/tb40-deepseekflash-20261103")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument("--campaigns", type=Path)
    args = parser.parse_args()
    web.run_app(
        create_app(args.campaign, read_json(args.campaigns) if args.campaigns else None),
        host=args.host,
        port=args.port,
        print=print,
    )
