"""Fit Anthropic-native histories to AgentFork's original reviewer budget.

Only the controller's diagnostic view is reduced. Saved actor requests,
checkpoint offsets and the history actually restored into Claude stay intact.
"""

import json
from copy import deepcopy

from agentfork.request_size import STAGES, Window, window_messages

DEFAULT_BUDGET = 400_000


def clip(value, limit):
    if isinstance(value, dict):
        return {k: clip(v, limit) for k, v in value.items() if k not in ("signature", "cache_control")}
    if isinstance(value, list):
        return [clip(v, limit) for v in value]
    if isinstance(value, str) and len(value) > limit:
        left = limit * 2 // 3
        return value[:left] + f" …[{len(value) - limit} chars elided]… " + value[-(limit - left) :]
    return value


def fit_reviewer(template, *, problem, rows, count_rule, budget=DEFAULT_BUDGET):
    def render(value):
        return template.format(
            problem=problem,
            reports=json.dumps(value, ensure_ascii=False),
            count_rule=count_rule,
        )

    prompt = render(rows)
    record = {
        "budget": budget,
        "chars_before": len(prompt),
        "chars_after": len(prompt),
        "stage": "full",
    }
    for stage in STAGES:
        if len(prompt) <= budget:
            break
        reduced = []
        for row in rows:
            native = row["native_history"]
            messages = native["messages"]
            window = stage.parent if row["name"] == "root-0" else stage.branches
            if window is not None:
                counts = native["prefix_message_counts"]
                candidates = row.get("analysis", {}).get("branch_candidates", [])
                cuts = [counts[str(c["step"])] for c in candidates if str(c.get("step")) in counts]
                messages, _ = window_messages(messages, cuts, window)
            if stage.clip:
                messages = clip(messages, stage.clip)
            reduced.append({**row, "native_history": {**native, "messages": messages}})
        prompt = render(reduced)
        record.update(chars_after=len(prompt), stage=stage.name)
    if len(prompt) > budget:
        raise ValueError("Reviewer reports alone exceed the configured character budget")
    return prompt, record


def fit_analyst(template, *, problem, verdict, native, steps, budget=DEFAULT_BUDGET):
    def render(value):
        return template.format(
            problem=problem,
            verdict=verdict,
            transcript=json.dumps(value, ensure_ascii=False),
            checkpoint_steps=json.dumps(steps),
            lo=min(steps),
            hi=max(steps),
            candidate_limit=8,
        )

    prompt = render(native)
    record = {
        "budget": budget,
        "chars_before": len(prompt),
        "chars_after": len(prompt),
        "stage": "full",
    }
    if len(prompt) <= budget:
        return prompt, record
    counts = native["prefix_message_counts"]
    # Give an analyst without existing candidates chronological coverage.
    sampled = sorted({steps[round(i * (len(steps) - 1) / 7)] for i in range(8)})
    cuts = [counts[str(step)] for step in sampled]
    for limit, window in [
        (2000, None),
        (2000, Window(4, 2, 16)),
        (1000, Window(3, 1, 8)),
        (300, Window(2, 1, 4)),
    ]:
        view = deepcopy(native)
        if window:
            view["messages"], _ = window_messages(view["messages"], cuts, window)
        view["messages"] = clip(view["messages"], limit)
        prompt = render(view)
        note = "\nAdapter: role=omitted marks absent messages at their original positions; clipped fields are not complete observations. Only choose evidence-backed points from the full valid checkpoint list.\n"
        prompt += note
        record.update(chars_after=len(prompt), stage=f"clip {limit}, window {window}")
        if len(prompt) <= budget:
            return prompt, record
    raise ValueError("Analyst request exceeds its budget after fitting")
