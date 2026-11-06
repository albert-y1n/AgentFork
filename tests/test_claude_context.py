from copy import deepcopy

import pytest
from agentfork.claude_context import restore_model_prefix

PARENT = {
    "system": [{"type": "text", "text": "original system"}],
    "messages": [
        {"role": "user", "content": "original task"},
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "call-1",
                    "name": "Bash",
                    "input": {"command": "cat answer"},
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "call-1",
                    "content": "early",
                    "cache_control": {"type": "ephemeral"},
                }
            ],
        },
    ],
}
MARKER = "__isolated_resume_control__"
DATE_NOTE = {
    "role": "system",
    "content": [
        {
            "type": "text",
            "text": "The date has changed. Today's date is now 2026-11-06. No need to announce the new date — the user's own clock shows it.",
        }
    ],
}


@pytest.mark.parametrize("regenerated", [False, True])
def test_date_attachment_can_move_after_resume_without_changing_or_duplicating_history(regenerated):
    parent = deepcopy(PARENT)
    parent["messages"].append(deepcopy(DATE_NOTE))
    request = deepcopy(PARENT)
    request["messages"].extend(
        [{"role": "assistant", "content": "No response requested."}, {"role": "user", "content": MARKER}]
    )
    if regenerated:
        request["messages"].append(deepcopy(DATE_NOTE))
    assert restore_model_prefix(request, parent, MARKER)["messages"] == parent["messages"]
    request["messages"][2]["content"][0]["content"] = "changed observation"
    with pytest.raises(ValueError):
        restore_model_prefix(request, parent, MARKER)


def test_newer_date_remains_a_new_observation_after_the_exact_inherited_prefix():
    parent = deepcopy(PARENT)
    parent["messages"].append(deepcopy(DATE_NOTE))
    newer = deepcopy(DATE_NOTE)
    newer["content"][0]["text"] = newer["content"][0]["text"].replace("2026-11-06", "2026-11-07")
    request = deepcopy(PARENT)
    request["messages"].extend([{"role": "user", "content": MARKER}, newer])
    assert restore_model_prefix(request, parent, MARKER)["messages"] == parent["messages"] + [newer]


def test_only_leading_duplicate_system_dates_are_removed_and_real_turns_remain():
    parent = deepcopy(PARENT)
    parent["messages"].append(deepcopy(DATE_NOTE))
    request = deepcopy(PARENT)
    later = {"role": "assistant", "content": "new actor action"}
    request["messages"].extend(
        [
            {"role": "user", "content": MARKER},
            {"role": "system", "content": "new runtime attachment"},
            deepcopy(DATE_NOTE),
            later,
            deepcopy(DATE_NOTE),
        ]
    )
    assert restore_model_prefix(request, parent, MARKER)["messages"] == parent["messages"] + [
        {"role": "system", "content": "new runtime attachment"},
        later,
        DATE_NOTE,
    ]


def test_date_like_user_messages_and_modified_system_messages_are_not_ignored():
    for modified in [
        {"role": "user", "content": DATE_NOTE["content"]},
        {"role": "system", "content": DATE_NOTE["content"][0]["text"] + " Extra instruction."},
    ]:
        parent = deepcopy(PARENT)
        parent["messages"].append(modified)
        request = deepcopy(PARENT)
        request["messages"].extend([{"role": "user", "content": MARKER}, deepcopy(modified)])
        with pytest.raises(ValueError):
            restore_model_prefix(request, parent, MARKER)


@pytest.mark.parametrize("repair", [False, True])
def test_resume_sends_exact_checkpoint_history_and_keeps_new_tool_results(repair):
    request = deepcopy(PARENT)
    request["messages"][-1]["content"][0].pop("cache_control")
    if repair:
        request["messages"].append({"role": "assistant", "content": "No response requested."})
    request["messages"].append({"role": "user", "content": [{"type": "text", "text": MARKER}]})
    suffix = [
        {"role": "assistant", "content": "new real action"},
        {"role": "user", "content": "new real observation"},
    ]
    request["messages"].extend(suffix)
    before = deepcopy(request)
    assert restore_model_prefix(request, PARENT, MARKER)["messages"] == PARENT["messages"] + suffix
    assert request == before


@pytest.mark.parametrize("change", ["observation", "system", "marker", "tool_input"])
def test_mismatched_history_or_control_is_refused(change):
    request = deepcopy(PARENT)
    request["messages"].append({"role": "user", "content": MARKER})
    if change == "observation":
        request["messages"][2]["content"][0]["content"] = "future"
    elif change == "system":
        request["system"][0]["text"] = "different system"
    elif change == "tool_input":
        request["messages"][1]["content"][0]["input"]["cache_control"] = "real argument"
    else:
        request["messages"][-1]["content"] = "a real user hint"
    with pytest.raises(ValueError):
        restore_model_prefix(request, PARENT, MARKER)


def test_saved_environment_updates_are_restored_when_cli_rebuilds_attachments():
    parent = deepcopy(PARENT)
    update = {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": "# Environment update\n - Primary working directory: /app/sub (was /app)",
            }
        ],
    }
    parent["messages"].append(update)
    resumed = deepcopy(PARENT)
    resumed["messages"].extend(
        [
            {"role": "assistant", "content": "No response requested."},
            {"role": "user", "content": MARKER},
        ]
    )
    assert restore_model_prefix(resumed, parent, MARKER)["messages"] == parent["messages"]


def test_transient_file_change_reminder_is_restored_from_saved_request():
    parent = deepcopy(PARENT)
    note = {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": "Note: /app/a.py changed on disk since you last read it. That's usually deliberate, so take it as the current state rather than reverting it.",
            }
        ],
    }
    parent["messages"].append(note)
    request = deepcopy(PARENT)
    request["messages"].append({"role": "user", "content": MARKER})
    restored = restore_model_prefix(request, parent, MARKER)
    assert restored["messages"] == parent["messages"]
    request["messages"][2]["content"][0]["content"] = "modified observation"
    with pytest.raises(ValueError):
        restore_model_prefix(request, parent, MARKER)


def test_image_coordinate_hint_can_be_missing_but_image_bytes_cannot_change():
    parent = deepcopy(PARENT)
    parent["messages"][-1]["content"][0]["content"] = [
        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "ORIGINAL"}}
    ]
    parent["messages"][-1]["content"].append(
        {
            "type": "text",
            "text": "[Image: original 2160x480, displayed at 2000x444. Multiply coordinates by 1.08 to map to original image.]",
        }
    )
    request = deepcopy(parent)
    request["messages"][-1]["content"].pop()
    request["messages"].append({"role": "user", "content": MARKER})
    assert restore_model_prefix(request, parent, MARKER)["messages"] == parent["messages"]
    request["messages"][2]["content"][0]["content"][0]["source"]["data"] = "DIFFERENT"
    with pytest.raises(ValueError):
        restore_model_prefix(request, parent, MARKER)


def test_output_limit_continuation_preserves_extra_text_and_strips_only_control():
    request = deepcopy(PARENT)
    continuation = {"type": "text", "text": "Output token limit hit. Resume directly."}
    request["messages"].extend(
        [
            {"role": "assistant", "content": "No response requested."},
            {"role": "user", "content": [{"type": "text", "text": MARKER + "\n"}, continuation]},
        ]
    )
    restored = restore_model_prefix(request, PARENT, MARKER)
    assert restored["messages"] == PARENT["messages"] + [{"role": "user", "content": [continuation]}]
    request["messages"][2]["content"][0]["content"] = "changed tool result"
    with pytest.raises(ValueError):
        restore_model_prefix(request, PARENT, MARKER)


def test_duplicate_embedded_controls_are_rejected():
    request = deepcopy(PARENT)
    request["messages"].append(
        {
            "role": "user",
            "content": [{"type": "text", "text": MARKER}, {"type": "text", "text": MARKER + "\n"}],
        }
    )
    with pytest.raises(ValueError):
        restore_model_prefix(request, PARENT, MARKER)


def test_next_resume_accepts_coalesced_user_blocks_without_losing_continuation():
    parent = deepcopy(PARENT)
    note = {"type": "text", "text": "Output token limit hit. Resume directly."}
    parent["messages"].append({"role": "user", "content": [note]})
    request = deepcopy(PARENT)
    request["messages"][-1]["content"].append(deepcopy(note))
    request["messages"].append({"role": "user", "content": MARKER})
    assert restore_model_prefix(request, parent, MARKER)["messages"] == parent["messages"]
    request["messages"][-2]["content"][-1]["text"] = "Changed continuation"
    with pytest.raises(ValueError):
        restore_model_prefix(request, parent, MARKER)
