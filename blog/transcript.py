#!/usr/bin/env python3
"""Query and fetch coding session transcripts from Codex, Claude CLI, and OpenCode.

Supports searching by keyword, date range, project directory, and git repository.
Outputs JSON for further processing in blogging workflows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Iterator


def _parse_iso_timestamp(s: str) -> datetime | None:
    """Parse various ISO timestamp formats."""
    if not s:
        return None
    s = s.strip()
    # Try with Z suffix
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    # Try Unix ms timestamp
    try:
        ts = int(s)
        if ts > 1_000_000_000_000:  # milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, OSError):
        pass
    return None


def _extract_text_from_payload(payload: dict) -> str | None:
    """Extract text content from a Codex/Claude payload."""
    # Try content array
    content = payload.get("content") or []
    if isinstance(content, list):
        texts = []
        for item in content:
            if isinstance(item, dict):
                # Check for input_text or output_text types
                if item.get("type") in ("input_text", "output_text"):
                    text = item.get("text")
                    if isinstance(text, str):
                        texts.append(text)
                # Also check for direct text field
                elif "text" in item:
                    texts.append(item["text"])
        if texts:
            return "\n".join(texts)
    # Try direct text field
    if "text" in payload:
        return payload["text"]
    return None


def _format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ==============
# Codex Source
# ==============

def _iter_codex_entries(sessions_dir: Path) -> Iterator[dict]:
    """Iterate over all Codex entries."""
    if not sessions_dir.exists():
        return
    for year_dir in sessions_dir.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir():
                continue
            for day_dir in month_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                for file_path in day_dir.glob("*.jsonl"):
                    session_id = file_path.stem.split("-")[-1] if "-" in file_path.stem else file_path.stem
                    try:
                        for line in file_path.open():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                entry = json.loads(line)
                                entry["_file"] = str(file_path)
                                entry["_session_id"] = session_id
                                yield entry
                            except (json.JSONDecodeError, ValueError):
                                continue
                    except OSError:
                        continue


def _parse_codex_entry(raw: dict) -> dict | None:
    """Parse a Codex entry into a standardized format."""
    entry_type = raw.get("type", "")
    timestamp_str = raw.get("timestamp", "")
    timestamp = _parse_iso_timestamp(timestamp_str)
    if not timestamp:
        return None

    payload = raw.get("payload", {})
    session_id = raw.get("_session_id", "")

    # Extract fields from payload
    cwd = payload.get("cwd")
    git_info = payload.get("git", {})
    git_repo = git_info.get("repository_url") if isinstance(git_info, dict) else None
    role = payload.get("role", "")

    # Extract text content
    text = _extract_text_from_payload(payload)
    if not text:
        # For session_meta, try to extract from instructions
        if entry_type == "session_meta":
            text = payload.get("instructions", "")
        # For event_msg, try message field
        elif entry_type == "event_msg":
            msg = payload.get("message")
            if isinstance(msg, str):
                text = msg

    return {
        "source": "codex",
        "type": entry_type,
        "timestamp": _format_timestamp(timestamp),
        "session_id": session_id,
        "role": role,
        "cwd": cwd,
        "git_repo": git_repo,
        "text": text or "",
    }


def _get_codex_session(sessions_dir: Path, session_id: str) -> dict | None:
    """Get a full Codex session by ID."""
    if not sessions_dir.exists():
        return None

    # Search for the session file
    for file in sessions_dir.rglob("*.jsonl"):
        file_session_id = file.stem.split("-")[-1] if "-" in file.stem else file.stem
        if session_id in file_session_id or file_session_id in session_id:
            entries = []
            meta = {}
            try:
                for line in file.open():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        entry_type = raw.get("type", "")
                        payload = raw.get("payload", {})

                        if entry_type == "session_meta":
                            meta = {
                                "id": payload.get("id", session_id),
                                "timestamp": payload.get("timestamp", ""),
                                "cwd": payload.get("cwd"),
                                "originator": payload.get("originator"),
                                "cli_version": payload.get("cli_version"),
                                "git": payload.get("git"),
                            }

                        # Collect conversation entries (only include entries with actual text)
                        text = _extract_text_from_payload(payload)
                        if not text:
                            # Try to get text from event_msg message field
                            if entry_type == "event_msg":
                                msg = payload.get("message")
                                if isinstance(msg, str):
                                    text = msg
                            # Still no text? Skip this entry
                            if not text:
                                continue

                        if entry_type in ("response_item", "turn_context", "event_msg"):
                            role = payload.get("role", "")
                            if entry_type == "event_msg":
                                role = "assistant"  # event_msg is typically assistant output
                            timestamp_str = raw.get("timestamp", "")
                            entries.append({
                                "role": role,
                                "timestamp": _format_timestamp(_parse_iso_timestamp(timestamp_str) or datetime.now(timezone.utc)),
                                "text": text,
                            })
                    except (json.JSONDecodeError, ValueError):
                        continue

                if entries:
                    return {
                        "source": "codex",
                        "session_id": session_id,
                        "meta": meta,
                        "conversation": entries,
                    }
            except OSError:
                continue
    return None


def _list_codex_sessions(sessions_dir: Path) -> list[dict]:
    """List all Codex sessions."""
    sessions = []
    for raw in _iter_codex_entries(sessions_dir):
        if raw.get("type") == "session_meta":
            parsed = _parse_codex_entry(raw)
            if parsed:
                sessions.append({
                    "source": "codex",
                    "id": parsed["session_id"],
                    "timestamp": parsed["timestamp"],
                    "cwd": parsed["cwd"],
                    "git_repo": parsed["git_repo"],
                })
    return sessions


# ==============
# Claude Source
# ==============

def _iter_claude_entries(history_file: Path) -> Iterator[dict]:
    """Iterate over all Claude history entries."""
    if not history_file.exists():
        return
    try:
        for line in history_file.open():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
    except OSError:
        return


def _parse_claude_entry(raw: dict) -> dict | None:
    """Parse a Claude entry into a standardized format."""
    # Claude history format: {display, timestamp, project, pastedContents}
    timestamp_ms = raw.get("timestamp")
    if isinstance(timestamp_ms, (int, str)):
        try:
            timestamp_ms = int(timestamp_ms)
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        except (ValueError, OSError):
            timestamp = None
    else:
        timestamp = None

    if not timestamp:
        return None

    display = raw.get("display", "")
    project = raw.get("project", "")

    return {
        "source": "claude",
        "type": "history",
        "timestamp": _format_timestamp(timestamp),
        "session_id": None,
        "role": "user",  # Claude history is mostly user prompts
        "cwd": project,
        "git_repo": None,
        "text": display,
    }


# ==============
# OpenCode Source
# ==============

def _iter_opencode_messages(storage_dir: Path) -> Iterator[dict]:
    """Iterate over all OpenCode messages."""
    message_dir = storage_dir / "message"
    part_dir = storage_dir / "part"
    session_dir = storage_dir / "session"

    if not message_dir.exists():
        return

    # Build session metadata cache
    session_cache = {}
    if session_dir.exists():
        for session_file in session_dir.rglob("*.json"):
            try:
                data = json.loads(session_file.read_text())
                session_id = data.get("id")
                if session_id:
                    session_cache[session_id] = {
                        "directory": data.get("directory"),
                        "title": data.get("title"),
                        "time_created": data.get("time", {}).get("created"),
                    }
            except (OSError, json.JSONDecodeError):
                pass

    # Iterate over messages
    for msg_file in message_dir.rglob("*.json"):
        try:
            msg_data = json.loads(msg_file.read_text())
        except (OSError, json.JSONDecodeError):
            continue

        msg_id = msg_data.get("id")
        session_id = msg_data.get("sessionID")
        role = msg_data.get("role", "")

        # Get session metadata
        session_meta = session_cache.get(session_id, {})
        cwd = session_meta.get("directory")
        title = session_meta.get("title")

        # Timestamp
        time_obj = msg_data.get("time", {})
        created_ms = time_obj.get("created") or time_obj.get("completed")
        if isinstance(created_ms, (int, str)):
            try:
                created_ms = int(created_ms)
                timestamp = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                timestamp = None
        else:
            timestamp = None

        if not timestamp:
            continue

        # Collect text from parts (including tool calls)
        text_parts = []
        tool_calls = []
        msg_part_dir = part_dir / msg_id
        if msg_part_dir.exists():
            for part_file in msg_part_dir.glob("*.json"):
                try:
                    part_data = json.loads(part_file.read_text())
                    part_type = part_data.get("type", "")

                    # Extract text from different part types
                    if part_type == "tool":
                        state = part_data.get("state", {})
                        tool_name = part_data.get("tool", "unknown")  # Tool name is at top level
                        output = state.get("output", "")
                        status = state.get("status", "")

                        # Format tool call with more context
                        tool_desc = f"🔧 {tool_name}"
                        if status:
                            tool_desc += f" [{status}]"
                        tool_calls.append(tool_desc)

                        # Include tool input args if meaningful
                        tool_input = state.get("input", {})
                        args_str = ""
                        if isinstance(tool_input, dict) and tool_input:
                            # Format args nicely (exclude huge outputs)
                            args_str_items = []
                            for k, v in tool_input.items():
                                if isinstance(v, str) and len(v) > 100:
                                    args_str_items.append(f"{k}=<{len(v)} chars>")
                                else:
                                    args_str_items.append(f"{k}={repr(v)}")
                            if args_str_items:
                                args_str = f" args: {args_str_items}"

                        # Add formatted tool output
                        if isinstance(output, str) and output:
                            text_parts.append(f"{tool_desc}{args_str}\n{output}")
                        elif output:
                            text_parts.append(f"{tool_desc}{args_str}\n{str(output)}")
                    elif part_type == "text":
                        text_content = part_data.get("text", "")
                        if isinstance(text_content, str):
                            text_parts.append(text_content)
                except (OSError, json.JSONDecodeError):
                    pass

        text = "\n".join(text_parts)

        result = {
            "source": "opencode",
            "type": "message",
            "timestamp": _format_timestamp(timestamp),
            "session_id": session_id,
            "session_title": title,
            "role": role,
            "cwd": cwd,
            "git_repo": None,
            "text": text,
        }
        if tool_calls:
            result["tool_calls"] = tool_calls
        yield result


def _get_opencode_session(storage_dir: Path, session_id: str) -> dict | None:
    """Get a full OpenCode session by ID."""
    session_dir = storage_dir / "session"

    # Search for the session file in all project directories
    session_file = None
    if session_dir.exists():
        for proj_dir in session_dir.iterdir():
            if not proj_dir.is_dir():
                continue
            # Check for exact match
            candidate = proj_dir / f"{session_id}.json"
            if candidate.exists():
                session_file = candidate
                break
            # Check for partial match
            for f in proj_dir.glob("*.json"):
                if session_id in f.stem or f.stem in session_id:
                    session_file = f
                    break
            if session_file:
                break

    if not session_file or not session_file.exists():
        return None

    try:
        session_data = json.loads(session_file.read_text())
    except (OSError, json.JSONDecodeError):
        return None

    # Get all messages for this session
    message_dir = storage_dir / "message"
    part_dir = storage_dir / "part"
    conversation = []

    session_msg_dir = message_dir / session_id
    if session_msg_dir.exists():
        for msg_file in sorted(session_msg_dir.glob("*.json")):
            try:
                msg_data = json.loads(msg_file.read_text())
                role = msg_data.get("role", "")
                time_obj = msg_data.get("time", {})
                created_ms = time_obj.get("created") or time_obj.get("completed")
                if created_ms:
                    timestamp = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)

                # Collect text from parts (including tool calls)
                text_parts = []
                tool_calls = []  # Track tool calls separately
                msg_part_dir = part_dir / msg_data.get("id")
                if msg_part_dir.exists():
                    for part_file in msg_part_dir.glob("*.json"):
                        try:
                            part_data = json.loads(part_file.read_text())
                            part_type = part_data.get("type", "")
                            if part_type == "tool":
                                state = part_data.get("state", {})
                                tool_input = state.get("input", {})
                                tool_name = tool_input.get("tool", "unknown")
                                output = state.get("output", "")
                                status = state.get("status", "")

                                # Format tool call with more context
                                tool_desc = f"🔧 {tool_name}"
                                if status:
                                    tool_desc += f" [{status}]"
                                tool_calls.append(tool_desc)

                                # Include tool input args if meaningful
                                args_str = ""
                                if isinstance(tool_input, dict):
                                    args = {k: v for k, v in tool_input.items() if k != "tool"}
                                    if args:
                                        args_str = f" args: {args}"

                                # Add formatted tool output
                                if isinstance(output, str) and output:
                                    text_parts.append(f"{tool_desc}{args_str}\n{output}")
                                elif output:
                                    text_parts.append(f"{tool_desc}{args_str}\n{str(output)}")
                            elif part_type == "text":
                                text_content = part_data.get("text", "")
                                if isinstance(text_content, str):
                                    text_parts.append(text_content)
                        except (OSError, json.JSONDecodeError):
                            pass

                # Build conversation entry
                entry = {
                    "role": role,
                    "timestamp": _format_timestamp(timestamp),
                    "text": "\n".join(text_parts),
                }
                if tool_calls:
                    entry["tool_calls"] = tool_calls
                conversation.append(entry)
            except (OSError, json.JSONDecodeError):
                pass

    return {
        "source": "opencode",
        "session_id": session_id,
        "meta": {
            "id": session_data.get("id"),
            "title": session_data.get("title"),
            "directory": session_data.get("directory"),
            "time_created": session_data.get("time", {}).get("created"),
        },
        "conversation": conversation,
    }


def _list_opencode_sessions(storage_dir: Path) -> list[dict]:
    """List all OpenCode sessions."""
    sessions = []
    session_dir = storage_dir / "session"
    if not session_dir.exists():
        return sessions

    for session_file in session_dir.glob("*/*.json"):
        try:
            data = json.loads(session_file.read_text())
            time_obj = data.get("time", {})
            created_ms = time_obj.get("created")
            if created_ms:
                timestamp = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)
            else:
                timestamp = None

            sessions.append({
                "source": "opencode",
                "id": data.get("id"),
                "title": data.get("title"),
                "timestamp": _format_timestamp(timestamp) if timestamp else None,
                "cwd": data.get("directory"),
                "git_repo": None,
            })
        except (OSError, json.JSONDecodeError):
            pass

    return sessions


# ==============
# Search Logic
# ==============

def _search_codex(
    sessions_dir: Path,
    query: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    cwd: str | None = None,
    git_repo: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Search Codex transcripts."""
    results = []
    for raw in _iter_codex_entries(sessions_dir):
        parsed = _parse_codex_entry(raw)
        if not parsed:
            continue

        # Skip session_meta entries in search results (they're not conversational)
        if parsed["type"] == "session_meta":
            continue

        # Date filter
        timestamp = _parse_iso_timestamp(parsed["timestamp"])
        if timestamp:
            if start_date and timestamp < start_date:
                continue
            if end_date and timestamp > end_date:
                continue

        # CWD filter
        if cwd and parsed["cwd"]:
            if cwd.lower() not in parsed["cwd"].lower():
                continue

        # Git repo filter
        if git_repo and parsed["git_repo"]:
            if git_repo.lower() not in parsed["git_repo"].lower():
                continue

        # Text search: if query provided, skip entries without matching text
        if query:
            if not parsed["text"] or query.lower() not in parsed["text"].lower():
                continue

        results.append(parsed)
        if len(results) >= limit:
            break
    return results


def _search_claude(
    history_file: Path,
    query: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    cwd: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Search Claude transcripts."""
    results = []
    for raw in _iter_claude_entries(history_file):
        parsed = _parse_claude_entry(raw)
        if not parsed:
            continue

        # Date filter
        timestamp = _parse_iso_timestamp(parsed["timestamp"])
        if timestamp:
            if start_date and timestamp < start_date:
                continue
            if end_date and timestamp > end_date:
                continue

        # CWD filter
        if cwd and parsed["cwd"]:
            if cwd.lower() not in parsed["cwd"].lower():
                continue

        # Text search: if query provided, skip entries without matching text
        if query:
            if not parsed["text"] or query.lower() not in parsed["text"].lower():
                continue

        results.append(parsed)
        if len(results) >= limit:
            break
    return results


def _search_opencode(
    storage_dir: Path,
    query: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    cwd: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Search OpenCode transcripts."""
    results = []
    for entry in _iter_opencode_messages(storage_dir):
        # Date filter
        timestamp = _parse_iso_timestamp(entry["timestamp"])
        if timestamp:
            if start_date and timestamp < start_date:
                continue
            if end_date and timestamp > end_date:
                continue

        # CWD filter
        if cwd and entry["cwd"]:
            if cwd.lower() not in entry["cwd"].lower():
                continue

        # Text search: if query provided, skip entries without matching text
        if query:
            if not entry["text"] or query.lower() not in entry["text"].lower():
                continue

        results.append(entry)
        if len(results) >= limit:
            break
    return results


# ==============
# CLI Commands
# ==============

def cmd_search(args: argparse.Namespace) -> int:
    """Search transcripts by keyword and/or filters."""
    query = args.query
    source = args.source
    start_date = args.start
    end_date = args.end
    cwd_filter = args.cwd
    git_repo_filter = args.git_repo
    limit = args.limit
    pretty = args.pretty

    # Make datetimes timezone-aware if they aren't already
    if start_date and start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date and end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    results = []

    if source in ("codex", "all"):
        codex_dir = Path("~/.codex/sessions").expanduser()
        results.extend(_search_codex(
            codex_dir, query, start_date, end_date, cwd_filter, git_repo_filter, limit
        ))

    if source in ("claude", "all"):
        claude_file = Path("~/.claude/history.jsonl").expanduser()
        results.extend(_search_claude(
            claude_file, query, start_date, end_date, cwd_filter, limit
        ))

    if source in ("opencode", "all"):
        opencode_dir = Path("~/.local/share/opencode/storage").expanduser()
        results.extend(_search_opencode(
            opencode_dir, query, start_date, end_date, cwd_filter, limit
        ))

    # Sort by timestamp (newest first) and apply limit
    results.sort(key=lambda x: x["timestamp"], reverse=True)
    results = results[:limit]

    if pretty:
        output = json.dumps(results, indent=2, ensure_ascii=False)
    else:
        output = json.dumps(results, ensure_ascii=False)

    print(output)
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    """Get a full session by ID."""
    session_id = args.session_id
    source = args.source
    pretty = args.pretty

    result = None

    if source == "codex":
        codex_dir = Path("~/.codex/sessions").expanduser()
        result = _get_codex_session(codex_dir, session_id)
    elif source == "opencode":
        opencode_dir = Path("~/.local/share/opencode/storage").expanduser()
        result = _get_opencode_session(opencode_dir, session_id)
    elif source == "claude":
        print("Session retrieval not supported for Claude source (flat history file)", file=sys.stderr)
        return 1

    if not result:
        print(f"Session not found: {session_id}", file=sys.stderr)
        return 1

    if pretty:
        output = json.dumps(result, indent=2, ensure_ascii=False)
    else:
        output = json.dumps(result, ensure_ascii=False)

    print(output)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    """List all available sessions."""
    source = args.source
    pretty = args.pretty

    sessions = []

    if source in ("codex", "all"):
        codex_dir = Path("~/.codex/sessions").expanduser()
        sessions.extend(_list_codex_sessions(codex_dir))

    if source in ("opencode", "all"):
        opencode_dir = Path("~/.local/share/opencode/storage").expanduser()
        sessions.extend(_list_opencode_sessions(opencode_dir))

    if source == "claude":
        print("Listing not supported for Claude source (flat history file)", file=sys.stderr)
        return 1

    # Sort by timestamp (newest first)
    sessions.sort(key=lambda x: x["timestamp"] or "", reverse=True)

    if pretty:
        output = json.dumps(sessions, indent=2, ensure_ascii=False)
    else:
        output = json.dumps(sessions, ensure_ascii=False)

    print(output)
    return 0


# ==============
# Main
# ==============

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query and fetch coding session transcripts for blogging."
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search transcripts")
    search_parser.add_argument(
        "query",
        nargs="?",
        help="Keyword search in message content",
    )
    search_parser.add_argument(
        "--source",
        choices=["codex", "claude", "opencode", "all"],
        default="all",
        help="Transcript source to search (default: all)",
    )
    search_parser.add_argument(
        "--start",
        type=lambda s: datetime.fromisoformat(s),
        help="Start date (YYYY-MM-DD or ISO timestamp)",
    )
    search_parser.add_argument(
        "--end",
        type=lambda s: datetime.fromisoformat(s),
        help="End date (YYYY-MM-DD or ISO timestamp)",
    )
    search_parser.add_argument(
        "--cwd",
        help="Filter by working directory (substring match)",
    )
    search_parser.add_argument(
        "--git-repo",
        help="Filter by git repository URL (substring match)",
    )
    search_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results (default: 20)",
    )
    search_parser.add_argument(
        "--pretty",
        "-p",
        action="store_true",
        help="Pretty print JSON output",
    )

    # Get command
    get_parser = subparsers.add_parser("get", help="Get full session by ID")
    get_parser.add_argument(
        "session_id",
        help="Session ID (UUID or partial match)",
    )
    get_parser.add_argument(
        "--source",
        choices=["codex", "claude", "opencode"],
        default="codex",
        help="Transcript source (default: codex)",
    )
    get_parser.add_argument(
        "--pretty",
        "-p",
        action="store_true",
        help="Pretty print JSON output",
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List all sessions")
    list_parser.add_argument(
        "--source",
        choices=["codex", "claude", "opencode", "all"],
        default="codex",
        help="Transcript source (default: codex)",
    )
    list_parser.add_argument(
        "--pretty",
        "-p",
        action="store_true",
        help="Pretty print JSON output",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "search":
        return cmd_search(args)
    elif args.command == "get":
        return cmd_get(args)
    elif args.command == "list":
        return cmd_list(args)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
