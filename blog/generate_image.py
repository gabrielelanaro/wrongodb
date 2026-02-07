#!/usr/bin/env python3
"""Generate a blog image using Nano Banana Pro (Gemini 3 Pro Image).

Supports an agentic draft→generate→critique loop in agentic mode and emits a
sidecar JSON summary. Reads API key from .env (GEMINI_API_KEY or GOOGLE_API_KEY).

Also supports structured diagram generation with HTML/CSS intermediate representation
via the --structured flag.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            os.environ.setdefault(key, value)
    except OSError:
        pass


def _require_api_key() -> None:
    if os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"):
        return
    print("Missing API key. Set GEMINI_API_KEY or GOOGLE_API_KEY in .env.", file=sys.stderr)
    sys.exit(2)


def _default_out_path(post_dir: str | None) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    if not post_dir:
        raise ValueError("post_dir is required for default output path")
    post_path = Path(post_dir)
    if post_path.parts and post_path.parts[0] != "blog":
        post_path = Path("blog") / post_path
    return post_path / "images" / f"generated_{stamp}.png"


def _extract_image_data(response) -> tuple[bytes, str | None] | None:
    parts = None
    if hasattr(response, "parts") and response.parts:
        parts = response.parts
    elif hasattr(response, "candidates") and response.candidates:
        candidate = response.candidates[0]
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None)

    if not parts:
        return None

    for part in parts:
        inline = getattr(part, "inline_data", None) or getattr(part, "inlineData", None)
        if inline is None:
            continue
        data = getattr(inline, "data", None)
        if data is None:
            continue
        mime = getattr(inline, "mime_type", None) or getattr(inline, "mimeType", None)
        if isinstance(data, bytes):
            return data, mime
        if isinstance(data, str):
            try:
                return base64.b64decode(data), mime
            except (ValueError, TypeError):
                return None

    return None


def _extract_text(response) -> str | None:
    parts = None
    if hasattr(response, "parts") and response.parts:
        parts = response.parts
    elif hasattr(response, "candidates") and response.candidates:
        candidate = response.candidates[0]
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None)

    if not parts:
        return None

    for part in parts:
        text = getattr(part, "text", None)
        if text:
            return text
    return None


def _response_text(response) -> str | None:
    text = getattr(response, "text", None)
    if text:
        return text
    return _extract_text(response)


def _parse_json_blob(text: str | None) -> dict | None:
    if not text:
        return None
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        data = json.loads(text[start : end + 1])
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


# --- Structured diagram generation (HTML intermediate) ---

def _generate_html_diagram(client, model: str, seed_prompt: str) -> tuple[str, dict]:
    """Generate HTML representation of the diagram."""
    html_instruction = (
        "You are a technical diagram designer. Create a clean, well-structured HTML/CSS "
        "representation of the diagram described. The HTML should:\n"
        "- Use semantic HTML with clear section labels\n"
        "- Include inline CSS for styling (clean, modern, diagram-like appearance)\n"
        "- Use CSS Grid or Flexbox for layout\n"
        "- Include visual elements: boxes, arrows, labels, colors\n"
        "- Be self-contained (no external dependencies)\n"
        "- Have a white or light background\n"
        "- Use a reasonable size (800-1200px wide)\n\n"
        "Return ONLY JSON with keys:\n"
        "{\n"
        '  "html": "complete HTML document as a single string",\n'
        '  "is_multi_panel": true or false,\n'
        '  "panels": ["description of each panel if multi-panel"],\n'
        '  "description": "brief description of the diagram structure",\n'
        '  "key_elements": ["list of main visual elements"]\n'
        "}\n\n"
        "If this is a multi-panel diagram (like 4 panels showing steps/progression), "
        "set is_multi_panel: true and provide panel descriptions.\n\n"
        "Diagram description:\n"
        f"{seed_prompt}\n"
    )

    response = client.models.generate_content(model=model, contents=html_instruction)
    text = _response_text(response)
    data = _parse_json_blob(text) or {}

    html = data.get("html", "")
    if not html or "<html" not in html.lower():
        # Fallback: wrap in basic HTML if model didn't return proper HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: system-ui, -apple-system, sans-serif; margin: 40px; background: #f5f5f5; }}
.diagram {{ background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
</style>
</head>
<body>
<div class="diagram">
{html if html else "<p>Diagram content could not be generated</p>"}
</div>
</body>
</html>"""

    return html, data


def _generate_panel_html(client, model: str, panel_description: str, panel_index: int) -> str:
    """Generate HTML for a specific panel in a multi-panel diagram."""
    html_instruction = (
        f"You are creating HTML/CSS for Panel {panel_index + 1} of a multi-panel diagram.\n\n"
        f"Panel description: {panel_description}\n\n"
        "Generate complete, self-contained HTML/CSS for this panel. "
        "Use the same styling conventions as other panels for consistency.\n\n"
        "Return ONLY the HTML code (no JSON wrapper, no markdown fences)."
    )

    response = client.models.generate_content(model=model, contents=html_instruction)
    text = _response_text(response) or ""

    # Clean up the response (remove markdown fences if present)
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    return text


def _render_html_to_image(html: str, width: int = 1200, height: int = 900) -> bytes:
    """Render HTML to PNG using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Missing dependency: playwright. Install with 'pip install playwright'.", file=sys.stderr)
        print("Then run: playwright install chromium", file=sys.stderr)
        sys.exit(3)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(html)
        html_path = f.name

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={'width': width, 'height': height})
            page.goto(f'file://{html_path}')
            page.wait_for_load_state('networkidle')

            # Get actual content height for better cropping
            content_height = page.evaluate('document.body.scrollHeight')
            page.set_viewport_size({'width': width, 'height': max(content_height + 40, 400)})

            screenshot = page.screenshot(full_page=True, type='png')
            browser.close()
            return screenshot
    finally:
        os.unlink(html_path)


def _generate_image_from_reference(
    client,
    model: str,
    seed_prompt: str,
    reference_image_bytes: bytes,
    description: str,
    aspect: str,
    size: str,
) -> tuple[bytes, str | None, str | None]:
    """Generate final image using reference rendering as visual guide."""
    from google.genai import types

    final_prompt = (
        "Create a polished, professional technical diagram illustration based on:\n\n"
        f"DESCRIPTION: {seed_prompt}\n\n"
        f"STRUCTURE REFERENCE: {description}\n\n"
        "Follow the layout and structure shown in the reference image closely. "
        "Transform it into a beautiful, modern technical illustration with:\n"
        "- Clean, consistent styling\n"
        "- Professional color palette suitable for technical documentation\n"
        "- Clear visual hierarchy\n"
        "- Crisp typography and icons\n"
        "- Subtle shadows and depth\n"
        "Maintain all structural relationships, labels, and positions from the reference."
    )

    image_part = types.Part.from_bytes(data=reference_image_bytes, mime_type="image/png")
    text_part = types.Part.from_text(text=final_prompt)
    content = types.Content(role="user", parts=[text_part, image_part])

    image_config = types.ImageConfig(aspect_ratio=aspect, image_size=size)
    config = types.GenerateContentConfig(image_config=image_config)

    response = client.models.generate_content(model=model, contents=[content], config=config)

    extracted = _extract_image_data(response)
    if not extracted:
        return b"", None, _response_text(response)

    return extracted[0], extracted[1], None


def _write_image(out_path: Path, image_bytes: bytes, mime: str | None) -> None:
    # If the model returns JPEG and the requested filename is .png, convert to PNG.
    if mime and mime != "image/png" and out_path.suffix.lower() == ".png":
        try:
            from io import BytesIO
            from PIL import Image

            img = Image.open(BytesIO(image_bytes))
            img.save(out_path, format="PNG")
            return
        except Exception:
            pass
    out_path.write_bytes(image_bytes)


def _draft_prompt(client, model: str, seed_prompt: str) -> tuple[str, dict, str | None]:
    draft_instruction = (
        "You are an art director. Expand the seed prompt into a self-contained image-generation "
        "prompt for a technical blog diagram. Include:\n"
        "- Story purpose (what the image should teach)\n"
        "- Key visual elements and layout\n"
        "- Labels/icons that reinforce the story\n"
        "- Style consistency notes (palette, line weight, typography)\n"
        "Return ONLY JSON with keys:\n"
        "{\n"
        '  "draft_prompt": "...",\n'
        '  "story_purpose": "...",\n'
        '  "key_elements": ["..."],\n'
        '  "labels_icons": ["..."],\n'
        '  "style_notes": "..." \n'
        "}\n"
        "Seed prompt:\n"
        f"{seed_prompt}\n"
    )
    response = client.models.generate_content(model=model, contents=draft_instruction)
    text = _response_text(response)
    data = _parse_json_blob(text) or {}
    draft_prompt = data.get("draft_prompt")
    if not draft_prompt:
        draft_prompt = text.strip() if text else seed_prompt
        data = {"draft_prompt": draft_prompt}
    return draft_prompt, data, text


def _normalize_dim(value) -> dict:
    if isinstance(value, dict):
        return {
            "pass": bool(value.get("pass")),
            "notes": str(value.get("notes", "")).strip(),
        }
    if isinstance(value, bool):
        return {"pass": value, "notes": ""}
    if isinstance(value, str):
        return {"pass": False, "notes": value.strip()}
    return {"pass": False, "notes": ""}


def _critique_image(
    client, model: str, prompt: str, image_bytes: bytes, mime: str | None
) -> tuple[dict, str | None]:
    from google.genai import types

    critique_instruction = (
        "You are an art director critiquing a technical blog diagram. "
        "Evaluate the image against:\n"
        "1) story_effectiveness (does it clearly teach the intended story?),\n"
        "2) visual_consistency (layout, labels, and style are consistent),\n"
        "3) pleasing (aesthetically pleasing, not cluttered).\n"
        "Return ONLY JSON with keys:\n"
        "{\n"
        '  "pass": true|false,\n'
        '  "story_effectiveness": {"pass": bool, "notes": "..."},\n'
        '  "visual_consistency": {"pass": bool, "notes": "..."},\n'
        '  "pleasing": {"pass": bool, "notes": "..."},\n'
        '  "revision_prompt": "A full revised prompt that fixes issues. If pass=true, provide a tightened prompt."\n'
        "}\n"
        "Prompt used:\n"
        f"{prompt}\n"
    )

    image_part = types.Part.from_bytes(
        data=image_bytes, mime_type=mime or "image/png"
    )
    text_part = types.Part.from_text(text=critique_instruction)
    content = types.Content(role="user", parts=[text_part, image_part])
    response = client.models.generate_content(model=model, contents=[content])
    text = _response_text(response)
    data = _parse_json_blob(text) or {}

    story = _normalize_dim(data.get("story_effectiveness"))
    visual = _normalize_dim(data.get("visual_consistency"))
    pleasing = _normalize_dim(data.get("pleasing"))
    overall = data.get("pass")
    if not isinstance(overall, bool):
        overall = story["pass"] and visual["pass"] and pleasing["pass"]

    critique = {
        "pass": overall,
        "story_effectiveness": story,
        "visual_consistency": visual,
        "pleasing": pleasing,
        "revision_prompt": str(data.get("revision_prompt", "")).strip(),
    }
    if text and not data:
        critique["raw_text"] = text.strip()
    return critique, text


def _generate_image_bytes(
    client, model: str, prompt: str, aspect: str, size: str
) -> tuple[bytes, str | None, str | None]:
    from google.genai import types

    image_config = types.ImageConfig(
        aspect_ratio=aspect,
        image_size=size,
    )
    config = types.GenerateContentConfig(image_config=image_config)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=config,
    )

    extracted = _extract_image_data(response)
    if not extracted:
        return b"", None, _response_text(response)
    image_bytes, mime = extracted
    return image_bytes, mime, None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a blog image with Nano Banana Pro (Gemini 3 Pro Image)."
    )
    parser.add_argument("prompt", help="Text prompt for the image")
    parser.add_argument(
        "--out",
        default=None,
        help="Output path (default: blog/<post-dir>/images/generated_<timestamp>.png when --post is set)",
    )
    parser.add_argument(
        "--post",
        default=None,
        help="Post directory (e.g., 02-watching-the-os-write or blog/02-watching-the-os-write)",
    )
    parser.add_argument(
        "--model",
        default="gemini-3-pro-image-preview",
        help="Model name (used for draft/critique/image in agentic mode)",
    )
    parser.add_argument(
        "--aspect",
        default="16:9",
        help="Aspect ratio like 16:9, 1:1, 4:3 (default: 16:9)",
    )
    parser.add_argument(
        "--size",
        default="2K",
        help="Image size: 1K, 2K, or 4K (default: 2K)",
    )
    parser.add_argument(
        "--agentic",
        action="store_true",
        help="Enable agentic draft→generate→critique loop",
    )
    parser.add_argument(
        "--structured",
        action="store_true",
        help="Generate structured diagrams using HTML/CSS intermediate (for technical diagrams)",
    )
    parser.add_argument(
        "--html-model",
        default=None,
        help="Model for HTML generation in structured mode (defaults to --model)",
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Save intermediate HTML and reference render (only with --structured)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Max critique/refine cycles in agentic mode (default: 2)",
    )

    args = parser.parse_args()

    _load_dotenv(Path(".env"))
    _require_api_key()

    try:
        from google import genai
    except ImportError:
        print(
            "Missing dependency: google-genai. Install with 'pip install google-genai'.",
            file=sys.stderr,
        )
        return 3

    if args.out:
        out_path = Path(args.out)
    else:
        if not args.post:
            print("Missing output location. Provide --out or --post.", file=sys.stderr)
            return 2
        out_path = _default_out_path(args.post)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    client = genai.Client()

    if args.agentic:
        if args.iterations < 1:
            print("--iterations must be >= 1 in agentic mode.", file=sys.stderr)
            return 2

        seed_prompt = args.prompt
        draft_prompt, draft_payload, _ = _draft_prompt(client, args.model, seed_prompt)
        current_prompt = draft_prompt or seed_prompt

        critiques: list[dict] = []
        final_image: bytes | None = None
        final_mime: str | None = None

        for idx in range(args.iterations):
            image_bytes, mime, error_text = _generate_image_bytes(
                client,
                args.model,
                current_prompt,
                args.aspect,
                args.size,
            )
            if error_text:
                print(error_text)
            if not image_bytes:
                print("No image data returned by the model.", file=sys.stderr)
                return 4

            critique, _ = _critique_image(
                client, args.model, current_prompt, image_bytes, mime
            )
            critique["iteration"] = idx + 1
            critique["prompt"] = current_prompt
            critique["image_mime"] = mime
            critiques.append(critique)

            final_image = image_bytes
            final_mime = mime

            if critique.get("pass") is True:
                break

            revision_prompt = critique.get("revision_prompt")
            if revision_prompt:
                current_prompt = revision_prompt
            else:
                notes = " ".join(
                    [
                        critique.get("story_effectiveness", {}).get("notes", ""),
                        critique.get("visual_consistency", {}).get("notes", ""),
                        critique.get("pleasing", {}).get("notes", ""),
                    ]
                ).strip()
                if notes:
                    current_prompt = f"{current_prompt}\n\nRefine: {notes}"

        if final_image is None:
            print("Agentic loop failed to produce an image.", file=sys.stderr)
            return 4

        _write_image(out_path, final_image, final_mime)

        sidecar_path = out_path.with_suffix(".json")
        sidecar_payload = {
            "seed_prompt": seed_prompt,
            "draft_prompt": draft_payload.get("draft_prompt", draft_prompt),
            "draft_payload": draft_payload,
            "final_prompt": current_prompt,
            "model": args.model,
            "aspect": args.aspect,
            "size": args.size,
            "iterations_requested": args.iterations,
            "iterations_used": len(critiques),
            "critiques": critiques,
            "passed": bool(critiques[-1]["pass"]) if critiques else False,
        }
        try:
            sidecar_path.write_text(
                json.dumps(sidecar_payload, indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
        except OSError:
            print(
                f"Failed to write sidecar file: {sidecar_path}",
                file=sys.stderr,
            )

        print(str(out_path))
        return 0

    if args.structured:
        return _run_structured_mode(client, args, out_path)

    image_bytes, mime, error_text = _generate_image_bytes(
        client,
        args.model,
        args.prompt,
        args.aspect,
        args.size,
    )
    if error_text:
        print(error_text)
    if not image_bytes:
        print("No image data returned by the model.", file=sys.stderr)
        return 4

    _write_image(out_path, image_bytes, mime)
    print(str(out_path))
    return 0


def _run_structured_mode(client, args, out_path: Path) -> int:
    """Run structured diagram generation using HTML/CSS intermediate."""
    print("Step 1: Generating HTML representation...", file=sys.stderr)

    # Use a text model for HTML generation (faster/cheaper than image model)
    html_model = getattr(args, 'html_model', None) or args.model

    html, html_data = _generate_html_diagram(client, html_model, args.prompt)

    is_multi_panel = html_data.get("is_multi_panel", False)
    panels = html_data.get("panels", [])

    htmls = []

    if is_multi_panel and panels:
        print(f"  Multi-panel diagram detected ({len(panels)} panels)...", file=sys.stderr)
        for i, panel_desc in enumerate(panels):
            print(f"  Generating Panel {i + 1}: {panel_desc[:50]}...", file=sys.stderr)
            panel_html = _generate_panel_html(client, html_model, panel_desc, i)
            htmls.append(panel_html)
    else:
        print("  Single panel diagram...", file=sys.stderr)
        htmls.append(html)

    # For multi-panel, combine into HTML grid
    if len(htmls) > 1:
        combined_html = f"""<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: system-ui, -apple-system, sans-serif; margin: 20px; background: #f5f5f5; }}
.grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; max-width: 1400px; margin: 0 auto; }}
.panel {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
.panel h3 {{ margin: 0 0 15px 0; font-size: 16px; color: #333; text-align: center; border-bottom: 1px solid #eee; padding-bottom: 10px; }}
</style>
</head>
<body>
<div class="grid">
"""
        for i, h in enumerate(htmls):
            # Extract body content from each panel's HTML
            body_start = h.find("<body>")
            body_end = h.find("</body>")
            body_content = h[body_start + 6:body_end] if body_start > 0 and body_end > 0 else h
            title = panels[i].split(":")[0] if i < len(panels) and ":" in panels[i] else f"Panel {i + 1}"
            combined_html += f'<div class="panel"><h3>{title}</h3>{body_content}</div>\n'
        combined_html += "</div></body></html>"
        html = combined_html

    if args.save_html:
        html_path = out_path.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")
        print(f"  Saved HTML: {html_path}", file=sys.stderr)

    print("Step 2: Rendering HTML to image...", file=sys.stderr)
    html_image = _render_html_to_image(html)

    if args.save_html:
        html_png_path = out_path.with_suffix(".ref.png")
        html_png_path.write_bytes(html_image)
        print(f"  Saved HTML render: {html_png_path}", file=sys.stderr)

    print("Step 3: Generating final image from reference...", file=sys.stderr)
    description = html_data.get("description", "")

    image_bytes, mime, error = _generate_image_from_reference(
        client,
        args.model,
        args.prompt,
        html_image,
        description,
        args.aspect,
        args.size,
    )

    if error:
        print(f"  Error: {error}", file=sys.stderr)
    if not image_bytes:
        print("Failed to generate final image.", file=sys.stderr)
        return 4

    _write_image(out_path, image_bytes, mime)
    print(f"Saved final image: {out_path}", file=sys.stderr)

    # Save sidecar metadata
    sidecar_path = out_path.with_suffix(".json")
    sidecar = {
        "seed_prompt": args.prompt,
        "html_model": html_model,
        "image_model": args.model,
        "aspect": args.aspect,
        "size": args.size,
        "is_multi_panel": is_multi_panel,
        "panels": panels,
        "html_description": description,
        "html_key_elements": html_data.get("key_elements", []),
        "intermediate_files": {
            "html": str(out_path.with_suffix(".html")) if args.save_html else None,
            "html_render": str(out_path.with_suffix(".ref.png")) if args.save_html else None,
        },
    }
    sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")
    print(f"Saved metadata: {sidecar_path}", file=sys.stderr)

    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
