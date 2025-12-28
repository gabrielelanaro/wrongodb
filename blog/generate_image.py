#!/usr/bin/env python3
"""Generate a blog image using Nano Banana Pro (Gemini 3 Pro Image).

Reads API key from .env (GEMINI_API_KEY or GOOGLE_API_KEY).
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
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
        help="Model name (default: gemini-3-pro-image-preview)",
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

    args = parser.parse_args()

    _load_dotenv(Path(".env"))
    _require_api_key()

    try:
        from google import genai
        from google.genai import types
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

    image_config = types.ImageConfig(
        aspect_ratio=args.aspect,
        image_size=args.size,
    )
    config = types.GenerateContentConfig(image_config=image_config)

    client = genai.Client()
    response = client.models.generate_content(
        model=args.model,
        contents=args.prompt,
        config=config,
    )

    extracted = _extract_image_data(response)
    if not extracted:
        text = _extract_text(response)
        if text:
            print(text)
        print("No image data returned by the model.", file=sys.stderr)
        return 4

    image_bytes, mime = extracted
    # If the model returns JPEG and the requested filename is .png, convert to PNG.
    if mime and mime != "image/png" and out_path.suffix.lower() == ".png":
        try:
            from io import BytesIO
            from PIL import Image

            img = Image.open(BytesIO(image_bytes))
            img.save(out_path, format="PNG")
        except Exception:
            out_path.write_bytes(image_bytes)
    else:
        out_path.write_bytes(image_bytes)
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
