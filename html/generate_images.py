#!/usr/bin/env python3
"""
Generate webpage images for 문서 분석 솔루션 using Stable Diffusion 3.5 Large
on Amazon Bedrock (same invoke pattern as analytics-for-bus-schedule).

Usage:
  python3 html/generate_images.py
  python3 html/generate_images.py --aspect-ratio 21:9
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import random
import sys
from typing import Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MODEL_ID = "stability.sd3-5-large-v1:0"
AWS_REGION = "us-west-2"
VALID_ASPECT_RATIOS = [
    "1:1",
    "16:9",
    "9:16",
    "4:3",
    "3:4",
    "2:3",
    "3:2",
    "21:9",
    "9:21",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGES_DIR = os.path.join(SCRIPT_DIR, "assets")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("generate_web_images")

# Theme: ESS electrical document analysis — 2x2 tiles for hero right side;
# warm cream paper + muted teal grading; editorial, no text/logos in frame.
NEGATIVE = (
    "blurry, low quality, distorted, cartoon, anime, illustration, "
    "architectural house blueprints, building floor plans, residential architecture, "
    "text, letters, watermark, logo, brand names, neon cyberpunk, "
    "crowded collage, poster design, UI screenshot, busy desk clutter, people faces"
)

IMAGE_SPECS: List[Dict] = [
    {
        "filename": "hero-1.png",
        "aspect_ratio": "1:1",
        "seed": 20260921,
        "prompt": (
            "square editorial photograph, close-up of electrical single-line diagram sheets "
            "on a warm cream desk, switchgear and power distribution schematic line drawings, "
            "soft teal lamp light, shallow depth of field, muted copper pencil nearby, "
            "photorealistic, calm professional mood, no readable text, no logos, no watermark"
        ),
        "negative_prompt": NEGATIVE,
    },
    {
        "filename": "hero-2.png",
        "aspect_ratio": "1:1",
        "seed": 20260922,
        "prompt": (
            "square editorial photograph, neat stack of electrical regulation binders and "
            "technical specification documents on warm sand paper, deep teal notebook accent, "
            "soft natural window light, shallow depth of field, quiet engineering office mood, "
            "photorealistic, no readable text, no logos, no watermark"
        ),
        "negative_prompt": NEGATIVE,
    },
    {
        "filename": "hero-3.png",
        "aspect_ratio": "1:1",
        "seed": 20260923,
        "prompt": (
            "square editorial photograph, laptop on a cream desk showing abstract document "
            "analysis panels softly out of focus, electrical test case printouts and circuit "
            "checklist sheets in the foreground, teal and copper accents, shallow depth of field, "
            "photorealistic, calm focused atmosphere, no readable text, no logos, no watermark"
        ),
        "negative_prompt": NEGATIVE,
    },
    {
        "filename": "hero-4.png",
        "aspect_ratio": "1:1",
        "seed": 20260924,
        "prompt": (
            "square editorial photograph, electrical panel and project wiring drawings "
            "spread on warm cream paper, industrial switchboard schematic details, "
            "soft teal and copper accents, drafting tools nearby, shallow depth of field, "
            "photorealistic, quiet engineering mood, no readable text, no logos, no watermark"
        ),
        "negative_prompt": NEGATIVE,
    },
    {
        "filename": "hero-5.png",
        "aspect_ratio": "1:1",
        "seed": 20260925,
        "prompt": (
            "square editorial photograph, close-up of an electrical switchboard single-line "
            "drawing on warm cream drafting paper, power feeder symbols and breaker bank "
            "layout lines, teal notebook edge and copper ruler in soft focus, "
            "shallow depth of field, photorealistic, calm engineering desk, "
            "no readable text, no logos, no watermark"
        ),
        "negative_prompt": NEGATIVE,
    },
]


def _bedrock_client(region: str):
    session_kwargs = {"region_name": region}
    if aws_profile := os.environ.get("AWS_PROFILE"):
        session_kwargs["profile_name"] = aws_profile
    return boto3.Session(**session_kwargs).client(
        "bedrock-runtime",
        config=Config(read_timeout=180, retries={"max_attempts": 2}),
    )


def invoke_sd35(client, request_body: dict) -> dict:
    response = client.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(request_body),
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(response["body"].read())


def generate_one(
    client,
    *,
    prompt: str,
    negative_prompt: Optional[str],
    aspect_ratio: str,
    seed: Optional[int],
    out_path: str,
) -> dict:
    if aspect_ratio not in VALID_ASPECT_RATIOS:
        raise ValueError(
            f"Invalid aspect_ratio '{aspect_ratio}'. Valid: {VALID_ASPECT_RATIOS}"
        )

    actual_seed = seed if seed is not None else random.randint(0, 4294967294)
    request_body = {
        "prompt": prompt,
        "mode": "text-to-image",
        "aspect_ratio": aspect_ratio,
        "seed": actual_seed,
        "output_format": "png",
    }
    if negative_prompt:
        request_body["negative_prompt"] = negative_prompt

    logger.info(
        "Generating %s (aspect=%s, seed=%s)",
        os.path.basename(out_path),
        aspect_ratio,
        actual_seed,
    )
    logger.info("Prompt: %s...", prompt[:80])

    result = invoke_sd35(client, request_body)
    finish_reasons = result.get("finish_reasons") or []
    if finish_reasons and finish_reasons[0] == "CONTENT_FILTERED":
        raise RuntimeError("Content was filtered. Revise the prompt.")

    images = result.get("images") or []
    if not images:
        raise RuntimeError(f"No images returned: {result}")

    image_bytes = base64.b64decode(images[0])
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(image_bytes)

    logger.info("Saved %s (%d bytes)", out_path, len(image_bytes))
    return {
        "path": out_path,
        "seed": (result.get("seeds") or [actual_seed])[0],
        "bytes": len(image_bytes),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate 문서 분석 솔루션 webpage images via Bedrock SD 3.5 Large"
    )
    parser.add_argument("--region", default=AWS_REGION, help="Bedrock region")
    parser.add_argument(
        "--aspect-ratio",
        default=None,
        help="Override aspect ratio for all images (e.g. 16:9, 21:9)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Override seed for all images",
    )
    parser.add_argument(
        "--out-dir",
        default=IMAGES_DIR,
        help=f"Output directory (default: {IMAGES_DIR})",
    )
    args = parser.parse_args()

    client = _bedrock_client(args.region)
    os.makedirs(args.out_dir, exist_ok=True)

    results = []
    for spec in IMAGE_SPECS:
        aspect = args.aspect_ratio or spec["aspect_ratio"]
        seed = args.seed if args.seed is not None else spec.get("seed")
        out_path = os.path.join(args.out_dir, spec["filename"])
        try:
            info = generate_one(
                client,
                prompt=spec["prompt"],
                negative_prompt=spec.get("negative_prompt"),
                aspect_ratio=aspect,
                seed=seed,
                out_path=out_path,
            )
            results.append({"filename": spec["filename"], **info, "status": "ok"})
        except ClientError as e:
            logger.error("Bedrock error for %s: %s", spec["filename"], e)
            results.append(
                {"filename": spec["filename"], "status": "error", "error": str(e)}
            )
            return 1
        except Exception as e:
            logger.error("Failed %s: %s", spec["filename"], e)
            results.append(
                {"filename": spec["filename"], "status": "error", "error": str(e)}
            )
            return 1

    manifest = {
        "modelId": MODEL_ID,
        "region": args.region,
        "images": results,
    }
    manifest_path = os.path.join(args.out_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        f.write("\n")
    logger.info("Wrote %s", manifest_path)
    logger.info("Done. Use assets/hero-1.png … hero-5.png in the webpage 2x2 grid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
