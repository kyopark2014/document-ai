"""API Gateway → documents + async jobs (DynamoDB) → InvokeHarness worker.

Public:
  GET  /health
  GET  /documents              → all kinds for authenticated Cognito user
  GET  /documents/{kind}       → regulations|projects|drawings|test_cases|test-cases
  GET  /documents/{kind}/{filename}/{view}
  GET  /artifacts/view/{path+} /artifacts/download/{path+}  → md/json/csv viewer
  GET  /download?key=…
  POST /jobs                   → 202 { jobId, status, sessionId }
  GET  /jobs/{jobId}           → job status / result
  POST /invoke                 → sync InvokeHarness (short prompts only)

Internal (async Event invoke):
  { "jobWorker": true, "jobId": "...", "prompt": "...", ... }
"""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import logging
import os
import re
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EventStreamError, ReadTimeoutError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HARNESS_ARN = os.environ.get("HARNESS_ARN", "")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION") or os.environ.get(
    "AWS_REGION", "us-west-2"
)
S3_BUCKET = os.environ.get("S3_BUCKET", "").strip()
SKILLS_S3_PREFIX = (os.environ.get("SKILLS_S3_PREFIX") or "skills").strip().strip("/")
JOBS_TABLE = (os.environ.get("JOBS_TABLE") or "").strip()
JOB_TTL_SECONDS = int(os.environ.get("JOB_TTL_SECONDS") or str(24 * 3600))
MAX_RESULT_CHARS = int(os.environ.get("MAX_RESULT_CHARS") or "350000")
# Align InvokeHarness HTTP read idle timeout with Lambda/Harness job budget (was 300s —
# long code-interpreter gaps produced a truncated stream that we wrongly marked SUCCEEDED).
LAMBDA_JOB_TIMEOUT_SECONDS = int(
    os.environ.get("LAMBDA_JOB_TIMEOUT_SECONDS") or "1800"
)
HARNESS_INVOKE_READ_TIMEOUT = int(
    os.environ.get("HARNESS_INVOKE_READ_TIMEOUT")
    or str(LAMBDA_JOB_TIMEOUT_SECONDS)
)
MIN_RESULT_CHARS = int(os.environ.get("MIN_RESULT_CHARS") or "400")
_INCOMPLETE_TAIL_MARKERS = (
    "진행하겠습니다",
    "시작하겠습니다",
    "확인하겠습니다",
    "분석하겠습니다",
    "평가를 진행",
    "선별하고 평가",
    "내려받겠습니다",
    "로드하겠습니다",
)
DEFAULT_SKILLS = [
    name.strip()
    for name in (
        os.environ.get("DEFAULT_SKILLS")
        or "regulation-evaluator,testcase-generator,pptx,docx,xlsx"
    ).split(",")
    if name.strip()
]

ESS_S3_BUCKET = (os.environ.get("ESS_S3_BUCKET") or "").strip()
ESS_SHARING_URL = (os.environ.get("ESS_SHARING_URL") or "").rstrip("/")
SHARING_URL = (os.environ.get("SHARING_URL") or "").rstrip("/")
COGNITO_USER_POOL_ID = (os.environ.get("COGNITO_USER_POOL_ID") or "").strip()
COGNITO_CLIENT_ID = (os.environ.get("COGNITO_CLIENT_ID") or "").strip()
COGNITO_REGION = (
    os.environ.get("COGNITO_REGION") or BEDROCK_REGION
).strip() or BEDROCK_REGION

KIND_REGISTRY = {
    "regulations": {
        "list_keys": [
            "app-data/{user}/ess/regulations_list.json",
            "agentcore-sessions/{user}/ess/regulations_list.json",
        ],
        "label": "Regulations",
        "kind": "regulation",
    },
    "projects": {
        "list_keys": [
            "app-data/{user}/ess/project_list.json",
            "agentcore-sessions/{user}/ess/project_list.json",
        ],
        "label": "Projects",
        "kind": "project",
    },
    "drawings": {
        "list_keys": [
            "app-data/{user}/ess/drawings_list.json",
            "agentcore-sessions/{user}/ess/drawings_list.json",
        ],
        "label": "Drawings",
        "kind": "drawing",
    },
    "test_cases": {
        "list_keys": [
            "app-data/{user}/ess/test_cases_list.json",
            "agentcore-sessions/{user}/ess/test_cases_list.json",
            "session-uploads/{user}/ess/test_cases_list.json",
        ],
        "label": "Test Cases",
        "kind": "test_case",
    },
}

KIND_ALIASES = {
    "regulation": "regulations",
    "regulations": "regulations",
    "project": "projects",
    "projects": "projects",
    "drawing": "drawings",
    "drawings": "drawings",
    "test_case": "test_cases",
    "test-case": "test_cases",
    "test_cases": "test_cases",
    "test-cases": "test_cases",
    "testcase": "test_cases",
    "testcases": "test_cases",
}

_runtime_client = None
_lambda_client = None
_ddb = None
_s3 = None
_cognito = None


def _client():
    global _runtime_client
    if _runtime_client is None:
        _runtime_client = boto3.client(
            "bedrock-agentcore",
            region_name=BEDROCK_REGION,
            config=Config(
                read_timeout=HARNESS_INVOKE_READ_TIMEOUT,
                connect_timeout=60,
                retries={"max_attempts": 0},
            ),
        )
        logger.info(
            "bedrock-agentcore client read_timeout=%ss",
            HARNESS_INVOKE_READ_TIMEOUT,
        )
    return _runtime_client


def _lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda", region_name=BEDROCK_REGION)
    return _lambda_client


def _table():
    global _ddb
    if _ddb is None:
        if not JOBS_TABLE:
            raise RuntimeError("JOBS_TABLE is not configured")
        _ddb = boto3.resource("dynamodb", region_name=BEDROCK_REGION).Table(JOBS_TABLE)
    return _ddb


def _s3_client():
    global _s3
    if _s3 is None:
        # Regional endpoint is required so browser presigned URLs verify
        # (global s3.amazonaws.com host → SignatureDoesNotMatch / 403).
        _s3 = boto3.client(
            "s3",
            region_name=BEDROCK_REGION,
            endpoint_url=f"https://s3.{BEDROCK_REGION}.amazonaws.com",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "virtual"},
            ),
        )
    return _s3


def _response(
    status_code: int,
    body: Dict[str, Any],
    *,
    cors: bool = True,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if cors:
        headers.update(
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            }
        )
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body, ensure_ascii=False, default=_json_default),
    }


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")
    if body is None:
        if "prompt" in event or event.get("jobWorker"):
            return {
                k: event[k]
                for k in (
                    "prompt",
                    "sessionId",
                    "session_id",
                    "actorId",
                    "actor_id",
                    "skills",
                    "documents",
                    "jobWorker",
                    "jobId",
                    "job_id",
                )
                if k in event
            }
        return {}
    if event.get("isBase64Encoded"):
        import base64

        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")
    if isinstance(body, str):
        body = body.strip()
        if not body:
            return {}
        return json.loads(body)
    if isinstance(body, dict):
        return body
    return {}


def _route_key(event: Dict[str, Any]) -> Tuple[str, str]:
    request_context = event.get("requestContext") or {}
    http = request_context.get("http") or {}
    method = (
        http.get("method")
        or event.get("httpMethod")
        or request_context.get("httpMethod")
        or "POST"
    ).upper()
    path = (
        http.get("path")
        or event.get("rawPath")
        or event.get("path")
        or "/"
    )
    return method, path


def _path_parts(path: str) -> List[str]:
    return [p for p in path.strip("/").split("/") if p]


def _job_id_from_event(event: Dict[str, Any], path: str) -> Optional[str]:
    params = event.get("pathParameters") or {}
    for key in ("jobId", "job_id", "id"):
        value = params.get(key)
        if value:
            return str(value)
    parts = _path_parts(path)
    if len(parts) >= 2 and parts[-2] == "jobs":
        return parts[-1]
    return None


def _kind_from_path(path: str, event: Dict[str, Any]) -> Optional[str]:
    params = event.get("pathParameters") or {}
    raw = params.get("kind") or params.get("proxy")
    if not raw:
        parts = _path_parts(path)
        if len(parts) >= 2 and parts[0] == "documents":
            raw = parts[1]
    if not raw:
        return None
    return KIND_ALIASES.get(str(raw).strip().lower())


def _normalize_session_id(session_id: Optional[str]) -> str:
    sid = (session_id or "").strip() or str(uuid.uuid4())
    if len(sid) < 33:
        sid = f"{sid}-{uuid.uuid4()}"
    return sid


def _auth_header(event: Dict[str, Any]) -> str:
    headers = event.get("headers") or {}
    for key, value in headers.items():
        if str(key).lower() == "authorization":
            return str(value or "").strip()
    return ""


def _query_params(event: Dict[str, Any]) -> Dict[str, str]:
    params = event.get("queryStringParameters") or {}
    if not isinstance(params, dict):
        return {}
    return {str(k): str(v) for k, v in params.items() if v is not None}


def _cognito_client():
    global _cognito
    if _cognito is None:
        _cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION)
    return _cognito


def _bearer_token(event: Dict[str, Any]) -> str:
    raw = _auth_header(event)
    if not raw:
        # New-tab viewers cannot set Authorization; accept access_token query.
        raw = (_query_params(event).get("access_token") or "").strip()
        if raw:
            return raw
        raise PermissionError("Missing Authorization bearer token")
    if raw.lower().startswith("bearer "):
        return raw[7:].strip()
    return raw


def _username_from_get_user(user: Dict[str, Any]) -> str:
    username = (user.get("Username") or "").strip()
    if username:
        return username
    for attr in user.get("UserAttributes") or []:
        if not isinstance(attr, dict):
            continue
        if attr.get("Name") in ("preferred_username", "email", "sub"):
            value = (attr.get("Value") or "").strip()
            if value:
                return value
    raise PermissionError("Unable to resolve username from Cognito GetUser")


def _require_user(event: Dict[str, Any]) -> str:
    """Validate Cognito Access Token via GetUser (no PyJWT/cryptography)."""
    token = _bearer_token(event)
    try:
        user = _cognito_client().get_user(AccessToken=token)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in (
            "NotAuthorizedException",
            "InvalidParameterException",
            "ResourceNotFoundException",
        ):
            raise PermissionError("Invalid or expired Cognito token") from e
        logger.exception("Cognito GetUser failed")
        raise PermissionError(f"Cognito authentication failed: {code or e}") from e
    return _username_from_get_user(user)


def _skill_names_from_payload(payload: Dict[str, Any]) -> List[str]:
    raw = payload.get("skills")
    if raw is None:
        return list(DEFAULT_SKILLS)
    if isinstance(raw, str):
        names = [n.strip() for n in raw.split(",") if n.strip()]
        return names or list(DEFAULT_SKILLS)
    if isinstance(raw, list):
        names: List[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                names.append(item.strip())
            elif isinstance(item, dict):
                return []
        return names or list(DEFAULT_SKILLS)
    return list(DEFAULT_SKILLS)


def build_harness_skills(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = payload.get("skills")
    if isinstance(raw, list) and raw and all(isinstance(x, dict) for x in raw):
        return raw  # type: ignore[return-value]

    names = _skill_names_from_payload(payload)
    if not names:
        return []
    if not S3_BUCKET:
        logger.warning("S3_BUCKET unset; cannot attach S3 skills %s", names)
        return [{"path": f"skills/{name}"} for name in names]

    return [
        {"s3": {"uri": f"s3://{S3_BUCKET}/{SKILLS_S3_PREFIX}/{name}/"}}
        for name in names
    ]


def _incomplete_result_reason(
    text: str, last_stop_reason: Optional[str]
) -> Optional[str]:
    """Return an error message when the stream ended without a usable report."""
    body = (text or "").strip()
    stop = (last_stop_reason or "").strip()
    stop_l = stop.lower()

    # Prefer explicit model/harness stop reasons over heuristic narration checks.
    if stop_l in (
        "max_output_tokens_exceeded",
        "max_tokens",
        "maxtokens",
        "length",
    ):
        return (
            "Harness hit the output token limit before finishing the report "
            f"(stopReason={stop or last_stop_reason}, chars={len(body)}). "
            "Try a narrower prompt, fewer documents, or raise harness maxTokens."
        )
    if stop_l in ("tool_use", "tooluse", "tool_use_forced"):
        return (
            f"Harness ended while tools were still in progress "
            f"(stopReason={stop or last_stop_reason}, chars={len(body)})"
        )
    if not body:
        reason = f" (stopReason={stop})" if stop else ""
        return f"Harness returned an empty result{reason}"
    tail = body[-160:]
    for marker in _INCOMPLETE_TAIL_MARKERS:
        if marker in tail:
            extra = f", stopReason={stop}" if stop else ""
            return (
                "Harness stopped before producing the final report "
                f"(incomplete narration, chars={len(body)}{extra})"
            )
    if len(body) < MIN_RESULT_CHARS:
        extra = f", stopReason={stop}" if stop else ""
        return (
            "Harness result is too short to be a completed analysis "
            f"(chars={len(body)}, min={MIN_RESULT_CHARS}{extra})"
        )
    return None


def _collect_harness_text(
    harness_arn: str,
    prompt: str,
    session_id: str,
    actor_id: Optional[str] = None,
    skills: Optional[List[Dict[str, Any]]] = None,
) -> str:
    kwargs: Dict[str, Any] = {
        "harnessArn": harness_arn,
        "runtimeSessionId": session_id,
        "messages": [
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
    }
    if actor_id:
        kwargs["actorId"] = actor_id
    if skills:
        kwargs["skills"] = skills

    response = _client().invoke_harness(**kwargs)
    stream = response.get("stream")
    if stream is None:
        raise RuntimeError(f"Empty Harness response: {response}")

    chunks: list[str] = []
    event_count = 0
    text_events = 0
    last_stop_reason: Optional[str] = None
    try:
        for event in stream:
            event_count += 1
            if "contentBlockDelta" in event:
                delta = event["contentBlockDelta"].get("delta") or {}
                text = delta.get("text")
                if text:
                    chunks.append(text)
                    text_events += 1
            elif "messageStop" in event:
                last_stop_reason = (event.get("messageStop") or {}).get("stopReason")
            elif "runtimeClientError" in event:
                msg = (event["runtimeClientError"] or {}).get("message") or "unknown error"
                raise RuntimeError(f"Harness runtime error: {msg}")
            elif "internalServerException" in event:
                msg = (event.get("internalServerException") or {}).get("message") or str(
                    event["internalServerException"]
                )
                raise RuntimeError(f"Harness internal error: {msg}")
            elif "validationException" in event:
                msg = (event.get("validationException") or {}).get("message") or str(
                    event["validationException"]
                )
                raise RuntimeError(f"Harness validation error: {msg}")
    except (ReadTimeoutError, EventStreamError) as e:
        partial = "".join(chunks)
        raise RuntimeError(
            "Harness stream interrupted "
            f"(events={event_count}, text_events={text_events}, "
            f"partial_chars={len(partial)}, stopReason={last_stop_reason}, "
            f"read_timeout={HARNESS_INVOKE_READ_TIMEOUT}s): {e}"
        ) from e

    text = "".join(chunks)
    logger.info(
        "harness stream done events=%s text_events=%s chars=%s stopReason=%s",
        event_count,
        text_events,
        len(text),
        last_stop_reason,
    )
    incomplete = _incomplete_result_reason(text, last_stop_reason)
    if incomplete:
        raise RuntimeError(incomplete)
    return text


def _truncate_result(text: str) -> Tuple[str, bool]:
    if len(text) <= MAX_RESULT_CHARS:
        return text, False
    return text[:MAX_RESULT_CHARS] + "\n\n…(결과가 길어 일부만 저장했습니다)", True


def _put_job(item: Dict[str, Any]) -> None:
    _table().put_item(Item=item)


def _update_job(job_id: str, **attrs: Any) -> None:
    names: Dict[str, str] = {}
    values: Dict[str, Any] = {}
    parts: List[str] = []
    for i, (key, value) in enumerate(attrs.items()):
        nk = f"#k{i}"
        nv = f":v{i}"
        names[nk] = key
        values[nv] = value
        parts.append(f"{nk} = {nv}")
    names["#updatedAt"] = "updatedAt"
    values[":updatedAt"] = int(time.time())
    parts.append("#updatedAt = :updatedAt")
    _table().update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET " + ", ".join(parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _get_job(job_id: str) -> Optional[Dict[str, Any]]:
    resp = _table().get_item(Key={"jobId": job_id})
    return resp.get("Item")


def _public_job(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "jobId": item.get("jobId"),
        "status": item.get("status"),
        "sessionId": item.get("sessionId"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
    }
    if item.get("status") == "SUCCEEDED":
        out["result"] = item.get("result") or ""
        if item.get("resultTruncated"):
            out["resultTruncated"] = True
        if item.get("skills"):
            try:
                out["skills"] = json.loads(item["skills"])
            except (TypeError, json.JSONDecodeError):
                out["skills"] = item.get("skills")
        if item.get("downloadLinks"):
            try:
                out["downloadLinks"] = json.loads(item["downloadLinks"])
            except (TypeError, json.JSONDecodeError):
                out["downloadLinks"] = item.get("downloadLinks")
    if item.get("status") == "FAILED":
        out["error"] = item.get("error") or "Job failed"
    return out


def _enqueue_worker(payload: Dict[str, Any], context: Any) -> None:
    function_name = (
        os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        or getattr(context, "function_name", None)
        or ""
    )
    if not function_name:
        raise RuntimeError("Cannot resolve Lambda function name for async worker")
    _lambda().invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def _load_json_from_s3(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        obj = _s3_client().get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {"documents": data}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        logger.warning("S3 get_object failed s3://%s/%s: %s", bucket, key, e)
    except Exception as e:
        logger.warning("Failed reading s3://%s/%s: %s", bucket, key, e)
    return None


def _presign(
    bucket: str,
    key: str,
    expires: int = 3600,
    *,
    inline: bool = False,
    content_type: Optional[str] = None,
    download_name: Optional[str] = None,
) -> Optional[str]:
    if not bucket or not key:
        return None
    try:
        params: Dict[str, Any] = {"Bucket": bucket, "Key": key}
        name = download_name or key.rsplit("/", 1)[-1]
        safe_name = name.replace('"', "")
        # Always set disposition so browsers download (attachment) vs preview (inline).
        if inline:
            params["ResponseContentDisposition"] = f'inline; filename="{safe_name}"'
        else:
            params["ResponseContentDisposition"] = _content_disposition(safe_name)
        if content_type:
            params["ResponseContentType"] = content_type
        return _s3_client().generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expires,
        )
    except Exception as e:
        logger.debug("presign failed %s/%s: %s", bucket, key, e)
        return None


def _content_disposition(file_name: str, *, disposition: str = "attachment") -> str:
    """Build latin-1-safe Content-Disposition (RFC 5987 filename*)."""
    raw = (file_name or "download").replace('"', "").replace("\r", "").replace("\n", "")
    ascii_name = raw.encode("ascii", "ignore").decode("ascii").strip(" .") or "download"
    ascii_name = re.sub(r"_+", "_", ascii_name).strip("._") or "download"
    _, ext = os.path.splitext(raw)
    if ext and not ascii_name.lower().endswith(ext.lower()):
        base = ascii_name if ascii_name != "download" else "download"
        ascii_name = f"{base}{ext}"
    return (
        f'{disposition}; filename="{ascii_name}"; '
        f"filename*=UTF-8''{quote(raw)}"
    )


def _attachment_response(
    data: bytes, file_name: str, media_type: str
) -> Dict[str, Any]:
    """Stream a file download through API Gateway (harness-work style)."""
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": media_type,
            "Content-Disposition": _content_disposition(file_name),
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
        },
        "body": base64.b64encode(data).decode("ascii"),
        "isBase64Encoded": True,
    }


def _load_s3_bytes(key: str, *, bucket: Optional[str] = None) -> bytes:
    use_bucket = (bucket or ESS_S3_BUCKET or S3_BUCKET or "").strip()
    if not use_bucket:
        raise RuntimeError("S3 bucket is not configured")
    obj = _s3_client().get_object(Bucket=use_bucket, Key=key)
    return obj["Body"].read()


def _sharing_url_for_key(key: str) -> Optional[str]:
    if not ESS_SHARING_URL or not key:
        return None
    return f"{ESS_SHARING_URL}/{quote(key, safe='/')}"


def _object_exists(bucket: str, key: str) -> bool:
    try:
        _s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied"):
            logger.warning("S3 head AccessDenied s3://%s/%s", bucket, key)
        return False


def _find_object_key(
    user: str,
    doc: Dict[str, Any],
    kind_key: str,
    *,
    suffixes: Tuple[str, ...],
) -> Optional[str]:
    for key in _candidate_object_keys(user, doc, kind_key):
        lower = key.lower()
        if not any(lower.endswith(suf) for suf in suffixes):
            continue
        if ESS_S3_BUCKET and _object_exists(ESS_S3_BUCKET, key):
            return key
    return None


def _candidate_object_keys(user: str, doc: Dict[str, Any], kind_key: str) -> List[str]:
    keys: List[str] = []
    for field in (
        "md_s3_key",
        "s3_key",
        "source_s3_key",
        "json_s3_key",
        "source_download_url",
    ):
        value = (doc.get(field) or "").strip()
        if value and not value.startswith("http"):
            keys.append(value.lstrip("/"))

    filename = (doc.get("filename") or "").strip()
    md_file = (doc.get("md_file") or "").strip()
    stem = ""
    if filename:
        stem = filename.rsplit(".", 1)[0]
    elif md_file:
        stem = md_file.rsplit(".", 1)[0]

    dir_name = {
        "regulations": "regulations",
        "projects": "projects",
        "drawings": "drawings",
        "test_cases": "test_cases",
    }.get(kind_key, "regulations")

    prefixes = [
        f"app-data/{user}/ess/{dir_name}/",
        f"agentcore-sessions/{user}/ess/{dir_name}/",
        f"session-uploads/{user}/ess/{dir_name}/",
        f"session-uploads/{user}/ess/",
        f"artifacts/ess-work/{user}/md/",
        f"agentcore-sessions/{user}/artifacts/md/",
        f"artifacts/ess-work/{user}/tc/",
    ]
    names: List[str] = []
    for n in (
        filename,
        md_file,
        doc.get("json_file"),
        f"{stem}.md",
        f"{stem}.json",
        f"{stem}.pdf",
        f"{stem}.xlsx",
    ):
        if n and isinstance(n, str) and n.strip():
            names.append(n.strip().lstrip("/"))
    for prefix in prefixes:
        for name in names:
            keys.append(f"{prefix}{name}")

    seen = set()
    out: List[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _enrich_document(user: str, doc: Dict[str, Any], kind_key: str) -> Dict[str, Any]:
    out = dict(doc)
    kind_meta = KIND_REGISTRY[kind_key]
    kind_slug = kind_meta["kind"]
    out.setdefault("kind", kind_slug)
    out.setdefault(
        "display_name",
        out.get("title")
        or out.get("original_filename")
        or out.get("filename")
        or out.get("md_file")
        or "document",
    )
    filename = (out.get("filename") or out.get("md_file") or out.get("display_name") or "").strip()

    md_key = _find_object_key(user, out, kind_key, suffixes=(".md", ".markdown"))
    pdf_key = _find_object_key(user, out, kind_key, suffixes=(".pdf",))
    json_key = _find_object_key(user, out, kind_key, suffixes=(".json",))
    xlsx_key = _find_object_key(user, out, kind_key, suffixes=(".xlsx", ".xlsm"))

    if md_key:
        out["md_available"] = True
        out["md_s3_key"] = md_key
        out["md_file"] = out.get("md_file") or md_key.rsplit("/", 1)[-1]
        out["md_download_url"] = _presign(ESS_S3_BUCKET, md_key)
        # Frontend opens API viewer with access_token query (new tab).
        out["md_viewer_url"] = (
            f"{kind_slug}/{quote(filename or out['md_file'], safe='')}/markdown"
        )

    if pdf_key:
        out["pdf_available"] = True
        out["source_s3_key"] = pdf_key
        out["pdf_view_url"] = _presign(
            ESS_S3_BUCKET,
            pdf_key,
            inline=True,
            content_type="application/pdf",
            download_name=pdf_key.rsplit("/", 1)[-1],
        )
        out["source_download_url"] = out["pdf_view_url"]
        out["pdf_viewer_url"] = (
            f"{kind_slug}/{quote(filename or pdf_key.rsplit('/', 1)[-1], safe='')}/pdf"
        )

    if json_key:
        out["json_available"] = True
        out["json_s3_key"] = json_key
        out["json_path"] = out.get("json_path") or json_key
        out["json_viewer_url"] = (
            f"{kind_slug}/{quote(filename or json_key.rsplit('/', 1)[-1], safe='')}/json"
        )

    if xlsx_key:
        out["xlsx_available"] = True
        out["xlsx_s3_key"] = xlsx_key
        out["xlsx_view_url"] = _presign(
            ESS_S3_BUCKET,
            xlsx_key,
            inline=False,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name=xlsx_key.rsplit("/", 1)[-1],
        )

    download_links: List[Dict[str, str]] = []
    for key, label_prefix in (
        (md_key, "md"),
        (pdf_key, "pdf"),
        (json_key, "json"),
        (xlsx_key, "xlsx"),
    ):
        if not key:
            continue
        url = _presign(ESS_S3_BUCKET, key) or _sharing_url_for_key(key)
        if url:
            download_links.append(
                {"label": f"{label_prefix}:{key.rsplit('/', 1)[-1]}", "url": url, "s3_key": key}
            )
    if download_links:
        out["download_links"] = download_links[:6]
    return out


def _documents_from_registry(user: str, kind_key: str) -> Dict[str, Any]:
    meta = KIND_REGISTRY[kind_key]
    if not ESS_S3_BUCKET:
        return {
            "kind": meta["kind"],
            "label": meta["label"],
            "doc_count": 0,
            "documents": [],
            "error": "ESS_S3_BUCKET is not configured",
        }

    payload: Optional[Dict[str, Any]] = None
    used_key = ""
    for template in meta["list_keys"]:
        key = template.format(user=user)
        payload = _load_json_from_s3(ESS_S3_BUCKET, key)
        if payload is not None:
            used_key = key
            break

    raw_docs = []
    if payload:
        if isinstance(payload.get("documents"), list):
            raw_docs = payload["documents"]
        elif isinstance(payload.get("items"), list):
            raw_docs = payload["items"]

    documents = [
        _enrich_document(user, d, kind_key)
        for d in raw_docs
        if isinstance(d, dict)
    ]
    return {
        "kind": meta["kind"],
        "label": meta["label"],
        "doc_count": len(documents),
        "doc_list_key": used_key or None,
        "doc_list_updated_at": (payload or {}).get("updated_at")
        or (payload or {}).get("doc_list_updated_at"),
        "documents": documents,
    }


def _simple_markdown_to_html(text: str) -> str:
    """Minimal Markdown → HTML (no external deps)."""
    escaped = html.escape(text or "")
    lines = escaped.splitlines()
    out: List[str] = []
    in_code = False
    in_ul = False
    for line in lines:
        if line.strip().startswith("```"):
            if in_code:
                out.append("</code></pre>")
                in_code = False
            else:
                if in_ul:
                    out.append("</ul>")
                    in_ul = False
                out.append("<pre><code>")
                in_code = True
            continue
        if in_code:
            out.append(line + "\n")
            continue
        heading = re.match(r"^(#{1,4})\s+(.*)$", line)
        if heading:
            if in_ul:
                out.append("</ul>")
                in_ul = False
            level = len(heading.group(1))
            out.append(f"<h{level}>{heading.group(2)}</h{level}>")
            continue
        if re.match(r"^[-*]\s+", line):
            if not in_ul:
                out.append("<ul>")
                in_ul = True
            out.append(f"<li>{re.sub(r'^[-*]\s+', '', line)}</li>")
            continue
        if in_ul:
            out.append("</ul>")
            in_ul = False
        if not line.strip():
            out.append("")
            continue
        rendered = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
        rendered = re.sub(r"`([^`]+)`", r"<code>\1</code>", rendered)
        out.append(f"<p>{rendered}</p>")
    if in_code:
        out.append("</code></pre>")
    if in_ul:
        out.append("</ul>")
    return "\n".join(out)


def _viewer_topbar_actions(
    *,
    download_href: str = "",
    raw_href: str = "",
) -> str:
    """Bare action links (shell wraps them in .topbar-actions — harness style)."""
    parts: List[str] = []
    if download_href:
        parts.append(
            f'<a class="action" href="{html.escape(download_href, quote=True)}">Download</a>'
        )
    if raw_href:
        parts.append(
            f'<a class="action" href="{html.escape(raw_href, quote=True)}" '
            f'target="_blank" rel="noopener noreferrer">Raw</a>'
        )
    return "".join(parts)


def _access_token_qs(event: Dict[str, Any]) -> str:
    try:
        token = _bearer_token(event)
    except Exception:
        return ""
    if not token:
        return ""
    return f"access_token={quote(token)}"


def markdown_to_safe_html(text: str) -> str:
    """Best-effort Markdown → HTML without client JS (CSP-safe)."""
    try:
        import markdown as md_lib  # type: ignore

        return md_lib.markdown(
            text,
            extensions=["fenced_code", "tables", "nl2br", "sane_lists"],
            output_format="html5",
        )
    except Exception:
        return _simple_markdown_to_html(text)


_MARKDOWN_BODY_CSS = """
    .markdown-body {
      background: transparent;
      color: #e6edf3;
      line-height: 1.6;
      font-size: 15px;
    }
    .markdown-body h1, .markdown-body h2, .markdown-body h3 {
      margin: 1.2em 0 0.5em;
      font-weight: 650;
      border-bottom: 1px solid #30363d;
      padding-bottom: 0.3em;
    }
    .markdown-body p { margin: 0.75em 0; }
    .markdown-body ul, .markdown-body ol { padding-left: 1.5em; }
    .markdown-body code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.9em;
      background: rgba(110, 118, 129, 0.2);
      padding: 0.15em 0.4em;
      border-radius: 4px;
    }
    .markdown-body pre {
      overflow-x: auto;
      padding: 12px 14px;
      border-radius: 8px;
      background: rgba(110, 118, 129, 0.15);
      border: 1px solid #30363d;
    }
    .markdown-body pre code {
      background: transparent;
      padding: 0;
    }
    .markdown-body table {
      border-collapse: collapse;
      width: 100%;
      margin: 1em 0;
      font-size: 14px;
    }
    .markdown-body th, .markdown-body td {
      border: 1px solid #30363d;
      padding: 6px 10px;
      text-align: left;
    }
    .markdown-body a { color: #58a6ff; }
    .markdown-body blockquote {
      margin: 0.75em 0;
      padding: 0 1em;
      border-left: 3px solid #30363d;
      color: #8b949e;
    }
    @media (prefers-color-scheme: light) {
      .markdown-body { color: #1f2328; }
      .markdown-body h1, .markdown-body h2, .markdown-body h3,
      .markdown-body th, .markdown-body td,
      .markdown-body pre, .markdown-body blockquote {
        border-color: #d0d7de;
      }
      .markdown-body blockquote { color: #656d76; }
    }
"""


def _build_markdown_viewer_page(
    file_name: str,
    text: str,
    *,
    topbar_right_html: str = "",
) -> str:
    """Full HTML document for markdown preview (CSP-safe, harness-work style)."""
    title = html.escape(file_name)
    body_inner = markdown_to_safe_html(text)
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{
      margin: 0;
      background: #0d1117;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    }}
    .topbar {{
      position: sticky; top: 0; z-index: 2;
      display: flex; align-items: center; justify-content: space-between; gap: 12px;
      padding: 10px 20px;
      border-bottom: 1px solid #30363d;
      background: rgba(13, 17, 23, 0.92);
      backdrop-filter: blur(8px);
    }}
    .topbar h1 {{
      margin: 0; font-size: 14px; font-weight: 600;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }}
    .topbar-actions {{
      display: flex; align-items: center; gap: 14px; flex-shrink: 0;
    }}
    .topbar a.action {{
      color: #58a6ff; text-decoration: none; font-size: 13px; white-space: nowrap;
    }}
    .topbar a.action:hover {{ text-decoration: underline; }}
    .wrap {{
      box-sizing: border-box;
      max-width: 980px;
      margin: 0 auto;
      padding: 24px 20px 64px;
    }}
    {_MARKDOWN_BODY_CSS}
    @media (prefers-color-scheme: light) {{
      body {{ background: #ffffff; color: #1f2328; }}
      .topbar {{ background: rgba(255,255,255,0.92); border-bottom-color: #d0d7de; }}
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <h1>{title}</h1>
    <div class="topbar-actions">{topbar_right_html}</div>
  </div>
  <div class="wrap">
    <article class="markdown-body">{body_inner}</article>
  </div>
</body>
</html>
"""


def _build_json_viewer_page(
    file_name: str,
    text: str,
    *,
    topbar_right_html: str = "",
) -> str:
    """Full HTML document for JSON preview (pretty-printed, CSP-safe)."""
    title = html.escape(file_name)
    try:
        data = json.loads(text)
        pretty = html.escape(json.dumps(data, ensure_ascii=False, indent=2))
        badge = "JSON viewer"
    except Exception:
        pretty = html.escape(text)
        badge = "JSON viewer (invalid JSON — raw text)"
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{
      margin: 0;
      background: #0d1117;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    }}
    .topbar {{
      position: sticky; top: 0; z-index: 2;
      display: flex; align-items: center; justify-content: space-between; gap: 12px;
      padding: 10px 20px;
      border-bottom: 1px solid #30363d;
      background: rgba(13, 17, 23, 0.92);
      backdrop-filter: blur(8px);
    }}
    .topbar h1 {{
      margin: 0; font-size: 14px; font-weight: 600;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }}
    .topbar-actions {{
      display: flex; align-items: center; gap: 14px; flex-shrink: 0;
    }}
    .topbar a.action {{
      color: #58a6ff; text-decoration: none; font-size: 13px; white-space: nowrap;
    }}
    .topbar a.action:hover {{ text-decoration: underline; }}
    .badge {{
      font-size: 12px; color: #8b949e; margin-right: 4px; white-space: nowrap;
    }}
    .wrap {{
      box-sizing: border-box;
      max-width: 1100px;
      margin: 0 auto;
      padding: 20px 16px 64px;
    }}
    pre.json {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 13px;
      line-height: 1.55;
      padding: 16px 18px;
      border-radius: 8px;
      background: rgba(110, 118, 129, 0.12);
      border: 1px solid #30363d;
    }}
    @media (prefers-color-scheme: light) {{
      body {{ background: #ffffff; color: #1f2328; }}
      .topbar {{ background: rgba(255,255,255,0.92); border-bottom-color: #d0d7de; }}
      pre.json {{ border-color: #d0d7de; background: rgba(175, 184, 193, 0.12); }}
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <h1>{title}</h1>
    <div class="topbar-actions">
      <span class="badge">{html.escape(badge)}</span>
      {topbar_right_html}
    </div>
  </div>
  <div class="wrap">
    <pre class="json">{pretty}</pre>
  </div>
</body>
</html>
"""


_CSV_MAX_PREVIEW_ROWS = 5000


def _build_csv_viewer_page(
    file_name: str,
    text: str,
    *,
    topbar_right_html: str = "",
    max_rows: int = _CSV_MAX_PREVIEW_ROWS,
) -> str:
    """Full HTML document for CSV preview as a table (CSP-safe)."""
    title = html.escape(file_name)
    sample = text[:4096] if text else ""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
    except Exception:
        dialect = csv.excel

    rows: List[List[str]] = []
    truncated = False
    try:
        reader = csv.reader(io.StringIO(text or ""), dialect)
        for i, row in enumerate(reader):
            if i >= max_rows:
                truncated = True
                break
            rows.append([str(cell) for cell in row])
        badge = f"CSV viewer · {len(rows)} rows"
        if truncated:
            badge += f" (first {max_rows} shown)"
    except Exception:
        body = f'<pre class="raw">{html.escape(text or "")}</pre>'
        return _csv_shell(
            title, "CSV viewer (parse failed — raw text)", topbar_right_html, body
        )

    if not rows:
        body = '<p class="empty">Empty CSV</p>'
        return _csv_shell(title, badge, topbar_right_html, body)

    header = rows[0]
    body_rows = rows[1:] if len(rows) > 1 else []
    thead = "".join(f"<th>{html.escape(c)}</th>" for c in header)
    tbody_parts: List[str] = []
    for row in body_rows:
        cells = list(row) + [""] * max(0, len(header) - len(row))
        cells = cells[: len(header)] if header else cells
        tbody_parts.append(
            "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>"
        )
    if len(rows) == 1:
        body = (
            "<table><tbody><tr>"
            + "".join(f"<td>{html.escape(c)}</td>" for c in header)
            + "</tr></tbody></table>"
        )
    else:
        body = (
            f"<table><thead><tr>{thead}</tr></thead>"
            f"<tbody>{''.join(tbody_parts)}</tbody></table>"
        )
    return _csv_shell(title, badge, topbar_right_html, body)


def _csv_shell(
    title: str, badge: str, topbar_right_html: str, body_inner: str
) -> str:
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{
      margin: 0;
      background: #0d1117;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    }}
    .topbar {{
      position: sticky; top: 0; z-index: 2;
      display: flex; align-items: center; justify-content: space-between; gap: 12px;
      padding: 10px 20px;
      border-bottom: 1px solid #30363d;
      background: rgba(13, 17, 23, 0.92);
      backdrop-filter: blur(8px);
    }}
    .topbar h1 {{
      margin: 0; font-size: 14px; font-weight: 600;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }}
    .topbar-actions {{
      display: flex; align-items: center; gap: 14px; flex-shrink: 0;
    }}
    .topbar a.action {{
      color: #58a6ff; text-decoration: none; font-size: 13px; white-space: nowrap;
    }}
    .topbar a.action:hover {{ text-decoration: underline; }}
    .badge {{
      font-size: 12px; color: #8b949e; margin-right: 4px; white-space: nowrap;
    }}
    .wrap {{
      box-sizing: border-box;
      max-width: 100%;
      margin: 0 auto;
      padding: 16px 12px 64px;
      overflow-x: auto;
    }}
    table {{
      border-collapse: collapse;
      width: max-content;
      min-width: 100%;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid #30363d;
      padding: 6px 10px;
      text-align: left;
      vertical-align: top;
      max-width: 420px;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    thead th {{
      position: sticky; top: 52px; z-index: 1;
      background: #161b22;
      font-weight: 600;
    }}
    tbody tr:nth-child(even) {{ background: rgba(110, 118, 129, 0.08); }}
    p.empty {{ color: #8b949e; padding: 24px; }}
    pre.raw {{
      margin: 0; padding: 16px; white-space: pre-wrap; word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px;
    }}
    @media (prefers-color-scheme: light) {{
      body {{ background: #ffffff; color: #1f2328; }}
      .topbar {{ background: rgba(255,255,255,0.92); border-bottom-color: #d0d7de; }}
      th, td {{ border-color: #d0d7de; }}
      thead th {{ background: #f6f8fa; }}
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <h1>{title}</h1>
    <div class="topbar-actions">
      <span class="badge">{html.escape(badge)}</span>
      {topbar_right_html}
    </div>
  </div>
  <div class="wrap">
    {body_inner}
  </div>
</body>
</html>
"""


def _html_response(status_code: int, page: str) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "text/html; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store",
        },
        "body": page,
    }


def _redirect_response(location: str) -> Dict[str, Any]:
    return {
        "statusCode": 302,
        "headers": {
            "Location": location,
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-store",
        },
        "body": "",
    }


def _load_s3_text(key: str, *, bucket: Optional[str] = None) -> str:
    use_bucket = (bucket or ESS_S3_BUCKET or "").strip()
    if not use_bucket:
        raise RuntimeError("S3 bucket is not configured")
    obj = _s3_client().get_object(Bucket=use_bucket, Key=key)
    raw = obj["Body"].read()
    for encoding in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _doc_stub_from_filename(filename: str) -> Dict[str, Any]:
    return {"filename": filename, "md_file": filename, "display_name": filename}


def _parse_document_view_path(parts: List[str]) -> Optional[Tuple[str, str, str]]:
    """documents / {kind} / {filename} / {view} → (kind_key, filename, view)."""
    if len(parts) < 4 or parts[0] != "documents":
        return None
    kind_key = KIND_ALIASES.get(parts[1].strip().lower())
    view = parts[-1].strip().lower()
    if not kind_key or view not in {"markdown", "pdf", "json", "xlsx"}:
        return None
    filename = unquote("/".join(parts[2:-1])).strip()
    if not filename:
        return None
    return kind_key, filename, view


def _download_href_for_key(event: Dict[str, Any], key: str) -> str:
    qs = _access_token_qs(event)
    href = f"/download?key={quote(key)}"
    if qs:
        href = f"{href}&{qs}"
    return href


def _handle_document_view(event: Dict[str, Any], parts: List[str]) -> Dict[str, Any]:
    parsed = _parse_document_view_path(parts)
    if not parsed:
        return _response(404, {"error": "Not found"})
    kind_key, filename, view = parsed
    try:
        user = _require_user(event)
    except PermissionError as e:
        if view in {"markdown", "json"}:
            return _html_response(
                401,
                f"<html><body><h1>Unauthorized</h1><p>{html.escape(str(e))}</p></body></html>",
            )
        return _response(401, {"error": str(e)})

    stub = _doc_stub_from_filename(filename)
    if view == "markdown":
        key = _find_object_key(user, stub, kind_key, suffixes=(".md", ".markdown"))
        if not key:
            # filename may be pdf — try stem.md
            stem = filename.rsplit(".", 1)[0]
            stub2 = _doc_stub_from_filename(f"{stem}.md")
            key = _find_object_key(user, stub2, kind_key, suffixes=(".md", ".markdown"))
        if not key:
            return _html_response(
                404,
                f"<html><body><h1>Markdown not found</h1><p>{html.escape(filename)}</p></body></html>",
            )
        text = _load_s3_text(key)
        actions = _viewer_topbar_actions(
            download_href=_download_href_for_key(event, key),
            raw_href=(
                f"{ESS_SHARING_URL}/{quote(key, safe='/')}" if ESS_SHARING_URL else ""
            ),
        )
        return _html_response(
            200,
            _build_markdown_viewer_page(
                key.rsplit("/", 1)[-1], text, topbar_right_html=actions
            ),
        )

    if view == "json":
        key = _find_object_key(user, stub, kind_key, suffixes=(".json",))
        if not key:
            stem = filename.rsplit(".", 1)[0]
            key = _find_object_key(
                user, _doc_stub_from_filename(f"{stem}.json"), kind_key, suffixes=(".json",)
            )
        if not key:
            return _html_response(
                404,
                f"<html><body><h1>JSON not found</h1><p>{html.escape(filename)}</p></body></html>",
            )
        text = _load_s3_text(key)
        actions = _viewer_topbar_actions(
            download_href=_download_href_for_key(event, key),
            raw_href=(
                f"{ESS_SHARING_URL}/{quote(key, safe='/')}" if ESS_SHARING_URL else ""
            ),
        )
        return _html_response(
            200,
            _build_json_viewer_page(
                key.rsplit("/", 1)[-1], text, topbar_right_html=actions
            ),
        )

    if view == "pdf":
        key = _find_object_key(user, stub, kind_key, suffixes=(".pdf",))
        if not key:
            return _response(404, {"error": f"PDF not found: {filename}"})
        url = _presign(
            ESS_S3_BUCKET,
            key,
            inline=True,
            content_type="application/pdf",
            download_name=key.rsplit("/", 1)[-1],
        )
        if not url:
            return _response(502, {"error": "Could not create PDF view URL"})
        return _redirect_response(url)

    if view == "xlsx":
        key = _find_object_key(user, stub, kind_key, suffixes=(".xlsx", ".xlsm"))
        if not key:
            return _response(404, {"error": f"Excel not found: {filename}"})
        url = _presign(
            ESS_S3_BUCKET,
            key,
            inline=False,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name=key.rsplit("/", 1)[-1],
        )
        if not url:
            return _response(502, {"error": "Could not create Excel URL"})
        return _redirect_response(url)

    return _response(404, {"error": "Not found"})


def _handle_documents(event: Dict[str, Any], path: str) -> Dict[str, Any]:
    parts = _path_parts(path)
    if _parse_document_view_path(parts):
        return _handle_document_view(event, parts)

    try:
        user = _require_user(event)
    except PermissionError as e:
        return _response(401, {"error": str(e)})
    except Exception as e:
        logger.exception("auth failed")
        return _response(401, {"error": f"Authentication failed: {e}"})

    kind_key = _kind_from_path(path, event)
    if kind_key:
        return _response(200, {"user": user, **_documents_from_registry(user, kind_key)})

    result: Dict[str, Any] = {"user": user, "kinds": {}}
    for key in ("regulations", "test_cases", "projects", "drawings"):
        result["kinds"][key] = _documents_from_registry(user, key)
    result["doc_count"] = sum(
        int(v.get("doc_count") or 0) for v in result["kinds"].values()
    )
    return _response(200, result)


def _build_analysis_prompt(
    user_prompt: str,
    documents: List[Dict[str, Any]],
    user: str,
) -> str:
    actor = (user or "user").strip() or "user"
    artifacts_dir = f"/mnt/workspace/{actor}/artifacts"
    lines = [
        "다음 ESS 문서를 분석하세요.",
        f"사용자: {user}",
        f"actor_id: {actor}",
        f"ARTIFACTS_DIR: {artifacts_dir}",
        "",
        "## 사용자 요청",
        user_prompt.strip() or "(요청 없음 — 선택된 문서를 요약·분석하세요)",
        "",
        "## 선택된 문서",
    ]
    if not documents:
        lines.append("(선택된 문서 없음)")
    for i, doc in enumerate(documents, 1):
        name = (
            doc.get("selected_name")
            or doc.get("display_name")
            or doc.get("md_file")
            or doc.get("filename")
            or doc.get("title")
            or f"document-{i}"
        )
        kind = doc.get("kind") or "document"
        lines.append(f"{i}. [{kind}] {name}")
        selected_path = (doc.get("selected_path") or "").strip()
        selected_type = (doc.get("selected_type") or "").strip()
        if selected_path:
            label = {
                "md": "selected md",
                "json": "selected json",
                "source": "selected source",
            }.get(selected_type, "selected path")
            lines.append(f"   - {label}: {selected_path}")
        for field, label in (
            ("md_workspace_path", "workspace md"),
            ("md_path", "md path"),
            ("json_path", "json path"),
            ("source_path", "source path"),
            ("md_s3_key", "md s3"),
            ("source_s3_key", "source s3"),
            ("md_download_url", "md download"),
            ("source_download_url", "source download"),
        ):
            value = (doc.get(field) or "").strip()
            if value and value != selected_path:
                lines.append(f"   - {label}: {value}")
        for link in doc.get("download_links") or []:
            if isinstance(link, dict) and link.get("url"):
                lines.append(
                    f"   - download ({link.get('label') or 'file'}): {link['url']}"
                )
        lines.append("")

    lines.extend(
        [
            "## 지침",
            "- 필요 시 regulation-evaluator / testcase-generator / pptx / docx / xlsx 스킬을 사용하세요.",
            f"- 모든 산출물은 반드시 ARTIFACTS_DIR ({artifacts_dir}) 아래에 저장하세요.",
            "- 산출물(xlsx/pptx/docx/pdf 등)을 생성했다면 최종 답변 전에 **반드시 doc-sharing** "
            "skill(`share_artifact.py`)로 CloudFront URL을 만든 뒤 그 URL만 사용자에게 전달하세요.",
            "- 로컬 경로(`/mnt/workspace/...`)만 안내하는 것은 금지입니다.",
            f"- 예: `python3 /tmp/doc-sharing/scripts/share_artifact.py "
            f"--filepath \"{artifacts_dir}/파일\" --actor-id \"{actor}\"`",
            "- 결과는 마크다운으로 작성하세요.",
            "- 문서 내용이 URL로만 주어지면 code 인터프리터로 내려받아 분석하세요.",
        ]
    )
    return "\n".join(lines)


def _extract_download_links_from_result(text: str) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    for match in re.finditer(r"\[([^\]]+)\]\((https?://[^)]+)\)", text or ""):
        links.append({"label": match.group(1), "url": match.group(2)})
    for match in re.finditer(
        r"(https?://[^\s<>\")\]]+\.(?:pdf|xlsx|xlsm|docx|pptx|md|json)(?:\?[^\s<>\")\]]*)?)",
        text or "",
        flags=re.IGNORECASE,
    ):
        url = match.group(1).rstrip(").,]")
        links.append({"label": url.split("?", 1)[0].rsplit("/", 1)[-1], "url": url})
    # Also catch bare s3 virtual-hosted URLs without extension in path but with query
    for match in re.finditer(
        r"(https?://[a-z0-9.\-]+\.s3[.\-][a-z0-9.\-]*\.amazonaws\.com/[^\s<>\")\]]+)",
        text or "",
        flags=re.IGNORECASE,
    ):
        url = match.group(1).rstrip(").,]")
        label = url.split("?", 1)[0].rsplit("/", 1)[-1] or "download"
        links.append({"label": label, "url": url})

    seen = set()
    out: List[Dict[str, str]] = []
    for item in links:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        out.append(item)
    return out[:30]


def _parse_s3_http_url(url: str) -> Optional[Tuple[str, str]]:
    """Return (bucket, key) from an S3 HTTPS URL, or None."""
    from urllib.parse import urlparse, unquote as _unquote

    try:
        parsed = urlparse(url)
    except Exception:
        return None
    host = (parsed.netloc or "").lower()
    path = _unquote((parsed.path or "").lstrip("/"))
    if not host or not path:
        return None

    # bucket.s3.amazonaws.com/key
    # bucket.s3.us-west-2.amazonaws.com/key
    # bucket.s3-us-west-2.amazonaws.com/key
    m = re.match(
        r"^([a-z0-9.\-]+)\.s3(?:[.\-][a-z0-9.\-]*)?\.amazonaws\.com$",
        host,
    )
    if m:
        return m.group(1), path

    # s3.us-west-2.amazonaws.com/bucket/key
    # s3.amazonaws.com/bucket/key
    if host.startswith("s3.") and host.endswith(".amazonaws.com"):
        parts = path.split("/", 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]
    return None


def _resign_download_url(url: str) -> Optional[str]:
    """Re-sign S3 object URLs with this Lambda's region credentials.

    Agent/code-interpreter often signs with us-east-1 credentials while the
    document-ai bucket lives in us-west-2, which yields browser 403s.
    """
    parsed = _parse_s3_http_url(url)
    if not parsed:
        return None
    bucket, key = parsed
    allowed = {b for b in (S3_BUCKET, ESS_S3_BUCKET) if b}
    if bucket not in allowed:
        # Still allow if key looks like our artifacts layout under document-ai bucket
        if S3_BUCKET and key.startswith("artifacts/"):
            bucket = S3_BUCKET
        else:
            return None
    name = key.rsplit("/", 1)[-1]
    lower = name.lower()
    inline = lower.endswith((".pdf", ".md", ".markdown", ".txt", ".json"))
    content_type = None
    if lower.endswith(".pdf"):
        content_type = "application/pdf"
    elif lower.endswith((".xlsx", ".xlsm")):
        content_type = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    return _presign(
        bucket,
        key,
        expires=6 * 3600,
        inline=inline,
        content_type=content_type,
        download_name=name,
    )


def _parse_api_artifact_viewer_url(url: str) -> Optional[str]:
    """If url is API /artifacts/view|download/{rest}, return rest; else None."""
    from urllib.parse import urlparse, unquote as _unquote

    try:
        parsed = urlparse(url)
    except Exception:
        return None
    path = _unquote((parsed.path or "").lstrip("/"))
    for prefix in ("artifacts/view/", "artifacts/download/"):
        if path.lower().startswith(prefix):
            rest = path[len(prefix) :]
            return rest or None
    return None


def _sharing_url_object_key(url: str) -> Optional[str]:
    """Extract object key from document-ai CloudFront / sharing URL."""
    from urllib.parse import urlparse, unquote as _unquote

    base = (SHARING_URL or "").rstrip("/")
    if not base or not url:
        return None
    try:
        if not url.startswith(base + "/") and url.rstrip("/") != base:
            return None
        parsed = urlparse(url)
        path = _unquote((parsed.path or "").lstrip("/"))
    except Exception:
        return None
    if path.startswith(("artifacts/", "images/", "docs/")):
        return path
    return None


def _rewrite_result_download_urls(
    text: str, user_id: str = ""
) -> Tuple[str, List[Dict[str, str]]]:
    """Replace broken agent S3 URLs; keep viewer URLs; attach real s3_key."""
    links = _extract_download_links_from_result(text)
    rewritten: List[Dict[str, str]] = []
    body = text or ""
    segment = _sanitize_user_segment(user_id) if user_id else ""

    for item in links:
        original = item["url"]
        label = item.get("label") or original.rsplit("/", 1)[-1]

        viewer_rest = _parse_api_artifact_viewer_url(original)
        if viewer_rest is not None:
            rest = (
                _collapse_duplicate_user_prefix(viewer_rest, segment)
                if segment
                else viewer_rest
            )
            s3_key = f"artifacts/{segment}/{rest}" if segment and rest else ""
            rewritten.append(
                {"label": label, "url": original, "s3_key": s3_key}
            )
            continue

        sharing_key = _sharing_url_object_key(original)
        if sharing_key:
            rewritten.append(
                {"label": label, "url": original, "s3_key": sharing_key}
            )
            continue

        fresh = _resign_download_url(original)
        if not fresh:
            parsed = _parse_s3_http_url(original)
            rewritten.append(
                {
                    "label": label,
                    "url": original,
                    "s3_key": (parsed[1] if parsed else ""),
                }
            )
            continue
        if original in body:
            body = body.replace(original, fresh)
        parsed = _parse_s3_http_url(original)
        rewritten.append(
            {
                "label": label,
                "url": fresh,
                "s3_key": (parsed[1] if parsed else ""),
            }
        )
    # de-dupe by url
    seen = set()
    out: List[Dict[str, str]] = []
    for item in rewritten:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        out.append(item)
    return body, out


_ARTIFACT_VIEW_EXTENSIONS = {".md", ".markdown", ".json", ".csv"}


def _sanitize_user_segment(user_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._\-]", "_", (user_id or "").strip()) or "user"


def _collapse_duplicate_user_prefix(rest: str, segment: str) -> str:
    """artifacts/{user}/{user}/file → file (under that user prefix)."""
    parts = [p for p in (rest or "").replace("\\", "/").split("/") if p]
    while len(parts) >= 2 and parts[0] == segment:
        parts = parts[1:]
    return "/".join(parts) if parts else (rest or "")


def _normalize_artifact_rest(file_path: str, user_id: str) -> str:
    raw = (file_path or "").strip().lstrip("/")
    parts = [p for p in raw.replace("\\", "/").split("/") if p]
    if not parts or any(p == ".." for p in parts):
        raise ValueError("Invalid file path")
    segment = _sanitize_user_segment(user_id)
    if len(parts) >= 2 and parts[0] == "artifacts":
        if parts[1] != segment:
            raise PermissionError("Artifact access denied")
        rest = "/".join(parts[2:])
        if not rest:
            raise ValueError("File path is required")
        return _collapse_duplicate_user_prefix(rest, segment)
    if parts[0] == segment:
        rest = "/".join(parts[1:])
        if not rest:
            raise ValueError("File path is required")
        return _collapse_duplicate_user_prefix(rest, segment)
    return _collapse_duplicate_user_prefix("/".join(parts), segment)


def _artifact_s3_key(user_id: str, file_path: str) -> Tuple[str, str]:
    rest = _normalize_artifact_rest(file_path, user_id)
    segment = _sanitize_user_segment(user_id)
    key = f"artifacts/{segment}/{rest}"
    return key, rest.rsplit("/", 1)[-1]


def _resolve_artifact_s3_key(
    bucket: str, user_id: str, file_path: str
) -> Tuple[str, str]:
    """Return (s3_key, file_name), including legacy double-actor keys."""
    rest = _normalize_artifact_rest(file_path, user_id)
    segment = _sanitize_user_segment(user_id)
    file_name = rest.rsplit("/", 1)[-1]
    candidates = [
        f"artifacts/{segment}/{rest}",
        # Legacy: share_artifact once nested actor under ARTIFACTS_DIR again
        f"artifacts/{segment}/{segment}/{rest}",
    ]
    seen = set()
    for key in candidates:
        if key in seen:
            continue
        seen.add(key)
        if _object_exists(bucket, key):
            return key, file_name
    return candidates[0], file_name


def _handle_artifacts(event: Dict[str, Any], parts: List[str]) -> Dict[str, Any]:
    """GET /artifacts/view|download/{file_path+} for shared md/json/csv."""
    if len(parts) < 3 or parts[0] != "artifacts" or parts[1] not in {"view", "download"}:
        return _response(404, {"error": "Not found"})
    action = parts[1]
    file_path = unquote("/".join(parts[2:])).strip()
    try:
        user = _require_user(event)
    except PermissionError as e:
        if action == "view":
            return _html_response(
                401,
                f"<html><body><h1>Unauthorized</h1><p>{html.escape(str(e))}</p></body></html>",
            )
        return _response(401, {"error": str(e)})

    try:
        # Validate path / ownership first (raises PermissionError / ValueError)
        _artifact_s3_key(user, file_path)
    except PermissionError as e:
        return _response(403, {"error": str(e)})
    except ValueError as e:
        return _response(400, {"error": str(e)})

    bucket = (S3_BUCKET or "").strip()
    if not bucket:
        return _response(500, {"error": "S3_BUCKET is not configured"})

    try:
        s3_key, file_name = _resolve_artifact_s3_key(bucket, user, file_path)
    except PermissionError as e:
        return _response(403, {"error": str(e)})
    except ValueError as e:
        return _response(400, {"error": str(e)})

    ext = os.path.splitext(file_name.lower())[1]
    if ext not in _ARTIFACT_VIEW_EXTENSIONS:
        return _response(
            400, {"error": "Viewer supports .md / .markdown / .json / .csv only"}
        )

    if not _object_exists(bucket, s3_key):
        return _response(404, {"error": f"Artifact not found: {s3_key}"})

    if action == "download":
        media = (
            "application/json; charset=utf-8"
            if ext == ".json"
            else (
                "text/csv; charset=utf-8"
                if ext == ".csv"
                else "text/markdown; charset=utf-8"
            )
        )
        try:
            data = _load_s3_bytes(s3_key, bucket=bucket)
        except Exception:
            logger.exception("Failed to download artifact %s", s3_key)
            return _response(502, {"error": "Failed to read artifact from S3"})
        return _attachment_response(data, file_name, media)

    try:
        text = _load_s3_text(s3_key, bucket=bucket)
    except Exception as e:
        logger.exception("Failed to load artifact %s", s3_key)
        return _html_response(
            502,
            f"<html><body><h1>Failed to load artifact</h1>"
            f"<p>{html.escape(str(e))}</p></body></html>",
        )

    qs = _access_token_qs(event)
    encoded_rest = quote(_normalize_artifact_rest(file_path, user), safe="/")
    download_href = f"/artifacts/download/{encoded_rest}"
    if qs:
        download_href = f"{download_href}?{qs}"
    raw_href = f"{SHARING_URL}/{quote(s3_key, safe='/')}" if SHARING_URL else ""
    actions = _viewer_topbar_actions(download_href=download_href, raw_href=raw_href)
    if ext == ".csv":
        page = _build_csv_viewer_page(file_name, text, topbar_right_html=actions)
    elif ext == ".json":
        page = _build_json_viewer_page(file_name, text, topbar_right_html=actions)
    else:
        page = _build_markdown_viewer_page(file_name, text, topbar_right_html=actions)
    return _html_response(200, page)


def _handle_download(event: Dict[str, Any]) -> Dict[str, Any]:
    """GET /download?key=artifacts/... → 302 to region-correct presigned URL."""
    try:
        user = _require_user(event)
    except PermissionError as e:
        return _response(401, {"error": str(e)})

    params = _query_params(event)
    key = (params.get("key") or "").lstrip("/")
    if not key or ".." in key:
        return _response(400, {"error": "Missing or invalid key"})
    bucket = (params.get("bucket") or S3_BUCKET or "").strip()
    if not bucket:
        return _response(500, {"error": "S3_BUCKET is not configured"})

    # Frontend once mis-parsed /artifacts/view/{file} as an S3 key.
    view_m = re.match(r"^artifacts/(?:view|download)/(.+)$", key, flags=re.IGNORECASE)
    if view_m:
        try:
            key, _ = _resolve_artifact_s3_key(bucket, user, view_m.group(1))
        except (PermissionError, ValueError) as e:
            return _response(400, {"error": str(e)})

    if not _object_exists(bucket, key):
        # fall back to ESS bucket for source docs
        if ESS_S3_BUCKET and bucket != ESS_S3_BUCKET and _object_exists(ESS_S3_BUCKET, key):
            bucket = ESS_S3_BUCKET
        else:
            return _response(404, {"error": f"Object not found: {key}"})

    name = key.rsplit("/", 1)[-1]
    lower = name.lower()
    inline = lower.endswith(".pdf")
    content_type = None
    if lower.endswith(".pdf"):
        content_type = "application/pdf"
    elif lower.endswith((".xlsx", ".xlsm")):
        content_type = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    elif lower.endswith(".json"):
        content_type = "application/json; charset=utf-8"
    elif lower.endswith(".csv"):
        content_type = "text/csv; charset=utf-8"
    elif lower.endswith((".md", ".markdown")):
        content_type = "text/markdown; charset=utf-8"
    url = _presign(
        bucket,
        key,
        expires=6 * 3600,
        inline=inline,
        content_type=content_type,
        download_name=name,
    )
    if not url:
        return _response(502, {"error": "Could not create download URL"})
    return _redirect_response(url)


def _handle_create_job(payload: Dict[str, Any], context: Any, user: str = "") -> Dict[str, Any]:
    if not JOBS_TABLE:
        return _response(500, {"error": "JOBS_TABLE is not configured"})
    if not HARNESS_ARN:
        return _response(500, {"error": "HARNESS_ARN is not configured"})

    user_prompt = (payload.get("prompt") or "").strip()
    documents = payload.get("documents") or []
    if not isinstance(documents, list):
        documents = []
    documents = [d for d in documents if isinstance(d, dict)]

    if not user_prompt and not documents:
        return _response(400, {"error": "Missing prompt or documents"})

    actor_id = payload.get("actorId") or payload.get("actor_id") or user or None
    session_id = _normalize_session_id(
        payload.get("sessionId") or payload.get("session_id")
    )
    skills = build_harness_skills(payload)
    full_prompt = _build_analysis_prompt(user_prompt, documents, user or str(actor_id or "user"))
    job_id = str(uuid.uuid4())
    now = int(time.time())

    item = {
        "jobId": job_id,
        "status": "QUEUED",
        "prompt": full_prompt,
        "userPrompt": user_prompt,
        "sessionId": session_id,
        "createdAt": now,
        "updatedAt": now,
        "ttl": now + JOB_TTL_SECONDS,
    }
    if actor_id:
        item["actorId"] = str(actor_id)
    if documents:
        item["documents"] = json.dumps(documents, ensure_ascii=False)[:200000]
    if skills:
        item["skills"] = json.dumps(skills, ensure_ascii=False)
    _put_job(item)

    worker_payload: Dict[str, Any] = {
        "jobWorker": True,
        "jobId": job_id,
        "prompt": full_prompt,
        "sessionId": session_id,
        "skills": skills,
    }
    if actor_id:
        worker_payload["actorId"] = actor_id

    try:
        _enqueue_worker(worker_payload, context)
    except Exception as e:
        logger.exception("Failed to enqueue job worker")
        _update_job(job_id, status="FAILED", error=f"Failed to enqueue worker: {e}")
        return _response(
            502,
            {
                "error": str(e),
                "jobId": job_id,
                "status": "FAILED",
                "sessionId": session_id,
            },
        )

    return _response(
        202,
        {
            "jobId": job_id,
            "status": "QUEUED",
            "sessionId": session_id,
            "pollPath": f"/jobs/{job_id}",
        },
    )


def _handle_get_job(job_id: str) -> Dict[str, Any]:
    if not JOBS_TABLE:
        return _response(500, {"error": "JOBS_TABLE is not configured"})
    if not job_id:
        return _response(400, {"error": "Missing jobId"})
    item = _get_job(job_id)
    if not item:
        return _response(404, {"error": "Job not found", "jobId": job_id})
    return _response(200, _public_job(item))


def _run_job_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = str(payload.get("jobId") or payload.get("job_id") or "").strip()
    if not job_id:
        logger.error("jobWorker missing jobId")
        return {"ok": False, "error": "missing jobId"}

    prompt = (payload.get("prompt") or "").strip()
    session_id = _normalize_session_id(
        payload.get("sessionId") or payload.get("session_id")
    )
    actor_id = payload.get("actorId") or payload.get("actor_id")
    skills = build_harness_skills(payload)

    try:
        _update_job(job_id, status="RUNNING")
    except Exception:
        logger.exception("Failed to mark job RUNNING: %s", job_id)

    if not HARNESS_ARN:
        _update_job(job_id, status="FAILED", error="HARNESS_ARN is not configured")
        return {"ok": False, "jobId": job_id, "error": "HARNESS_ARN missing"}

    if not prompt:
        item = _get_job(job_id) or {}
        prompt = (item.get("prompt") or "").strip()
    if not prompt:
        _update_job(job_id, status="FAILED", error="Missing prompt")
        return {"ok": False, "jobId": job_id, "error": "missing prompt"}

    try:
        result = _collect_harness_text(
            HARNESS_ARN,
            prompt,
            session_id,
            actor_id=actor_id,
            skills=skills,
        )
        result, truncated = _truncate_result(result)
        result, links = _rewrite_result_download_urls(
            result, user_id=str(actor_id or "")
        )
        attrs: Dict[str, Any] = {
            "status": "SUCCEEDED",
            "result": result,
            "sessionId": session_id,
        }
        if truncated:
            attrs["resultTruncated"] = True
        if skills:
            attrs["skills"] = json.dumps(skills, ensure_ascii=False)
        if links:
            attrs["downloadLinks"] = json.dumps(links, ensure_ascii=False)
        _update_job(job_id, **attrs)
        logger.info("job %s SUCCEEDED chars=%s links=%s", job_id, len(result), len(links))
        return {"ok": True, "jobId": job_id, "status": "SUCCEEDED"}
    except Exception as e:
        logger.exception("job %s FAILED", job_id)
        _update_job(job_id, status="FAILED", error=str(e), sessionId=session_id)
        return {"ok": False, "jobId": job_id, "status": "FAILED", "error": str(e)}


def _handle_sync_invoke(payload: Dict[str, Any], user: str = "") -> Dict[str, Any]:
    if not HARNESS_ARN:
        return _response(500, {"error": "HARNESS_ARN is not configured"})

    user_prompt = (payload.get("prompt") or "").strip()
    documents = payload.get("documents") or []
    if not isinstance(documents, list):
        documents = []
    documents = [d for d in documents if isinstance(d, dict)]
    if not user_prompt and not documents:
        return _response(400, {"error": "Missing prompt or documents"})

    session_id = _normalize_session_id(
        payload.get("sessionId") or payload.get("session_id")
    )
    actor_id = payload.get("actorId") or payload.get("actor_id") or user or None
    skills = build_harness_skills(payload)
    full_prompt = _build_analysis_prompt(
        user_prompt, documents, user or str(actor_id or "user")
    )
    logger.info("sync invoke skills=%s", skills)

    try:
        result = _collect_harness_text(
            HARNESS_ARN,
            full_prompt,
            session_id,
            actor_id=actor_id,
            skills=skills,
        )
    except Exception as e:
        logger.exception("InvokeHarness failed")
        return _response(502, {"error": str(e), "sessionId": session_id})

    result, links = _rewrite_result_download_urls(
        result, user_id=str(actor_id or user or "")
    )
    return _response(
        200,
        {
            "result": result,
            "sessionId": session_id,
            "skills": skills,
            "downloadLinks": links,
        },
    )


def handler(event, context):
    logger.info(
        "event keys=%s", list(event.keys()) if isinstance(event, dict) else type(event)
    )

    if isinstance(event, dict) and event.get("jobWorker"):
        return _run_job_worker(event)

    method, path = _route_key(event)
    parts = _path_parts(path)

    if method == "OPTIONS":
        return _response(200, {"ok": True})

    if method == "GET" and parts and parts[-1] == "health":
        return _response(
            200,
            {
                "status": "ok",
                "harnessConfigured": bool(HARNESS_ARN),
                "skillsConfigured": bool(S3_BUCKET and DEFAULT_SKILLS),
                "jobsTableConfigured": bool(JOBS_TABLE),
                "essBucketConfigured": bool(ESS_S3_BUCKET),
                "cognitoConfigured": bool(COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID),
                "defaultSkills": DEFAULT_SKILLS,
                "region": BEDROCK_REGION,
                "mode": "async-jobs",
            },
        )

    if method == "GET" and parts and parts[0] == "download":
        return _handle_download(event)

    if method == "GET" and parts and parts[0] == "artifacts":
        return _handle_artifacts(event, parts)

    if method == "GET" and parts and parts[0] == "documents":
        return _handle_documents(event, path)

    if method == "POST" and parts and parts[-1] == "jobs":
        try:
            payload = _parse_body(event)
        except json.JSONDecodeError as e:
            return _response(400, {"error": f"Invalid JSON body: {e}"})
        try:
            user = _require_user(event)
        except PermissionError as e:
            return _response(401, {"error": str(e)})
        except Exception as e:
            logger.exception("auth failed")
            return _response(401, {"error": f"Authentication failed: {e}"})
        return _handle_create_job(payload, context, user=user)

    if method == "GET" and parts and "jobs" in parts:
        job_id = _job_id_from_event(event, path)
        return _handle_get_job(job_id or "")

    if method == "POST" and parts and parts[-1] == "invoke":
        try:
            payload = _parse_body(event)
        except json.JSONDecodeError as e:
            return _response(400, {"error": f"Invalid JSON body: {e}"})
        try:
            user = _require_user(event)
        except PermissionError as e:
            return _response(401, {"error": str(e)})
        except Exception as e:
            return _response(401, {"error": f"Authentication failed: {e}"})
        return _handle_sync_invoke(payload, user=user)

    if method == "POST" and (not parts or parts == [""]):
        try:
            payload = _parse_body(event)
        except json.JSONDecodeError as e:
            return _response(400, {"error": f"Invalid JSON body: {e}"})
        try:
            user = _require_user(event)
        except Exception:
            user = ""
        if JOBS_TABLE:
            return _handle_create_job(payload, context, user=user)
        return _handle_sync_invoke(payload, user=user)

    return _response(405, {"error": f"Method not allowed: {method} {path}"})
