#!/usr/bin/env python3
"""
AWS Infrastructure Installer for document-ai.

Provisions harness + web stack (S3, skills, DynamoDB jobs, AgentCore harness,
Lambda, API Gateway, CloudFront) with ESS-work Cognito/S3 integration.
Does NOT provision any businfo Kinesis/Glue/Firehose pipeline.
"""

import argparse
import io
import json
import logging
import mimetypes
import os
import re
import subprocess
import sys
import time
import uuid
import zipfile
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Project constants
# ---------------------------------------------------------------------------
project_name = "document-ai"
bucket_name_prefix = "storage-for-document-ai"
jobs_table_name = "dynamodb-document-ai-jobs"
lambda_harness_name = "lambda-harness-document-ai"
lambda_harness_role_name = "lambda-harness-document-ai-role"
lambda_lmi_operator_role_name = "lambda-lmi-operator-document-ai"
lambda_capacity_provider_name = "cp-document-ai"
lambda_lmi_sg_name = "document-ai-lmi-sg"
api_harness_name = "api-harness-document-ai"
code_interpreter_name = "document_ai_code"
DEFAULT_HARNESS_SKILLS = [
    "regulation-evaluator",
    "testcase-generator",
    "pptx",
    "docx",
    "xlsx",
]
DEFAULT_MODEL_ID = "global.anthropic.claude-sonnet-4-6"
# Async Event invoke on Lambda Managed Instances (sync API paths still ≤15m).
LAMBDA_JOB_TIMEOUT_SECONDS = 1800
HARNESS_TIMEOUT_SECONDS = 1800
LMI_MAX_VCPU_COUNT = 30
LMI_MIN_EXECUTION_ENVIRONMENTS = 3
LMI_MAX_EXECUTION_ENVIRONMENTS = 6
WEB_S3_PREFIX = "web"
region = "us-west-2"
COMPANY_NAME = "문서 분석 솔루션"

_HARNESS_NAME_API_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,39}$")

bucket_name = ""
lambda_python_runtime = "python3.13"

script_dir = os.path.dirname(os.path.abspath(__file__))
lambda_base_dir = script_dir
HTML_DIR = os.path.join(script_dir, "html")
SKILLS_DIR = os.path.join(script_dir, "skills")
SKILLS_S3_PREFIX = "skills"
ESS_WORK_CONFIG_PATH = os.path.normpath(
    os.path.join(script_dir, "..", "ess-work", "application", "config.json")
)

sts_client = boto3.client("sts", region_name=region)
account_id = sts_client.get_caller_identity()["Account"]

s3_client = boto3.client("s3", region_name=region)
iam_client = boto3.client("iam", region_name=region)
dynamodb_client = boto3.client("dynamodb", region_name=region)
lambda_client = boto3.client("lambda", region_name=region)
ec2_client = boto3.client("ec2", region_name=region)
apigatewayv2_client = boto3.client("apigatewayv2", region_name=region)
cloudfront_client = boto3.client("cloudfront", region_name="us-east-1")
agentcore_control_client = boto3.client(
    "bedrock-agentcore-control",
    region_name=region,
)


def setup_logging(log_level=logging.INFO):
    """Setup logging configuration."""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# ---------------------------------------------------------------------------
# IAM / shared helpers
# ---------------------------------------------------------------------------
def wait_for_iam_role(role_name: str, max_wait: int = 30, propagation_delay: int = 10):
    """Wait until IAM role is available and propagated for Lambda."""
    waited = 0
    while waited < max_wait:
        try:
            iam_client.get_role(RoleName=role_name)
            break
        except ClientError:
            time.sleep(2)
            waited += 2
    else:
        logger.warning(f"IAM role {role_name} may not exist yet")
        return

    if propagation_delay:
        logger.debug(
            f"Waiting {propagation_delay}s for IAM role propagation: {role_name}"
        )
        time.sleep(propagation_delay)


def create_iam_role(
    role_name: str,
    assume_role_policy: Dict,
    description: str = "",
    managed_policies: Optional[List[str]] = None,
) -> str:
    """Create IAM role."""
    logger.debug(f"Creating IAM role: {role_name}")

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description=description or f"Role for {role_name}",
        )
        role_arn = response["Role"]["Arn"]
        logger.debug(f"Role created: {role_arn}")

        if managed_policies:
            for policy_arn in managed_policies:
                iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn,
                )

        wait_for_iam_role(role_name)
        logger.info(f"✓ IAM role created: {role_name}")
        return role_arn

    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            logger.warning(f"IAM role already exists: {role_name}")
            response = iam_client.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]

            iam_client.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(assume_role_policy),
            )

            if managed_policies:
                attached = iam_client.list_attached_role_policies(RoleName=role_name)
                current = {p["PolicyArn"] for p in attached["AttachedPolicies"]}
                for policy_arn in managed_policies:
                    if policy_arn not in current:
                        iam_client.attach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy_arn,
                        )

            wait_for_iam_role(role_name)
            return role_arn
        logger.error(f"Failed to create IAM role {role_name}: {e}")
        raise


def attach_inline_policy(role_name: str, policy_name: str, policy_document: Dict):
    """Attach or update inline policy to IAM role."""
    logger.debug(f"Attaching inline policy {policy_name} to {role_name}")
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_document),
    )


def resolve_bucket_name(acct_id: str, aws_region: str) -> str:
    """Build a globally unique S3 bucket name for this account and region."""
    return f"{bucket_name_prefix}-{acct_id}-{aws_region}".lower()


def load_bucket_name_from_config() -> Optional[str]:
    """Load bucket name from config.json if available."""
    config_path = os.path.join(script_dir, "config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return None
            data = json.loads(content)
            return data.get("bucketName")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def load_config_json() -> Dict:
    config_path = os.path.join(script_dir, "config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {}


def load_ess_work_config() -> Dict[str, str]:
    """Load Cognito + S3 sharing settings from sibling ess-work config."""
    logger.info(f"Loading ESS-work config: {ESS_WORK_CONFIG_PATH}")
    try:
        with open(ESS_WORK_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.warning(
            f"ESS-work config not found at {ESS_WORK_CONFIG_PATH}; "
            "Cognito/ESS S3 env vars will be empty"
        )
        return {
            "cognito_user_pool_id": "",
            "cognito_client_id": "",
            "cognito_region": region,
            "s3_bucket": "",
            "sharing_url": "",
        }
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read ESS-work config: {e}")
        return {
            "cognito_user_pool_id": "",
            "cognito_client_id": "",
            "cognito_region": region,
            "s3_bucket": "",
            "sharing_url": "",
        }

    ess = {
        "cognito_user_pool_id": (data.get("cognito_user_pool_id") or "").strip(),
        "cognito_client_id": (data.get("cognito_client_id") or "").strip(),
        "cognito_region": (
            data.get("cognito_region") or data.get("region") or region
        ).strip(),
        "s3_bucket": (data.get("s3_bucket") or "").strip(),
        "sharing_url": (data.get("sharing_url") or "").strip().rstrip("/"),
    }
    logger.info(
        f"  ESS S3={ess['s3_bucket'] or '(none)'} "
        f"Cognito pool={ess['cognito_user_pool_id'] or '(none)'} "
        f"sharing={ess['sharing_url'] or '(none)'}"
    )
    return ess


def verify_s3_bucket_access(target_bucket: Optional[str] = None) -> bool:
    """Verify that the S3 bucket exists and is accessible."""
    target_bucket = target_bucket or bucket_name
    try:
        s3_client.head_bucket(Bucket=target_bucket)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket", "NotFound"):
            return False
        raise


def configure_s3_bucket(target_bucket: Optional[str] = None):
    """Apply standard bucket configuration."""
    target_bucket = target_bucket or bucket_name
    s3_client.put_public_access_block(
        Bucket=target_bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3_client.put_bucket_versioning(
        Bucket=target_bucket,
        VersioningConfiguration={"Status": "Suspended"},
    )


def create_s3_bucket() -> Dict[str, str]:
    """Create S3 bucket for skills + static web."""
    logger.info(f"Creating S3 bucket: {bucket_name}")

    if verify_s3_bucket_access():
        logger.warning(f"S3 bucket already exists: {bucket_name}")
        try:
            configure_s3_bucket()
        except ClientError as e:
            logger.debug(f"Bucket configuration skipped or already applied: {e}")
    else:
        try:
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            configure_s3_bucket()
            logger.info(f"✓ S3 bucket created: {bucket_name}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "BucketAlreadyOwnedByYou":
                if not verify_s3_bucket_access():
                    raise RuntimeError(
                        f"S3 bucket '{bucket_name}' is owned by this account but is not "
                        f"accessible in region '{region}'."
                    ) from e
                logger.warning(f"S3 bucket already exists: {bucket_name}")
                try:
                    configure_s3_bucket()
                except ClientError as config_error:
                    logger.debug(
                        f"Bucket configuration skipped or already applied: {config_error}"
                    )
            elif error_code == "BucketAlreadyExists":
                raise RuntimeError(
                    f"S3 bucket name '{bucket_name}' is already taken by another AWS account. "
                    "Use a globally unique bucket name."
                ) from e
            else:
                logger.error(f"Failed to create S3 bucket: {e}")
                raise

    if not verify_s3_bucket_access():
        raise RuntimeError(
            f"S3 bucket '{bucket_name}' is not accessible. "
            f"Verify the bucket exists in region '{region}' and your credentials have access."
        )

    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    return {
        "bucket_name": bucket_name,
        "bucket_arn": bucket_arn,
        "s3_path": f"s3://{bucket_name}",
    }


# ---------------------------------------------------------------------------
# Lambda packaging / deploy
# ---------------------------------------------------------------------------
def package_lambda(
    source_dir: str,
    include_node_modules: bool = False,
    pip_packages: Optional[List[str]] = None,
) -> bytes:
    """Package Lambda function source into a zip archive."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        if include_node_modules:
            for root, dirs, files in os.walk(source_dir):
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                for filename in files:
                    if filename.startswith("."):
                        continue
                    filepath = os.path.join(root, filename)
                    arcname = os.path.relpath(filepath, source_dir)
                    zf.write(filepath, arcname)
        else:
            py_handler = os.path.join(source_dir, "lambda_function.py")
            index_path = os.path.join(source_dir, "index.js")
            if os.path.isfile(py_handler):
                for filename in sorted(os.listdir(source_dir)):
                    if not filename.endswith(".py"):
                        continue
                    filepath = os.path.join(source_dir, filename)
                    if os.path.isfile(filepath):
                        zf.write(filepath, filename)
            elif os.path.isfile(index_path):
                zf.write(index_path, "index.js")
            else:
                raise FileNotFoundError(
                    f"No lambda_function.py or index.js in {source_dir}"
                )

        if pip_packages:
            import tempfile

            with tempfile.TemporaryDirectory(prefix="lambda-pip-") as tmp:
                logger.info(
                    f"  Bundling pip packages for Lambda: {', '.join(pip_packages)}"
                )
                # Install manylinux wheels so macOS-hosted installs work on Lambda.
                pip_cmd = [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--quiet",
                    "--upgrade",
                    "--target",
                    tmp,
                    "--platform",
                    "manylinux2014_x86_64",
                    "--implementation",
                    "cp",
                    "--python-version",
                    "3.12",
                    "--only-binary=:all:",
                    *pip_packages,
                ]
                try:
                    subprocess.run(pip_cmd, check=True)
                except subprocess.CalledProcessError:
                    logger.warning(
                        "  manylinux binary install failed; falling back to host pip "
                        "(native extensions may break on Lambda)"
                    )
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "pip",
                            "install",
                            "--quiet",
                            "--upgrade",
                            "--target",
                            tmp,
                            *pip_packages,
                        ],
                        check=True,
                    )
                for root, dirs, files in os.walk(tmp):
                    dirs[:] = [
                        d
                        for d in dirs
                        if d not in {"__pycache__", "*.dist-info"}
                        and not d.endswith(".dist-info")
                    ]
                    for filename in files:
                        if filename.endswith((".pyc", ".pyo")):
                            continue
                        filepath = os.path.join(root, filename)
                        arcname = os.path.relpath(filepath, tmp)
                        if "__pycache__" in arcname.split(os.sep):
                            continue
                        zf.write(filepath, arcname.replace(os.sep, "/"))

    buffer.seek(0)
    return buffer.read()


def harness_name_for_api(name: str) -> str:
    """Map projectName to CreateHarness harnessName (hyphens → underscores)."""
    normalized = (name or "").replace("-", "_")
    if not _HARNESS_NAME_API_RE.fullmatch(normalized):
        logger.error(
            "CreateHarness harnessName must match [a-zA-Z][a-zA-Z0-9_]{0,39} "
            f"(after '-'→'_'): got {normalized!r} from projectName={name!r}"
        )
        sys.exit(1)
    return normalized


def get_max_output_tokens(model_id: str = "") -> int:
    """Return max output tokens per Amazon Bedrock Anthropic Claude model cards."""
    mid = model_id.lower()
    if "claude-opus-4-7" in mid or "claude-opus-4-6" in mid:
        return 128000
    if "claude-opus-4-5" in mid:
        return 64000
    if "claude-opus-4" in mid or "claude-4-opus" in mid:
        return 128000
    if "claude-sonnet-4" in mid or "claude-4-sonnet" in mid or "claude-haiku-4" in mid:
        return 64000
    return 8192


BASE_SYSTEM_PROMPT = (
    "당신은 ESS(Enterprise Shared Storage)에 저장된 문서를 분석하는 문서 분석 에이전트입니다.\n"
    "한국어로 답변하세요.\n"
    "모르는 내용은 추측하지 말고 모른다고 말하세요.\n"
    "\n"
    "## 역할\n"
    "- 사용자가 선택한 ESS 문서를 읽고 분석·요약·규정 적합성 평가·테스트케이스 도출을 수행합니다.\n"
    "- 분석 결과는 마크다운으로 반환하고, 생성한 산출물(pptx/docx/xlsx 등)에는 다운로드 링크를 포함하세요.\n"
    "\n"
    "## Skills\n"
    "- **regulation-evaluator**: 규정/컴플라이언스 평가 및 리포트 생성\n"
    "- **testcase-generator**: 요구사항·문서 기반 테스트케이스 생성\n"
    "- **pptx / docx / xlsx**: Office 문서 생성·편집 (프레젠테이션, 워드, 스프레드시트)\n"
    "- code 인터프리터에 skill 파일이 미리 마운트되지 않을 수 있습니다. 필요 시 S3에서 동기화하세요:\n"
    "  `aws s3 sync s3://{skills_bucket}/skills/<skill-name>/ /tmp/<skill-name>/`\n"
    "\n"
    "## 도구\n"
    "- code 인터프리터로 skill 스크립트를 실행하세요.\n"
    "- 필요 시 aws_knowledge로 AWS 서비스 문서를 참고하세요.\n"
    "- 결과는 근거와 함께 구조화된 마크다운으로 제시하세요.\n"
)


def create_lambda_execution_role(
    role_name: str, extra_policies: Optional[List[Dict]] = None
) -> str:
    """Create Lambda execution IAM role."""
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    role_arn = create_iam_role(
        role_name,
        assume_role_policy,
        managed_policies=[
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        ],
    )

    if extra_policies:
        for policy in extra_policies:
            attach_inline_policy(role_name, policy["name"], policy["document"])

    return role_arn


def create_lmi_operator_role() -> str:
    """IAM role Lambda uses to manage EC2 for Managed Instances capacity providers."""
    logger.info(f"Creating LMI operator role: {lambda_lmi_operator_role_name}")
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    return create_iam_role(
        lambda_lmi_operator_role_name,
        assume_role_policy,
        managed_policies=[
            "arn:aws:iam::aws:policy/AWSLambdaManagedEC2ResourceOperator"
        ],
    )


def resolve_lmi_vpc_config() -> Dict[str, List[str]]:
    """Use default VPC public subnets + a dedicated security group for LMI."""
    logger.info("Resolving VPC config for Lambda Managed Instances")
    vpcs = ec2_client.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}]
    ).get("Vpcs") or []
    if not vpcs:
        raise RuntimeError(
            "No default VPC found. Create a default VPC or pass custom subnets."
        )
    vpc_id = vpcs[0]["VpcId"]
    subnets = ec2_client.describe_subnets(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default-for-az", "Values": ["true"]},
        ]
    ).get("Subnets") or []
    if len(subnets) < 2:
        subnets = ec2_client.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        ).get("Subnets") or []
    # Prefer public MapPublicIpOnLaunch subnets across AZs.
    public = [s for s in subnets if s.get("MapPublicIpOnLaunch")]
    chosen = public or subnets
    by_az: Dict[str, str] = {}
    for s in sorted(chosen, key=lambda x: x.get("AvailabilityZone") or ""):
        az = s.get("AvailabilityZone") or ""
        if az and az not in by_az:
            by_az[az] = s["SubnetId"]
    subnet_ids = list(by_az.values())[:3]
    if not subnet_ids:
        raise RuntimeError(f"No subnets available in default VPC {vpc_id}")

    sg_id = None
    existing = ec2_client.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": [lambda_lmi_sg_name]},
        ]
    ).get("SecurityGroups") or []
    if existing:
        sg_id = existing[0]["GroupId"]
        logger.info(f"  Reusing security group: {sg_id}")
    else:
        created = ec2_client.create_security_group(
            GroupName=lambda_lmi_sg_name,
            Description="Outbound access for document-ai Lambda Managed Instances",
            VpcId=vpc_id,
            TagSpecifications=[
                {
                    "ResourceType": "security-group",
                    "Tags": [
                        {"Key": "Project", "Value": project_name},
                        {"Key": "Name", "Value": lambda_lmi_sg_name},
                    ],
                }
            ],
        )
        sg_id = created["GroupId"]
        # Default SG already allows all egress; ensure it.
        try:
            ec2_client.authorize_security_group_egress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )
        except ClientError as e:
            if e.response["Error"]["Code"] not in (
                "InvalidPermission.Duplicate",
                "InvalidPermission.Duplicate",
            ):
                # Some accounts already have default allow-all egress.
                logger.debug(f"  egress authorize skipped: {e}")
        logger.info(f"  ✓ Security group created: {sg_id}")

    logger.info(f"  VPC={vpc_id} subnets={subnet_ids} sg={sg_id}")
    return {"SubnetIds": subnet_ids, "SecurityGroupIds": [sg_id]}


def create_or_get_capacity_provider(operator_role_arn: str) -> str:
    """Create or reuse the Lambda Managed Instances capacity provider."""
    logger.info(f"Creating capacity provider: {lambda_capacity_provider_name}")
    expected_arn = (
        f"arn:aws:lambda:{region}:{account_id}:capacity-provider:"
        f"{lambda_capacity_provider_name}"
    )
    try:
        existing = lambda_client.get_capacity_provider(
            CapacityProviderName=lambda_capacity_provider_name
        )
        arn = (
            (existing.get("CapacityProvider") or {}).get("CapacityProviderArn")
            or existing.get("CapacityProviderArn")
            or expected_arn
        )
        logger.info(f"  Capacity provider already exists: {arn}")
        return arn
    except ClientError as e:
        if e.response["Error"]["Code"] not in (
            "ResourceNotFoundException",
            "CapacityProviderNotFoundException",
        ):
            # Older error code variants
            if "NotFound" not in e.response["Error"]["Code"]:
                raise

    vpc_config = resolve_lmi_vpc_config()
    try:
        response = lambda_client.create_capacity_provider(
            CapacityProviderName=lambda_capacity_provider_name,
            VpcConfig=vpc_config,
            PermissionsConfig={
                "CapacityProviderOperatorRoleArn": operator_role_arn
            },
            InstanceRequirements={"Architectures": ["x86_64"]},
            CapacityProviderScalingConfig={
                "ScalingMode": "Auto",
                "MaxVCpuCount": LMI_MAX_VCPU_COUNT,
            },
            Tags={"Project": project_name},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        response = lambda_client.get_capacity_provider(
            CapacityProviderName=lambda_capacity_provider_name
        )

    arn = (
        (response.get("CapacityProvider") or {}).get("CapacityProviderArn")
        or response.get("CapacityProviderArn")
        or expected_arn
    )
    logger.info(f"✓ Capacity provider ready: {arn}")
    return arn


def publish_lmi_function(function_name: str) -> str:
    """Publish $LATEST.PUBLISHED so the function runs on Managed Instances."""
    logger.info(f"Publishing LMI version for {function_name}")
    wait_for_lambda_function_ready(function_name)
    response = lambda_client.publish_version(
        FunctionName=function_name,
        PublishTo="LATEST_PUBLISHED",
        Description="document-ai LMI published version",
    )
    version = response.get("Version") or "$LATEST.PUBLISHED"
    published_arn = response.get("FunctionArn") or (
        f"arn:aws:lambda:{region}:{account_id}:function:{function_name}:{version}"
    )
    # Wait until the published qualifier is Active.
    deadline = time.time() + 600
    while time.time() < deadline:
        cfg = lambda_client.get_function_configuration(
            FunctionName=function_name,
            Qualifier=version,
        )
        state = cfg.get("State") or ""
        last = cfg.get("LastUpdateStatus") or ""
        if state == "Active" and last in ("Successful", ""):
            break
        if state == "Failed" or last == "Failed":
            raise RuntimeError(
                f"LMI publish failed for {function_name}: "
                f"{cfg.get('StateReason') or cfg.get('LastUpdateStatusReason')}"
            )
        time.sleep(5)
    else:
        raise TimeoutError(f"Timed out waiting for {function_name}:{version} Active")

    try:
        lambda_client.put_function_scaling_config(
            FunctionName=function_name,
            Qualifier=version,
            FunctionScalingConfig={
                "MinExecutionEnvironments": LMI_MIN_EXECUTION_ENVIRONMENTS,
                "MaxExecutionEnvironments": LMI_MAX_EXECUTION_ENVIRONMENTS,
            },
        )
        logger.info(
            f"  ✓ Scaling config min={LMI_MIN_EXECUTION_ENVIRONMENTS} "
            f"max={LMI_MAX_EXECUTION_ENVIRONMENTS}"
        )
    except ClientError as e:
        logger.warning(f"  Could not set function scaling config: {e}")

    logger.info(f"✓ Published {published_arn}")
    return published_arn


def lambda_function_exists(function_name: str) -> bool:
    """Check whether a Lambda function already exists."""
    try:
        lambda_client.get_function(FunctionName=function_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def wait_for_lambda_function_ready(function_name: str):
    """Wait until Lambda function update completes."""
    logger.debug(f"Waiting for Lambda function update: {function_name}")
    waiter = lambda_client.get_waiter("function_updated")
    waiter.wait(FunctionName=function_name)


def update_lambda_function_configuration(
    function_name: str,
    role_arn: str,
    handler: str,
    runtime: str,
    description: str,
    timeout: int,
    memory_size: int = 128,
    environment: Optional[Dict[str, str]] = None,
    capacity_provider_arn: Optional[str] = None,
    max_retries: int = 6,
):
    """Update Lambda configuration with retry while function update is in progress."""
    update_kwargs = {
        "FunctionName": function_name,
        "Role": role_arn,
        "Handler": handler,
        "Runtime": runtime,
        "Description": description,
        "Timeout": timeout,
        "MemorySize": memory_size,
    }
    if environment:
        update_kwargs["Environment"] = {"Variables": environment}
    if capacity_provider_arn:
        update_kwargs["CapacityProviderConfig"] = {
            "LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": capacity_provider_arn,
                "PerExecutionEnvironmentMaxConcurrency": 10,
            }
        }

    for attempt in range(max_retries):
        try:
            lambda_client.update_function_configuration(**update_kwargs)
            return
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"].get("Message", "")
            if (
                error_code == "ResourceConflictException"
                and "update is in progress" in error_message
                and attempt < max_retries - 1
            ):
                wait_time = 5 * (attempt + 1)
                logger.warning(
                    f"Lambda update in progress for {function_name}, "
                    f"retrying configuration update in {wait_time}s "
                    f"({attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                wait_for_lambda_function_ready(function_name)
                continue
            raise


def deploy_lambda_function(
    function_name: str,
    role_arn: str,
    source_dir: str,
    description: str,
    handler: str = "lambda_function.handler",
    runtime: str = lambda_python_runtime,
    timeout: int = 3,
    memory_size: int = 128,
    environment: Optional[Dict[str, str]] = None,
    include_node_modules: bool = False,
    pip_packages: Optional[List[str]] = None,
    capacity_provider_arn: Optional[str] = None,
) -> str:
    """Create or update Lambda function (optionally on Managed Instances)."""
    logger.info(f"Deploying Lambda function: {function_name}")

    zip_bytes = package_lambda(
        source_dir,
        include_node_modules=include_node_modules,
        pip_packages=pip_packages,
    )
    function_exists = lambda_function_exists(function_name)
    capacity_cfg = None
    if capacity_provider_arn:
        capacity_cfg = {
            "LambdaManagedInstancesCapacityProviderConfig": {
                "CapacityProviderArn": capacity_provider_arn,
                "PerExecutionEnvironmentMaxConcurrency": 10,
            }
        }

    # Existing "Lambda Default" functions cannot gain CapacityProviderConfig via update.
    if function_exists and capacity_provider_arn:
        cfg = lambda_client.get_function_configuration(FunctionName=function_name)
        has_cp = bool(cfg.get("CapacityProviderConfig"))
        if not has_cp:
            logger.warning(
                f"Recreating {function_name}: CapacityProviderConfig requires a new "
                "Managed Instances function (cannot convert Lambda Default in place)"
            )
            try:
                lambda_client.delete_function_url_config(FunctionName=function_name)
            except ClientError:
                pass
            lambda_client.delete_function(FunctionName=function_name)
            # Wait until name is free
            for _ in range(30):
                if not lambda_function_exists(function_name):
                    break
                time.sleep(2)
            function_exists = False

    if not function_exists:
        create_kwargs = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Role": role_arn,
            "Handler": handler,
            "Code": {"ZipFile": zip_bytes},
            "Description": description,
            "Timeout": timeout,
            "MemorySize": memory_size,
            "Architectures": ["x86_64"],
        }
        if environment:
            create_kwargs["Environment"] = {"Variables": environment}
        if capacity_cfg:
            create_kwargs["CapacityProviderConfig"] = capacity_cfg

        max_retries = 6
        for attempt in range(max_retries):
            try:
                response = lambda_client.create_function(**create_kwargs)
                function_arn = response["FunctionArn"]
                logger.info(f"✓ Lambda function created: {function_arn}")
                wait_for_lambda_function_ready(function_name)
                if capacity_provider_arn:
                    publish_lmi_function(function_name)
                    try:
                        lambda_client.put_function_event_invoke_config(
                            FunctionName=function_name,
                            MaximumRetryAttempts=1,
                            MaximumEventAgeInSeconds=6 * 3600,
                        )
                    except ClientError as e:
                        logger.warning(f"  Could not set event invoke config: {e}")
                return function_arn

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"].get("Message", "")

                if (
                    error_code == "ResourceConflictException"
                    and "already exist" in error_message.lower()
                ):
                    function_exists = True
                    break

                if (
                    error_code == "InvalidParameterValueException"
                    and "cannot be assumed by Lambda" in error_message
                    and attempt < max_retries - 1
                ):
                    wait_time = 5 * (attempt + 1)
                    logger.warning(
                        f"IAM role not yet assumable by Lambda, retrying in {wait_time}s "
                        f"({attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                logger.error(f"Failed to deploy Lambda function {function_name}: {e}")
                raise
        else:
            raise RuntimeError(f"Failed to deploy Lambda function {function_name}")

    logger.warning(f"Lambda function already exists, updating: {function_name}")
    lambda_client.update_function_code(
        FunctionName=function_name,
        ZipFile=zip_bytes,
    )
    wait_for_lambda_function_ready(function_name)
    update_lambda_function_configuration(
        function_name=function_name,
        role_arn=role_arn,
        handler=handler,
        runtime=runtime,
        description=description,
        timeout=timeout,
        memory_size=memory_size,
        environment=environment,
        capacity_provider_arn=capacity_provider_arn,
    )
    wait_for_lambda_function_ready(function_name)
    if capacity_provider_arn:
        publish_lmi_function(function_name)
        try:
            lambda_client.put_function_event_invoke_config(
                FunctionName=function_name,
                MaximumRetryAttempts=1,
                MaximumEventAgeInSeconds=6 * 3600,
            )
            logger.info("  ✓ Async invoke retry=1 (avoid multi-hour stuck RUNNING)")
        except ClientError as e:
            logger.warning(f"  Could not set event invoke config: {e}")

    response = lambda_client.get_function(FunctionName=function_name)
    function_arn = response["Configuration"]["FunctionArn"]
    logger.info(f"✓ Lambda function updated: {function_arn}")
    return function_arn


# ---------------------------------------------------------------------------
# Harness execution role / AgentCore
# ---------------------------------------------------------------------------
def create_harness_execution_role(
    s3_bucket: str, ess_s3_bucket: str = ""
) -> str:
    """Create IAM execution role for Bedrock AgentCore harness (PUBLIC, no memory)."""
    logger.info("Creating Harness execution IAM role")
    role_name = f"role-harness-for-{project_name}-{region}"
    if len(role_name) > 64:
        logger.error(
            f"IAM RoleName exceeds 64 characters ({len(role_name)}): {role_name!r}"
        )
        sys.exit(1)

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAgentCoreAssumeHarness",
                "Effect": "Allow",
                "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    role_created = False
    try:
        iam_client.get_role(RoleName=role_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            role_created = True
        else:
            raise

    role_arn = create_iam_role(
        role_name,
        assume_role_policy,
        description="Execution role for Bedrock AgentCore harness (document-ai)",
    )

    statements: List[Dict] = [
        {
            "Sid": "BedrockModelInvocation",
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel",
                "bedrock:InvokeModelWithResponseStream",
                "bedrock:GetInferenceProfile",
                "bedrock:GetFoundationModel",
            ],
            "Resource": [
                "arn:aws:bedrock:*::foundation-model/*",
                f"arn:aws:bedrock:{region}:{account_id}:inference-profile/*",
            ],
        },
        {
            "Sid": "AgentCoreAccess",
            "Effect": "Allow",
            "Action": ["bedrock-agentcore:*"],
            "Resource": ["*"],
        },
        {
            "Sid": "CloudWatchLogsAgentCore",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams",
            ],
            "Resource": [
                f"arn:aws:logs:{region}:{account_id}:log-group:/aws/bedrock-agentcore/*",
            ],
        },
        {
            "Sid": "EcrManagedImagePull",
            "Effect": "Allow",
            "Action": [
                "ecr:BatchGetImage",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchCheckLayerAvailability",
            ],
            "Resource": [f"arn:aws:ecr:{region}:*:repository/harness-*"],
        },
        {
            "Sid": "EcrManagedImageToken",
            "Effect": "Allow",
            "Action": ["ecr:GetAuthorizationToken"],
            "Resource": ["*"],
        },
        {
            "Sid": "ProjectS3List",
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": [f"arn:aws:s3:::{s3_bucket}"],
        },
        {
            "Sid": "ProjectS3GetObject",
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{s3_bucket}/*"],
        },
        {
            "Sid": "ProjectS3PutArtifacts",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
            ],
            "Resource": [
                f"arn:aws:s3:::{s3_bucket}/artifacts/*",
                f"arn:aws:s3:::{s3_bucket}/reports/*",
            ],
        },
        {
            "Sid": "SkillsS3Read",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{s3_bucket}"],
            "Condition": {
                "StringLike": {"s3:prefix": [f"{SKILLS_S3_PREFIX}/*"]}
            },
        },
        {
            "Sid": "SkillsS3GetObject",
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{s3_bucket}/{SKILLS_S3_PREFIX}/*"],
        },
    ]

    if ess_s3_bucket:
        statements.extend(
            [
                {
                    "Sid": "EssS3List",
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                    "Resource": [f"arn:aws:s3:::{ess_s3_bucket}"],
                },
                {
                    "Sid": "EssS3GetObject",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{ess_s3_bucket}/*"],
                },
            ]
        )

    harness_execution_policy = {
        "Version": "2012-10-17",
        "Statement": statements,
    }

    attach_inline_policy(
        role_name,
        f"harness-exec-inline-for-{project_name}",
        harness_execution_policy,
    )
    if role_created:
        wait_seconds = 20
        logger.info(
            f"  Waiting {wait_seconds}s for IAM role/policy propagation "
            "before CreateHarness..."
        )
        time.sleep(wait_seconds)
    logger.info(f"✓ Harness execution role ready: {role_arn}")
    return role_arn


def _paginate_list_harnesses() -> List[Dict]:
    items: List[Dict] = []
    token = None
    while True:
        kw: Dict = {"maxResults": 50}
        if token:
            kw["nextToken"] = token
        resp = agentcore_control_client.list_harnesses(**kw)
        items.extend(resp.get("harnesses") or [])
        token = resp.get("nextToken")
        if not token:
            break
    return items


def find_harness_by_api_name(harness_api_name: str) -> Optional[Dict]:
    for h in _paginate_list_harnesses():
        if h.get("harnessName") == harness_api_name:
            return h
    return None


def wait_for_harness_ready(harness_id: str, timeout_seconds: int = 300) -> str:
    """Poll until harness reaches READY; return harness ARN."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        res = agentcore_control_client.get_harness(harnessId=harness_id)
        h = res["harness"]
        status = h["status"]
        if status == "READY":
            harness_arn = h["arn"]
            logger.info(f"✓ Harness ready: {harness_arn}")
            return harness_arn
        if status in (
            "FAILED",
            "CREATE_FAILED",
            "UPDATE_FAILED",
            "DELETING",
            "DELETE_UNSUCCESSFUL",
            "DELETE_FAILED",
        ):
            reason = h.get("failureReason") or h.get("statusReason") or ""
            raise RuntimeError(
                f"Harness {harness_id} entered terminal status: {status}"
                + (f" — {reason}" if reason else "")
            )
        logger.info(f"  Waiting for harness ({harness_id}) status: {status}")
        time.sleep(5)
    raise TimeoutError(
        f"Harness {harness_id} did not reach READY within {timeout_seconds}s"
    )


def update_harness_safe(harness_id: str, *, timeout_seconds: int = 600, **kwargs) -> None:
    """UpdateHarness only when READY; retry on ConflictException."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        remaining = max(30, int(deadline - time.time()))
        wait_for_harness_ready(harness_id, timeout_seconds=remaining)
        try:
            agentcore_control_client.update_harness(harnessId=harness_id, **kwargs)
            return
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "")
            msg = (e.response.get("Error") or {}).get("Message", "")
            if code != "ConflictException" and "while it is UPDATING" not in msg:
                raise
            logger.warning(
                "  UpdateHarness ConflictException; waiting and retrying..."
            )
            time.sleep(8)
    raise TimeoutError(
        f"Timed out updating harness {harness_id} within {timeout_seconds}s"
    )


def _default_harness_tools(code_interpreter_arn: str = "") -> List[Dict]:
    code_config: Dict = {"agentCoreCodeInterpreter": {}}
    if code_interpreter_arn:
        code_config = {
            "agentCoreCodeInterpreter": {
                "codeInterpreterArn": code_interpreter_arn,
            }
        }
    return [
        {
            "type": "remote_mcp",
            "name": "aws_knowledge",
            "config": {
                "remoteMcp": {
                    "url": "https://knowledge-mcp.global.api.aws",
                }
            },
        },
        {
            "type": "agentcore_code_interpreter",
            "name": "code",
            "config": code_config,
        },
    ]


def _paginate_list_code_interpreters() -> List[Dict]:
    items: List[Dict] = []
    token = None
    while True:
        kw: Dict = {"maxResults": 50}
        if token:
            kw["nextToken"] = token
        resp = agentcore_control_client.list_code_interpreters(**kw)
        items.extend(resp.get("codeInterpreterSummaries") or [])
        token = resp.get("nextToken")
        if not token:
            break
    return items


def wait_for_code_interpreter_ready(
    code_interpreter_id: str,
    timeout_seconds: int = 300,
) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        detail = agentcore_control_client.get_code_interpreter(
            codeInterpreterId=code_interpreter_id
        )
        status = detail.get("status")
        if status == "READY":
            arn = detail["codeInterpreterArn"]
            logger.info(f"✓ Code Interpreter ready: {arn}")
            return arn
        if status in ("FAILED", "DELETE_FAILED", "DELETED"):
            reason = detail.get("failureReason") or ""
            raise RuntimeError(
                f"Code Interpreter {code_interpreter_id} status={status}"
                + (f" — {reason}" if reason else "")
            )
        logger.info(
            f"  Waiting for Code Interpreter ({code_interpreter_id}) status: {status}"
        )
        time.sleep(5)
    raise TimeoutError(
        f"Code Interpreter {code_interpreter_id} not READY within {timeout_seconds}s"
    )


def create_or_get_code_interpreter(execution_role_arn: str) -> Dict[str, str]:
    """Custom Code Interpreter with IAM role (PUBLIC network)."""
    logger.info(f"Creating Code Interpreter: {code_interpreter_name}")
    existing_id = ""
    for item in _paginate_list_code_interpreters():
        if item.get("name") == code_interpreter_name:
            existing_id = item.get("codeInterpreterId") or ""
            break

    if existing_id:
        logger.info(
            f"  Code Interpreter {code_interpreter_name!r} exists "
            f"(id={existing_id}); reusing"
        )
        arn = wait_for_code_interpreter_ready(existing_id)
        detail = agentcore_control_client.get_code_interpreter(
            codeInterpreterId=existing_id
        )
        current_role = detail.get("executionRoleArn") or ""
        if current_role and current_role != execution_role_arn:
            logger.warning(
                f"  Code Interpreter role differs "
                f"(current={current_role}, desired={execution_role_arn}); "
                "recreate manually if S3 access fails"
            )
        return {
            "code_interpreter_id": existing_id,
            "code_interpreter_arn": arn,
            "code_interpreter_name": code_interpreter_name,
        }

    response = agentcore_control_client.create_code_interpreter(
        name=code_interpreter_name,
        description="Document-ai S3/skills code interpreter for AgentCore Harness",
        executionRoleArn=execution_role_arn,
        networkConfiguration={"networkMode": "PUBLIC"},
        clientToken=str(uuid.uuid4()),
        tags={"Project": project_name, "Env": "dev"},
    )
    code_interpreter_id = response["codeInterpreterId"]
    logger.info(f"  ✓ Code Interpreter created: {code_interpreter_id}")
    arn = wait_for_code_interpreter_ready(code_interpreter_id)
    return {
        "code_interpreter_id": code_interpreter_id,
        "code_interpreter_arn": arn,
        "code_interpreter_name": code_interpreter_name,
    }


def _public_harness_environment() -> Dict:
    return {
        "agentCoreRuntimeEnvironment": {
            "lifecycleConfiguration": {
                "idleRuntimeSessionTimeout": 600,
                "maxLifetime": 14400,
            },
            "networkConfiguration": {"networkMode": "PUBLIC"},
        }
    }


def ensure_harness_memory_disabled(harness_id: str) -> None:
    """Ensure harness has memory explicitly disabled (stateless)."""
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    memory = h.get("memory") or {}
    if isinstance(memory, dict) and "disabled" in memory:
        logger.info("  Harness memory already disabled")
        return
    logger.info(f"  Disabling harness memory (harnessId={harness_id})")
    update_harness_safe(
        harness_id,
        memory={"optionalValue": {"disabled": {}}},
    )


def ensure_harness_environment_public(harness_id: str) -> None:
    desired = _public_harness_environment()
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current_rt = (h.get("environment") or {}).get("agentCoreRuntimeEnvironment") or {}
    current_mode = (current_rt.get("networkConfiguration") or {}).get("networkMode")
    if current_mode == "PUBLIC":
        logger.info("  Harness environment already PUBLIC")
        return
    logger.info(f"  Updating harness networkMode {current_mode!r} -> PUBLIC")
    update_harness_safe(harness_id, environment=desired)


def _system_prompt_text(s3_bucket: str) -> str:
    return BASE_SYSTEM_PROMPT.replace("{skills_bucket}", s3_bucket)


def ensure_harness_system_prompt(harness_id: str, s3_bucket: str) -> None:
    desired = [{"text": _system_prompt_text(s3_bucket)}]
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current = h.get("systemPrompt") or []
    if current == desired:
        logger.info("  Harness systemPrompt already up to date")
        return
    logger.info(f"  Updating harness systemPrompt (harnessId={harness_id})")
    update_harness_safe(harness_id, systemPrompt=desired)


def ensure_harness_model(harness_id: str, model_id: str = DEFAULT_MODEL_ID) -> None:
    """Keep CreateHarness modelId in sync on re-install."""
    desired_max = get_max_output_tokens(model_id)
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current = ((h.get("model") or {}).get("bedrockModelConfig") or {})
    if current.get("modelId") == model_id and current.get("maxTokens") == desired_max:
        logger.info(f"  Harness model already {model_id}")
        return
    logger.info(
        f"  Updating harness model: {current.get('modelId')!r} -> {model_id!r}"
    )
    update_harness_safe(
        harness_id,
        model={
            "bedrockModelConfig": {
                "modelId": model_id,
                "maxTokens": desired_max,
            }
        },
    )


def ensure_harness_timeout(harness_id: str, timeout_seconds: int) -> None:
    """Keep harness wall-clock timeout in sync (e.g. 1800s for long evaluations)."""
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current = h.get("timeoutSeconds")
    if current == timeout_seconds:
        logger.info(f"  Harness timeoutSeconds already {timeout_seconds}")
        return
    logger.info(f"  Updating harness timeoutSeconds {current!r} -> {timeout_seconds}")
    update_harness_safe(harness_id, timeoutSeconds=timeout_seconds)


def ensure_harness_tools(harness_id: str, code_interpreter_arn: str = "") -> None:
    desired = _default_harness_tools(code_interpreter_arn)
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current = h.get("tools") or []
    current_by_name = {
        t.get("name"): t for t in current if isinstance(t, dict) and t.get("name")
    }
    needs_update = False
    for tool in desired:
        existing = current_by_name.get(tool["name"])
        if existing != tool:
            needs_update = True
            break
    if not needs_update and {t["name"] for t in desired}.issubset(current_by_name):
        logger.info("  Harness tools already up to date")
        return
    merged = dict(current_by_name)
    for tool in desired:
        merged[tool["name"]] = tool
    logger.info("  Updating harness tools (custom code interpreter)")
    update_harness_safe(harness_id, tools=list(merged.values()))


def ensure_harness_skills(harness_id: str, s3_bucket: str) -> None:
    """Attach default S3 skills on the harness configuration."""
    desired = build_default_harness_skills(s3_bucket)
    h = agentcore_control_client.get_harness(harnessId=harness_id)["harness"]
    current = h.get("skills") or []
    desired_uris = {
        ((s.get("s3") or {}).get("uri") or "").rstrip("/")
        for s in desired
        if isinstance(s, dict)
    }
    current_uris = {
        ((s.get("s3") or {}).get("uri") or "").rstrip("/")
        for s in current
        if isinstance(s, dict)
    }
    if desired_uris and desired_uris.issubset(current_uris):
        logger.info("  Harness skills already include default S3 skills")
        return
    logger.info(
        f"  Updating harness skills → {', '.join(sorted(desired_uris)) or '(none)'}"
    )
    update_harness_safe(harness_id, skills=desired)


def create_or_get_harness(
    execution_role_arn: str,
    s3_bucket: str,
    code_interpreter_arn: str = "",
    ess_s3_bucket: str = "",
) -> Dict[str, str]:
    """Create AgentCore Harness (PUBLIC, memory disabled) or reuse by name."""
    logger.info("Creating AgentCore Harness (PUBLIC, memory disabled)")

    harness_api_name = harness_name_for_api(project_name)
    logger.info(
        f"  harnessName: {harness_api_name} (from projectName={project_name!r})"
    )

    model_id = DEFAULT_MODEL_ID
    system_prompt = [{"text": _system_prompt_text(s3_bucket)}]
    environment = _public_harness_environment()
    tools = _default_harness_tools(code_interpreter_arn)
    skills = build_default_harness_skills(s3_bucket)

    env_vars = {
        "LOG_LEVEL": "info",
        "S3_BUCKET": s3_bucket,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
        "BEDROCK_REGION": region,
    }
    if ess_s3_bucket:
        env_vars["ESS_S3_BUCKET"] = ess_s3_bucket

    existing = find_harness_by_api_name(harness_api_name)
    if existing:
        harness_id = existing["harnessId"]
        try:
            status = agentcore_control_client.get_harness(harnessId=harness_id)[
                "harness"
            ].get("status")
        except ClientError:
            status = None
        if status in ("CREATE_FAILED", "UPDATE_FAILED", "FAILED", "DELETE_FAILED"):
            reason = ""
            try:
                reason = (
                    agentcore_control_client.get_harness(harnessId=harness_id)[
                        "harness"
                    ].get("failureReason")
                    or ""
                )
            except ClientError:
                pass
            logger.warning(
                f"Harness {harness_api_name!r} is {status} "
                f"(harnessId={harness_id}); deleting to recreate."
                + (f" reason={reason!r}" if reason else "")
            )
            try:
                agentcore_control_client.delete_harness(
                    harnessId=harness_id,
                    clientToken=str(uuid.uuid4()),
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                    raise
            deadline = time.time() + 600
            while time.time() < deadline:
                try:
                    agentcore_control_client.get_harness(harnessId=harness_id)
                    time.sleep(5)
                except ClientError as e:
                    if (
                        e.response.get("Error", {}).get("Code")
                        == "ResourceNotFoundException"
                    ):
                        break
                    raise
            else:
                raise TimeoutError(f"Timed out deleting failed harness {harness_id}")
            existing = None
        else:
            logger.warning(
                f"Harness {harness_api_name!r} already exists "
                f"(harnessId={harness_id}); skipping CreateHarness."
            )

    if not existing:
        try:
            response = agentcore_control_client.create_harness(
                harnessName=harness_api_name,
                executionRoleArn=execution_role_arn,
                model={
                    "bedrockModelConfig": {
                        "modelId": model_id,
                        "maxTokens": get_max_output_tokens(model_id),
                    }
                },
                systemPrompt=system_prompt,
                tools=tools,
                skills=skills,
                memory={"disabled": {}},
                truncation={
                    "strategy": "sliding_window",
                    "config": {"slidingWindow": {"messagesCount": 50}},
                },
                maxIterations=20,
                maxTokens=50000,
                timeoutSeconds=HARNESS_TIMEOUT_SECONDS,
                environment=environment,
                environmentVariables=env_vars,
                tags={"Project": project_name, "Env": "dev"},
            )
            harness_id = response["harness"]["harnessId"]
            logger.info(f"  ✓ Harness created: {harness_id}")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConflictException":
                raise
            rerun = find_harness_by_api_name(harness_api_name)
            if not rerun:
                raise
            harness_id = rerun["harnessId"]
            logger.info(
                f"CreateHarness conflict; using existing harnessId={harness_id}"
            )

    ensure_harness_memory_disabled(harness_id)
    ensure_harness_environment_public(harness_id)
    ensure_harness_model(harness_id, DEFAULT_MODEL_ID)
    ensure_harness_system_prompt(harness_id, s3_bucket)
    ensure_harness_tools(harness_id, code_interpreter_arn)
    ensure_harness_skills(harness_id, s3_bucket)
    ensure_harness_timeout(harness_id, HARNESS_TIMEOUT_SECONDS)
    harness_arn = wait_for_harness_ready(harness_id)
    return {
        "harness_id": harness_id,
        "harness_arn": harness_arn,
        "harness_name": harness_api_name,
    }


# ---------------------------------------------------------------------------
# Skills upload
# ---------------------------------------------------------------------------
def _should_skip_skill_path(rel_path: str) -> bool:
    parts = rel_path.replace("\\", "/").split("/")
    if any(p in {"__pycache__", ".git", "node_modules", ".DS_Store"} for p in parts):
        return True
    basename = parts[-1] if parts else ""
    return basename.endswith((".pyc", ".pyo", ".DS_Store"))


def _local_skill_names() -> set:
    if not os.path.isdir(SKILLS_DIR):
        return set()
    return {
        name
        for name in os.listdir(SKILLS_DIR)
        if os.path.isdir(os.path.join(SKILLS_DIR, name))
        and not name.startswith(".")
        and name not in {"__pycache__", "node_modules"}
    }


def _prune_removed_skills_from_s3(s3_bucket_name: str) -> int:
    """Delete S3 skill prefixes that no longer exist under local skills/."""
    local_names = _local_skill_names()
    prefix = f"{SKILLS_S3_PREFIX}/"
    removed = 0
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        remote_names: set = set()
        for page in paginator.paginate(
            Bucket=s3_bucket_name, Prefix=prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes") or []:
                p = (cp.get("Prefix") or "").rstrip("/")
                name = p.split("/")[-1] if p else ""
                if name:
                    remote_names.add(name)
        for name in sorted(remote_names - local_names):
            orphan_prefix = f"{prefix}{name}/"
            logger.info(
                f"  pruning removed skill from S3: "
                f"s3://{s3_bucket_name}/{orphan_prefix}"
            )
            for page in paginator.paginate(
                Bucket=s3_bucket_name, Prefix=orphan_prefix
            ):
                objs = page.get("Contents") or []
                if not objs:
                    continue
                s3_client.delete_objects(
                    Bucket=s3_bucket_name,
                    Delete={
                        "Objects": [{"Key": o["Key"]} for o in objs],
                        "Quiet": True,
                    },
                )
                removed += len(objs)
    except ClientError as e:
        logger.warning(f"  skill prune skipped: {e}")
        return removed
    if removed:
        logger.info(f"✓ Pruned {removed} stale skill object(s) from S3")
    return removed


def upload_skills_to_s3(s3_bucket_name: str) -> int:
    """Upload skills/ to s3://{bucket}/skills/ for InvokeHarness S3 skill attach."""
    logger.info(f"Uploading skills to s3://{s3_bucket_name}/{SKILLS_S3_PREFIX}/")
    if not os.path.isdir(SKILLS_DIR):
        logger.warning(f"Skills directory not found: {SKILLS_DIR}; skipping upload")
        return 0

    uploaded = 0
    failed = 0
    for root, dirs, files in os.walk(SKILLS_DIR):
        dirs[:] = [d for d in dirs if d not in {"__pycache__", ".git", "node_modules"}]
        for filename in files:
            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, SKILLS_DIR)
            if _should_skip_skill_path(rel_path):
                continue
            s3_key = f"{SKILLS_S3_PREFIX}/{rel_path.replace(os.sep, '/')}"
            content_type, _ = mimetypes.guess_type(local_path)
            upload_kwargs = {}
            if content_type:
                upload_kwargs["ExtraArgs"] = {"ContentType": content_type}
            try:
                s3_client.upload_file(
                    local_path,
                    s3_bucket_name,
                    s3_key,
                    **upload_kwargs,
                )
                uploaded += 1
                logger.debug(f"  uploaded: s3://{s3_bucket_name}/{s3_key}")
            except ClientError as e:
                failed += 1
                logger.error(f"  failed: {rel_path}: {e}")

    if failed:
        raise RuntimeError(
            f"Skills upload incomplete: {uploaded} ok, {failed} failed "
            f"(from {SKILLS_DIR})"
        )

    _prune_removed_skills_from_s3(s3_bucket_name)
    logger.info(
        f"✓ Uploaded {uploaded} skill file(s) to "
        f"s3://{s3_bucket_name}/{SKILLS_S3_PREFIX}/"
    )
    return uploaded


def build_default_harness_skills(s3_bucket_name: str) -> List[Dict]:
    """InvokeHarness skills payload for DEFAULT_HARNESS_SKILLS."""
    return [
        {"s3": {"uri": f"s3://{s3_bucket_name}/{SKILLS_S3_PREFIX}/{name}/"}}
        for name in DEFAULT_HARNESS_SKILLS
    ]


# ---------------------------------------------------------------------------
# Jobs table / Lambda harness / API Gateway
# ---------------------------------------------------------------------------
def create_jobs_table() -> Dict[str, str]:
    """Create DynamoDB table for async Harness job status/results (TTL 24h)."""
    logger.info(f"Creating jobs DynamoDB table: {jobs_table_name}")
    try:
        dynamodb_client.create_table(
            TableName=jobs_table_name,
            KeySchema=[{"AttributeName": "jobId", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "jobId", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Project", "Value": project_name},
                {"Key": "Purpose", "Value": "harness-async-jobs"},
            ],
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
        logger.warning(f"Jobs table already exists: {jobs_table_name}")

    waiter = dynamodb_client.get_waiter("table_exists")
    waiter.wait(TableName=jobs_table_name)
    for _ in range(30):
        desc = dynamodb_client.describe_table(TableName=jobs_table_name)
        if desc["Table"]["TableStatus"] == "ACTIVE":
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Jobs table {jobs_table_name} did not become ACTIVE")

    try:
        dynamodb_client.update_time_to_live(
            TableName=jobs_table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
        logger.info("✓ Jobs table TTL enabled on attribute 'ttl'")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in (
            "ValidationException",
            "ResourceInUseException",
        ):
            logger.warning(f"Could not enable TTL on {jobs_table_name}: {e}")

    table_arn = dynamodb_client.describe_table(TableName=jobs_table_name)["Table"][
        "TableArn"
    ]
    logger.info(f"✓ Jobs table ready: {table_arn}")
    return {"jobsTableName": jobs_table_name, "jobsTableArn": table_arn}


def create_lambda_harness(
    harness_arn: str,
    s3_bucket: str,
    jobs_table: str,
    ess_config: Dict[str, str],
) -> str:
    """Deploy Lambda: API jobs/documents API + async worker → InvokeHarness."""
    logger.info(f"Creating Lambda: {lambda_harness_name}")

    jobs_table_arn = (
        f"arn:aws:dynamodb:{region}:{account_id}:table/{jobs_table}"
    )
    lambda_arn = (
        f"arn:aws:lambda:{region}:{account_id}:function:{lambda_harness_name}"
    )
    ess_s3 = (ess_config.get("s3_bucket") or "").strip()

    statements: List[Dict] = [
        {
            "Sid": "InvokeHarness",
            "Effect": "Allow",
            "Action": [
                "bedrock-agentcore:InvokeHarness",
                "bedrock-agentcore:InvokeAgentRuntime",
                "bedrock-agentcore:InvokeAgentRuntimeForUser",
            ],
            "Resource": ["*"],
        },
        {
            "Sid": "JobsTable",
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:DescribeTable",
            ],
            "Resource": [jobs_table_arn],
        },
        {
            "Sid": "SelfAsyncInvoke",
            "Effect": "Allow",
            "Action": ["lambda:InvokeFunction"],
            "Resource": [lambda_arn],
        },
        {
            "Sid": "ProjectS3List",
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": [f"arn:aws:s3:::{s3_bucket}"],
        },
        {
            "Sid": "ProjectS3ReadWrite",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
            ],
            "Resource": [f"arn:aws:s3:::{s3_bucket}/*"],
        },
    ]

    if ess_s3:
        statements.extend(
            [
                {
                    "Sid": "EssS3List",
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                    "Resource": [f"arn:aws:s3:::{ess_s3}"],
                },
                {
                    "Sid": "EssS3GetObject",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{ess_s3}/*"],
                },
            ]
        )

    # Optional Cognito GetUser (JWT may be validated via JWKS without this)
    statements.append(
        {
            "Sid": "CognitoGetUser",
            "Effect": "Allow",
            "Action": ["cognito-idp:GetUser"],
            "Resource": ["*"],
        }
    )

    invoke_policy = {
        "Version": "2012-10-17",
        "Statement": statements,
    }

    role_arn = create_lambda_execution_role(
        lambda_harness_role_name,
        extra_policies=[
            {"name": "invoke-harness-policy", "document": invoke_policy}
        ],
    )

    operator_role_arn = create_lmi_operator_role()
    capacity_provider_arn = create_or_get_capacity_provider(operator_role_arn)

    source_dir = os.path.join(lambda_base_dir, "lambda-harness")
    environment = {
        "HARNESS_ARN": harness_arn,
        "BEDROCK_REGION": region,
        "DEFAULT_MODEL_ID": DEFAULT_MODEL_ID,
        "S3_BUCKET": s3_bucket,
        "SKILLS_S3_PREFIX": SKILLS_S3_PREFIX,
        "DEFAULT_SKILLS": ",".join(DEFAULT_HARNESS_SKILLS),
        "JOBS_TABLE": jobs_table,
        "JOB_TTL_SECONDS": str(24 * 3600),
        "ESS_S3_BUCKET": ess_s3,
        "ESS_SHARING_URL": ess_config.get("sharing_url") or "",
        "COGNITO_USER_POOL_ID": ess_config.get("cognito_user_pool_id") or "",
        "COGNITO_CLIENT_ID": ess_config.get("cognito_client_id") or "",
        "COGNITO_REGION": ess_config.get("cognito_region") or region,
        "LAMBDA_COMPUTE": "managed-instances",
        "LAMBDA_JOB_TIMEOUT_SECONDS": str(LAMBDA_JOB_TIMEOUT_SECONDS),
    }

    return deploy_lambda_function(
        function_name=lambda_harness_name,
        role_arn=role_arn,
        source_dir=source_dir,
        description=(
            "LMI async jobs/documents API + worker proxy to AgentCore Harness"
        ),
        handler="lambda_function.handler",
        runtime=lambda_python_runtime,
        timeout=LAMBDA_JOB_TIMEOUT_SECONDS,
        memory_size=2048,
        pip_packages=["boto3>=1.40.0"],
        environment=environment,
        capacity_provider_arn=capacity_provider_arn,
    )


def create_lambda_function_url(function_name: str) -> str:
    """Create or reuse IAM-auth Function URL for long-running invokes."""
    logger.info(f"Creating Lambda Function URL: {function_name}")
    try:
        existing = lambda_client.get_function_url_config(FunctionName=function_name)
        url = existing["FunctionUrl"]
        auth = existing.get("AuthType")
        if auth != "AWS_IAM":
            lambda_client.update_function_url_config(
                FunctionName=function_name,
                AuthType="AWS_IAM",
            )
        logger.info(f"✓ Function URL ready: {url}")
        return url
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    response = lambda_client.create_function_url_config(
        FunctionName=function_name,
        AuthType="AWS_IAM",
        Cors={
            "AllowOrigins": ["*"],
            "AllowMethods": ["GET", "POST"],
            "AllowHeaders": [
                "content-type",
                "authorization",
                "accept",
                "origin",
                "x-requested-with",
            ],
            "MaxAge": 3600,
        },
    )
    url = response["FunctionUrl"]
    logger.info(f"✓ Function URL created: {url}")
    return url


def _find_http_api_by_name(api_name: str) -> Optional[Dict]:
    paginator = apigatewayv2_client.get_paginator("get_apis")
    for page in paginator.paginate():
        for api in page.get("Items", []):
            if api.get("Name") == api_name:
                return api
    return None


def create_api_gateway(lambda_arn: str) -> Dict[str, str]:
    """Create HTTP API with jobs, invoke, health, and documents routes."""
    logger.info(f"Creating API Gateway HTTP API: {api_harness_name}")

    cors_configuration = {
        "AllowOrigins": ["*"],
        "AllowMethods": ["GET", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"],
        # POST JSON + Authorization also requests Accept in browser preflight.
        "AllowHeaders": [
            "authorization",
            "content-type",
            "accept",
            "origin",
            "x-requested-with",
        ],
        "ExposeHeaders": ["content-type"],
        "MaxAge": 3600,
    }

    existing = _find_http_api_by_name(api_harness_name)
    if existing:
        api_id = existing["ApiId"]
        api_endpoint = existing["ApiEndpoint"]
        logger.warning(f"HTTP API already exists: {api_harness_name} ({api_id})")
        try:
            apigatewayv2_client.update_api(
                ApiId=api_id,
                CorsConfiguration=cors_configuration,
            )
            logger.info("  ✓ Updated HTTP API CORS configuration")
        except ClientError as e:
            logger.warning(f"  Could not update CORS: {e}")
    else:
        created = apigatewayv2_client.create_api(
            Name=api_harness_name,
            ProtocolType="HTTP",
            Description="Document-ai Harness invoke / jobs / documents API",
            CorsConfiguration=cors_configuration,
            Tags={"Project": project_name},
        )
        api_id = created["ApiId"]
        api_endpoint = created["ApiEndpoint"]
        logger.info(f"✓ HTTP API created: {api_id}")

    integrations = apigatewayv2_client.get_integrations(ApiId=api_id).get("Items") or []
    integration_id = None
    for integ in integrations:
        if integ.get("IntegrationUri") == lambda_arn:
            integration_id = integ["IntegrationId"]
            break
    if not integration_id and integrations:
        for integ in integrations:
            if integ.get("IntegrationType") == "AWS_PROXY":
                integration_id = integ["IntegrationId"]
                apigatewayv2_client.update_integration(
                    ApiId=api_id,
                    IntegrationId=integration_id,
                    IntegrationUri=lambda_arn,
                    PayloadFormatVersion="2.0",
                    TimeoutInMillis=30000,
                )
                break
    if not integration_id:
        integ = apigatewayv2_client.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=lambda_arn,
            PayloadFormatVersion="2.0",
            TimeoutInMillis=30000,
        )
        integration_id = integ["IntegrationId"]
        logger.info(f"✓ Integration created: {integration_id}")

    existing_routes = {
        r["RouteKey"]: r["RouteId"]
        for r in (apigatewayv2_client.get_routes(ApiId=api_id).get("Items") or [])
    }
    # Let HTTP API CorsConfiguration answer OPTIONS preflight (do not proxy to Lambda).
    for route_key, route_id in list(existing_routes.items()):
        if route_key.startswith("OPTIONS "):
            try:
                apigatewayv2_client.delete_route(ApiId=api_id, RouteId=route_id)
                logger.info(f"  Removed Lambda OPTIONS route: {route_key}")
                existing_routes.pop(route_key, None)
            except ClientError as e:
                logger.warning(f"  Could not delete {route_key}: {e}")

    for route_key in (
        "POST /jobs",
        "GET /jobs/{jobId}",
        "POST /invoke",
        "GET /health",
        "GET /documents",
        "GET /documents/{kind}",
        "GET /documents/{kind}/{filename}/{view}",
        "GET /download",
    ):
        if route_key in existing_routes:
            continue
        try:
            apigatewayv2_client.create_route(
                ApiId=api_id,
                RouteKey=route_key,
                Target=f"integrations/{integration_id}",
            )
            logger.info(f"  Route created: {route_key}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConflictException":
                raise

    try:
        apigatewayv2_client.create_stage(
            ApiId=api_id,
            StageName="$default",
            AutoDeploy=True,
        )
        logger.info("✓ Stage $default created")
    except ClientError as e:
        if e.response["Error"]["Code"] not in (
            "ConflictException",
            "BadRequestException",
        ):
            raise

    source_arn = (
        f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*/*"
    )
    try:
        lambda_client.add_permission(
            FunctionName=lambda_harness_name,
            StatementId=f"apigw-{api_harness_name}",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=source_arn,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise

    jobs_url = f"{api_endpoint}/jobs"
    invoke_url = f"{api_endpoint}/invoke"
    health_url = f"{api_endpoint}/health"
    documents_url = f"{api_endpoint}/documents"
    logger.info(
        f"✓ API Gateway ready: jobs={jobs_url} documents={documents_url} "
        f"invoke={invoke_url}"
    )
    return {
        "api_id": api_id,
        "api_endpoint": api_endpoint,
        "api_jobs_url": jobs_url,
        "api_invoke_url": invoke_url,
        "api_health_url": health_url,
        "api_documents_url": documents_url,
    }


def deploy_harness_stack(
    s3_bucket: Optional[str] = None,
    ess_config: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Provision Harness + jobs table + Lambda + API Gateway + Function URL."""
    target_bucket = s3_bucket or bucket_name or resolve_bucket_name(account_id, region)
    ess = ess_config or load_ess_work_config()
    ess_s3 = (ess.get("s3_bucket") or "").strip()

    upload_skills_to_s3(target_bucket)
    jobs_info = create_jobs_table()
    execution_role_arn = create_harness_execution_role(target_bucket, ess_s3)
    code_info = create_or_get_code_interpreter(execution_role_arn)
    harness_info = create_or_get_harness(
        execution_role_arn,
        target_bucket,
        code_interpreter_arn=code_info["code_interpreter_arn"],
        ess_s3_bucket=ess_s3,
    )
    lambda_arn = create_lambda_harness(
        harness_info["harness_arn"],
        target_bucket,
        jobs_info["jobsTableName"],
        ess,
    )
    function_url = create_lambda_function_url(lambda_harness_name)
    api_info = create_api_gateway(lambda_arn)
    skill_uris = [
        item["s3"]["uri"] for item in build_default_harness_skills(target_bucket)
    ]
    return {
        "HARNESS_ARN": harness_info["harness_arn"],
        "HARNESS_ID": harness_info["harness_id"],
        "harnessName": harness_info["harness_name"],
        "harnessExecutionRole": execution_role_arn,
        "codeInterpreterId": code_info["code_interpreter_id"],
        "codeInterpreterArn": code_info["code_interpreter_arn"],
        "codeInterpreterName": code_info["code_interpreter_name"],
        "lambdaHarnessName": lambda_harness_name,
        "lambdaHarnessArn": lambda_arn,
        "lambdaCapacityProvider": lambda_capacity_provider_name,
        "lambdaCompute": "managed-instances",
        "lambdaJobTimeoutSeconds": LAMBDA_JOB_TIMEOUT_SECONDS,
        "harnessTimeoutSeconds": HARNESS_TIMEOUT_SECONDS,
        "jobsTableName": jobs_info["jobsTableName"],
        "jobsTableArn": jobs_info["jobsTableArn"],
        "apiGatewayId": api_info["api_id"],
        "apiJobsUrl": api_info["api_jobs_url"],
        "apiGatewayUrl": api_info["api_jobs_url"],
        "apiGatewayInvokeUrl": api_info["api_invoke_url"],
        "apiGatewayHealthUrl": api_info["api_health_url"],
        "apiDocumentsUrl": api_info["api_documents_url"],
        "lambdaFunctionUrl": function_url,
        "skillsS3Prefix": f"s3://{target_bucket}/{SKILLS_S3_PREFIX}/",
        "defaultSkills": ",".join(DEFAULT_HARNESS_SKILLS),
        "defaultSkillUris": skill_uris,
        "cognito_user_pool_id": ess.get("cognito_user_pool_id") or "",
        "cognito_client_id": ess.get("cognito_client_id") or "",
        "cognito_region": ess.get("cognito_region") or region,
        "ess_s3_bucket": ess_s3,
        "ess_sharing_url": ess.get("sharing_url") or "",
    }


# ---------------------------------------------------------------------------
# CloudFront / static web
# ---------------------------------------------------------------------------
def _cloudfront_comment() -> str:
    return f"CloudFront-S3-for-{project_name}"


def _oai_comment() -> str:
    return f"OAI for {project_name} web"


def discover_web_resources() -> Dict[str, str]:
    """Look up existing S3 + CloudFront (by comment) and return config keys."""
    logger.info("Discovering S3 / CloudFront for web hosting")
    cfg = load_config_json()
    found: Dict[str, str] = {}

    target_bucket = (
        cfg.get("bucketName")
        or bucket_name
        or resolve_bucket_name(account_id, region)
    )
    if verify_s3_bucket_access(target_bucket):
        found["bucketName"] = target_bucket
        found["s3Arn"] = f"arn:aws:s3:::{target_bucket}"
        found["s3Path"] = f"s3://{target_bucket}"
        logger.info(f"  ✓ S3 bucket: {target_bucket}")
    else:
        logger.warning(f"  S3 bucket not accessible: {target_bucket}")

    comment = _cloudfront_comment()
    try:
        marker = None
        while True:
            kwargs: Dict = {}
            if marker:
                kwargs["Marker"] = marker
            resp = cloudfront_client.list_distributions(**kwargs)
            dist_list = resp.get("DistributionList") or {}
            for dist in dist_list.get("Items") or []:
                if comment in (dist.get("Comment") or ""):
                    found["cloudfrontId"] = dist["Id"]
                    found["cloudfrontDomain"] = dist["DomainName"]
                    found["cloudfrontUrl"] = f"https://{dist['DomainName']}"
                    logger.info(
                        f"  ✓ CloudFront: {dist['DomainName']} ({dist['Id']})"
                    )
                    break
            if found.get("cloudfrontId"):
                break
            if not dist_list.get("IsTruncated"):
                break
            marker = dist_list.get("NextMarker")
    except ClientError as e:
        logger.warning(f"  Could not list CloudFront distributions: {e}")

    if not found.get("cloudfrontId"):
        logger.info(f"  CloudFront with comment {comment!r} not found yet")
    return found


def _ensure_cloudfront_oai() -> str:
    oai_cmt = _oai_comment()
    try:
        oai_list = cloudfront_client.list_cloud_front_origin_access_identities(
            MaxItems="100"
        )
        items = (oai_list.get("CloudFrontOriginAccessIdentityList") or {}).get(
            "Items"
        ) or []
        for oai in items:
            if oai_cmt in (oai.get("Comment") or ""):
                logger.info(f"  Using existing OAI: {oai['Id']}")
                return oai["Id"]
    except ClientError as e:
        logger.debug(f"list OAI: {e}")

    oai_response = cloudfront_client.create_cloud_front_origin_access_identity(
        CloudFrontOriginAccessIdentityConfig={
            "CallerReference": f"{project_name}-web-oai-{int(time.time())}",
            "Comment": oai_cmt,
        }
    )
    oai_id = oai_response["CloudFrontOriginAccessIdentity"]["Id"]
    logger.info(f"  Created OAI: {oai_id}")
    return oai_id


def _merge_s3_cloudfront_bucket_policy(s3_bucket_name: str, oai_id: str) -> None:
    """Allow CloudFront OAI GetObject without wiping unrelated statements."""
    oai = cloudfront_client.get_cloud_front_origin_access_identity(Id=oai_id)
    canonical_user = oai["CloudFrontOriginAccessIdentity"]["S3CanonicalUserId"]
    cf_statement = {
        "Sid": "AllowCloudFrontWebAccess",
        "Effect": "Allow",
        "Principal": {"CanonicalUser": canonical_user},
        "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:::{s3_bucket_name}/{WEB_S3_PREFIX}/*",
    }
    policy: Dict = {"Version": "2012-10-17", "Statement": []}
    try:
        existing = s3_client.get_bucket_policy(Bucket=s3_bucket_name)
        policy = json.loads(existing["Policy"])
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("NoSuchBucketPolicy", "NoSuchBucket"):
            raise

    statements = policy.get("Statement") or []
    if isinstance(statements, dict):
        statements = [statements]
    statements = [
        s
        for s in statements
        if not (isinstance(s, dict) and s.get("Sid") == "AllowCloudFrontWebAccess")
    ]
    statements.append(cf_statement)
    policy["Statement"] = statements
    s3_client.put_bucket_policy(Bucket=s3_bucket_name, Policy=json.dumps(policy))
    logger.info("  Updated S3 bucket policy for CloudFront web access")


def create_cloudfront_distribution(s3_bucket_name: str) -> Dict[str, str]:
    """Create or reuse CloudFront distribution for html/ static site on S3."""
    logger.info("Creating CloudFront distribution (S3 web)")
    discovered = discover_web_resources()
    if discovered.get("cloudfrontId") and discovered.get("cloudfrontDomain"):
        return {
            "id": discovered["cloudfrontId"],
            "domain": discovered["cloudfrontDomain"],
        }

    oai_id = _ensure_cloudfront_oai()
    time.sleep(5)
    _merge_s3_cloudfront_bucket_policy(s3_bucket_name, oai_id)

    origin_id = f"s3-{project_name}-web"
    distribution_config = {
        "CallerReference": f"{project_name}-web-{int(time.time())}",
        "Comment": _cloudfront_comment(),
        "DefaultRootObject": "index.html",
        "DefaultCacheBehavior": {
            "TargetOriginId": origin_id,
            "ViewerProtocolPolicy": "redirect-to-https",
            "AllowedMethods": {
                "Quantity": 2,
                "Items": ["GET", "HEAD"],
                "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]},
            },
            "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
            "Compress": True,
        },
        "Origins": {
            "Quantity": 1,
            "Items": [
                {
                    "Id": origin_id,
                    "DomainName": f"{s3_bucket_name}.s3.{region}.amazonaws.com",
                    "OriginPath": f"/{WEB_S3_PREFIX}",
                    "S3OriginConfig": {
                        "OriginAccessIdentity": (
                            f"origin-access-identity/cloudfront/{oai_id}"
                        )
                    },
                }
            ],
        },
        "Enabled": True,
        "PriceClass": "PriceClass_200",
    }

    response = cloudfront_client.create_distribution(
        DistributionConfig=distribution_config
    )
    distribution_id = response["Distribution"]["Id"]
    distribution_domain = response["Distribution"]["DomainName"]
    logger.info(f"✓ CloudFront created: {distribution_domain}")
    return {"id": distribution_id, "domain": distribution_domain}


def _derive_api_base(url: str) -> str:
    """Strip trailing known path segments to get API endpoint base."""
    u = (url or "").rstrip("/")
    for suffix in ("/jobs", "/invoke", "/documents", "/health"):
        if u.endswith(suffix):
            return u[: -len(suffix)]
    return u


def write_html_config_js(
    api_gateway_url: str,
    api_gateway_health_url: str = "",
    api_jobs_url: str = "",
    api_documents_url: str = "",
    ess_config: Optional[Dict[str, str]] = None,
) -> str:
    """Write html/config.js from deployment config for the static site."""
    os.makedirs(HTML_DIR, exist_ok=True)
    path = os.path.join(HTML_DIR, "config.js")
    prior = load_config_json()
    ess = ess_config or {}

    jobs = (api_jobs_url or prior.get("apiJobsUrl") or api_gateway_url or "").rstrip(
        "/"
    )
    if jobs.endswith("/invoke"):
        jobs = jobs[: -len("/invoke")] + "/jobs"
    if jobs and not jobs.endswith("/jobs"):
        base = _derive_api_base(jobs)
        jobs = f"{base}/jobs" if base else jobs

    health = (api_gateway_health_url or prior.get("apiGatewayHealthUrl") or "").rstrip(
        "/"
    )
    if not health and jobs:
        health = f"{_derive_api_base(jobs)}/health"

    documents = (
        api_documents_url or prior.get("apiDocumentsUrl") or ""
    ).rstrip("/")
    if not documents and jobs:
        documents = f"{_derive_api_base(jobs)}/documents"

    cognito_pool = (
        ess.get("cognito_user_pool_id")
        or prior.get("cognito_user_pool_id")
        or ""
    )
    cognito_client = (
        ess.get("cognito_client_id") or prior.get("cognito_client_id") or ""
    )
    cognito_region = (
        ess.get("cognito_region")
        or prior.get("cognito_region")
        or region
    )
    ess_sharing = (
        ess.get("sharing_url")
        or prior.get("ess_sharing_url")
        or ""
    ).rstrip("/")

    content = (
        "// Generated by installer.py — do not edit by hand for deploy.\n"
        "window.APP_CONFIG = {\n"
        f"  apiJobsUrl: {json.dumps(jobs)},\n"
        f"  apiGatewayUrl: {json.dumps(jobs)},\n"
        f"  apiGatewayHealthUrl: {json.dumps(health)},\n"
        f"  apiDocumentsUrl: {json.dumps(documents)},\n"
        f"  projectName: {json.dumps(project_name)},\n"
        f"  companyName: {json.dumps(COMPANY_NAME)},\n"
        f"  cognitoUserPoolId: {json.dumps(cognito_pool)},\n"
        f"  cognitoClientId: {json.dumps(cognito_client)},\n"
        f"  cognitoRegion: {json.dumps(cognito_region)},\n"
        f"  essSharingUrl: {json.dumps(ess_sharing)},\n"
        "};\n"
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"✓ Wrote {path}")
    return path


def upload_web_to_s3(
    s3_bucket_name: str,
    api_gateway_url: str,
    api_gateway_health_url: str = "",
    api_jobs_url: str = "",
    api_documents_url: str = "",
    ess_config: Optional[Dict[str, str]] = None,
) -> int:
    """Upload html/ assets to s3://{bucket}/web/."""
    logger.info(f"Uploading html/ → s3://{s3_bucket_name}/{WEB_S3_PREFIX}/")
    if not os.path.isdir(HTML_DIR):
        raise FileNotFoundError(f"Missing HTML directory: {HTML_DIR}")

    write_html_config_js(
        api_gateway_url,
        api_gateway_health_url,
        api_jobs_url,
        api_documents_url=api_documents_url,
        ess_config=ess_config,
    )
    uploaded = 0
    skip_names = {"generate_images.py", "manifest.json"}
    for root, dirs, files in os.walk(HTML_DIR):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
        for filename in sorted(files):
            if filename.startswith(".") or filename in skip_names:
                continue
            if filename.endswith(".py"):
                continue
            filepath = os.path.join(root, filename)
            rel = os.path.relpath(filepath, HTML_DIR).replace(os.sep, "/")
            key = f"{WEB_S3_PREFIX}/{rel}"
            content_type, _ = mimetypes.guess_type(filename)
            if filename.endswith(".js"):
                content_type = "application/javascript"
            elif filename.endswith(".css"):
                content_type = "text/css"
            elif filename.endswith(".html"):
                content_type = "text/html; charset=utf-8"
            elif filename.endswith(".png"):
                content_type = "image/png"
            elif filename.endswith(".jpg") or filename.endswith(".jpeg"):
                content_type = "image/jpeg"
            elif filename.endswith(".webp"):
                content_type = "image/webp"
            extra = {"ContentType": content_type or "application/octet-stream"}
            if filename in ("config.js", "index.html", "app.js") or rel.startswith(
                "images/hero"
            ):
                extra["CacheControl"] = "no-cache, max-age=0"
            else:
                extra["CacheControl"] = "public, max-age=300"
            s3_client.upload_file(
                filepath,
                s3_bucket_name,
                key,
                ExtraArgs=extra,
            )
            uploaded += 1
            logger.info(f"  ↑ {key}")
    logger.info(f"✓ Uploaded {uploaded} files")
    return uploaded


def invalidate_cloudfront(distribution_id: str) -> None:
    if not distribution_id:
        return
    try:
        cloudfront_client.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/*"]},
                "CallerReference": f"{project_name}-web-{int(time.time())}",
            },
        )
        logger.info(f"✓ CloudFront invalidation submitted: {distribution_id}")
    except ClientError as e:
        logger.warning(f"CloudFront invalidation skipped: {e}")


def deploy_web_stack(
    s3_bucket_name: str,
    api_gateway_url: str = "",
    api_gateway_health_url: str = "",
    api_jobs_url: str = "",
    api_documents_url: str = "",
    ess_config: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Ensure CloudFront + upload html/ and return config fields."""
    discovered = discover_web_resources()
    target_bucket = s3_bucket_name or discovered.get("bucketName") or bucket_name
    if not target_bucket:
        raise RuntimeError("S3 bucket name is required for web deploy")

    prior = load_config_json()
    jobs = (
        api_jobs_url
        or prior.get("apiJobsUrl")
        or api_gateway_url
        or prior.get("apiGatewayUrl")
        or ""
    )
    health = (
        api_gateway_health_url
        or prior.get("apiGatewayHealthUrl")
        or ""
    )
    documents = (
        api_documents_url
        or prior.get("apiDocumentsUrl")
        or ""
    )

    cf = create_cloudfront_distribution(target_bucket)
    upload_web_to_s3(
        target_bucket,
        jobs,
        health,
        api_jobs_url=jobs,
        api_documents_url=documents,
        ess_config=ess_config,
    )
    invalidate_cloudfront(cf["id"])

    return {
        "bucketName": target_bucket,
        "s3Arn": f"arn:aws:s3:::{target_bucket}",
        "s3Path": f"s3://{target_bucket}",
        "webS3Prefix": WEB_S3_PREFIX,
        "cloudfrontId": cf["id"],
        "cloudfrontDomain": cf["domain"],
        "cloudfrontUrl": f"https://{cf['domain']}",
        "websiteUrl": f"https://{cf['domain']}",
    }


def update_config_json(resource_info: Dict):
    """Update config.json with deployed resource information."""
    config_path = os.path.join(script_dir, "config.json")
    config_data = {}

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                config_data = json.loads(content)
    except FileNotFoundError:
        logger.info(f"Creating new {config_path}")
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse existing {config_path}: {e}")
    except Exception as e:
        logger.warning(f"Could not read existing {config_path}: {e}")

    config_data.update(resource_info)

    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        logger.info(f"✓ Updated {config_path}")
    except Exception as e:
        logger.warning(f"Could not update {config_path}: {e}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    """Main deployment function."""
    global region, sts_client, account_id, bucket_name
    global s3_client, iam_client, dynamodb_client, lambda_client
    global apigatewayv2_client, agentcore_control_client, cloudfront_client

    parser = argparse.ArgumentParser(
        description="AWS Infrastructure Installer for document-ai"
    )
    parser.add_argument(
        "--region",
        default=region,
        help=f"AWS region (default: {region})",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--harness-only",
        action="store_true",
        help="Provision S3 + Harness + API Gateway + Lambda only (skip web)",
    )
    parser.add_argument(
        "--skip-harness",
        action="store_true",
        help="Skip Harness / API Gateway / Lambda provisioning",
    )
    parser.add_argument(
        "--web-only",
        action="store_true",
        help="Discover/create CloudFront + upload html/ only (uses config.json)",
    )
    parser.add_argument(
        "--skip-web",
        action="store_true",
        help="Skip CloudFront / static web upload",
    )
    args = parser.parse_args()

    if args.harness_only and args.skip_harness:
        parser.error("--harness-only and --skip-harness are mutually exclusive")
    if args.harness_only and args.web_only:
        parser.error("--harness-only and --web-only are mutually exclusive")
    if args.web_only and args.skip_web:
        parser.error("--web-only and --skip-web are mutually exclusive")

    if args.debug:
        logger.setLevel(logging.DEBUG)

    region = args.region
    sts_client = boto3.client("sts", region_name=region)
    account_id = sts_client.get_caller_identity()["Account"]
    bucket_name = load_bucket_name_from_config() or resolve_bucket_name(
        account_id, region
    )
    s3_client = boto3.client("s3", region_name=region)
    iam_client = boto3.client("iam", region_name=region)
    dynamodb_client = boto3.client("dynamodb", region_name=region)
    lambda_client = boto3.client("lambda", region_name=region)
    apigatewayv2_client = boto3.client("apigatewayv2", region_name=region)
    cloudfront_client = boto3.client("cloudfront", region_name="us-east-1")
    agentcore_control_client = boto3.client(
        "bedrock-agentcore-control",
        region_name=region,
    )

    ess_config = load_ess_work_config()

    logger.info("=" * 60)
    logger.info("Starting document-ai Infrastructure Deployment")
    logger.info("=" * 60)
    logger.info(f"Project: {project_name}")
    logger.info(f"Region: {region}")
    logger.info(f"Account ID: {account_id}")
    logger.info(f"Bucket Name: {bucket_name}")
    logger.info(f"Harness API name: {harness_name_for_api(project_name)}")
    logger.info(f"Harness only: {args.harness_only}")
    logger.info(f"Skip harness: {args.skip_harness}")
    logger.info(f"Web only: {args.web_only}")
    logger.info(f"Skip web: {args.skip_web}")
    logger.info("=" * 60)

    start_time = time.time()
    config_payload: Dict = {
        "projectName": project_name,
        "accountId": account_id,
        "region": region,
        "cognito_user_pool_id": ess_config.get("cognito_user_pool_id") or "",
        "cognito_client_id": ess_config.get("cognito_client_id") or "",
        "cognito_region": ess_config.get("cognito_region") or region,
        "ess_s3_bucket": ess_config.get("s3_bucket") or "",
        "ess_sharing_url": ess_config.get("sharing_url") or "",
    }

    try:
        if args.web_only:
            existing = load_config_json()
            config_payload.update(existing)
            discovered = discover_web_resources()
            config_payload.update(discovered)
            target_bucket = (
                discovered.get("bucketName")
                or existing.get("bucketName")
                or bucket_name
            )
            if not verify_s3_bucket_access(target_bucket):
                s3_info = create_s3_bucket()
                target_bucket = s3_info["bucket_name"]
            api_url = existing.get("apiGatewayUrl") or existing.get("apiJobsUrl") or ""
            web_config = deploy_web_stack(
                target_bucket,
                api_url,
                api_gateway_health_url=existing.get("apiGatewayHealthUrl") or "",
                api_jobs_url=existing.get("apiJobsUrl") or api_url,
                api_documents_url=existing.get("apiDocumentsUrl") or "",
                ess_config=ess_config,
            )
            config_payload.update(web_config)
            update_config_json(config_payload)
            elapsed_time = time.time() - start_time
            logger.info("")
            logger.info("=" * 60)
            logger.info("Web deployment completed")
            logger.info(f"  Website: {web_config.get('websiteUrl')}")
            logger.info(f"  S3: s3://{target_bucket}/{WEB_S3_PREFIX}/")
            logger.info(f"  API: {api_url or '(missing apiGatewayUrl in config.json)'}")
            logger.info(f"Total time: {elapsed_time / 60:.2f} minutes")
            logger.info("=" * 60)
            return

        # Always ensure project S3 bucket exists
        s3_info = create_s3_bucket()
        config_payload.update(
            {
                "bucketName": s3_info["bucket_name"],
                "s3Arn": s3_info["bucket_arn"],
                "s3Path": s3_info["s3_path"],
            }
        )

        harness_config: Dict[str, str] = {}
        if not args.skip_harness:
            harness_config = deploy_harness_stack(
                s3_info["bucket_name"], ess_config=ess_config
            )
            config_payload.update(harness_config)
        else:
            logger.warning("Skipping Harness / API Gateway (--skip-harness)")

        web_config: Dict[str, str] = {}
        skip_web = args.skip_web or args.harness_only
        if not skip_web:
            prior_cfg = load_config_json()
            jobs_url = (
                config_payload.get("apiJobsUrl")
                or prior_cfg.get("apiJobsUrl")
                or config_payload.get("apiGatewayUrl")
                or prior_cfg.get("apiGatewayUrl")
                or ""
            )
            health_url = (
                config_payload.get("apiGatewayHealthUrl")
                or prior_cfg.get("apiGatewayHealthUrl")
                or ""
            )
            documents_url = (
                config_payload.get("apiDocumentsUrl")
                or prior_cfg.get("apiDocumentsUrl")
                or ""
            )
            web_config = deploy_web_stack(
                s3_info["bucket_name"],
                api_gateway_url=jobs_url,
                api_gateway_health_url=health_url,
                api_jobs_url=jobs_url,
                api_documents_url=documents_url,
                ess_config=ess_config,
            )
            config_payload.update(web_config)
        else:
            logger.warning("Skipping CloudFront / web upload (--skip-web/--harness-only)")

        elapsed_time = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info("Infrastructure Deployment Completed Successfully!")
        logger.info("=" * 60)
        logger.info(f"  S3 Bucket: {config_payload.get('bucketName')}")
        if harness_config:
            logger.info(f"  Harness ARN: {harness_config.get('HARNESS_ARN')}")
            logger.info(f"  Harness ID: {harness_config.get('HARNESS_ID')}")
            logger.info(
                f"  Lambda Harness ARN: {harness_config.get('lambdaHarnessArn')}"
            )
            logger.info(f"  API Jobs URL: {harness_config.get('apiJobsUrl')}")
            logger.info(
                f"  API Documents URL: {harness_config.get('apiDocumentsUrl')}"
            )
            logger.info(
                f"  API Invoke URL (sync/short): "
                f"{harness_config.get('apiGatewayInvokeUrl')}"
            )
            logger.info(f"  Jobs table: {harness_config.get('jobsTableName')}")
            logger.info(
                f"  Lambda Function URL: {harness_config.get('lambdaFunctionUrl')}"
            )
        if web_config:
            logger.info(f"  Website URL: {web_config.get('websiteUrl')}")
            logger.info(
                f"  Web S3: s3://{web_config.get('bucketName')}/{WEB_S3_PREFIX}/"
            )
        logger.info("")
        logger.info(f"Total deployment time: {elapsed_time / 60:.2f} minutes")
        logger.info("=" * 60)

        update_config_json(config_payload)

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error("")
        logger.error("=" * 60)
        logger.error("Deployment Failed!")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error(f"Deployment time before failure: {elapsed_time / 60:.2f} minutes")
        logger.error("=" * 60)
        import traceback

        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
