#!/usr/bin/env python3
"""
AWS Infrastructure Uninstaller for document-ai.

Deletes harness + web stack resources created by installer.py
(API Gateway, Function URL, Lambda, jobs table, harness, code interpreter,
IAM roles; optionally CloudFront and S3).
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# Configuration
project_name = "document-ai"
region = "us-west-2"

bucket_name_prefix = "storage-for-document-ai"
bucket_name = ""
jobs_table_name = "dynamodb-document-ai-jobs"
lambda_harness_name = "lambda-harness-document-ai"
api_harness_name = "api-harness-document-ai"
lambda_harness_role_name = "lambda-harness-document-ai-role"
code_interpreter_name = "document_ai_code"

script_dir = os.path.dirname(os.path.abspath(__file__))

sts_client = boto3.client("sts", region_name=region)
account_id = sts_client.get_caller_identity()["Account"]

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


def resolve_bucket_name(acct_id: str, aws_region: str) -> str:
    """Build the default S3 bucket name for this account and region."""
    return f"{bucket_name_prefix}-{acct_id}-{aws_region}".lower()


def _cloudfront_comment() -> str:
    return f"CloudFront-S3-for-{project_name}"


def _oai_comment() -> str:
    return f"OAI for {project_name} web"


def load_config() -> Dict:
    """Load deployment configuration from config.json."""
    config_path = os.path.join(script_dir, "config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.debug(f"Could not load config.json: {e}")
    return {}


def update_config_json(deletion_summary: Dict):
    """Update config.json after infrastructure deletion."""
    config_path = os.path.join(script_dir, "config.json")
    config_data = load_config()

    resource_keys = [
        "bucketName",
        "s3Arn",
        "s3Path",
        "HARNESS_ARN",
        "HARNESS_ID",
        "harnessName",
        "harnessExecutionRole",
        "codeInterpreterId",
        "codeInterpreterArn",
        "codeInterpreterName",
        "lambdaHarnessName",
        "lambdaHarnessArn",
        "apiGatewayId",
        "apiGatewayUrl",
        "apiGatewayHealthUrl",
        "lambdaFunctionUrl",
        "jobsTableName",
        "jobsTableArn",
        "apiJobsUrl",
        "apiGatewayInvokeUrl",
        "apiDocumentsUrl",
        "skillsS3Prefix",
        "defaultSkills",
        "defaultSkillUris",
        "webS3Prefix",
        "cloudfrontId",
        "cloudfrontDomain",
        "cloudfrontUrl",
        "websiteUrl",
        "cognito_user_pool_id",
        "cognito_client_id",
        "cognito_region",
        "ess_s3_bucket",
        "ess_sharing_url",
    ]
    for key in resource_keys:
        config_data.pop(key, None)

    config_data.update(
        {
            "projectName": project_name,
            "accountId": account_id,
            "region": region,
            "deploymentStatus": "uninstalled",
            "uninstalledAt": datetime.now(timezone.utc).isoformat(),
            "deletionSummary": deletion_summary,
        }
    )

    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        logger.info(f"✓ Updated {config_path}")
    except Exception as e:
        logger.warning(f"Could not update {config_path}: {e}")


def _find_http_api_by_name(api_name: str) -> Optional[Dict]:
    paginator = apigatewayv2_client.get_paginator("get_apis")
    for page in paginator.paginate():
        for api in page.get("Items", []):
            if api.get("Name") == api_name:
                return api
    return None


def delete_api_gateway(config: Dict) -> bool:
    """Delete HTTP API for Harness invoke / jobs / documents."""
    logger.info(f"Deleting API Gateway HTTP API: {api_harness_name}")
    api_id = config.get("apiGatewayId")
    if not api_id:
        found = _find_http_api_by_name(api_harness_name)
        api_id = found["ApiId"] if found else None
    if not api_id:
        logger.warning(f"HTTP API not found: {api_harness_name}")
        return False
    try:
        apigatewayv2_client.delete_api(ApiId=api_id)
        logger.info(f"✓ HTTP API deleted: {api_id}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NotFoundException":
            logger.warning(f"HTTP API not found: {api_id}")
            return False
        raise


def delete_lambda_function_url(function_name: str) -> bool:
    """Delete Lambda Function URL if present."""
    try:
        lambda_client.delete_function_url_config(FunctionName=function_name)
        logger.info(f"✓ Function URL deleted: {function_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(f"Function URL not found: {function_name}")
            return False
        raise


def delete_lambda_function(function_name: str) -> bool:
    """Delete a single Lambda function."""
    try:
        lambda_client.delete_function(FunctionName=function_name)
        logger.info(f"✓ Deleted Lambda function: {function_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(f"Lambda function not found: {function_name}")
            return False
        raise


def delete_jobs_table(config: Dict) -> bool:
    """Delete async Harness jobs DynamoDB table."""
    name = config.get("jobsTableName") or jobs_table_name
    logger.info(f"Deleting jobs DynamoDB table: {name}")
    try:
        dynamodb_client.delete_table(TableName=name)
        logger.info(f"✓ Jobs table deleted: {name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(f"Jobs table not found: {name}")
            return False
        raise


def delete_code_interpreter(config: Dict) -> bool:
    """Delete custom AgentCore Code Interpreter used by the harness."""
    logger.info("Deleting AgentCore Code Interpreter")
    code_id = (config.get("codeInterpreterId") or "").strip()
    code_name = (config.get("codeInterpreterName") or code_interpreter_name).strip()

    if not code_id:
        try:
            token = None
            while True:
                kw: Dict = {"maxResults": 50}
                if token:
                    kw["nextToken"] = token
                resp = agentcore_control_client.list_code_interpreters(**kw)
                for item in resp.get("codeInterpreterSummaries") or []:
                    if item.get("name") == code_name:
                        code_id = item.get("codeInterpreterId") or ""
                        break
                if code_id or not resp.get("nextToken"):
                    break
                token = resp.get("nextToken")
        except ClientError as e:
            logger.warning(f"Could not list code interpreters: {e}")

    if not code_id:
        logger.warning("Code Interpreter ID not found; skipping")
        return False

    try:
        agentcore_control_client.delete_code_interpreter(
            codeInterpreterId=code_id,
            clientToken=str(uuid.uuid4()),
        )
        logger.info(f"✓ Code Interpreter delete requested: {code_id}")
        deadline = time.time() + 300
        while time.time() < deadline:
            try:
                agentcore_control_client.get_code_interpreter(
                    codeInterpreterId=code_id
                )
                time.sleep(5)
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                    logger.info(f"✓ Code Interpreter deleted: {code_id}")
                    return True
                raise
        logger.warning(f"Code Interpreter delete still in progress: {code_id}")
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            logger.warning(f"Code Interpreter not found: {code_id}")
            return False
        raise


def delete_harness(config: Dict) -> bool:
    """Delete AgentCore Harness."""
    logger.info("Deleting AgentCore Harness")
    harness_id = config.get("HARNESS_ID")
    if not harness_id:
        harness_name = config.get("harnessName") or project_name.replace("-", "_")
        try:
            token = None
            while True:
                kw: Dict = {"maxResults": 50}
                if token:
                    kw["nextToken"] = token
                resp = agentcore_control_client.list_harnesses(**kw)
                for h in resp.get("harnesses") or []:
                    if h.get("harnessName") == harness_name:
                        harness_id = h.get("harnessId")
                        break
                if harness_id:
                    break
                token = resp.get("nextToken")
                if not token:
                    break
        except ClientError as e:
            logger.warning(f"Could not list harnesses: {e}")

    if not harness_id:
        logger.warning("Harness ID not found; skipping harness deletion")
        return False

    try:
        agentcore_control_client.delete_harness(
            harnessId=harness_id,
            clientToken=str(uuid.uuid4()),
        )
        logger.info(f"✓ Harness delete requested: {harness_id}")
        deadline = time.time() + 600
        while time.time() < deadline:
            try:
                agentcore_control_client.get_harness(harnessId=harness_id)
                time.sleep(5)
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                    logger.info(f"✓ Harness deleted: {harness_id}")
                    return True
                raise
        logger.warning(f"Harness delete still in progress: {harness_id}")
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            logger.warning(f"Harness not found: {harness_id}")
            return False
        raise


def delete_iam_role(role_name: str) -> bool:
    """Delete IAM role and attached policies."""
    try:
        attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
        for policy in attached_policies.get("AttachedPolicies", []):
            iam_client.detach_role_policy(
                RoleName=role_name,
                PolicyArn=policy["PolicyArn"],
            )

        inline_policies = iam_client.list_role_policies(RoleName=role_name)
        for policy_name in inline_policies.get("PolicyNames", []):
            iam_client.delete_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
            )

        iam_client.delete_role(RoleName=role_name)
        logger.info(f"  ✓ Deleted IAM role: {role_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.warning(f"  IAM role not found: {role_name}")
            return False
        logger.warning(f"  Could not delete IAM role {role_name}: {e}")
        return False


def delete_iam_roles() -> int:
    """Delete harness + lambda IAM roles created by installer.py."""
    logger.info("Deleting IAM roles")
    role_names = [
        lambda_harness_role_name,
        f"role-harness-for-{project_name}-{region}",
    ]
    deleted_count = 0
    for role_name in role_names:
        if delete_iam_role(role_name):
            deleted_count += 1
    logger.info(f"✓ IAM roles deleted: {deleted_count}")
    return deleted_count


def _matches_cloudfront(dist: dict) -> bool:
    return _cloudfront_comment() in (dist.get("Comment") or "")


def disable_cloudfront_distributions(config: Dict) -> bool:
    """Disable project CloudFront distribution(s)."""
    logger.info("Disabling CloudFront distributions")
    dist_id = (config.get("cloudfrontId") or "").strip()
    disabled_any = False
    try:
        distributions = cloudfront_client.list_distributions()
        for dist in distributions.get("DistributionList", {}).get("Items", []) or []:
            if dist_id and dist.get("Id") != dist_id and not _matches_cloudfront(dist):
                continue
            if not dist_id and not _matches_cloudfront(dist):
                continue
            if not dist.get("Enabled", True):
                logger.info(f"  Already disabled: {dist['Id']}")
                disabled_any = True
                continue
            current_id = dist["Id"]
            logger.info(f"  Disabling: {current_id}")
            cfg_resp = cloudfront_client.get_distribution_config(Id=current_id)
            cfg = cfg_resp["DistributionConfig"]
            cfg["Enabled"] = False
            cloudfront_client.update_distribution(
                Id=current_id,
                DistributionConfig=cfg,
                IfMatch=cfg_resp["ETag"],
            )
            disabled_any = True
        if disabled_any:
            logger.info("✓ CloudFront disable requested")
        else:
            logger.warning("No matching CloudFront distribution found")
        return disabled_any
    except Exception as e:
        logger.error(f"Error disabling CloudFront: {e}")
        return False


def wait_for_cloudfront_disabled(
    config: Dict, max_wait: int = 600, poll_interval: int = 20
) -> bool:
    """Wait until matching CloudFront distributions are disabled."""
    logger.info("  Waiting for CloudFront to become disabled...")
    dist_id = (config.get("cloudfrontId") or "").strip()
    waited = 0
    while waited < max_wait:
        still = []
        distributions = cloudfront_client.list_distributions()
        for dist in distributions.get("DistributionList", {}).get("Items", []) or []:
            if dist_id and dist.get("Id") != dist_id and not _matches_cloudfront(dist):
                continue
            if not dist_id and not _matches_cloudfront(dist):
                continue
            if dist.get("Enabled", True):
                still.append(dist["Id"])
        if not still:
            logger.info("  ✓ Matching CloudFront distributions disabled")
            return True
        logger.info(f"  Still enabled: {still} ({waited}s/{max_wait}s)")
        time.sleep(poll_interval)
        waited += poll_interval
    logger.warning("  Timed out waiting for CloudFront disable")
    return False


def delete_cloudfront_distributions(config: Dict) -> bool:
    """Delete disabled project CloudFront distribution(s)."""
    logger.info("Deleting CloudFront distributions")
    dist_id = (config.get("cloudfrontId") or "").strip()
    deleted = False
    try:
        distributions = cloudfront_client.list_distributions()
        for dist in distributions.get("DistributionList", {}).get("Items", []) or []:
            if dist_id and dist.get("Id") != dist_id and not _matches_cloudfront(dist):
                continue
            if not dist_id and not _matches_cloudfront(dist):
                continue
            if dist.get("Enabled", True):
                logger.info(f"  Skipping enabled distribution: {dist['Id']}")
                continue
            current_id = dist["Id"]
            try:
                cfg_resp = cloudfront_client.get_distribution_config(Id=current_id)
                cloudfront_client.delete_distribution(
                    Id=current_id, IfMatch=cfg_resp["ETag"]
                )
                logger.info(f"  ✓ Deleted distribution: {current_id}")
                deleted = True
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in {"DistributionNotDisabled", "NoSuchDistribution"}:
                    logger.info(f"  Skip {current_id}: {code}")
                else:
                    logger.warning(f"  Could not delete {current_id}: {e}")
        if deleted:
            logger.info("✓ CloudFront distributions processed")
        return deleted
    except Exception as e:
        logger.error(f"Error deleting CloudFront: {e}")
        return False


def delete_cloudfront_oai() -> bool:
    """Delete project CloudFront Origin Access Identity."""
    logger.info("Deleting CloudFront Origin Access Identities")
    deleted = False
    try:
        oai_list = cloudfront_client.list_cloud_front_origin_access_identities()
        for oai in oai_list.get("CloudFrontOriginAccessIdentityList", {}).get(
            "Items", []
        ) or []:
            if _oai_comment() not in (oai.get("Comment") or ""):
                continue
            oai_id = oai["Id"]
            try:
                cfg = cloudfront_client.get_cloud_front_origin_access_identity_config(
                    Id=oai_id
                )
                cloudfront_client.delete_cloud_front_origin_access_identity(
                    Id=oai_id, IfMatch=cfg["ETag"]
                )
                logger.info(f"  ✓ Deleted OAI: {oai_id}")
                deleted = True
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchCloudFrontOriginAccessIdentity":
                    logger.warning(f"  Could not delete OAI {oai_id}: {e}")
    except Exception as e:
        logger.warning(f"  Error deleting OAI: {e}")
    return deleted


def empty_s3_bucket(target_bucket: str):
    """Delete all objects and versions from an S3 bucket."""
    try:
        paginator = s3_client.get_paginator("list_object_versions")
        delete_keys: List[Dict[str, str]] = []

        for page in paginator.paginate(Bucket=target_bucket):
            for version in page.get("Versions", []):
                delete_keys.append(
                    {
                        "Key": version["Key"],
                        "VersionId": version["VersionId"],
                    }
                )
            for marker in page.get("DeleteMarkers", []):
                delete_keys.append(
                    {
                        "Key": marker["Key"],
                        "VersionId": marker["VersionId"],
                    }
                )

        if delete_keys:
            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i : i + 1000]
                s3_client.delete_objects(
                    Bucket=target_bucket,
                    Delete={"Objects": batch},
                )
            logger.info(
                f"  ✓ Deleted {len(delete_keys)} objects/versions from {target_bucket}"
            )

        paginator = s3_client.get_paginator("list_objects_v2")
        object_keys = []
        for page in paginator.paginate(Bucket=target_bucket):
            for obj in page.get("Contents", []):
                object_keys.append({"Key": obj["Key"]})

        if object_keys:
            for i in range(0, len(object_keys), 1000):
                batch = object_keys[i : i + 1000]
                s3_client.delete_objects(
                    Bucket=target_bucket,
                    Delete={"Objects": batch},
                )
            logger.info(
                f"  ✓ Deleted {len(object_keys)} objects from {target_bucket}"
            )
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchBucket":
            raise


def delete_s3_bucket() -> bool:
    """Delete S3 bucket and all objects."""
    logger.info(f"Deleting S3 bucket: {bucket_name}")

    if not bucket_name:
        logger.warning("S3 bucket name is empty, skipping bucket deletion")
        return False

    try:
        empty_s3_bucket(bucket_name)
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"✓ S3 bucket deleted: {bucket_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.warning(f"S3 bucket not found: {bucket_name}")
            return False
        logger.error(f"Failed to delete S3 bucket: {e}")
        raise


def main():
    """Main cleanup function."""
    global region, sts_client, account_id, bucket_name
    global s3_client, iam_client, dynamodb_client, lambda_client
    global apigatewayv2_client, agentcore_control_client, cloudfront_client

    parser = argparse.ArgumentParser(
        description="AWS Infrastructure Uninstaller for document-ai"
    )
    parser.add_argument(
        "--region",
        default=region,
        help=f"AWS region (default: {region})",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt and proceed with deletion",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--harness-only",
        action="store_true",
        help="Delete only Harness / API Gateway / lambda-harness resources",
    )
    parser.add_argument(
        "--keep-s3",
        action="store_true",
        help="Retain the project S3 bucket (default: delete when not --harness-only)",
    )
    parser.add_argument(
        "--keep-cloudfront",
        action="store_true",
        help="Retain CloudFront / OAI (default: delete when not --harness-only)",
    )
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    region = args.region
    sts_client = boto3.client("sts", region_name=region)
    account_id = sts_client.get_caller_identity()["Account"]
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

    config = load_config()
    bucket_name = config.get("bucketName") or resolve_bucket_name(account_id, region)

    delete_s3 = (not args.harness_only) and (not args.keep_s3)
    delete_cf = (not args.harness_only) and (not args.keep_cloudfront)

    logger.info("=" * 60)
    logger.info("Starting document-ai Infrastructure Cleanup")
    logger.info("=" * 60)
    logger.info(f"Project: {project_name}")
    logger.info(f"Region: {region}")
    logger.info(f"Account ID: {account_id}")
    logger.info(f"Bucket Name: {bucket_name}")
    logger.info(f"Harness only: {args.harness_only}")
    logger.info(f"Keep S3: {args.keep_s3 or args.harness_only}")
    logger.info(f"Keep CloudFront: {args.keep_cloudfront or args.harness_only}")
    logger.info("=" * 60)

    if not args.yes:
        print("\n" + "=" * 60)
        print("WARNING: This will delete resources created by installer.py")
        print("=" * 60)
        response = input("\nAre you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            print("Uninstallation cancelled.")
            sys.exit(0)

    start_time = time.time()
    deletion_summary: Dict = {}

    try:
        if delete_cf:
            deletion_summary["cloudfrontDisable"] = disable_cloudfront_distributions(
                config
            )

        deletion_summary["apiGateway"] = delete_api_gateway(config)
        deletion_summary["lambdaFunctionUrl"] = delete_lambda_function_url(
            lambda_harness_name
        )
        deletion_summary["lambdaHarness"] = delete_lambda_function(lambda_harness_name)
        deletion_summary["jobsTable"] = delete_jobs_table(config)
        deletion_summary["harness"] = delete_harness(config)
        deletion_summary["codeInterpreter"] = delete_code_interpreter(config)
        deletion_summary["iamRoles"] = delete_iam_roles()

        if delete_cf:
            wait_for_cloudfront_disabled(config)
            deletion_summary["cloudfront"] = delete_cloudfront_distributions(config)
            deletion_summary["cloudfrontOai"] = delete_cloudfront_oai()
        else:
            logger.info("CloudFront retained (--keep-cloudfront / --harness-only)")

        if delete_s3:
            deletion_summary["s3Bucket"] = delete_s3_bucket()
        else:
            logger.info("S3 bucket retained (--keep-s3 / --harness-only)")

        elapsed_time = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info("Infrastructure Cleanup Completed Successfully!")
        logger.info("=" * 60)
        logger.info(f"Total cleanup time: {elapsed_time / 60:.2f} minutes")
        logger.info("=" * 60)

        update_config_json(deletion_summary)

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error("")
        logger.error("=" * 60)
        logger.error("Cleanup Failed!")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error(f"Cleanup time before failure: {elapsed_time / 60:.2f} minutes")
        logger.error("=" * 60)
        import traceback

        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
