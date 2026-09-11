# document-ai

ESS 문서 분석용 Agent를 Web으로 활용하는 방법에 대해 설명합니다. 여기서는 AgentCore Harness를 이용해 agent를 구현하고, 웹페이지도 빠르게 이해하기 쉽도록 정적 웹 스택을 사용합니다. 

## Architecture

```
Browser (CloudFront → S3 web/)
    │  Cognito JWT (ess-work)
    ▼
API Gateway HTTP API
  POST /jobs, GET /jobs/{jobId}
  POST /invoke, GET /health
  GET /documents, GET /documents/{kind}
    ▼
Lambda (lambda-harness-document-ai)
  • DynamoDB jobs table (TTL)
  • ESS S3 list / presign
  • InvokeHarness
    ▼
AgentCore Harness (document_ai, PUBLIC, memory off)
  + Code Interpreter (document_ai_code)
  + Skills: regulation-evaluator, testcase-generator, pptx, docx, xlsx
```

**ESS-work 연동:** 형제 경로 `../ess-work/application/config.json`에서 Cognito·S3·sharing URL을 읽어 Lambda 환경 변수와 `html/config.js`에 주입합니다.

| Resource | Name |
|---|---|
| S3 | `storage-for-document-ai-{account}-{region}` |
| Jobs table | `dynamodb-document-ai-jobs` |
| Lambda | `lambda-harness-document-ai` |
| API | `api-harness-document-ai` |
| Harness API name | `document_ai` |
| Code interpreter | `document_ai_code` |
| Region | `us-west-2` |

## Prerequisites

- AWS credentials with rights to create S3, DynamoDB, Lambda, API Gateway, IAM, CloudFront, Bedrock AgentCore
- Sibling project config: `../ess-work/application/config.json`
- Python 3.10+ with `boto3`
- `lambda-harness/lambda_function.py` and `html/` assets present before deploy

## Install

```bash
# Full stack (S3 + harness + web)
python3 installer.py

# Harness only (skip CloudFront/web)
python3 installer.py --harness-only

# Web only (reuse config.json API URLs)
python3 installer.py --web-only

# Other flags
python3 installer.py --skip-harness   # S3 + web
python3 installer.py --skip-web       # S3 + harness
python3 installer.py --region us-west-2 --debug
```

배포 결과는 `config.json`과 `html/config.js`에 기록됩니다.

## Uninstall

```bash
# Full cleanup (asks for confirmation)
python3 uninstaller.py

# Non-interactive
python3 uninstaller.py --yes

# Harness stack only (keep S3 + CloudFront)
python3 uninstaller.py --yes --harness-only

# Keep storage / CDN
python3 uninstaller.py --yes --keep-s3 --keep-cloudfront

python3 uninstaller.py --region us-west-2 --debug
```

삭제 순서: API Gateway → Function URL → Lambda → jobs table → harness → code interpreter → IAM roles → (optional) CloudFront → (optional) S3.

## Notes

- Cognito User Pool은 ess-work가 소유합니다. document-ai uninstaller는 Cognito를 삭제하지 않습니다.
- Lambda는 Cognito Access Token을 `GetUser`로 검증합니다 (PyJWT/cryptography 미사용).
- Lambda에 `boto3>=1.40.0`을 manylinux 휠로 번들합니다 (InvokeHarness skills.s3).
- 웹은 비동기 `POST /jobs` + `GET /jobs/{jobId}` 폴링을 사용합니다 (API Gateway ~29s 제한 회피).
- HTTP API CORS가 OPTIONS preflight를 처리합니다 (Lambda OPTIONS 라우트 없음).
