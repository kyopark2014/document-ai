# Document AI

문서 분석용 Agent를 Web으로 활용하는 방법에 대해 설명합니다. 여기서는 AgentCore Harness를 이용해 agent를 구현하고, 웹페이지도 빠르게 이해하기 쉽도록 정적 웹 스택을 사용합니다. 

## Architecture

```
Browser (CloudFront → S3 web/)
    │  Cognito JWT (ess-work)
    ▼
API Gateway HTTP API  (~30s integration timeout)
  POST /jobs → 202 + jobId   (짧게 반환)
  GET  /jobs/{jobId}         (폴링)
  GET  /documents, /download …
    ▼
Lambda Managed Instances  (lambda-harness-document-ai)
  Capacity Provider + VPC (default VPC public subnets)
  • API 경로: 동기 호출 (문서 조회·job 생성, ≤15분 한도이나 실제는 수 초)
  • Worker: InvocationType=Event 비동기 (최대 30분, LMI async)
      → DynamoDB status RUNNING → InvokeHarness 스트림 대기
      → SUCCEEDED / FAILED
    ▼
AgentCore Harness (document_ai, PUBLIC, memory off)
  timeoutSeconds=1800
  + Code Interpreter (document_ai_code)
  + Skills: doc-sharing, regulation-evaluator, testcase-generator, pptx, docx, xlsx
```

**ESS-work 연동:** 형제 경로 `../ess-work/application/config.json`에서 Cognito·S3·sharing URL을 읽어 Lambda 환경 변수와 `html/config.js`에 주입합니다.

| Resource | Name |
|---|---|
| S3 | `storage-for-document-ai-{account}-{region}` |
| Jobs table | `dynamodb-document-ai-jobs` |
| Lambda (LMI) | `lambda-harness-document-ai` |
| Capacity provider | `cp-document-ai` |
| LMI operator role | `lambda-lmi-operator-document-ai` |
| LMI security group | `document-ai-lmi-sg` |
| API | `api-harness-document-ai` |
| Harness API name | `document_ai` |
| Code interpreter | `document_ai_code` |
| Region | `us-west-2` |

## Skills

기본으로 Harness에 붙는 S3 skill은 `skills/` 아래를 `s3://{bucket}/skills/`에 올린 뒤 InvokeHarness `skills.s3`로 연결합니다.

| Skill | 역할 |
|---|---|
| **doc-sharing** | 산출물을 document-ai S3로 올리고 CloudFront 다운로드 URL 반환 |
| regulation-evaluator | 규격 TC + 대상 md 적합성 평가 → Excel 리포트 |
| testcase-generator | 문서 기반 테스트케이스 생성 |
| pptx / docx / xlsx | Office 문서 생성·편집 |

### doc-sharing

산출물(xlsx/pptx/docx/pdf 등)은 Code Interpreter의 로컬 경로(`/mnt/workspace/{actor_id}/artifacts/…`)에만 존재하므로, 사용자가 받을 수 있는 **CloudFront URL**이 필요합니다. MCP로 파일을 한 번 더 넘기지 않고, **같은 code 인터프리터에서 로컬 파일을 S3에 PutObject** 합니다.

```
ARTIFACTS_DIR 로컬 파일
    → s3://{S3_BUCKET}/artifacts/{actor_id}/…
    → {SHARING_URL}/artifacts/{actor_id}/…
```

- **버킷 / CDN:** document-ai 전용 S3·CloudFront (`config.json`의 `bucketName`, `cloudfrontUrl`). ESS sharing URL을 쓰지 않습니다.
- **환경 변수:** Harness에 `S3_BUCKET`, `SHARING_URL`을 주입합니다. skill `config.json`(배포 시 생성)은 fallback입니다.
- **스크립트:** `skills/doc-sharing/scripts/share_artifact.py`

```bash
aws s3 sync s3://$S3_BUCKET/skills/doc-sharing/ /tmp/doc-sharing/
python3 /tmp/doc-sharing/scripts/share_artifact.py \
  --filepath "$ARTIFACTS_DIR/reports/report.xlsx" \
  --actor-id "<actor_id>"
```

성공 시 stdout JSON의 `url`을 최종 답변에 넣습니다. system prompt는 산출물이 있으면 **반드시 doc-sharing으로 URL을 만든 뒤** 응답하도록 강제합니다 (로컬 경로만 안내 금지).

## 비동기 처리

적합성 평가·테스트케이스 생성처럼 Harness 한 번이 수분~수십 분 걸릴 수 있습니다. API Gateway·일반 Lambda timeout으로는 그 시간을 붙잡을 수 없어, document-ai는 **짧은 HTTP + 긴 worker** 로 나눕니다.

### Gateway

Amazon API Gateway **HTTP API**의 integration timeout은 **최대 30초**이며 상향할 수 없습니다. (REST API Regional/Private는 쿼터 신청으로 29초를 넘길 수 있지만, 그래도 수십 분 대기는 불가에 가깝습니다.)

그래서 브라우저는 Gateway에 “분석이 끝날 때까지” 붙지 않습니다.

1. **`POST /jobs`**  
   - Cognito Access Token 검증  
   - DynamoDB에 job 레코드 생성 (`QUEUED`)  
   - 같은 Lambda를 **`InvocationType=Event`** 로 한 번 더 호출해 worker를 enqueue  
   - 즉시 **`202 Accepted`** + `{ jobId, status, sessionId }` 반환  
   - Gateway 왕복은 보통 1초 미만 → **30초 한도에 걸리지 않음**

2. **Worker (비동기 Event 호출)**  
   - job을 `RUNNING`으로 갱신  
   - `InvokeHarness` 스트림을 끝까지 읽어 결과를 DynamoDB에 저장 (`SUCCEEDED` / `FAILED`)  
   - 이 경로는 API Gateway를 거치지 않음 → Gateway timeout과 무관

3. **`GET /jobs/{jobId}` 폴링**  
   - UI가 수 초 간격으로 상태만 조회  
   - 각 요청은 수 ms~수백 ms → Gateway 한도 안쪽

4. **의도적으로 쓰지 않는 패턴**  
   - `POST /invoke`로 Harness 완료까지 Gateway가 동기 대기 → 30초에서 504  
   - 긴 작업을 API Gateway timeout만 늘려서 해결하려 함 → HTTP API에서는 불가

정리하면, Gateway는 **job 생성·상태 조회·문서 URL 발급**만 담당하고(**Gateway timeout 30초**), **긴 대기** 는 Lambda Managed Instances의 **비동기 Event worker** 가 담당합니다. LMI async 한도는 최대 60분이지만, 이 프로젝트에서는 worker·Harness timeout을 **30분**으로 관리합니다.

### Lambda Managed Instances

[Lambda Managed Instances (LMI)](https://docs.aws.amazon.com/lambda/latest/dg/lambda-managed-instances.html)는 함수 코드·이벤트 모델은 기존 Lambda와 같으면서, 실행 환경을 **계정 안의 EC2 인스턴스(Lambda가 운영)** 위에서 돌리는 형태입니다.

LMI의 **비동기(Event) 호출**은 서비스 한도상 **최대 60분**까지 연속 실행이 가능합니다. 다만 document-ai에서는 worker·Harness **timeout을 30분(1800초)으로 관리**합니다. API Gateway HTTP 경로는 여전히 **30초** integration timeout이며, Gateway는 job 생성·폴링처럼 짧은 요청만 받고 긴 Harness 대기는 Event worker가 수행합니다.

**무엇이 다른가**

| | Lambda (default) | Lambda Managed Instances |
|--|--|--|
| 실행 위치 | 공유 Firecracker 플릿 | 내 계정의 EC2 (Nitro 컨테이너) |
| 동시성 | 실행 환경당 보통 1 invoke | 실행 환경당 **다중 concurrent** 가능 |
| 스케일 | 요청 시 스케일, 유휴 시 0 | CPU 기반 스케일, **최소 용량 유지 가능** |
| 가격 | 요청·GB-초 | **EC2 요금 + 관리 수수료(~15%)** |
| Function timeout | 최대 **15분** | **async / ESM: 최대 60분**(서비스 한도; 실제는 90분까지 가능한 경우도 있음), sync: 여전히 15분 |

**document-ai에서의 동작**

1. installer가 **Capacity Provider** (`cp-document-ai`)를 만들고, default VPC의 **퍼블릭 서브넷** + 전용 SG에 붙입니다. (아웃바운드로 Bedrock·S3·DynamoDB·CloudWatch 접근)  
2. **Operator IAM role** (`lambda-lmi-operator-document-ai` + `AWSLambdaManagedEC2ResourceOperator`)로 Lambda가 EC2를 기동·종료합니다.  
3. `lambda-harness-document-ai`에 `CapacityProviderConfig`를 붙이고 worker `Timeout=1800`(30분)으로 설정합니다. HTTP(API Gateway) 쪽은 **30초** integration timeout입니다.  
4. `$LATEST.PUBLISHED` 를 publish하면 LMI에서 활성이 됩니다. 비한정 ARN 호출은 `$LATEST.PUBLISHED`로 갑니다.  
5. **API Gateway → Lambda** 는 동기이지만 job 생성·폴링만 하므로 **30초 Gateway 한도** 안에서 끝납니다.  
6. **Worker Event invoke** 는 Gateway를 거치지 않는 비동기 경로이며, LMI로는 최대 60분까지 가능하지만 **이 프로젝트는 30분**으로 제한합니다. Harness `timeoutSeconds`도 1800으로 맞춥니다.

**비용 (요약)**

- **청구 단위:** Capacity Provider가 띄운 **EC2 인스턴스 시간** + Lambda **관리 수수료(약 15%)**. 요청당 GB-초 요금 모델이 아닙니다.  
- **Savings Plans / RI** 등 EC2 할인은 **인스턴스 요금에만** 적용되고, 관리 수수료에는 적용되지 않습니다.  
- publish 시 AZ 복원력 때문에 **인스턴스가 여러 대** 뜰 수 있습니다. installer는 `MaxVCpuCount`와 function scaling(min/max EE)으로 상한을 낮춰 두지만, **유휴 시에도 최소 용량이 남아 과금**될 수 있습니다. 데모 후 `uninstaller.py`로 capacity provider를 지우면 인스턴스가 정리됩니다.  
- 상세·지역별 단가: [Lambda Pricing](https://aws.amazon.com/lambda/pricing/) 의 Managed Instances 표.

**주의**

- LMI는 **VPC가 필수**입니다.  
- 일반 Lambda를 LMI로 바꾼 뒤에도, **동기 호출만**으로는 15분을 넘길 수 없습니다. 긴 작업은 반드시 **Event(async)** 경로를 써야 합니다.  
- **API Gateway timeout(30초)** · **이 프로젝트 worker timeout(30분)** · **LMI async 한도(최대 60분)** 를 혼동하지 마세요. Gateway는 짧은 API만, 긴 분석은 Event worker(여기선 30분)입니다.  
- Durable Functions(최대 1년)과는 별개입니다. Durable은 checkpoint/wait로 **여러 invocation**을 이어 붙이는 모델이고, document-ai worker는 **한 번의 async invocation** 안에서 Harness 완료를 기다립니다.

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

삭제 순서: API Gateway → Function URL → Lambda → Capacity Provider → LMI SG → jobs table → harness → code interpreter → IAM roles → (optional) CloudFront → (optional) S3.

## Notes

- Cognito User Pool은 ess-work가 소유합니다. document-ai uninstaller는 Cognito를 삭제하지 않습니다.
- Lambda는 Cognito Access Token을 `GetUser`로 검증합니다 (PyJWT/cryptography 미사용).
- Lambda에 `boto3>=1.40.0`을 manylinux 휠로 번들합니다 (InvokeHarness skills.s3). Runtime은 LMI 지원을 위해 **python3.13** 입니다.
- 산출물 공유는 **doc-sharing** skill이 document-ai S3·CloudFront URL을 반환합니다. MCP artifact-share는 사용하지 않습니다. 자세한 내용은 [doc-sharing](#doc-sharing)을 보세요.
- 긴 분석은 `POST /jobs` + Event worker(LMI, 최대 30분) + `GET /jobs/{jobId}` 폴링입니다. 자세한 내용은 [비동기 처리](#비동기-처리)를 보세요.
- HTTP API CORS가 OPTIONS preflight를 처리합니다 (Lambda OPTIONS 라우트 없음).
- LMI capacity provider는 EC2 인스턴스를 띄우므로, 사용 후 `uninstaller.py`로 정리하는 것을 권장합니다.

## 실행 결과

아래와 같이 로그인을 수행합니다.

<img width="1100" height="478" alt="image" src="https://github.com/user-attachments/assets/b1a2ee10-b84b-481a-a541-c2947e91e9b4" />

이제 아래와 같이 문서를 선택합니다.

<img width="1103" height="362" alt="image" src="https://github.com/user-attachments/assets/47645b0d-c018-4ec5-8ad6-06e3816b04b5" />

아래와 같이 문서 분석을 요청합니다.

<img width="1100" height="232" alt="image" src="https://github.com/user-attachments/assets/58afcfe8-a496-4e71-ad31-310975a8d6c0" />

이때의 결과는 아래와 같습니다. 결과분석으로 얻어진 파일을 다운로드 할 수 있습니다.

<img width="1103" height="419" alt="image" src="https://github.com/user-attachments/assets/bae866e2-50a4-4345-b7b5-149f25f2cae4" />
