# Document AI

문서 분석용 Agent를 Web으로 활용하는 방법에 대해 설명합니다. 여기서는 AgentCore Harness를 이용해 agent를 구현하고, 웹페이지도 빠르게 이해하기 쉽도록 정적 웹 스택을 사용합니다. 

## Architecture

전체적인 architecture는 아래와 같습니다.

<img width="1000" alt="image" src="https://github.com/user-attachments/assets/1859d94d-c8e0-49ea-8a90-cef03195019f" />

사용자는 HTTPS로 CloudFront에 접속하고, CloudFront가 S3에 올린 정적 웹(html/js/css)을 제공합니다. API 호출은 API Gateway(`/jobs` 등)로 들어가며, Amazon Cognito(IdP)로 인증합니다.

API Gateway 뒤의 **AWS Lambda (LMI: Lambda Managed Instances)** 가 job 생성·폴링·문서 API를 처리하고, job 상태는 **DynamoDB**에 둡니다. 긴 분석은 Lambda가 **Amazon Bedrock AgentCore**의 Agent Runtime(Harness)을 호출해 수행합니다.

AgentCore 안에서는 Harness가 **MCP**(websearch, code interpreter)와 **Skills**(docx, pdf, pptx, doc sharing 등)로 문서를 다루고, 산출물·스킬 파일은 **S3**에 저장합니다. 추론은 **Amazon Bedrock**의 Anthropic Claude / OpenAI GPT 모델을 사용할 수 있습니다.


요청 흐름 요약:

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
  Capacity Provider + 전용 VPC private subnets
  • API 경로: 동기 호출 (문서 조회·job 생성, ≤15분 한도이나 실제는 수 초)
  • Worker: InvocationType=Event 비동기 (최대 30분, LMI async)
      → DynamoDB status RUNNING → InvokeHarness 스트림 대기
      → SUCCEEDED / FAILED
    ▼
AgentCore Harness (document_ai, VPC private, memory off)
  timeoutSeconds=1800
  + Code Interpreter (document_ai_code, PUBLIC)
  + Skills: doc-sharing, regulation-evaluator, testcase-generator, pptx, docx, xlsx
  + VPC endpoints: S3/DynamoDB Gateway, Bedrock Runtime/AgentCore, ECR, Logs, Secrets Manager
  + NAT: Cognito·기타 퍼블릭 HTTPS
```

**ESS-work 연동:** 형제 경로 `../ess-work/application/config.json`에서 Cognito·S3·sharing URL을 읽어 Lambda 환경 변수와 `html/config.js`에 주입합니다.

| Resource | Name |
|---|---|
| VPC | `vpc-for-document-ai` (2 AZ public/private + NAT) |
| S3 Gateway VPCE | `s3-endpoint-document-ai` |
| DynamoDB Gateway VPCE | `dynamodb-endpoint-document-ai` |
| Interface VPCEs | ECR, Logs, Secrets Manager, Bedrock Runtime, AgentCore, AgentCore Control |
| Agent runtime SG | `agent-runtime-sg-for-document-ai` |
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

### Cognito 인증

Cognito User Pool은 **ess-work가 소유**하며, document-ai는 이를 공유해 사용합니다. installer가 `../ess-work/application/config.json`에서 pool/client/region을 읽어 Lambda 환경 변수와 `html/config.js`에 주입합니다. uninstaller는 Cognito를 삭제하지 않습니다.

역할은 **로그인(토큰 발급)** 과 **API 검증** 으로 나뉩니다.

| 단계 | 주체 | 내용 |
|------|------|------|
| 로그인 | 브라우저 JS (`html/app.js`) | `amazon-cognito-identity-js`로 Cognito에 ID/비밀번호를 보내고 JWT(`idToken`, `accessToken`)를 받아 `localStorage`에 저장 |
| API 호출 | 브라우저 | `Authorization: Bearer <accessToken>` 헤더를 API Gateway에 전달 |
| 토큰 검증 | Lambda | Cognito `GetUser`로 Access Token을 검증 (PyJWT/cryptography 미사용). 유효하면 username을 추출해 API 처리 |

```
Browser (CloudFront → S3 html/)
    │  1) amazon-cognito-identity-js → Cognito User Pool 로그인
    │     JWT 발급 → localStorage
    │  2) API 요청 + Bearer Access Token
    ▼
API Gateway → Lambda
    │  cognito-idp:GetUser(AccessToken)
    ▼
문서/job API 처리
```

로그인 자체는 Lambda가 하지 않습니다. Lambda는 요청마다 토큰이 유효한지만 확인합니다.

### Harness Agent의 활용

「분석하기」는 Harness를 Gateway에 동기 붙잡지 않습니다. **짧은 job 생성 + 비동기 worker + 폴링**으로 긴 분석을 처리합니다. (API Gateway HTTP API integration timeout은 최대 30초)

#### 브라우저 → job 생성

`html/app.js`의 `analyze()`가 선택 문서·프롬프트로 `POST /jobs`를 호출합니다.

```903:938:document-ai/html/app.js
  async function analyze(prompt) {
    // ...
      var created = await createJob(q, sessionId, selectedDocumentsPayload());
      // ...
      setResult("loading", "에이전트가 문서를 분석하는 중입니다…");
      var job = await pollJob(created.jobId);
```

```807:821:document-ai/html/app.js
  async function createJob(prompt, sessionId, documents) {
    var response = await fetch(jobsUrl, {
      method: "POST",
      headers: Object.assign(
        {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        authHeader()
      ),
      body: JSON.stringify({
        prompt: prompt,
        sessionId: sessionId,
        documents: documents,
        actorId: auth && auth.username,
      }),
    });
```

1. Cognito Access Token을 `Authorization: Bearer …`로 붙임  
2. body: `{ prompt, sessionId, documents, actorId }`  
3. Lambda가 토큰 검증 후 DynamoDB(`dynamodb-document-ai-jobs`)에 job을 `QUEUED`로 저장  
4. 같은 Lambda를 `InvocationType=Event`로 한 번 더 호출해 worker를 enqueue  
5. 즉시 **`202 Accepted`** + `{ jobId, status, sessionId }` 반환 (Gateway 왕복은 수 초 이내)

```1960:2010:document-ai/lambda-harness/lambda_function.py
    item = {
        "jobId": job_id,
        "status": "QUEUED",
        "prompt": full_prompt,
        # ...
    }
    _put_job(item)

    worker_payload: Dict[str, Any] = {
        "jobWorker": True,
        "jobId": job_id,
        "prompt": full_prompt,
        "sessionId": session_id,
        "skills": skills,
    }
    # ...
    _enqueue_worker(worker_payload, context)

    return _response(
        202,
        {
            "jobId": job_id,
            "status": "QUEUED",
            "sessionId": session_id,
            "pollPath": f"/jobs/{job_id}",
        },
    )
```

```589:601:document-ai/lambda-harness/lambda_function.py
def _enqueue_worker(payload: Dict[str, Any], context: Any) -> None:
    # ...
    _lambda().invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
```

#### Lambda worker → InvokeHarness

비동기 worker(`jobWorker: true`)는 API Gateway를 거치지 않습니다.

1. job 상태를 `RUNNING`으로 갱신  
2. `_collect_harness_text()` → boto3 **`invoke_harness`** 로 Harness에 user 메시지를 보내고, 응답 **이벤트 스트림**을 끝까지 소비  
3. 산출물 다운로드 URL을 정리한 뒤 DynamoDB에 저장  
   - 성공: `status=SUCCEEDED`, `result`(분석 텍스트), `downloadLinks`  
   - 실패: `status=FAILED`, `error`  
4. Harness/Code Interpreter가 만든 파일(xlsx/pptx 등)은 skill(doc-sharing 등)이 **S3**(`artifacts/…`)에 올리고, 그 CloudFront/다운로드 URL이 `result`·`downloadLinks`에 포함됨  

스트림을 끝까지 소비한 뒤에야 DynamoDB에 `SUCCEEDED`를 씁니다.

```2038:2075:document-ai/lambda-harness/lambda_function.py
        _update_job(job_id, status="RUNNING")
    # ...
        result = _collect_harness_text(
            HARNESS_ARN,
            prompt,
            session_id,
            actor_id=actor_id,
            skills=skills,
        )
        result, truncated = _truncate_result(result)
        result, links = _rewrite_result_download_urls(result)
        attrs: Dict[str, Any] = {
            "status": "SUCCEEDED",
            "result": result,
            "sessionId": session_id,
        }
        # ...
        if links:
            attrs["downloadLinks"] = json.dumps(links, ensure_ascii=False)
        _update_job(job_id, **attrs)
```

`invoke_harness` 요청 인자:

| 인자 | 역할 |
|------|------|
| `harnessArn` | document-ai Harness 런타임 ARN |
| `runtimeSessionId` | 브라우저 `sessionId` — 같은 세션에서 대화/도구 상태 유지 |
| `messages` | `role=user` + 분석 프롬프트(선택 문서 경로 포함) |
| `actorId` | Cognito username — skill·artifacts 경로(`/mnt/workspace/{actor}/…`)에 사용 |
| `skills` | S3 skill 목록(doc-sharing, regulation-evaluator 등) |

스트림 이벤트 처리:

| 이벤트 | Lambda 동작 |
|--------|-------------|
| `contentBlockDelta` | `delta.text`를 chunk로 이어 붙여 최종 응답 텍스트 구성 |
| `messageStop` | `stopReason` 기록 (완료/중단 판별) |
| `runtimeClientError` / `internalServerException` / `validationException` | 즉시 `RuntimeError` → job `FAILED` |
| 읽기 타임아웃·스트림 끊김 | partial 텍스트와 함께 에러 (idle timeout은 job budget과 맞춤, 기본 1800초) |

스트림이 끝난 뒤 `stopReason=max_output_tokens_exceeded`이거나, 텍스트가 너무 짧거나 “진행하겠습니다” 같은 미완료 꼬리면 `_incomplete_result_reason`이 실패로 올려, 잘린 응답을 성공으로 저장하지 않습니다. Harness 세션 예산은 `maxTokens=200000`(installer `HARNESS_MAX_TOKENS`)입니다. Harness 안에서는 모델이 MCP(websearch, code interpreter)와 skills로 문서를 읽고 산출물을 만들며, 그 **최종 assistant 텍스트**만 `contentBlockDelta`로 Lambda에 전달됩니다.

```146:161:document-ai/lambda-harness/lambda_function.py
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
```

```455:523:document-ai/lambda-harness/lambda_function.py
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
    # ...
    try:
        for event in stream:
            if "contentBlockDelta" in event:
                # delta.text → chunks
            elif "messageStop" in event:
                # stopReason 기록
            elif "runtimeClientError" in event:
                raise RuntimeError(...)
            # internalServerException / validationException 동일
    except (ReadTimeoutError, EventStreamError) as e:
        raise RuntimeError(...) from e

    text = "".join(chunks)
    incomplete = _incomplete_result_reason(text, last_stop_reason)
    if incomplete:
        raise RuntimeError(incomplete)
    return text
```

#### 브라우저 폴링 → 화면 표시

```
분석하기 클릭
  → POST /jobs  (202 + jobId)
  → GET /jobs/{jobId} 을 2.5초마다 반복 (최대 30분)
       QUEUED / RUNNING → 로딩 메시지
       SUCCEEDED → result·downloadLinks 표시
       FAILED → 에러 표시
```

```21:23:document-ai/html/app.js
  const POLL_INTERVAL_MS = 2500;
  // Match Lambda/Harness job budget (30 min).
  const POLL_MAX_MS = 30 * 60 * 1000;
```

```853:871:document-ai/html/app.js
  async function pollJob(jobId) {
    var started = Date.now();
    var lastStatus = "";
    while (Date.now() - started < POLL_MAX_MS) {
      var job = await getJob(jobId);
      var status = String(job.status || "");
      // ... QUEUED / RUNNING UI ...
      if (status === "SUCCEEDED") return job;
      if (status === "FAILED") {
        throw new Error(job.error || "분석 작업이 실패했습니다.");
      }
      await sleep(POLL_INTERVAL_MS);
    }
```

| 항목 | 값 |
|------|-----|
| 폴링 API | `GET /jobs/{jobId}` (API Gateway → Lambda → DynamoDB GetItem) |
| 주기 | **2.5초** (`POLL_INTERVAL_MS = 2500`) |
| 상한 | **30분** (`POLL_MAX_MS`, Lambda/Harness job budget과 동일) |
| job·결과 저장소 | **DynamoDB** `dynamodb-document-ai-jobs` (`result`, `downloadLinks`, `status`) |
| 산출물 파일 | **S3** (doc-sharing 등이 `artifacts/`에 PutObject) |
| UI 캐시 | 진행 중 job은 `sessionStorage`, 마지막 성공 결과는 `localStorage`에 잠시 보관 (새로고침 재개용) |

`SUCCEEDED`이면 `job.result`를 마크다운으로 렌더하고, `downloadLinks`로 다운로드 버튼을 그립니다. Gateway는 job 생성·상태 조회만 담당하고, 긴 대기는 Event worker가 맡습니다.

```291:304:document-ai/html/app.js
  function applySucceededJob(job, prompt) {
    // ...
    var resultText = (job && job.result) || "(응답이 비어 있습니다)";
    setResult("idle", resultText, { markdown: true });
    setDownloads((job && job.downloadLinks) || []);
    saveLastResult({
      prompt: prompt || "",
      resultText: resultText,
      downloadLinks: (job && job.downloadLinks) || [],
    });
```

### VPC를 이용한 보완

PUBLIC 네트워크에 Harness·Lambda를 두면 AWS API 호출이 인터넷(또는 계정 default VPC)을 경유합니다. document-ai는 **전용 VPC**를 만들고 Harness와 Lambda Managed Instances를 **같은 private 서브넷**에 두어, (1) 런타임 아웃바운드를 서브넷·SG로 제한하고 (2) S3/DynamoDB/Bedrock 등 AWS 서비스는 **VPC Endpoint**로만 PrivateLink/Gateway 경로를 타게 합니다. 패턴은 ess-work 네트워크 모듈과 동일합니다.

#### 왜 VPC인가 (접근 제어)

| 계층 | 역할 |
|------|------|
| **Private subnet** | Harness ENI·LMI EC2에 public IP를 주지 않음. 인바운드 인터넷 직접 노출 없음 |
| **Security Group** | `agent-runtime-sg-for-document-ai`, `document-ai-lmi-sg`로 egress 허용 범위를 관리. VPCE SG(`vpce-sg-for-document-ai`)는 VPC CIDR → `:443`만 허용 |
| **VPC Endpoint** | S3·DynamoDB·Bedrock 트래픽이 NAT/인터넷을 거치지 않고 AWS 백본으로만 이동 (데이터 경로·비용·노출면 축소) |
| **NAT Gateway** | Cognito JWKS 등 **endpoint가 없는** HTTPS만 private → NAT → IGW |
| **IAM** | 네트워크와 직교. Harness role의 `VpcNetworkInterface`로 ENI 생성 권한, S3/Bedrock은 별도 정책 |

정리하면 **누가 어디에 붙을 수 있는지**는 VPC/SG가, **어떤 AWS API를 호출할 수 있는지**는 IAM이 담당합니다.

#### 트래픽 흐름

```
                    ┌──────────── vpc-for-document-ai ────────────┐
                    │  public ×2 AZ          private ×2 AZ        │
API GW ──invoke──►  │  [NAT + IGW]  ◄──0.0.0.0/0──  ┌─────────┐ │
                    │                               │ LMI CP  │ │
                    │                               │ Lambda  │ │
                    │                               └────┬────┘ │
                    │                                    │      │
                    │                               ┌────┴────┐ │
                    │                               │ Harness │ │
                    │                               │ (VPC)   │ │
                    │                               └────┬────┘ │
                    │  Gateway VPCE: S3, DynamoDB ◄──────┤      │
                    │  Interface VPCE: Bedrock*, ECR,    │      │
                    │    Logs, Secrets Manager     ◄─────┘      │
                    │  (그 외 HTTPS → NAT)                       │
                    └───────────────────────────────────────────┘
```

1. **Lambda (LMI)** 와 **Harness** 가 동일 `private_subnets`를 사용 → 같은 VPC 라우팅·DNS·endpoint를 공유합니다.  
2. Lambda → DynamoDB(jobs), S3(문서/artifacts), `InvokeHarness`(AgentCore data plane) 호출은 private DNS로 **해당 VPCE**에 붙습니다.  
3. Harness → Bedrock 모델·AgentCore·ECR 이미지 pull·S3 skills/artifacts 도 동일하게 endpoint(또는 S3 Gateway)를 탑니다.  
4. Code Interpreter는 현재 `networkMode: PUBLIC`(관리형 샌드박스). Harness↔CI 제어면은 AgentCore API(VPC endpoint)로 연결됩니다.

#### 프로비저닝 순서

`deploy_harness_stack`이 먼저 VPC를 만든 뒤, 같은 `vpc_info`를 Harness·Lambda에 넘깁니다.

```2331:2349:document-ai/installer.py
    vpc_info = _vpc_provisioner().ensure_vpc()
    upload_skills_to_s3(target_bucket)
    jobs_info = create_jobs_table()
    execution_role_arn = create_harness_execution_role(target_bucket, ess_s3)
    code_info = create_or_get_code_interpreter(execution_role_arn)
    harness_info = create_or_get_harness(
        execution_role_arn,
        target_bucket,
        vpc_info,
        code_interpreter_arn=code_info["code_interpreter_arn"],
        ess_s3_bucket=ess_s3,
    )
    lambda_arn = create_lambda_harness(
        harness_info["harness_arn"],
        target_bucket,
        jobs_info["jobsTableName"],
        ess,
        vpc_info,
    )
```

#### Harness: `networkMode: VPC`

Create/UpdateHarness 시 private 서브넷 + agent runtime SG를 지정합니다. 런타임 ENI가 해당 서브넷에만 생기고, 아웃바운드는 SG·라우트·endpoint 규칙을 따릅니다.

```117:144:document-ai/vpc_network.py
    def build_harness_runtime_environment(
        self, vpc_info: Dict[str, object]
    ) -> Dict:
        """CreateHarness/UpdateHarness environment with networkMode VPC."""
        return {
            "agentCoreRuntimeEnvironment": {
                "lifecycleConfiguration": {
                    "idleRuntimeSessionTimeout": 600,
                    "maxLifetime": 14400,
                },
                "networkConfiguration": {
                    "networkMode": "VPC",
                    "networkModeConfig": {
                        "subnets": list(vpc_info.get("private_subnets") or []),
                        "securityGroups": list(
                            vpc_info.get("agent_runtime_security_groups") or []
                        ),
                    },
                },
            }
        }

    def lmi_vpc_config(self, vpc_info: Dict[str, object]) -> Dict[str, List[str]]:
        """VpcConfig for Lambda Managed Instances capacity provider."""
        return {
            "SubnetIds": list(vpc_info.get("private_subnets") or []),
            "SecurityGroupIds": list(vpc_info.get("lmi_security_groups") or []),
        }
```

Harness execution role에는 VPC ENI 조작 권한이 필요합니다.

```1127:1140:document-ai/installer.py
        {
            "Sid": "VpcNetworkInterface",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeVpcs",
                "ec2:AssignPrivateIpAddresses",
                "ec2:UnassignPrivateIpAddresses",
            ],
            "Resource": ["*"],
        },
```

#### Lambda: 같은 VPC의 Capacity Provider

LMI는 Capacity Provider의 `VpcConfig`로 EC2를 띄웁니다. `lmi_vpc_config`가 Harness와 **동일한 private_subnets** + `document-ai-lmi-sg`를 넘기므로 Lambda와 Harness가 같은 네트워크 평면에 놓입니다. 기존 CP가 default VPC에 있으면 installer가 CP·함수를 지우고 전용 VPC로 재생성합니다.

#### S3 Gateway / DynamoDB·Bedrock Interface endpoint

`ensure_private_subnet_vpc_endpoints`가 private(및 public) 라우트 테이블에 Gateway를 묶고, private 서브넷에 Interface endpoint ENI를 둡니다.

| 타입 | 서비스 | 용도 |
|------|--------|------|
| **Gateway** | `s3` | skills sync, artifacts Put/Get, ECR 레이어 — 라우트 테이블 prefix list |
| **Gateway** | `dynamodb` | jobs 테이블 Get/Put/Update — Lambda worker 경로 |
| **Interface** | `bedrock-runtime` | Claude 등 모델 호출 |
| **Interface** | `bedrock-agentcore` / `…-control` | InvokeHarness·런타임/제어면 |
| **Interface** | `ecr.api` / `ecr.dkr` | Harness 관리형 이미지 pull |
| **Interface** | `logs`, `secretsmanager` | 런타임 로그·시크릿 |

```532:578:document-ai/vpc_network.py
        interface_services = [
            (f"com.amazonaws.{self.region}.ecr.api", f"ecr-api-endpoint-{self.project_name}"),
            (f"com.amazonaws.{self.region}.ecr.dkr", f"ecr-dkr-endpoint-{self.project_name}"),
            (f"com.amazonaws.{self.region}.logs", f"logs-endpoint-{self.project_name}"),
            (
                f"com.amazonaws.{self.region}.secretsmanager",
                f"secretsmanager-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-runtime",
                f"bedrock-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-agentcore",
                f"bedrock-agentcore-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-agentcore-control",
                f"bedrock-agentcore-control-endpoint-{self.project_name}",
            ),
        ]
        # ...
        endpoint_ids["s3"] = self._create_gateway_vpc_endpoint(
            vpc_id, route_table_ids, "s3", f"s3-endpoint-{self.project_name}"
        )
        endpoint_ids["dynamodb"] = self._create_gateway_vpc_endpoint(
            vpc_id,
            route_table_ids,
            "dynamodb",
            f"dynamodb-endpoint-{self.project_name}",
        )
```

Interface endpoint는 `PrivateDnsEnabled=True`이므로 boto3가 쓰는 리전 엔드포인트 호스트명이 VPC 안에서 VPCE IP로 해석됩니다. Gateway(S3/DynamoDB)는 라우트 테이블에 prefix list 경로가 추가되어, private 서브넷의 `0.0.0.0/0 → NAT`보다 **더 specific한 경로**로 AWS 백본에 붙습니다.

#### 구현·정리 파일

| 파일 | 역할 |
|------|------|
| `vpc_network.py` | VPC/서브넷/NAT/SG/VPCE 멱등 생성 |
| `installer.py` | `ensure_vpc` → Harness VPC 모드 → LMI CP 동일 서브넷 |
| `uninstaller.py` | VPCE → NAT/EIP → 서브넷/SG → VPC 삭제 |
| `config.json` | `vpc_id`, `private_subnets`, `agent_runtime_security_groups` 등 기록 |

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

1. installer가 전용 VPC(`vpc-for-document-ai`)를 만들고, **Capacity Provider** (`cp-document-ai`)를 **private 서브넷** + `document-ai-lmi-sg`에 붙입니다. S3/DynamoDB는 Gateway endpoint, Bedrock·ECR·Logs 등은 Interface endpoint(+ NAT)로 접근합니다.  
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
