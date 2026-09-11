(function () {
  const cfg = window.APP_CONFIG || {};
  const jobsUrl = String(cfg.apiJobsUrl || cfg.apiGatewayUrl || "")
    .trim()
    .replace(/\/$/, "");
  const healthUrl =
    String(cfg.apiGatewayHealthUrl || "").trim() ||
    (jobsUrl.endsWith("/jobs")
      ? jobsUrl.slice(0, -"/jobs".length) + "/health"
      : "");
  const documentsUrl =
    String(cfg.apiDocumentsUrl || "").trim().replace(/\/$/, "") ||
    (jobsUrl.endsWith("/jobs")
      ? jobsUrl.slice(0, -"/jobs".length) + "/documents"
      : "");

  const AUTH_KEY = "documentAiAuth";
  const SESSION_KEY = "documentAiSessionId";
  const LAST_RESULT_KEY = "documentAiLastResult";
  const ACTIVE_JOB_KEY = "documentAiActiveJob";
  const POLL_INTERVAL_MS = 2500;
  // Match Lambda/Harness job budget (30 min).
  const POLL_MAX_MS = 30 * 60 * 1000;

  const authGate = document.getElementById("auth-gate");
  const app = document.getElementById("app");
  const loginForm = document.getElementById("login-form");
  const loginError = document.getElementById("login-error");
  const loginBtn = document.getElementById("login-btn");
  const logoutBtn = document.getElementById("logout-btn");
  const userLabel = document.getElementById("user-label");
  const docList = document.getElementById("doc-list");
  const refreshDocs = document.getElementById("refresh-docs");
  const loadedFiles = document.getElementById("loaded-files");
  const selectedChips = document.getElementById("selected-chips");
  const form = document.getElementById("ask-form");
  const input = document.getElementById("prompt");
  const button = document.getElementById("ask-btn");
  const result = document.getElementById("result");
  const hints = document.getElementById("hints");
  const downloads = document.getElementById("downloads");
  const downloadChips = document.getElementById("download-chips");

  /** @type {{username:string,idToken:string,accessToken:string,expiresAt:number}|null} */
  let auth = null;
  /** @type {Record<string, any[]>} */
  let docsByKind = {
    regulations: [],
    test_cases: [],
    projects: [],
    drawings: [],
  };
  let activeKind = "regulations";
  /** @type {Map<string, any>} */
  const selected = new Map();

  if (window.marked && typeof window.marked.setOptions === "function") {
    window.marked.setOptions({ gfm: true, breaks: true });
  }

  function renderMarkdown(text) {
    var source = String(text || "");
    if (!window.marked || typeof window.marked.parse !== "function") return null;
    try {
      var html = window.marked.parse(source);
      if (window.DOMPurify && typeof window.DOMPurify.sanitize === "function") {
        html = window.DOMPurify.sanitize(html, { USE_PROFILES: { html: true } });
      }
      return html;
    } catch (err) {
      console.warn("markdown render failed:", err);
      return null;
    }
  }

  function setResult(state, text, options) {
    var opts = options || {};
    result.dataset.state = state;
    var asMarkdown = opts.markdown === true && state === "idle";
    if (asMarkdown) {
      var source = rewriteMarkdownS3Links(String(text || ""));
      var html = renderMarkdown(source);
      if (html != null) {
        result.classList.add("markdown");
        result.innerHTML = html;
        // Open all result links in a new tab.
        result.querySelectorAll("a[href]").forEach(function (a) {
          a.target = "_blank";
          a.rel = "noopener noreferrer";
        });
        return;
      }
    }
    result.classList.remove("markdown");
    result.textContent = text;
  }

  function rewriteMarkdownS3Links(text) {
    return String(text || "").replace(
      /https?:\/\/[^\s)<>"]+/g,
      function (url) {
        var clean = url.replace(/[),.;]+$/, "");
        var key = extractS3Key(clean);
        if (!key) return url;
        return rewriteSharedArtifactUrl(clean, key) + url.slice(clean.length);
      }
    );
  }

  function rewriteSharedArtifactUrl(url, s3Key) {
    var key = s3Key || extractS3Key(url);
    if (!key || !documentsUrl) return rewriteDownloadUrl(url, key);
    var lower = key.toLowerCase();
    var isViewer =
      /^artifacts\//i.test(key) &&
      /\.(md|markdown|json|csv)$/i.test(lower);
    if (!isViewer) return rewriteDownloadUrl(url, key);

    var base = String(documentsUrl).replace(/\/documents\/?$/, "");
    var token = (auth && (auth.accessToken || auth.idToken)) || "";
    var parts = key.split("/");
    // artifacts/{user}/rest...
    var rest = parts.length >= 3 ? parts.slice(2).join("/") : parts.slice(1).join("/");
    var encoded = rest
      .split("/")
      .filter(Boolean)
      .map(function (p) {
        return encodeURIComponent(p);
      })
      .join("/");
    return (
      base.replace(/\/$/, "") +
      "/artifacts/view/" +
      encoded +
      (token ? "?access_token=" + encodeURIComponent(token) : "")
    );
  }

  function rewriteDownloadUrl(url, s3Key) {
    var key = s3Key || extractS3Key(url);
    if (key && documentsUrl) {
      var base = String(documentsUrl).replace(/\/documents\/?$/, "");
      var token = (auth && (auth.accessToken || auth.idToken)) || "";
      return (
        base.replace(/\/$/, "") +
        "/download?key=" +
        encodeURIComponent(key) +
        (token ? "&access_token=" + encodeURIComponent(token) : "")
      );
    }
    return url;
  }

  function extractS3Key(url) {
    try {
      var u = new URL(url);
      var host = u.hostname;
      var path = decodeURIComponent(u.pathname.replace(/^\/+/, ""));
      // bucket.s3...amazonaws.com/key
      if (/\.s3[.\-].*\.amazonaws\.com$/i.test(host) || /\.s3\.amazonaws\.com$/i.test(host)) {
        return path;
      }
      // s3.region.amazonaws.com/bucket/key
      if (/^s3[.\-]/i.test(host) && host.indexOf("amazonaws.com") !== -1) {
        var parts = path.split("/");
        if (parts.length >= 2) return parts.slice(1).join("/");
      }
      // CloudFront / custom sharing host: path is the object key
      if (/^(artifacts|images|docs)\//i.test(path)) {
        return path;
      }
      // Already an API viewer URL — leave as-is (caller won't rewrite)
      if (/\/artifacts\/(view|download)\//i.test(u.pathname)) {
        return "";
      }
    } catch (_) {}
    return "";
  }

  function setDownloads(links) {
    downloadChips.innerHTML = "";
    var items = Array.isArray(links) ? links : [];
    if (!items.length) {
      downloads.hidden = true;
      return;
    }
    downloads.hidden = false;
    items.forEach(function (link) {
      var a = document.createElement("a");
      a.className = "chip";
      a.href = rewriteDownloadUrl(link.url, link.s3_key);
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.textContent = link.label || link.url;
      downloadChips.appendChild(a);
    });
  }

  function saveLastResult(payload) {
    try {
      localStorage.setItem(
        LAST_RESULT_KEY,
        JSON.stringify({
          username: (auth && auth.username) || "",
          prompt: String((payload && payload.prompt) || ""),
          resultText: String((payload && payload.resultText) || ""),
          downloadLinks: Array.isArray(payload && payload.downloadLinks)
            ? payload.downloadLinks
            : [],
          savedAt: Date.now(),
        })
      );
    } catch (_) {}
  }

  function loadLastResult() {
    try {
      var raw = localStorage.getItem(LAST_RESULT_KEY);
      if (!raw) return null;
      var data = JSON.parse(raw);
      if (!data || !data.resultText) return null;
      if (
        auth &&
        data.username &&
        auth.username &&
        data.username !== auth.username
      ) {
        return null;
      }
      return data;
    } catch (_) {
      return null;
    }
  }

  function restoreLastResult() {
    var data = loadLastResult();
    if (!data) return false;
    if (data.prompt) input.value = data.prompt;
    setResult("idle", data.resultText, { markdown: true });
    setDownloads(data.downloadLinks || []);
    return true;
  }

  function saveActiveJob(payload) {
    try {
      localStorage.setItem(
        ACTIVE_JOB_KEY,
        JSON.stringify({
          username: (auth && auth.username) || "",
          jobId: String((payload && payload.jobId) || ""),
          prompt: String((payload && payload.prompt) || ""),
          sessionId: String((payload && payload.sessionId) || ""),
          startedAt: Date.now(),
        })
      );
    } catch (_) {}
  }

  function clearActiveJob() {
    try {
      localStorage.removeItem(ACTIVE_JOB_KEY);
    } catch (_) {}
  }

  function loadActiveJob() {
    try {
      var raw = localStorage.getItem(ACTIVE_JOB_KEY);
      if (!raw) return null;
      var data = JSON.parse(raw);
      if (!data || !data.jobId) return null;
      if (
        auth &&
        data.username &&
        auth.username &&
        data.username !== auth.username
      ) {
        return null;
      }
      // Drop stale jobs older than poll budget + buffer.
      if (data.startedAt && Date.now() - data.startedAt > POLL_MAX_MS + 5 * 60 * 1000) {
        clearActiveJob();
        return null;
      }
      return data;
    } catch (_) {
      return null;
    }
  }

  function applySucceededJob(job, prompt) {
    if (job && job.sessionId) {
      try {
        sessionStorage.setItem(SESSION_KEY, job.sessionId);
      } catch (_) {}
    }
    var resultText = (job && job.result) || "(응답이 비어 있습니다)";
    setResult("idle", resultText, { markdown: true });
    setDownloads((job && job.downloadLinks) || []);
    saveLastResult({
      prompt: prompt || "",
      resultText: resultText,
      downloadLinks: (job && job.downloadLinks) || [],
    });
    clearActiveJob();
  }

  function getSessionId() {
    try {
      let id = sessionStorage.getItem(SESSION_KEY);
      if (!id || id.length < 33) {
        id =
          (crypto.randomUUID && crypto.randomUUID()) ||
          "sess-" + Date.now() + "-" + Math.random().toString(16).slice(2);
        while (id.length < 33) id += "-x";
        sessionStorage.setItem(SESSION_KEY, id);
      }
      return id;
    } catch (_) {
      return "sess-" + Date.now() + "-" + Math.random().toString(16).slice(2) + "-pad";
    }
  }

  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function loadAuth() {
    try {
      var raw = localStorage.getItem(AUTH_KEY);
      if (!raw) return null;
      var data = JSON.parse(raw);
      if (!data || !data.idToken || !data.username) return null;
      if (data.expiresAt && Date.now() > data.expiresAt - 30 * 1000) return null;
      return data;
    } catch (_) {
      return null;
    }
  }

  function saveAuth(data) {
    auth = data;
    try {
      localStorage.setItem(AUTH_KEY, JSON.stringify(data));
    } catch (_) {}
  }

  function clearAuth() {
    auth = null;
    try {
      localStorage.removeItem(AUTH_KEY);
    } catch (_) {}
  }

  function authHeader() {
    if (!auth) return {};
    // Cognito GetUser requires an Access Token.
    var token = auth.accessToken || auth.idToken;
    return { Authorization: "Bearer " + token };
  }

  function showLogin(message) {
    app.hidden = true;
    authGate.hidden = false;
    if (message) {
      loginError.hidden = false;
      loginError.textContent = message;
    } else {
      loginError.hidden = true;
      loginError.textContent = "";
    }
  }

  function showApp() {
    authGate.hidden = true;
    app.hidden = false;
    userLabel.textContent = auth ? auth.username : "";
    // Prefer resuming an in-flight job over restoring a previous result.
    if (!loadActiveJob()) restoreLastResult();
  }

  function cognitoAvailable() {
    return !!(
      window.AmazonCognitoIdentity &&
      cfg.cognitoUserPoolId &&
      cfg.cognitoClientId
    );
  }

  function loginWithCognito(username, password) {
    return new Promise(function (resolve, reject) {
      if (!cognitoAvailable()) {
        reject(
          new Error(
            "Cognito 설정이 없습니다. installer.py를 실행해 config.js를 생성하세요."
          )
        );
        return;
      }
      var pool = new AmazonCognitoIdentity.CognitoUserPool({
        UserPoolId: cfg.cognitoUserPoolId,
        ClientId: cfg.cognitoClientId,
      });
      var cognitoUser = new AmazonCognitoIdentity.CognitoUser({
        Username: username,
        Pool: pool,
      });
      var authDetails = new AmazonCognitoIdentity.AuthenticationDetails({
        Username: username,
        Password: password,
      });
      cognitoUser.authenticateUser(authDetails, {
        onSuccess: function (session) {
          var idToken = session.getIdToken().getJwtToken();
          var accessToken = session.getAccessToken().getJwtToken();
          var exp = session.getIdToken().getExpiration() * 1000;
          resolve({
            username: username,
            idToken: idToken,
            accessToken: accessToken,
            expiresAt: exp,
          });
        },
        onFailure: function (err) {
          reject(err || new Error("로그인 실패"));
        },
        newPasswordRequired: function () {
          reject(
            new Error(
              "새 비밀번호 설정이 필요합니다. ess-work에서 비밀번호를 변경한 뒤 다시 시도하세요."
            )
          );
        },
      });
    });
  }

  function docKey(doc) {
    return [
      doc.kind || "",
      doc.filename || "",
      doc.md_file || "",
      doc.display_name || "",
      doc.title || "",
    ].join("|");
  }

  function basenamePath(path) {
    var s = String(path || "").trim().replace(/\/+$/, "");
    if (!s) return "";
    var parts = s.split(/[/\\]/);
    return parts[parts.length - 1] || s;
  }

  function isTestCaseKind(kind) {
    var k = String(kind || "").toLowerCase().replace(/-/g, "_");
    return (
      k === "test_case" ||
      k === "test_cases" ||
      k === "testcase" ||
      k === "testcases"
    );
  }

  /** Prefer MD for regulations/projects/drawings; JSON for test cases (ess-work copy behavior). */
  function selectedArtifact(doc) {
    if (isTestCaseKind(doc.kind)) {
      var jsonPath = String(doc.json_path || doc.json_s3_key || "").trim();
      if (jsonPath) {
        return {
          name: basenamePath(jsonPath),
          path: String(doc.json_path || "").trim() || jsonPath,
          type: "json",
        };
      }
      var src = String(doc.source_path || doc.xlsx_s3_key || "").trim();
      if (src) {
        return { name: basenamePath(src), path: src, type: "source" };
      }
    }

    var mdFile = String(doc.md_file || "").trim();
    if (mdFile) {
      return {
        name: basenamePath(mdFile),
        path:
          String(doc.md_workspace_path || doc.md_path || "").trim() || mdFile,
        type: "md",
      };
    }
    var mdPath = String(doc.md_workspace_path || doc.md_path || "").trim();
    if (mdPath) {
      return { name: basenamePath(mdPath), path: mdPath, type: "md" };
    }

    var fallback = String(
      doc.filename || doc.display_name || doc.title || ""
    ).trim();
    if (fallback && !isTestCaseKind(doc.kind)) {
      var stem = fallback.replace(/\.[^.]+$/, "");
      return { name: stem + ".md", path: "", type: "md" };
    }
    return {
      name: fallback || "document",
      path: "",
      type: isTestCaseKind(doc.kind) ? "json" : "md",
    };
  }

  function formatBytes(bytes) {
    if (bytes == null || !Number.isFinite(bytes) || bytes < 0) return "";
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
    return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  }

  function openSelectedDocument(doc) {
    var artifact = selectedArtifact(doc);
    var type = String(artifact.type || "").toLowerCase();
    if (type === "json") {
      openJson(doc);
      return;
    }
    if (type === "md" || type === "markdown") {
      openMarkdown(doc);
      return;
    }
    if (type === "source" || type === "xlsx" || type === "xlsm") {
      openXlsx(doc);
      return;
    }
    if (type === "pdf") {
      openPdf(doc);
      return;
    }
    // Fallback: prefer available viewer in common order.
    if (doc.json_available || doc.json_viewer_url) openJson(doc);
    else if (doc.md_available || doc.md_viewer_url) openMarkdown(doc);
    else if (doc.xlsx_available || doc.xlsx_view_url) openXlsx(doc);
    else if (doc.pdf_available || doc.pdf_view_url) openPdf(doc);
  }

  function renderSelectedChips() {
    selectedChips.innerHTML = "";
    if (selected.size === 0) {
      loadedFiles.hidden = true;
      return;
    }
    loadedFiles.hidden = false;
    selected.forEach(function (doc, key) {
      var chip = document.createElement("div");
      chip.className = "chip chip-selectable";
      var label = document.createElement("button");
      label.type = "button";
      label.className = "chip-open";
      var artifact = selectedArtifact(doc);
      label.textContent =
        (doc.kind ? "[" + doc.kind + "] " : "") + artifact.name;
      label.title = "새 탭에서 열기";
      label.addEventListener("click", function () {
        openSelectedDocument(doc);
      });
      var remove = document.createElement("button");
      remove.type = "button";
      remove.setAttribute("aria-label", "제거");
      remove.textContent = "×";
      remove.addEventListener("click", function (event) {
        event.stopPropagation();
        selected.delete(key);
        renderSelectedChips();
        renderDocList();
      });
      chip.appendChild(label);
      chip.appendChild(remove);
      selectedChips.appendChild(chip);
    });
  }

  function openInNewTab(url) {
    if (!url) return;
    window.open(url, "_blank", "noopener,noreferrer");
  }

  function viewerApiUrl(doc, view) {
    if (!documentsUrl || !auth) return "";
    var kind = doc.kind || "regulation";
    var filename =
      doc.filename || doc.md_file || doc.display_name || doc.title || "";
    if (!filename) return "";
    var token = auth.accessToken || auth.idToken || "";
    return (
      documentsUrl.replace(/\/$/, "") +
      "/" +
      encodeURIComponent(kind) +
      "/" +
      encodeURIComponent(filename) +
      "/" +
      encodeURIComponent(view) +
      "?access_token=" +
      encodeURIComponent(token)
    );
  }

  function openMarkdown(doc) {
    openInNewTab(viewerApiUrl(doc, "markdown"));
  }

  function openPdf(doc) {
    if (doc.pdf_view_url) {
      openInNewTab(doc.pdf_view_url);
      return;
    }
    openInNewTab(viewerApiUrl(doc, "pdf"));
  }

  function openJson(doc) {
    openInNewTab(viewerApiUrl(doc, "json"));
  }

  function openXlsx(doc) {
    if (doc.xlsx_view_url) {
      openInNewTab(doc.xlsx_view_url);
      return;
    }
    openInNewTab(viewerApiUrl(doc, "xlsx"));
  }

  function addActionButton(container, label, enabled, onClick, title) {
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "doc-action-btn";
    btn.textContent = label;
    btn.disabled = !enabled;
    if (title) btn.title = title;
    if (enabled) {
      btn.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        onClick();
      });
    }
    container.appendChild(btn);
  }

  function renderDocList() {
    var docs = docsByKind[activeKind] || [];
    docList.innerHTML = "";
    if (docList.dataset.state === "loading") {
      docList.textContent = "문서 목록을 불러오는 중…";
      return;
    }
    if (docList.dataset.state === "error") {
      return;
    }
    if (!docs.length) {
      docList.dataset.state = "empty";
      docList.textContent = "등록된 문서가 없습니다.";
      return;
    }
    docList.dataset.state = "ready";
    docs.forEach(function (doc) {
      var key = docKey(doc);
      var row = document.createElement("div");
      row.className = "doc-item";

      var checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = selected.has(key);
      checkbox.addEventListener("change", function () {
        if (checkbox.checked) selected.set(key, doc);
        else selected.delete(key);
        renderSelectedChips();
      });

      var meta = document.createElement("div");
      meta.className = "doc-meta";
      var title = document.createElement("p");
      title.className = "doc-title";
      title.textContent = doc.display_name || doc.filename || doc.title || "document";
      var sub = document.createElement("p");
      sub.className = "doc-sub";
      var parts = [
        doc.status || "",
        formatBytes(doc.bytes),
        doc.extracted_at || doc.created_at || "",
      ].filter(Boolean);
      sub.textContent = parts.join(" · ") || "—";
      meta.appendChild(title);
      meta.appendChild(sub);

      var actions = document.createElement("div");
      actions.className = "doc-actions";

      if (activeKind === "test_cases") {
        addActionButton(
          actions,
          "JSON",
          Boolean(doc.json_available || doc.json_viewer_url),
          function () {
            openJson(doc);
          },
          "JSON viewer (새 탭)"
        );
        addActionButton(
          actions,
          "Excel",
          Boolean(doc.xlsx_available || doc.xlsx_view_url),
          function () {
            openXlsx(doc);
          },
          "Excel 열기"
        );
      } else {
        addActionButton(
          actions,
          "Markdown",
          Boolean(doc.md_available || doc.md_viewer_url),
          function () {
            openMarkdown(doc);
          },
          "Markdown viewer (새 탭)"
        );
        addActionButton(
          actions,
          "PDF",
          Boolean(doc.pdf_available || doc.pdf_view_url),
          function () {
            openPdf(doc);
          },
          "PDF (새 탭)"
        );
      }

      row.appendChild(checkbox);
      row.appendChild(meta);
      row.appendChild(actions);
      docList.appendChild(row);
    });
  }

  async function fetchDocuments() {
    if (!documentsUrl) {
      docList.dataset.state = "error";
      docList.textContent =
        "문서 API 주소가 없습니다. installer.py --web-only 를 실행하세요.";
      return;
    }
    if (!auth) {
      showLogin("로그인이 필요합니다.");
      return;
    }
    docList.dataset.state = "loading";
    docList.textContent = "문서 목록을 불러오는 중…";
    try {
      var resp = await fetch(documentsUrl, {
        method: "GET",
        headers: Object.assign({ Accept: "application/json" }, authHeader()),
      });
      var data = await resp.json().catch(function () {
        return {};
      });
      if (resp.status === 401) {
        clearAuth();
        showLogin(data.error || "인증이 만료되었습니다. 다시 로그인하세요.");
        return;
      }
      if (!resp.ok) {
        throw new Error(data.error || "문서 조회 실패 (" + resp.status + ")");
      }
      var kinds = data.kinds || {};
      docsByKind = {
        regulations: (kinds.regulations && kinds.regulations.documents) || [],
        test_cases: (kinds.test_cases && kinds.test_cases.documents) || [],
        projects: (kinds.projects && kinds.projects.documents) || [],
        drawings: (kinds.drawings && kinds.drawings.documents) || [],
      };
      docList.dataset.state = "ready";
      renderDocList();
    } catch (err) {
      docList.dataset.state = "error";
      docList.textContent = (err && err.message) || String(err);
    }
  }

  async function checkHealth() {
    if (!healthUrl) return;
    try {
      var resp = await fetch(healthUrl, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      var data = await resp.json().catch(function () {
        return {};
      });
      if (!resp.ok) throw new Error(data.error || "Health check failed");
      if (data.harnessConfigured === false) {
        throw new Error("Lambda에 HARNESS_ARN이 설정되지 않았습니다.");
      }
    } catch (err) {
      console.warn("health check:", err);
    }
  }

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
    var data = await response.json().catch(function () {
      return {};
    });
    if (response.status === 401) {
      clearAuth();
      showLogin(data.error || "인증이 만료되었습니다. 다시 로그인하세요.");
      throw new Error(data.error || "Unauthorized");
    }
    if (!response.ok && response.status !== 202) {
      throw new Error(data.error || "작업 생성 실패 (" + response.status + ")");
    }
    if (!data.jobId) throw new Error(data.error || "jobId가 응답에 없습니다.");
    return data;
  }

  async function getJob(jobId) {
    var url = jobsUrl.replace(/\/$/, "") + "/" + encodeURIComponent(jobId);
    var response = await fetch(url, {
      method: "GET",
      headers: Object.assign({ Accept: "application/json" }, authHeader()),
    });
    var data = await response.json().catch(function () {
      return {};
    });
    if (!response.ok) {
      throw new Error(data.error || "작업 조회 실패 (" + response.status + ")");
    }
    return data;
  }

  async function pollJob(jobId) {
    var started = Date.now();
    var lastStatus = "";
    while (Date.now() - started < POLL_MAX_MS) {
      var job = await getJob(jobId);
      var status = String(job.status || "");
      if (status !== lastStatus) {
        lastStatus = status;
        if (status === "QUEUED") {
          setResult("loading", "요청을 접수했습니다. 에이전트 작업을 기다리는 중…");
        } else if (status === "RUNNING") {
          setResult("loading", "에이전트가 문서를 분석하는 중입니다…");
        }
      }
      if (status === "SUCCEEDED") return job;
      if (status === "FAILED") {
        throw new Error(job.error || "분석 작업이 실패했습니다.");
      }
      await sleep(POLL_INTERVAL_MS);
    }
    throw new Error(
      "분석이 30분 안에 끝나지 않았습니다. 잠시 후 페이지를 새로고침하면 진행 중인 작업을 이어서 확인합니다."
    );
  }

  function selectedDocumentsPayload() {
    return Array.from(selected.values()).map(function (doc) {
      var artifact = selectedArtifact(doc);
      return {
        kind: doc.kind,
        filename: doc.filename,
        display_name: artifact.name,
        selected_name: artifact.name,
        selected_path: artifact.path,
        selected_type: artifact.type,
        title: doc.title,
        md_file: doc.md_file,
        md_path: doc.md_path,
        md_workspace_path: doc.md_workspace_path,
        source_path: doc.source_path,
        json_path: doc.json_path,
        md_s3_key: doc.md_s3_key,
        source_s3_key: doc.source_s3_key,
        md_download_url: doc.md_download_url,
        source_download_url: doc.source_download_url,
        download_links: doc.download_links || [],
      };
    });
  }

  async function analyze(prompt) {
    var q = (prompt || "").trim();
    if (!q && selected.size === 0) return;

    if (!auth) {
      showLogin("분석을 위해 로그인하세요.");
      return;
    }
    if (!jobsUrl) {
      setResult(
        "error",
        "API 주소가 설정되지 않았습니다. config.js의 apiJobsUrl을 확인하거나 python installer.py --web-only 를 실행하세요."
      );
      return;
    }

    button.disabled = true;
    setDownloads([]);
    setResult("loading", "요청을 접수하는 중입니다…");
    var sessionId = getSessionId();

    try {
      await checkHealth();
      var created = await createJob(q, sessionId, selectedDocumentsPayload());
      if (created.sessionId) {
        try {
          sessionStorage.setItem(SESSION_KEY, created.sessionId);
        } catch (_) {}
      }
      saveActiveJob({
        jobId: created.jobId,
        prompt: q,
        sessionId: created.sessionId || sessionId,
      });
      setResult("loading", "에이전트가 문서를 분석하는 중입니다…");
      var job = await pollJob(created.jobId);
      applySucceededJob(job, q);
    } catch (err) {
      // Keep active job on timeout so refresh can resume; clear on hard failures.
      var msg = (err && err.message) || String(err);
      if (msg.indexOf("30분") === -1) {
        clearActiveJob();
      }
      setResult("error", msg);
    } finally {
      button.disabled = false;
    }
  }

  async function resumeActiveJobIfAny() {
    var active = loadActiveJob();
    if (!active || !active.jobId || !jobsUrl || !auth) return false;
    if (active.prompt) input.value = active.prompt;
    button.disabled = true;
    setResult("loading", "이전 분석 작업을 이어서 확인하는 중…");
    try {
      var job = await pollJob(active.jobId);
      applySucceededJob(job, active.prompt || "");
      return true;
    } catch (err) {
      var msg = (err && err.message) || String(err);
      if (msg.indexOf("30분") === -1 && msg.indexOf("찾을 수 없") === -1) {
        // Job finished as FAILED or gone — stop auto-resume.
        clearActiveJob();
      }
      setResult("error", msg);
      return false;
    } finally {
      button.disabled = false;
    }
  }

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    var username = String(document.getElementById("login-id").value || "").trim();
    var password = String(document.getElementById("login-password").value || "");
    if (!username || !password) return;
    loginBtn.disabled = true;
    loginError.hidden = true;
    try {
      var session = await loginWithCognito(username, password);
      saveAuth(session);
      showApp();
      await fetchDocuments();
      await resumeActiveJobIfAny();
    } catch (err) {
      var message =
        (err && (err.message || err.code)) || String(err) || "로그인 실패";
      loginError.hidden = false;
      loginError.textContent = message;
    } finally {
      loginBtn.disabled = false;
    }
  });

  logoutBtn.addEventListener("click", function () {
    clearAuth();
    selected.clear();
    renderSelectedChips();
    showLogin();
  });

  refreshDocs.addEventListener("click", function () {
    fetchDocuments();
  });

  document.querySelectorAll(".tab").forEach(function (tab) {
    tab.addEventListener("click", function () {
      document.querySelectorAll(".tab").forEach(function (t) {
        t.classList.remove("is-active");
      });
      tab.classList.add("is-active");
      activeKind = tab.getAttribute("data-kind") || "regulations";
      renderDocList();
    });
  });

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    analyze(input.value);
  });

  hints.addEventListener("click", function (event) {
    var target = event.target.closest("button[data-prompt]");
    if (!target) return;
    input.value = target.getAttribute("data-prompt") || "";
    input.focus();
  });

  auth = loadAuth();
  if (auth) {
    showApp();
    fetchDocuments().then(function () {
      return resumeActiveJobIfAny();
    });
  } else {
    showLogin();
  }
})();
