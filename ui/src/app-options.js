import { buildHeroGaugeOption } from "./chart-builders";
import { workflowApi } from "./workflow-api.js";
import { runtimeToMonitor } from "./workflow-monitor.js";

const quaternaryKeys = ["ti_at", "zr_at", "nb_at", "v_at"];

function createScheduleFormDefaults() {
  return {
    name: "",
    target: "",
    material_system: "Ti-Zr-Nb-V quaternary thin film",
    sample_batch: "",
    model_version: "",
    substrate: "",
    deposition_chamber: "",
    sputter_component_id: "",
    xrd_component_id: "xrd-lab-01",
    eds_component_id: "eds-lab-01",
    nanoindentation_component_id: "nanoindentation-lab-01",
    objective: "",
    ti_at: "",
    zr_at: "",
    nb_at: "",
    v_at: "",
    target_hardness_gpa: "",
    composition_tolerance_at: "2.0",
    optimize_goal: "maximize",
    measurement_metric: "hardness_gpa",
    priority: "6",
    base_interval_seconds: "1800",
    min_interval_seconds: "300",
    max_interval_seconds: "7200",
    retry_delay_seconds: "180",
    feedback_timeout_seconds: "900",
    max_consecutive_failures: "30",
    start_in_seconds: "0",
    notes: "",
  };
}

function createHintSchedule() {
  return {
    ...createScheduleFormDefaults(),
    target: "materials/sputter-cluster-01",
    substrate: "Si(100)",
    deposition_chamber: "Sputter-Cluster-01",
    sputter_component_id: "Sputter-Cluster-01",
    objective: "screen the current high-throughput mat_model composition space",
    composition_tolerance_at: "2.0",
    notes: "每轮使用 mat_model 预测空间生成 13x13 共 169 个实验样本。",
  };
}

function feedbackDraftDefaults(execution, schedule) {
  const payload = execution?.dispatch_payload || schedule?.payload || {};
  const observation = execution?.feedback_observation || {};
  const predicted =
    payload.mat_model_candidate_composition_at_percent ||
    payload.predicted_composition_at_percent ||
    {};
  const measured = observation.measured_composition_at_percent || {};
  return {
    outcome: "success",
    quality_score: "0.90",
    latency_ms: "1800",
    deposition_sample_id:
      observation.deposition_sample_id || payload.experiment_batch_id || payload.sample_batch || "",
    sputter_run_id: observation.sputter_run_id || "",
    xrd_phase: observation.xrd_phase || "",
    xrd_note: observation.xrd_note || "",
    hardness_gpa: observation.hardness_gpa ?? "",
    eds_ti_at: measured.ti_at ?? observation.eds_ti_at ?? predicted.ti_at ?? "",
    eds_zr_at: measured.zr_at ?? observation.eds_zr_at ?? predicted.zr_at ?? "",
    eds_nb_at: measured.nb_at ?? observation.eds_nb_at ?? predicted.nb_at ?? "",
    eds_v_at: measured.v_at ?? observation.eds_v_at ?? predicted.v_at ?? "",
    note: "",
  };
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, item]) => item !== "" && item !== null && item !== undefined)
  );
}

function toNumber(value) {
  return Number(value);
}

function toOptionalNumber(value) {
  return value === "" || value === null || value === undefined ? null : Number(value);
}

function safeNumber(value) {
  return value === "" || value === null || value === undefined ? 0 : Number(value);
}

function buildCompactTimestamp() {
  return new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 12);
}

function modelSpaceSummary(space) {
  return (
    space?.summary ||
    space?.predicted_composition_space?.summary ||
    "未加载 mat_model 成分空间"
  );
}

function resolveDetailRunId() {
  const params = new URLSearchParams(window.location.search);
  return params.get("run_id") || "";
}

function resolveThemeMode() {
  const stored = window.localStorage.getItem("smart_scheduler_theme");
  return stored === "dark" ? "dark" : "light";
}

const appOptions = {
  data() {
    return {
      themeMode: resolveThemeMode(),
      isWorkflowBuilderMode: new URLSearchParams(window.location.search).get("view") === "workflow-builder",
      workflowHealth: null,
      detailRunId: resolveDetailRunId(),
      executionDetail: null,
      executionSamplePlan: null,
      detailLoading: false,
      schedules: [],
      executions: [],
      pendingExecutions: [],
      recommendations: [],
      feedbackDrafts: {},
      modelSpace: null,
      scheduleForm: createHintSchedule(),
      activeTargetHardnessDraft: "",
      activeTargetHardnessScheduleId: "",
      activeTargetHardnessDirty: false,
      autoRefreshEnabled: true,
      recentTimelineExpanded: false,
      message: {
        text: "准备就绪。",
        tone: "neutral",
      },
      lastUpdatedAt: null,
      actionState: {},
      refreshTimer: null,
    };
  },

  computed: {
    isExecutionDetailMode() {
      return Boolean(this.detailRunId);
    },

    detailExecution() {
      return this.executionDetail?.execution || null;
    },

    detailRecommendations() {
      return this.executionDetail?.recommendations || [];
    },

    detailDataPackages() {
      return this.executionDetail?.data_packages || [];
    },

    detailEvents() {
      return this.executionDetail?.events || [];
    },

    lastUpdatedText() {
      if (!this.lastUpdatedAt) {
        return "等待首次同步";
      }
      return `最近同步 ${this.formatTime(this.lastUpdatedAt)}`;
    },

    predictedCompositionSpace() {
      return this.modelSpace?.predicted_composition_space || null;
    },

    predictedCompositionSummary() {
      return modelSpaceSummary(this.modelSpace);
    },

    predictedCompositionCards() {
      const labels = {
        ti_at: "Ti",
        zr_at: "Zr",
        nb_at: "Nb",
        v_at: "V",
      };

      return quaternaryKeys.map((key) => ({
        key,
        label: labels[key],
        value: this.rangeLabel(this.predictedCompositionSpace?.elements?.[key]),
        detail: this.medianLabel(this.predictedCompositionSpace?.elements?.[key]),
      }));
    },

    predictedIntervalRows() {
      return this.intervalRowsFromSpace(this.predictedCompositionSpace);
    },

    activeSchedule() {
      const activeSchedules = this.schedules.filter((item) => item.status === "active");
      if (activeSchedules.length) {
        return [...activeSchedules].sort(
          (left, right) =>
            new Date(right.updated_at || right.last_dispatched_at || 0).getTime() -
            new Date(left.updated_at || left.last_dispatched_at || 0).getTime()
        )[0];
      }

      const awaitingSchedules = this.schedules.filter((item) => item.awaiting_feedback);
      if (awaitingSchedules.length) {
        return [...awaitingSchedules].sort(
          (left, right) =>
            new Date(right.updated_at || right.last_dispatched_at || 0).getTime() -
            new Date(left.updated_at || left.last_dispatched_at || 0).getTime()
        )[0];
      }

      const latestExecutionScheduleId = this.executions[0]?.schedule_id || null;
      if (latestExecutionScheduleId) {
        const matched = this.schedules.find((item) => item.id === latestExecutionScheduleId);
        if (matched) {
          return matched;
        }
      }

      return (
        [...this.schedules].sort(
          (left, right) =>
            new Date(right.updated_at || right.last_dispatched_at || 0).getTime() -
            new Date(left.updated_at || left.last_dispatched_at || 0).getTime()
        )[0] || null
      );
    },

    activeScheduleId() {
      return this.activeSchedule?.id || null;
    },

    editableTargetSchedule() {
      return this.activeSchedule || null;
    },

    sortedPendingExecutions() {
      const pendingExecutions = this.activeScheduleId
        ? this.pendingExecutions.filter((item) => item.schedule_id === this.activeScheduleId)
        : this.pendingExecutions;

      return [...pendingExecutions].sort((left, right) => {
        const leftActivity = this.uploadActivityTimestamp(left);
        const rightActivity = this.uploadActivityTimestamp(right);
        if (leftActivity !== null || rightActivity !== null) {
          return (rightActivity || 0) - (leftActivity || 0);
        }
        return new Date(right.updated_at || right.dispatched_at || 0).getTime()
          - new Date(left.updated_at || left.dispatched_at || 0).getTime();
      });
    },

    currentPredictionExecution() {
      if (this.activeScheduleId) {
        return (
          this.sortedPendingExecutions[0] ||
          this.executions.find((item) => item.schedule_id === this.activeScheduleId) ||
          null
        );
      }
      return this.sortedPendingExecutions[0] || this.executions[0] || null;
    },

    latestClosedExecution() {
      const executions = this.activeScheduleId
        ? this.executions.filter((item) => item.schedule_id === this.activeScheduleId)
        : this.executions;
      return (
        executions.find((item) => item.status !== "awaiting_feedback") || null
      );
    },

    latestRenderableClosedExecution() {
      const pending = this.currentPredictionExecution;
      if (!pending) {
        return this.latestClosedExecution;
      }

      const sameScheduleClosed = this.executions.filter(
        (item) =>
          item.schedule_id === pending.schedule_id && item.status !== "awaiting_feedback"
      );
      const preferred = sameScheduleClosed.find((item) => this.executionIsRenderableClosedLoop(item));
      return preferred || sameScheduleClosed[0] || this.latestClosedExecution;
    },

    currentMonitorHandoffPair() {
      for (const pending of this.sortedPendingExecutions) {
        if (
          !this.executionHasModelDispatch(pending) ||
          this.executionHasUploadedInstruments(pending)
        ) {
          continue;
        }

        const latestClosed = this.executions.find((item) => {
          if (
            item.schedule_id !== pending.schedule_id ||
            item.status === "awaiting_feedback"
          ) {
            return false;
          }
          return this.executionIsRenderableClosedLoop(item);
        });

        if (latestClosed) {
          return { pending, latestClosed };
        }
      }
      return null;
    },

    currentMonitorExecution() {
      const handoff = this.currentMonitorHandoffPair;
      if (handoff) {
        return handoff.latestClosed;
      }

      const pending = this.currentPredictionExecution;
      const latestClosed = this.latestRenderableClosedExecution;

      if (
        pending &&
        latestClosed &&
        pending.id !== latestClosed.id &&
        this.executionHasModelDispatch(pending) &&
        !this.executionHasUploadedInstruments(pending)
      ) {
        return latestClosed;
      }

      return pending || latestClosed || null;
    },

    currentMonitorDisplayExecution() {
      if (this.currentPredictionExecution?.workflow_runtime || this.currentPredictionExecution?.dispatch_payload?.workflow_version_id) {
        return this.currentPredictionExecution;
      }
      return this.currentMonitorHandoffPair?.latestClosed || this.currentMonitorExecution;
    },

    currentMonitorPendingExecution() {
      return this.currentMonitorHandoffPair?.pending || null;
    },

    currentRoundProgress() {
      const execution = this.currentMonitorDisplayExecution;
      if (!execution) {
        return { percent: 0, completed: 0, total: 0, round: "-", label: "暂无轮次", arcD: "M 30 120 A 90 90 0 0 1 30 120" };
      }
      const nodes = this.executionFlowNodes(execution);
      const runtimeMonitor = execution.workflow_runtime?.graph ? runtimeToMonitor(execution.workflow_runtime) : null;
      const total = runtimeMonitor ? runtimeMonitor.total : nodes.length || 1;
      const completed = runtimeMonitor ? runtimeMonitor.completed : nodes.filter(
        (n) => n.status === "completed" || n.status === "uploaded" || n.status === "triggered"
      ).length;
      const percent = total ? Math.round((completed / total) * 100) : runtimeMonitor?.status === "completed" ? 100 : 0;
      const round = this.executionRound(execution);
      const label = percent >= 100 ? "本轮完成" : `${completed}/${total} 阶段`;
      // semicircle arc: center (120,120), r=90, sweep 180°
      // angle goes from π (left) to 0 (right), sweep along bottom
      const r = 90;
      const cx = 120, cy = 120;
      const angle = Math.PI - (percent / 100) * Math.PI; // π at 0%, 0 at 100%
      const nodeX = cx + r * Math.cos(angle);
      const nodeY = cy - r * Math.sin(angle); // negative because SVG y goes down
      // build arc path from start (30,120) to current position
      const sweep = (percent / 100) * Math.PI;
      const endX = cx + r * Math.cos(Math.PI - sweep);
      const endY = cy - r * Math.sin(Math.PI - sweep);
      const largeArc = percent > 50 ? 1 : 0;
      const arcD = `M 30 120 A ${r} ${r} 0 0 1 ${endX} ${endY}`;
      return {
        percent,
        completed,
        total,
        round,
        label,
        nodeX,
        nodeY,
        arcD,
      };
    },

    currentMonitorSourceText() {
      const handoff = this.currentMonitorHandoffPair;
      if (handoff) {
        return "上一轮刚完成";
      }

      const pending = this.currentPredictionExecution;
      const latestClosed = this.latestRenderableClosedExecution;
      const monitor = this.currentMonitorExecution;

      if (
        monitor &&
        latestClosed &&
        monitor.id === latestClosed.id &&
        pending &&
        pending.id !== latestClosed.id &&
        this.executionHasModelDispatch(pending) &&
        !this.executionHasUploadedInstruments(pending)
      ) {
        return "上一轮刚完成";
      }
      if (monitor && pending && monitor.id === pending.id && pending.status === "awaiting_feedback") {
        return "当前待上传轮次";
      }
      if (monitor && latestClosed && monitor.id === latestClosed.id) {
        return "最近一轮回看";
      }
      return "暂无执行可监控";
    },

    heroOverviewOption() {
      const execution = this.currentMonitorDisplayExecution;
      return buildHeroGaugeOption({
        execution,
        monitor: this.executionMonitor(execution),
        scheduleName: execution ? this.scheduleName(execution) : "等待首轮派发",
        roundLabel: execution ? `第 ${this.executionRound(execution)} 轮` : "暂无轮次",
        targetHardness: execution ? this.executionContext(execution).target_hardness_gpa : null,
      });
    },

    currentExecutionPredictionSpace() {
      const execution = this.currentPredictionExecution;
      return execution ? this.executionContext(execution).predicted_composition_space || null : null;
    },

    currentExecutionPredictionRows() {
      return this.intervalRowsFromSpace(this.currentExecutionPredictionSpace);
    },

    predictionDeckData() {
      if (this.primaryRecommendation) {
        return {
          kicker: "Next Space",
          title: "下一轮建议",
          meta: "下一轮建议由系统自动应用；全程仅目标硬度可调整",
          source: this.primaryRecommendation.suggested_payload || {},
          rangeSpace: this.primaryRecommendation.suggested_payload?.predicted_composition_space || null,
          rangeTheme: "sage",
          rowKeyPrefix: `next-${this.primaryRecommendation.schedule_id}`,
        };
      }

      if (this.currentPredictionExecution) {
        return {
          kicker: "Current Space",
          title: "当前执行预测结果",
          meta:
            this.executionContext(this.currentPredictionExecution).target_hardness_gpa !== undefined &&
            this.executionContext(this.currentPredictionExecution).target_hardness_gpa !== null
              ? `第 ${this.executionRound(this.currentPredictionExecution)} 轮 · 目标硬度 ${this.executionContext(this.currentPredictionExecution).target_hardness_gpa} GPa`
              : `第 ${this.executionRound(this.currentPredictionExecution)} 轮`,
          source: this.executionContext(this.currentPredictionExecution),
          rangeSpace: this.currentExecutionPredictionSpace,
          rangeTheme: "warm",
          rowKeyPrefix: "current",
        };
      }

      return null;
    },

    currentMonitorModelPayload() {
      const execution = this.currentMonitorDisplayExecution;
      if (!execution) {
        return null;
      }

      const executionPayload = this.executionContext(execution);
      const recommendation = this.recommendationForExecution(execution);
      const recommendationPayload =
        recommendation && recommendation.source_execution_id === execution.id
          ? recommendation.suggested_payload || null
          : null;
      const hasModelResult = (payload) =>
        Boolean(
          payload &&
            (payload.mat_model_candidate_rank !== null &&
              payload.mat_model_candidate_rank !== undefined)
        ) || Boolean(payload?.mat_model_online_refresh_applied);

      if (execution.status === "awaiting_feedback" && hasModelResult(executionPayload)) {
        return executionPayload;
      }
      if (hasModelResult(recommendationPayload)) {
        return recommendationPayload;
      }
      if (hasModelResult(executionPayload)) {
        return executionPayload;
      }
      return null;
    },

    currentMonitorModelResult() {
      const payload = this.currentMonitorModelPayload;
      if (!payload) {
        return null;
      }

      const rank = payload.mat_model_candidate_rank;
      const predictedHardness = payload.mat_model_predicted_hardness_gpa;
      const predictedStd = payload.mat_model_predicted_std_gpa;
      const generatedAt = payload.mat_model_online_refresh_generated_at;
      const refreshSampleCount = payload.mat_model_online_refresh_sample_count;

      return {
        title: payload.predicted_composition_space
          ? "成分区间已生成"
          : payload.mat_model_online_refresh_applied
            ? "模型刷新已完成"
            : "模型结果",
        summary: this.compositionSummary(payload),
        composition:
          payload.mat_model_candidate_composition_at_percent ||
          payload.predicted_composition_at_percent
            ? `当前调度代表点：${this.formatComposition(payload)}`
            : null,
        rank,
        predictedHardness:
          predictedHardness !== null && predictedHardness !== undefined
            ? Number(predictedHardness).toFixed(2)
            : null,
        predictedStd:
          predictedStd !== null && predictedStd !== undefined
            ? Number(predictedStd).toFixed(2)
            : null,
        roleLabel: payload.mat_model_selection_role_label || null,
        generatedAt,
        refreshSampleCount:
          refreshSampleCount !== null && refreshSampleCount !== undefined
            ? Number(refreshSampleCount)
            : null,
        nextAction: payload.next_action || null,
      };
    },

    primaryRecommendation() {
      const scheduleId =
        this.currentMonitorPendingExecution?.schedule_id ||
        this.currentMonitorDisplayExecution?.schedule_id ||
        this.currentPredictionExecution?.schedule_id ||
        null;

      if (scheduleId) {
        const matched = this.recommendations.find((item) => item.schedule_id === scheduleId);
        if (matched) {
          return matched;
        }
      }

      return this.recommendations[0] || null;
    },

    dashboardComposerMode() {
      if (!this.activeSchedule && this.pendingExecutions.length === 0) {
        return "create";
      }
      return "wait";
    },

  },

  watch: {
    autoRefreshEnabled() {
      this.syncAutoRefresh();
    },
    themeMode(value) {
      this.applyThemeMode(value);
    },
  },

  mounted() {
    this.applyThemeMode(this.themeMode);
    if (this.isWorkflowBuilderMode) return;
    if (this.isExecutionDetailMode) {
      void this.refreshExecutionDetail();
    } else {
      void this.refreshDashboard();
    }
    this.syncAutoRefresh();
  },

  beforeUnmount() {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
  },

  methods: {
    applyThemeMode(mode) {
      const normalized = mode === "dark" ? "dark" : "light";
      document.documentElement.dataset.theme = normalized;
      document.body.classList.toggle("theme-dark", normalized === "dark");
      document.body.classList.toggle("theme-light", normalized === "light");
      window.localStorage.setItem("smart_scheduler_theme", normalized);
    },

    toggleTheme() {
      this.themeMode = this.themeMode === "dark" ? "light" : "dark";
    },

    executionHasModelDispatch(execution) {
      const payload = execution?.dispatch_payload || {};
      const backendStatus = execution?.model_inference?.status || "";
      return Boolean(
        backendStatus === "dispatch_scheduled" ||
          backendStatus === "dispatched" ||
          backendStatus === "candidate_ready" ||
          backendStatus === "refreshed" ||
          payload.mat_model_online_refresh_applied ||
          (payload.mat_model_candidate_rank !== null &&
            payload.mat_model_candidate_rank !== undefined)
      );
    },

    executionObservation(execution) {
      return execution?.feedback_observation || {};
    },

    executionAutoDispatch(execution) {
      const autoDispatch = this.executionObservation(execution).auto_dispatch;
      return autoDispatch && typeof autoDispatch === "object" ? autoDispatch : {};
    },

    instrumentObservationParsed(instrumentCode, observation) {
      if (!observation || typeof observation !== "object") {
        return false;
      }
      if (instrumentCode === "xrd") {
        return Boolean(
          String(observation.xrd_phase || "").trim() || String(observation.xrd_note || "").trim()
        );
      }
      if (instrumentCode === "eds") {
        return this.hasMeasuredComposition(observation);
      }
      if (instrumentCode === "nanoindentation") {
        return observation.hardness_gpa !== undefined && observation.hardness_gpa !== null;
      }
      if (instrumentCode === "magnetron_sputtering") {
        return Boolean(
          observation.deposition_completed ||
            observation.deposition_sample_id ||
            observation.sputter_run_id
        );
      }
      return false;
    },

    executionFeedbackSnapshot(execution) {
      const snapshot = execution?.feedback_snapshot;
      if (snapshot && typeof snapshot === "object") {
        return snapshot;
      }

      const observation = this.executionObservation(execution);
      const workflowProgress =
        observation.workflow_progress && typeof observation.workflow_progress === "object"
          ? observation.workflow_progress
          : execution?.dispatch_payload?.workflow_run &&
              typeof execution.dispatch_payload.workflow_run === "object"
            ? execution.dispatch_payload.workflow_run
            : null;

      if (!workflowProgress) {
        return null;
      }

      return {
        execution_id: execution?.id || "",
        closed_loop_ready: Boolean(observation.closed_loop_ready || workflowProgress.is_complete),
        current_step: workflowProgress.current_step || null,
        current_output: workflowProgress.current_output || null,
        completed_count: Number(workflowProgress.completed_count || 0),
        total_steps: Number(
          workflowProgress.total_steps ||
            (Array.isArray(workflowProgress.steps) ? workflowProgress.steps.length : 0)
        ),
        completion_ratio: Number(workflowProgress.completion_ratio || 0),
        missing_steps: Array.isArray(workflowProgress.missing_steps)
          ? workflowProgress.missing_steps
          : [],
        workflow_progress: workflowProgress,
      };
    },

    executionWorkflowProgress(execution) {
      return this.executionFeedbackSnapshot(execution)?.workflow_progress || null;
    },

    executionInstrumentStateMap(execution) {
      const stateMap = {};
      const structuredStates = Array.isArray(execution?.instrument_states)
        ? execution.instrument_states
        : [];

      for (const row of structuredStates) {
        if (!row?.instrument_code) {
          continue;
        }
        stateMap[row.instrument_code] = {
          instrument_code: row.instrument_code,
          component_id: row.component_id || "",
          status: this.normalizedUploadStatus(row),
          triggered_at: row.triggered_at || null,
          uploaded_at: row.uploaded_at || null,
          parsed_at: row.parsed_at || null,
          failed_at: row.failed_at || null,
          collected_at: row.collected_at || null,
          note: row.note || "",
          error_message: row.error_message || "",
        };
      }

      const observation = this.executionObservation(execution);
      const legacyUploads = observation.instrument_uploads || {};

      for (const [instrumentCode, uploadState] of Object.entries(legacyUploads)) {
        if (stateMap[instrumentCode]) {
          continue;
        }
        if (!uploadState || typeof uploadState !== "object") {
          continue;
        }

        let status = this.normalizedUploadStatus(uploadState);
        const parsedByObservation = this.instrumentObservationParsed(instrumentCode, observation);
        if (status !== "failed" && parsedByObservation) {
          status = "parsed";
        }

        stateMap[instrumentCode] = {
          instrument_code: instrumentCode,
          component_id: uploadState.component_id || "",
          status,
          triggered_at:
            uploadState.triggered_at ||
            uploadState.uploaded_at ||
            uploadState.failed_at ||
            uploadState.collected_at ||
            null,
          uploaded_at:
            uploadState.uploaded_at ||
            (status === "parsed" ? uploadState.collected_at || null : null),
          parsed_at:
            uploadState.parsed_at ||
            (status === "parsed"
              ? uploadState.uploaded_at || uploadState.collected_at || null
              : null),
          failed_at: uploadState.failed_at || null,
          collected_at: uploadState.collected_at || null,
          note: uploadState.note || "",
          error_message: uploadState.error_message || uploadState.error || "",
        };
      }

      return stateMap;
    },

    executionClosedLoopReady(execution) {
      const snapshot = this.executionFeedbackSnapshot(execution);
      return Boolean(snapshot?.closed_loop_ready || snapshot?.workflow_progress?.is_complete);
    },

    executionIsRenderableClosedLoop(execution) {
      return Boolean(
        this.executionClosedLoopReady(execution) ||
          this.executionAutoDispatch(execution).triggered ||
          execution?.status === "success" ||
          execution?.status === "partial"
      );
    },

    uploadActivityTimestamp(execution) {
      const uploads = this.executionInstrumentStateMap(execution);
      let latest = null;
      for (const upload of Object.values(uploads)) {
        if (!upload || typeof upload !== "object") {
          continue;
        }
        for (const key of [
          "parsed_at",
          "uploaded_at",
          "failed_at",
          "triggered_at",
          "collected_at",
        ]) {
          const value = upload[key];
          if (!value) {
            continue;
          }
          const timestamp = new Date(value).getTime();
          if (Number.isFinite(timestamp) && (latest === null || timestamp > latest)) {
            latest = timestamp;
          }
        }
      }
      return latest;
    },

    actionKey(action, id) {
      return `${action}:${id}`;
    },

    isPending(key) {
      return Boolean(this.actionState[key]);
    },

    async runWithAction(key, callback) {
      if (this.actionState[key]) {
        return;
      }
      this.actionState[key] = true;
      try {
        await callback();
      } finally {
        this.actionState[key] = false;
      }
    },

    setMessage(text, tone = "neutral") {
      this.message = { text, tone };
    },

    statusLabel(status) {
      const labels = {
        active: "Active",
        paused: "Paused",
        awaiting_feedback: "待上传",
        success: "Success",
        partial: "Partial",
        failure: "Failure",
        timeout: "Timeout",
      };
      return labels[status] || status;
    },

    stageStateLabel(status, code = "") {
      if (code === "model_inference" && status === "pending") {
        return "待预测开始";
      }
      if (["xrd", "eds", "nanoindentation"].includes(code) && ["current", "triggered", "pending"].includes(status)) {
        return "待上传";
      }
      const labels = {
        completed: "已完成",
        current: "进行中",
        triggered: "已触发",
        uploaded: "已完成",
        pending: "待上传",
        failed: "失败",
      };
      return labels[status] || status;
    },

    formatDateTime(value) {
      if (!value) {
        return "-";
      }
      return new Intl.DateTimeFormat("zh-CN", {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      }).format(new Date(value));
    },

    formatTime(value) {
      return new Intl.DateTimeFormat("zh-CN", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(value);
    },

    rangeLabel(stats) {
      if (!stats) {
        return "-";
      }
      return `${Number(stats.min).toFixed(1)}-${Number(stats.max).toFixed(1)}`;
    },

    medianLabel(stats) {
      if (!stats || stats.median === undefined || stats.median === null) {
        return "at.%";
      }
      return `P50 ${Number(stats.median).toFixed(1)} at.%`;
    },

    intervalValue(stats, key) {
      const value = stats?.[key];
      if (value === undefined || value === null || Number.isNaN(Number(value))) {
        return "-";
      }
      return Number(value).toFixed(1);
    },

    intervalRowsFromSpace(space) {
      const labels = {
        ti_at: "Ti",
        zr_at: "Zr",
        nb_at: "Nb",
        v_at: "V",
      };

      return quaternaryKeys.map((key) => {
        const stats = space?.elements?.[key] || {};
        return {
          key,
          label: labels[key],
          min: this.intervalValue(stats, "min"),
          p10: this.intervalValue(stats, "p10"),
          p50: this.intervalValue(stats, "median"),
          p90: this.intervalValue(stats, "p90"),
          max: this.intervalValue(stats, "max"),
        };
      });
    },

    persistedTargetHardness(schedule) {
      const payload = this.schedulePayload(schedule);
      return toOptionalNumber(
        payload?.target_hardness_gpa ?? payload?.target_metric_value ?? null
      );
    },

    syncActiveTargetHardnessDraft(force = false) {
      const schedule = this.editableTargetSchedule;
      const scheduleId = schedule?.id || "";
      const nextValue = schedule ? this.persistedTargetHardness(schedule) : null;
      const serializedValue =
        nextValue === null || nextValue === undefined ? "" : String(nextValue);

      if (
        force ||
        scheduleId !== this.activeTargetHardnessScheduleId ||
        !this.activeTargetHardnessDirty
      ) {
        this.activeTargetHardnessDraft = serializedValue;
        this.activeTargetHardnessScheduleId = scheduleId;
        this.activeTargetHardnessDirty = false;
      }
    },

    markActiveTargetHardnessDirty() {
      this.activeTargetHardnessDirty = true;
    },

    sputterPowerRows(source) {
      const plan = source?.sputter_power_plan_w;
      if (!plan || typeof plan !== "object") {
        return [];
      }

      const labels = {
        ti_at: "Ti",
        zr_at: "Zr",
        nb_at: "Nb",
        v_at: "V",
      };

      return quaternaryKeys
        .map((key) => {
          const value = plan[key];
          if (value === undefined || value === null || Number.isNaN(Number(value))) {
            return null;
          }
          return {
            key,
            label: labels[key],
            value: `${Number(value).toFixed(1)} W`,
          };
        })
        .filter(Boolean);
    },

    predictionElementCards(source) {
      const intervalRows = this.intervalRowsFromSpace(source?.predicted_composition_space || null);
      const powerRows = Object.fromEntries(
        this.sputterPowerRows(source).map((row) => [row.key, row])
      );

      return intervalRows.map((row) => ({
        ...row,
        power: powerRows[row.key]?.value || "-",
      }));
    },

    formatComposition(source) {
      const composition =
        source?.mat_model_candidate_composition_at_percent ||
        source?.predicted_composition_at_percent ||
        source?.measured_composition_at_percent ||
        compactObject({
          ti_at: toOptionalNumber(source?.ti_at),
          zr_at: toOptionalNumber(source?.zr_at),
          nb_at: toOptionalNumber(source?.nb_at),
          v_at: toOptionalNumber(source?.v_at),
        });

      const keys = quaternaryKeys.filter((key) => composition[key] !== undefined);
      if (keys.length !== 4) {
        return "未定义四元成分";
      }
      return keys
        .map((key) => `${key.split("_")[0].toUpperCase()} ${Number(composition[key]).toFixed(1)}`)
        .join(" / ") + " at.%";
    },

    compositionSpace(source) {
      return source?.predicted_composition_space || null;
    },

    compositionSummary(source) {
      const space = this.compositionSpace(source);
      if (space?.summary) {
        return space.summary;
      }
      if (source?.composition_summary) {
        return source.composition_summary;
      }
      return this.formatComposition(source);
    },

    batchPlan(source) {
      return source?.predicted_batch_plan || null;
    },

    batchPlanLabel(source) {
      const plan = this.batchPlan(source);
      if (!plan) {
        return null;
      }
      return `${plan.rows}×${plan.cols} / ${plan.total_samples} 点`;
    },

    findSchedule(scheduleId) {
      return this.schedules.find((item) => item.id === scheduleId) || null;
    },

    schedulePayload(schedule) {
      return schedule?.payload || {};
    },

    workflowSteps(schedule) {
      return this.schedulePayload(schedule).workflow_steps || [];
    },

    executionWorkflow(execution) {
      const feedbackProgress = this.executionWorkflowProgress(execution);
      if (feedbackProgress?.steps?.length) {
        return feedbackProgress;
      }

      const dispatchProgress = execution?.dispatch_payload?.workflow_run;
      if (dispatchProgress?.steps?.length) {
        return dispatchProgress;
      }

      const steps = (execution?.dispatch_payload?.workflow_steps || []).map((step, index) => ({
        ...step,
        status: index === 0 ? "current" : "pending",
      }));
      return {
        steps,
        completed_count: 0,
        total_steps: steps.length,
        current_step: steps[0]?.code || null,
        is_complete: false,
        missing_steps: steps.map((step) => ({ code: step.code, title: step.title })),
      };
    },

    workflowSummary(execution) {
      const monitor = this.executionMonitor(execution);
      if (!monitor) {
        return "未定义工序计划";
      }
      return monitor.summary;
    },

    workflowMissingText(execution) {
      if (execution?.workflow_runtime?.graph) return runtimeToMonitor(execution.workflow_runtime).hint || "流程已完成";
      if (execution?.dispatch_payload?.workflow_version_id) return "固定版本流程图未加载";
      return this.executionMonitor(execution)?.hint || "当前无缺失步骤";
    },

    stepStatusLabel(status) {
      const labels = {
        completed: "已完成",
        current: "当前",
        pending: "待执行",
      };
      return labels[status] || status;
    },

    executionContext(execution) {
      return execution.dispatch_payload || this.findSchedule(execution.schedule_id)?.payload || {};
    },

    executionRound(execution) {
      return execution?.dispatch_payload?.experiment_round_index || execution.attempt || "-";
    },

    executionBatch(execution) {
      return (
        execution?.dispatch_payload?.experiment_batch_id ||
        execution?.dispatch_payload?.sample_batch ||
        "-"
      );
    },

    scheduleName(execution) {
      return this.findSchedule(execution.schedule_id)?.name || execution.schedule_id;
    },

    executionTarget(execution) {
      return this.findSchedule(execution.schedule_id)?.target || "未知目标";
    },

    currentExecutionForSchedule(schedule) {
      if (!schedule) {
        return null;
      }

      return (
        this.pendingExecutions.find((item) => item.schedule_id === schedule.id) ||
        this.executions.find((item) => item.schedule_id === schedule.id) ||
        null
      );
    },

    scheduleCurrentRound(schedule) {
      const execution = this.currentExecutionForSchedule(schedule);
      if (!execution) {
        return "-";
      }
      return `第 ${this.executionRound(execution)} 轮`;
    },

    scheduleCurrentRoundStartedAt(schedule) {
      const execution = this.currentExecutionForSchedule(schedule);
      return execution?.dispatched_at || execution?.scheduled_for || null;
    },

    scheduleObjective(schedule) {
      const payload = this.schedulePayload(schedule);
      return payload.objective || schedule.description || "未填写实验目标";
    },

    scheduleTags(schedule) {
      const payload = this.schedulePayload(schedule);
      return [
        payload.material_system,
        payload.workflow_name,
        payload.deposition_chamber,
        payload.measurement_metric,
      ].filter(Boolean);
    },

    scheduleConditionChips(schedule) {
      const payload = this.schedulePayload(schedule);
      return [
        this.compositionSummary(payload),
        payload.target_hardness_gpa ? `目标硬度 ${payload.target_hardness_gpa} GPa` : null,
        payload.substrate ? `基底 ${payload.substrate}` : null,
        payload.model_version ? `模型 ${payload.model_version}` : null,
        ...this.optimizationChips(payload),
      ].filter(Boolean);
    },

    optimizationChips(source) {
      if (!source || typeof source !== "object") {
        return [];
      }

      const modelVersion = String(source.model_version || "");
      const applied =
        Boolean(source.mat_model_online_refresh_applied) ||
        modelVersion.endsWith("+online");
      const sampleCount = source.mat_model_online_refresh_sample_count;

      if (applied) {
        return [
          "在线增量优化",
          sampleCount !== undefined && sampleCount !== null
            ? `闭环样本 ${Number(sampleCount)} 条`
            : null,
        ].filter(Boolean);
      }

      if (modelVersion || source.mat_model_candidate_rank) {
        return ["基础 mat_model 候选"];
      }

      return [];
    },

    instrumentComponents(source) {
      return source?.instrument_components || {};
    },

    normalizedUploadStatus(uploadState) {
      const status = String(uploadState?.status || "").trim().toLowerCase();
      if (status === "success") {
        return "uploaded";
      }
      return status || "pending";
    },

    uploadFailureDetail(uploadState, fallbackTitle) {
      const failedAt = uploadState?.failed_at
        ? `上传失败 ${this.formatDateTime(uploadState.failed_at)}`
        : `${fallbackTitle} 上传失败`;
      const note = String(uploadState?.note || "").trim();
      return note ? `${failedAt}，${note}` : failedAt;
    },

    instrumentComponentChips(schedule) {
      const components = this.instrumentComponents(this.schedulePayload(schedule));
      return Object.values(components).map(
        (component) => `${component.title} · ${component.component_id}`
      );
    },

    executionInstrumentStates(execution) {
      const components = this.instrumentComponents(execution.dispatch_payload || {});
      const uploads = this.executionInstrumentStateMap(execution);
      return Object.entries(components).map(([code, component]) => {
        const state = uploads[code] || {};
        const uploadStatus = this.normalizedUploadStatus(state);
        const parsed = uploadStatus === "parsed";
        return {
          code,
          title: component.title,
          componentId: component.component_id,
          detail:
            uploadStatus === "failed"
              ? this.uploadFailureDetail(state, component.title)
              : parsed || state.uploaded_at
                ? `最近上传 ${this.formatDateTime(state.parsed_at || state.uploaded_at)}`
                : uploadStatus === "triggered"
                  ? "上传已触发，等待数据上送"
                  : "等待上传",
          status:
            uploadStatus === "failed"
              ? "failed"
              : parsed || state.uploaded_at
                ? "completed"
                : uploadStatus === "triggered"
                  ? "triggered"
                  : "pending",
        };
      });
    },

    instrumentComponentId(execution, instrumentCode) {
      return this.instrumentComponents(execution.dispatch_payload || {})[instrumentCode]?.component_id || "";
    },

    recommendationForExecution(execution) {
      if (!execution) {
        return null;
      }
      return this.recommendations.find((item) => item.schedule_id === execution.schedule_id) || null;
    },

    executionHasUploadedInstruments(execution) {
      const uploads = this.executionInstrumentStateMap(execution);
      return Object.values(uploads).some((item) =>
        Boolean(item?.uploaded_at || item?.parsed_at || item?.failed_at || item?.triggered_at)
      );
    },

    pendingModelHandoffExecution(execution) {
      if (!execution) {
        return null;
      }

      return (
        this.sortedPendingExecutions.find((item) => {
          if (item.schedule_id !== execution.schedule_id) {
            return false;
          }
          if (item.id === execution.id) {
            return false;
          }
          if (!this.executionHasModelDispatch(item)) {
            return false;
          }
          return !this.executionHasUploadedInstruments(item);
        }) || null
      );
    },

    hasMeasuredComposition(observation) {
      const measured = observation?.measured_composition_at_percent;
      if (
        measured &&
        Object.values(measured).some((value) => value !== null && value !== undefined && value !== "")
      ) {
        return true;
      }

      return ["eds_ti_at", "eds_zr_at", "eds_nb_at", "eds_v_at"].some(
        (key) => observation?.[key] !== null && observation?.[key] !== undefined && observation?.[key] !== ""
      );
    },

    executionMonitor(execution) {
      if (execution?.workflow_runtime?.graph) return runtimeToMonitor(execution.workflow_runtime);
      if (execution?.dispatch_payload?.workflow_version_id) return null;
      if (!execution) {
        return null;
      }

      const observation = this.executionObservation(execution);
      const uploads = this.executionInstrumentStateMap(execution);
      const feedbackSnapshot = this.executionFeedbackSnapshot(execution);
      const workflowProgress =
        feedbackSnapshot?.workflow_progress && typeof feedbackSnapshot.workflow_progress === "object"
          ? feedbackSnapshot.workflow_progress
          : execution.dispatch_payload?.workflow_run && typeof execution.dispatch_payload.workflow_run === "object"
            ? execution.dispatch_payload.workflow_run
            : null;
      const workflowStepStatus = Object.fromEntries(
        Array.isArray(workflowProgress?.steps)
          ? workflowProgress.steps
              .filter((step) => step && typeof step === "object" && step.code)
              .map((step) => [step.code, step.status || "pending"])
          : []
      );
      const components = this.instrumentComponents(execution.dispatch_payload || {});
      const autoDispatch = this.executionAutoDispatch(execution);
      const recommendation = this.recommendationForExecution(execution);
      const recommendationPayload = recommendation?.suggested_payload || {};
      const dispatchPayload = execution.dispatch_payload || {};
      const backendInference = execution.model_inference || null;
      const backendInferenceStatus = String(backendInference?.status || "");
      const backendInferenceFailed = backendInferenceStatus === "failed";
      const backendInferenceFailureText = String(
        backendInference?.error || backendInference?.summary || ""
      ).trim();
      const pendingHandoff = this.pendingModelHandoffExecution(execution);
      const handoffDispatchPayload = pendingHandoff?.dispatch_payload || {};
      const prepStarted = Boolean(
        execution.dispatched_at ||
          execution.scheduled_for ||
          execution.id ||
          execution.dispatch_payload?.experiment_batch_id
      );

      const prepComplete = prepStarted;
      const xrdUploadStatus = this.normalizedUploadStatus(uploads.xrd);
      const edsUploadStatus = this.normalizedUploadStatus(uploads.eds);
      const nanoUploadStatus = this.normalizedUploadStatus(uploads.nanoindentation);
      const xrdUploaded = Boolean(uploads.xrd?.uploaded_at || uploads.xrd?.parsed_at);
      const edsUploaded = Boolean(uploads.eds?.uploaded_at || uploads.eds?.parsed_at);
      const nanoUploaded = Boolean(
        uploads.nanoindentation?.uploaded_at || uploads.nanoindentation?.parsed_at
      );
      const xrdParsed = xrdUploadStatus === "parsed";
      const edsParsed = edsUploadStatus === "parsed";
      const nanoParsed = nanoUploadStatus === "parsed";
      const xrdFailed = xrdUploadStatus === "failed";
      const edsFailed = edsUploadStatus === "failed";
      const nanoFailed = nanoUploadStatus === "failed";
      const xrdComplete = workflowStepStatus.xrd === "completed" || xrdParsed || xrdUploaded;
      const edsComplete = workflowStepStatus.eds === "completed" || edsParsed || edsUploaded;
      const nanoComplete =
        workflowStepStatus.nanoindentation === "completed" || nanoParsed || nanoUploaded;
      const characterizationCompleteCount = [xrdComplete, edsComplete, nanoComplete].filter(Boolean).length;
      const characterizationUploadedCount = [xrdUploaded, edsUploaded, nanoUploaded].filter(Boolean).length;
      const failedCharacterizations = [
        xrdFailed ? "XRD" : null,
        edsFailed ? "EDS" : null,
        nanoFailed ? "纳米压痕" : null,
      ].filter(Boolean);
      const missingCharacterizations = [
        !xrdComplete ? "XRD" : null,
        !edsComplete ? "EDS" : null,
        !nanoComplete ? "纳米压痕" : null,
      ].filter(Boolean);

      const inferenceUnlocked = Boolean(
        feedbackSnapshot?.closed_loop_ready ||
        workflowProgress?.is_complete ||
        characterizationCompleteCount === 3
      );
      const inferenceTriggered =
        backendInferenceStatus === "dispatched" || Boolean(autoDispatch.triggered);
      const inferenceScheduled =
        backendInferenceStatus === "dispatch_scheduled" || Boolean(autoDispatch.scheduled);
      const dispatchedModelRefreshApplied = Boolean(
        dispatchPayload.mat_model_online_refresh_applied ||
        handoffDispatchPayload.mat_model_online_refresh_applied
      );
      const dispatchedCandidateReady =
        (dispatchPayload.mat_model_candidate_rank !== null &&
          dispatchPayload.mat_model_candidate_rank !== undefined) ||
        (handoffDispatchPayload.mat_model_candidate_rank !== null &&
          handoffDispatchPayload.mat_model_candidate_rank !== undefined);
      const inferenceRefreshApplied = Boolean(
        backendInferenceStatus === "refreshed" ||
        dispatchedModelRefreshApplied ||
          (recommendation &&
            recommendation.source_execution_id === execution.id &&
            recommendationPayload.mat_model_online_refresh_applied)
      );
      const inferenceCandidateReady = Boolean(
        backendInferenceStatus === "candidate_ready" ||
          backendInferenceStatus === "dispatch_scheduled" ||
          backendInferenceStatus === "dispatched" ||
        dispatchedCandidateReady ||
          (recommendation &&
            recommendation.source_execution_id === execution.id &&
            recommendationPayload.mat_model_candidate_rank !== null &&
            recommendationPayload.mat_model_candidate_rank !== undefined)
      );
      const currentRoundAwaitingPredictionStart = Boolean(
        execution.status === "awaiting_feedback" && characterizationCompleteCount < 3
      );
      const inferenceInvoked =
        !currentRoundAwaitingPredictionStart &&
        (inferenceTriggered || inferenceCandidateReady || inferenceRefreshApplied);
      const inferenceCompleted =
        !currentRoundAwaitingPredictionStart &&
        (inferenceTriggered ||
          inferenceScheduled ||
          inferenceCandidateReady ||
          inferenceRefreshApplied);
      const gateStatus = currentRoundAwaitingPredictionStart
        ? "locked"
        : inferenceTriggered
          ? "triggered"
          : inferenceUnlocked
            ? "open"
            : "locked";

      const prepDetail = observation.deposition_sample_id
        ? `样品 ${observation.deposition_sample_id}`
        : observation.sputter_run_id
          ? `Run ${observation.sputter_run_id}`
          : execution.dispatch_payload?.experiment_batch_id
            ? `已派发批次 ${execution.dispatch_payload.experiment_batch_id}`
            : "本轮一经开始即视为制备完成";

      const xrdDetail = xrdComplete
        ? (uploads.xrd?.parsed_at || uploads.xrd?.uploaded_at
            ? `已上传 ${this.formatDateTime(uploads.xrd?.parsed_at || uploads.xrd?.uploaded_at)}`
            : "XRD 批次已完成")
        : xrdFailed
          ? this.uploadFailureDetail(uploads.xrd, "XRD")
          : prepComplete
            ? "并行表征已开放，等待 XRD 上传"
            : "制备完成后开放";

      const edsDetail = edsComplete
        ? (uploads.eds?.parsed_at || uploads.eds?.uploaded_at
            ? `已上传 ${this.formatDateTime(uploads.eds?.parsed_at || uploads.eds?.uploaded_at)}`
            : "EDS 批次已完成")
        : edsFailed
          ? this.uploadFailureDetail(uploads.eds, "EDS")
          : prepComplete
            ? "并行表征已开放，等待 EDS 上传"
            : "制备完成后开放";

      const nanoDetail =
        nanoComplete
          ? (uploads.nanoindentation?.parsed_at || uploads.nanoindentation?.uploaded_at
              ? `已上传 ${this.formatDateTime(uploads.nanoindentation?.parsed_at || uploads.nanoindentation?.uploaded_at)}`
              : "纳米压痕批次已完成")
          : nanoFailed
            ? this.uploadFailureDetail(uploads.nanoindentation, "纳米压痕")
            : prepComplete
              ? "并行表征已开放，等待纳米压痕上传"
              : "制备完成后开放";

      const inferenceDetail = backendInferenceFailed
        ? backendInferenceFailureText || "模型推理未生成下一轮候选"
        : inferenceTriggered
        ? autoDispatch.next_round_index
          ? `模型已调用，并已派发第 ${autoDispatch.next_round_index} 轮候选`
          : "模型已调用，系统已基于完整反馈触发下一轮候选"
        : inferenceScheduled
          ? (backendInference?.scheduled_for || autoDispatch.scheduled_for)
            ? `模型已完成，下一轮候选计划于 ${this.formatDateTime(backendInference?.scheduled_for || autoDispatch.scheduled_for)} 自动派发`
            : "模型已完成，下一轮候选已排队等待自动派发"
        : pendingHandoff && handoffDispatchPayload.mat_model_candidate_rank !== null &&
            handoffDispatchPayload.mat_model_candidate_rank !== undefined
          ? `模型已完成本轮刷新，并已派发第 ${this.executionRound(pendingHandoff)} 轮候选 #${handoffDispatchPayload.mat_model_candidate_rank}`
        : pendingHandoff && handoffDispatchPayload.mat_model_online_refresh_applied
          ? `模型已完成本轮刷新，并已派发第 ${this.executionRound(pendingHandoff)} 轮候选`
        : execution.status === "awaiting_feedback" && dispatchedCandidateReady
          ? `上一轮模型已完成，本轮已载入候选 #${dispatchPayload.mat_model_candidate_rank}`
        : execution.status === "awaiting_feedback" && dispatchedModelRefreshApplied
          ? "上一轮模型已完成，本轮已载入最新刷新结果"
        : inferenceCandidateReady
          ? `模型已完成本轮刷新，候选 #${recommendationPayload.mat_model_candidate_rank} 已生成`
        : inferenceRefreshApplied
            ? "模型已完成本轮刷新，结果已回到调度决策层"
        : failedCharacterizations.length
          ? `${failedCharacterizations.join(" / ")} 上传失败，需重新上传后再继续`
        : inferenceUnlocked
          ? "三个表征已全部满足，模型正在推理中"
        : characterizationUploadedCount === 3
            ? "三项表征已全部上传，等待模型推理启动"
        : prepComplete
            ? `待 ${missingCharacterizations.join(" / ")} 全部完成`
            : "制备完成并收齐三项表征后解锁";

      const linkStatusFromBranch = (nodeStatus, isComplete) => {
        if (!prepComplete) {
          return "pending";
        }
        if (nodeStatus === "failed") {
          return "failed";
        }
        if (isComplete) {
          return "completed";
        }
        return nodeStatus === "triggered" ? "triggered" : "current";
      };

      const linkStatusToInference = (nodeStatus, isComplete) => {
        if (inferenceTriggered) {
          return "completed";
        }
        if (nodeStatus === "failed") {
          return "failed";
        }
        if (inferenceUnlocked && isComplete) {
          return "completed";
        }
        return "pending";
      };

      const xrdNodeStatus = xrdFailed
        ? "failed"
        : xrdComplete
          ? "completed"
          : "pending";
      const edsNodeStatus = edsFailed
        ? "failed"
        : edsComplete
          ? "completed"
          : "pending";
      const nanoNodeStatus = nanoFailed
        ? "failed"
        : nanoComplete
          ? "completed"
          : "pending";

      return {
        summary: !prepComplete
          ? "等待制备节点完成后，三个表征节点并行开放"
          : failedCharacterizations.length
            ? `${failedCharacterizations.join(" / ")} 上传失败，当前轮次已中断`
          : backendInferenceFailed
            ? "三个表征已齐套，但模型未生成下一轮候选"
          : inferenceTriggered
            ? "三个表征已齐套，模型推理已调用并完成下一轮派发"
            : inferenceScheduled
              ? "三个表征已齐套，模型推理已完成，下一轮候选正在等待自动派发"
            : pendingHandoff && inferenceCandidateReady
              ? "三个表征已齐套，模型推理已完成并生成下一轮候选"
            : execution.status === "awaiting_feedback" && inferenceCandidateReady
              ? "模型推理已完成，当前轮次就是模型派发出的候选批次"
            : inferenceInvoked
              ? "三个表征已齐套，模型推理已调用"
            : inferenceUnlocked
              ? "三个表征已齐套，模型推理已解锁"
              : characterizationUploadedCount === 3
                ? "三项表征已全部上传，等待模型推理启动"
                : `并行表征结果已完成 ${characterizationCompleteCount}/3，待 ${missingCharacterizations.join(" / ")}`,
        hint: !prepComplete
          ? "制备是开始节点；XRD、EDS、纳米压痕没有先后约束。"
          : failedCharacterizations.length
            ? "上传失败不会自动推进；重新上传成功后，系统会继续解析并恢复门控推进。"
          : backendInferenceFailed
            ? backendInferenceFailureText || "后端已记录模型推理失败，需要先修复调度策略或模型刷新条件。"
          : inferenceTriggered
            ? "系统已用本轮闭环结果完成模型调用，并衔接下一轮候选。"
            : inferenceScheduled
              ? "模型结果已经写回，系统会在设定延迟后自动切到下一轮。"
            : pendingHandoff && inferenceCandidateReady
              ? "当前门控图展示的是刚完成的上一轮；新一轮候选已经生成并等待上传。"
            : execution.status === "awaiting_feedback" && inferenceCandidateReady
              ? "当前正在执行的这一轮，就是上一轮模型推理后自动派发出来的候选。"
            : inferenceCandidateReady
              ? "模型已完成本轮刷新，下一轮候选已经生成。"
              : inferenceRefreshApplied
                ? "模型已完成本轮刷新，当前等待调度决策写回。"
            : inferenceUnlocked
              ? "三个表征完全满足以后，才能进入模型推理节点。"
              : characterizationUploadedCount === 3
                ? "当前三项表征已经全部上传，系统将继续推进模型推理。"
                : `当前仍缺 ${missingCharacterizations.join(" / ")}；三项表征全部满足以后，才会进入模型推理。`,
        gateStatus,
        gateLabel:
          gateStatus === "triggered"
            ? "门已通过"
            : gateStatus === "open"
              ? "门已打开"
              : "表征实验未结束",
        nodes: [
          {
            code: "magnetron_sputtering",
            slot: "start",
            short: "制备",
            title: "制备",
            status: prepComplete ? "completed" : "current",
            stateLabel: prepComplete ? "已完成" : "进行中",
            componentId: components.magnetron_sputtering?.component_id || "",
            detail: prepDetail,
            isStart: true,
            isInference: false,
          },
          {
            code: "xrd",
            slot: "xrd",
            short: "XRD",
            title: "XRD 表征",
            status: xrdNodeStatus,
            stateLabel: xrdFailed ? "上传失败" : xrdComplete ? "已完成" : xrdUploaded ? "上传成功" : prepComplete ? "待上传" : "未开放",
            componentId: components.xrd?.component_id || "",
            detail: xrdDetail,
            isStart: false,
            isInference: false,
          },
          {
            code: "eds",
            slot: "eds",
            short: "EDS",
            title: "EDS 表征",
            status: edsNodeStatus,
            stateLabel: edsFailed ? "上传失败" : edsComplete ? "已完成" : edsUploaded ? "上传成功" : prepComplete ? "待上传" : "未开放",
            componentId: components.eds?.component_id || "",
            detail: edsDetail,
            isStart: false,
            isInference: false,
          },
          {
            code: "nanoindentation",
            slot: "nano",
            short: "纳米压痕",
            title: "纳米压痕",
            status: nanoNodeStatus,
            stateLabel: nanoFailed ? "上传失败" : nanoComplete ? "已完成" : nanoUploaded ? "上传成功" : prepComplete ? "待上传" : "未开放",
            componentId: components.nanoindentation?.component_id || "",
            detail: nanoDetail,
            isStart: false,
            isInference: false,
          },
          {
            code: "model_inference",
            slot: "inference",
            short: "MAT",
            title: "模型推理",
            status: currentRoundAwaitingPredictionStart ? "pending" : backendInferenceFailed ? "failed" : inferenceCompleted ? "completed" : inferenceUnlocked ? "current" : "pending",
            stateLabel: currentRoundAwaitingPredictionStart ? "待预测开始" : backendInferenceFailed ? "推理失败" : inferenceCompleted ? "待预测开始" : inferenceUnlocked ? "推理中" : "待预测开始",
            componentId: "",
            detail: inferenceDetail,
            isStart: false,
            isInference: true,
          },
        ],
        links: [
          { key: "start-xrd", lane: "branch", x1: 18, y1: 50, x2: 48, y2: 18, status: linkStatusFromBranch(xrdNodeStatus, xrdComplete) },
          { key: "start-eds", lane: "branch", x1: 18, y1: 50, x2: 48, y2: 50, status: linkStatusFromBranch(edsNodeStatus, edsComplete) },
          { key: "start-nano", lane: "branch", x1: 18, y1: 50, x2: 48, y2: 82, status: linkStatusFromBranch(nanoNodeStatus, nanoComplete) },
          { key: "xrd-inference", lane: "gate", x1: 52, y1: 18, x2: 82, y2: 50, status: linkStatusToInference(xrdNodeStatus, xrdComplete) },
          { key: "eds-inference", lane: "gate", x1: 52, y1: 50, x2: 82, y2: 50, status: linkStatusToInference(edsNodeStatus, edsComplete) },
          { key: "nano-inference", lane: "gate", x1: 52, y1: 82, x2: 82, y2: 50, status: linkStatusToInference(nanoNodeStatus, nanoComplete) },
        ],
      };
    },

    executionMonitorNodes(execution) {
      return this.executionMonitor(execution)?.nodes || [];
    },

    executionMonitorLinks(execution) {
      return this.executionMonitor(execution)?.links || [];
    },

    executionMonitorGateStatus(execution) {
      return this.executionMonitor(execution)?.gateStatus || "locked";
    },

    executionMonitorGateLabel(execution) {
      return this.executionMonitor(execution)?.gateLabel || "表征实验未结束";
    },

    nodeShortLabel(code) {
      const labels = {
        magnetron_sputtering: "制备",
        xrd: "XRD",
        eds: "EDS",
        nanoindentation: "纳米压痕",
      };
      return labels[code] || code;
    },

    nodeStatusLabel(status) {
      const labels = {
        completed: "已完成",
        current: "进行中",
        triggered: "已触发",
        failed: "失败",
        pending: "未开始",
      };
      return labels[status] || status;
    },

    executionNodeDetail(code, status, observation, uploadState) {
      const uploadStatus = this.normalizedUploadStatus(uploadState);
      if (code === "magnetron_sputtering") {
        if (observation.deposition_sample_id) {
          return `样品 ${observation.deposition_sample_id}`;
        }
        if (observation.sputter_run_id) {
          return `Run ${observation.sputter_run_id}`;
        }
        return status === "current" ? "等待制备结果回写" : "待开始制备";
      }

      if (code === "xrd") {
        if (uploadState?.uploaded_at || uploadStatus === "parsed" || observation.xrd_phase) {
          return uploadState.uploaded_at || uploadState.parsed_at
            ? `已上传 ${this.formatDateTime(uploadState.uploaded_at || uploadState.parsed_at)}`
            : "XRD 批次已完成";
        }
        if (uploadStatus === "failed") {
          return this.uploadFailureDetail(uploadState, "XRD");
        }
        return status === "triggered" ? "等待 XRD 上传" : status === "current" ? "等待 XRD 上传" : "待前序节点完成";
      }

      if (code === "eds") {
        if (uploadState?.uploaded_at || uploadStatus === "parsed" || this.hasMeasuredComposition(observation)) {
          return uploadState.uploaded_at || uploadState.parsed_at
            ? `已上传 ${this.formatDateTime(uploadState.uploaded_at || uploadState.parsed_at)}`
            : "EDS 批次已完成";
        }
        if (uploadStatus === "failed") {
          return this.uploadFailureDetail(uploadState, "EDS");
        }
        return status === "triggered" ? "等待 EDS 上传" : status === "current" ? "等待 EDS 上传" : "待前序节点完成";
      }

      if (code === "nanoindentation") {
        if (
          uploadState?.uploaded_at ||
          uploadStatus === "parsed" ||
          (observation.hardness_gpa !== undefined && observation.hardness_gpa !== null)
        ) {
          return uploadState.uploaded_at || uploadState.parsed_at
            ? `已上传 ${this.formatDateTime(uploadState.uploaded_at || uploadState.parsed_at)}`
            : "纳米压痕批次已完成";
        }
        if (uploadStatus === "failed") {
          return this.uploadFailureDetail(uploadState, "纳米压痕");
        }
        return status === "triggered" ? "等待纳米压痕上传" : status === "current" ? "等待纳米压痕上传" : "待前序节点完成";
      }

      return status === "completed" ? "已完成" : "等待中";
    },

    executionVisualNodes(execution) {
      const workflow = this.executionWorkflow(execution);
      const uploads = this.executionInstrumentStateMap(execution);
      const observation = this.executionObservation(execution);
      const components = this.instrumentComponents(execution.dispatch_payload || {});

      return (workflow.steps || []).map((step, index) => {
        const component = Object.values(components).find(
          (item) => item.step_code === step.code || item.instrument_code === step.code
        );
        const uploadState = uploads[step.code] || {};
        const uploadStatus = this.normalizedUploadStatus(uploadState);
        const isCharacterization = ["xrd", "eds", "nanoindentation"].includes(step.code);
        const hasCharacterizationResult =
          step.code === "xrd"
            ? Boolean(uploadState?.uploaded_at || uploadStatus === "parsed" || observation.xrd_phase)
            : step.code === "eds"
              ? Boolean(uploadState?.uploaded_at || uploadStatus === "parsed" || this.hasMeasuredComposition(observation))
              : step.code === "nanoindentation"
                ? Boolean(
                    uploadState?.uploaded_at ||
                    uploadStatus === "parsed" ||
                    (observation.hardness_gpa !== undefined && observation.hardness_gpa !== null)
                  )
                : false;
        const displayStatus =
          isCharacterization && uploadStatus === "failed"
            ? "failed"
            : isCharacterization && hasCharacterizationResult
              ? "completed"
              : isCharacterization
                ? "pending"
                : step.status || "pending";
        return {
          code: step.code,
          isStart: index === 0,
          short: this.nodeShortLabel(step.code),
          title: step.title,
          status: displayStatus,
          componentId: component?.component_id || "",
          detail: this.executionNodeDetail(step.code, displayStatus, observation, uploadState),
        };
      });
    },

    inferenceFlowStatus(execution) {
      const gate = this.executionInferenceGate(execution);
      if (gate.status === "triggered") {
        return "completed";
      }
      if (gate.status === "open") {
        return "current";
      }
      return "pending";
    },

    executionFlowNodes(execution) {
      if (execution?.workflow_runtime?.graph) return runtimeToMonitor(execution.workflow_runtime).nodes;
      if (execution?.dispatch_payload?.workflow_version_id) return [];
      const gate = this.executionInferenceGate(execution);
      return [
        ...this.executionVisualNodes(execution),
        {
          code: "model_inference",
          isStart: false,
          isInference: true,
          short: "MAT",
          title: "模型推理",
          status: this.inferenceFlowStatus(execution),
          componentId: "",
          detail: gate.detail,
        },
      ];
    },

    executionInferenceGate(execution) {
      const workflow = this.executionWorkflow(execution);
      const missingSteps = workflow.missing_steps || [];
      const autoDispatch = this.executionAutoDispatch(execution);

      if (autoDispatch.triggered) {
        return {
          status: "triggered",
          title: "下一轮模型推理已触发",
          detail: autoDispatch.next_round_index
            ? `系统已派发第 ${autoDispatch.next_round_index} 轮候选`
            : "系统已基于完整反馈触发下一轮候选",
        };
      }

      if (this.executionClosedLoopReady(execution) || workflow.is_complete) {
        return {
          status: "open",
          title: "模型推理已解锁",
          detail: "三项表征已经齐套，本轮结果可进入下一轮模型推理",
        };
      }

      const characterizationMissing = missingSteps
        .filter((item) => ["xrd", "eds", "nanoindentation"].includes(item.code))
        .map((item) => item.title);

      if (characterizationMissing.length) {
        return {
          status: "locked",
          title: "模型推理已锁定",
          detail: `需先完成 ${characterizationMissing.join(" / ")}`,
        };
      }

      return {
        status: "locked",
        title: "模型推理已锁定",
        detail: "需先完成本轮四个实验节点",
      };
    },

    recommendationChips(recommendation) {
      const payload = recommendation.suggested_payload || {};
      return [
        payload.target_hardness_gpa ? `目标硬度 ${payload.target_hardness_gpa} GPa` : null,
        payload.model_version ? `模型 ${payload.model_version}` : null,
        payload.mat_model_predicted_hardness_gpa
          ? `预测硬度 ${payload.mat_model_predicted_hardness_gpa} GPa`
          : null,
        payload.mat_model_selection_role_label
          ? `批次角色 ${payload.mat_model_selection_role_label}`
          : null,
        payload.next_action ? `下一动作 ${payload.next_action}` : null,
        ...this.optimizationChips(payload),
      ].filter(Boolean);
    },

    recommendationIntervalRows(recommendation) {
      return this.intervalRowsFromSpace(
        recommendation?.suggested_payload?.predicted_composition_space || null
      );
    },

    observationText(execution) {
      const observation = this.executionObservation(execution);
      const uploads = this.executionInstrumentStateMap(execution);
      const segments = [];

      if (observation.deposition_sample_id) {
        segments.push("样品已制备");
      }
      if (uploads.xrd?.uploaded_at || this.normalizedUploadStatus(uploads.xrd) === "parsed" || observation.xrd_phase) {
        segments.push("结构批次已上传");
      }
      if (
        uploads.eds?.uploaded_at ||
        this.normalizedUploadStatus(uploads.eds) === "parsed" ||
        this.hasMeasuredComposition(observation)
      ) {
        segments.push("成分批次已上传");
      }
      if (
        uploads.nanoindentation?.uploaded_at ||
        this.normalizedUploadStatus(uploads.nanoindentation) === "parsed" ||
        (observation.hardness_gpa !== undefined && observation.hardness_gpa !== null)
      ) {
        segments.push("硬度批次已上传");
      }
      return segments.join(" | ") || "尚无结构化观测";
    },

    applyModelSpace(modelSpace) {
      this.modelSpace = modelSpace;
      const center =
        modelSpace?.center_composition_at_percent ||
        modelSpace?.predicted_composition_space?.center_composition_at_percent ||
        {};
      Object.assign(this.scheduleForm, {
        model_version: modelSpace?.model_version || this.scheduleForm.model_version,
        ti_at: center.ti_at ?? this.scheduleForm.ti_at,
        zr_at: center.zr_at ?? this.scheduleForm.zr_at,
        nb_at: center.nb_at ?? this.scheduleForm.nb_at,
        v_at: center.v_at ?? this.scheduleForm.v_at,
      });
    },

    validateModelSpace() {
      if (!this.modelSpace?.predicted_composition_space || !this.modelSpace?.predicted_batch_plan) {
        throw new Error("当前未加载 mat_model 预测空间，不能创建高通量批次调度");
      }
      return true;
    },

    generatedSampleBatch() {
      return `TZNV-${buildCompactTimestamp()}`;
    },

    generatedScheduleName(sampleBatch) {
      return `TiZrNbV-hardness-${sampleBatch}`;
    },

    buildSchedulePayload() {
      this.validateModelSpace();
      const sampleBatch = this.scheduleForm.sample_batch.trim() || this.generatedSampleBatch();
      const center =
        this.modelSpace.center_composition_at_percent ||
        this.modelSpace.predicted_composition_space.center_composition_at_percent ||
        {};

      return compactObject({
        material_system: this.scheduleForm.material_system.trim(),
        sample_batch: sampleBatch,
        model_version: String(this.modelSpace.model_version || this.scheduleForm.model_version || "").trim(),
        substrate: this.scheduleForm.substrate.trim(),
        deposition_chamber: this.scheduleForm.deposition_chamber.trim(),
        sputter_component_id:
          this.scheduleForm.sputter_component_id.trim() ||
          this.scheduleForm.deposition_chamber.trim(),
        xrd_component_id: this.scheduleForm.xrd_component_id.trim(),
        eds_component_id: this.scheduleForm.eds_component_id.trim(),
        nanoindentation_component_id: this.scheduleForm.nanoindentation_component_id.trim(),
        objective: this.scheduleForm.objective.trim(),
        measurement_metric: this.scheduleForm.measurement_metric.trim(),
        optimize_goal: this.scheduleForm.optimize_goal,
        ti_at: toOptionalNumber(center.ti_at),
        zr_at: toOptionalNumber(center.zr_at),
        nb_at: toOptionalNumber(center.nb_at),
        v_at: toOptionalNumber(center.v_at),
        predicted_composition_space: this.modelSpace.predicted_composition_space,
        predicted_batch_plan: this.modelSpace.predicted_batch_plan,
        composition_summary: this.modelSpace.summary,
        high_throughput_batch_size: this.modelSpace.predicted_batch_plan.total_samples,
        high_throughput_grid_rows: this.modelSpace.predicted_batch_plan.rows,
        high_throughput_grid_cols: this.modelSpace.predicted_batch_plan.cols,
        target_hardness_gpa: toOptionalNumber(this.scheduleForm.target_hardness_gpa),
        target_metric_value: toOptionalNumber(this.scheduleForm.target_hardness_gpa),
        composition_tolerance_at: toOptionalNumber(this.scheduleForm.composition_tolerance_at),
        notes: this.scheduleForm.notes.trim(),
      });
    },

    buildScheduleRequestBody() {
      const payload = this.buildSchedulePayload();
      const sampleBatch = payload.sample_batch || this.generatedSampleBatch();
      return {
        name: this.scheduleForm.name.trim() || this.generatedScheduleName(sampleBatch),
        description: payload.notes || "",
        target: this.scheduleForm.target.trim() || "materials/sputter-cluster-01",
        payload,
        base_interval_seconds: toNumber(this.scheduleForm.base_interval_seconds),
        min_interval_seconds: toNumber(this.scheduleForm.min_interval_seconds),
        max_interval_seconds: toNumber(this.scheduleForm.max_interval_seconds),
        retry_delay_seconds: toNumber(this.scheduleForm.retry_delay_seconds),
        feedback_timeout_seconds: toNumber(this.scheduleForm.feedback_timeout_seconds),
        max_consecutive_failures: toNumber(this.scheduleForm.max_consecutive_failures),
        priority: toNumber(this.scheduleForm.priority),
        start_in_seconds: toNumber(this.scheduleForm.start_in_seconds),
      };
    },

    feedbackCompositionTotalText(executionId) {
      const draft = this.feedbackDrafts[executionId];
      if (!draft) {
        return "0.0 at.%";
      }
      const total =
        safeNumber(draft.eds_ti_at) +
        safeNumber(draft.eds_zr_at) +
        safeNumber(draft.eds_nb_at) +
        safeNumber(draft.eds_v_at);
      return `${total.toFixed(1)} at.%`;
    },

    buildMeasuredComposition(draft) {
      const values = [
        draft.eds_ti_at,
        draft.eds_zr_at,
        draft.eds_nb_at,
        draft.eds_v_at,
      ];
      const hasAny = values.some((item) => item !== "");
      if (!hasAny) {
        return null;
      }
      const hasAll = values.every((item) => item !== "");
      if (!hasAll) {
        throw new Error("EDS Ti/Zr/Nb/V 成分必须完整填写");
      }
      const measured = {
        ti_at: toNumber(draft.eds_ti_at),
        zr_at: toNumber(draft.eds_zr_at),
        nb_at: toNumber(draft.eds_nb_at),
        v_at: toNumber(draft.eds_v_at),
      };
      const total = measured.ti_at + measured.zr_at + measured.nb_at + measured.v_at;
      if (Math.abs(total - 100) > 0.5) {
        throw new Error("EDS 实测 Ti/Zr/Nb/V at% 总和必须接近 100");
      }
      return measured;
    },

    buildInstrumentUploadObservation(execution, instrumentCode, draft) {
      if (instrumentCode === "magnetron_sputtering") {
        if (!draft.deposition_sample_id.trim() && !draft.sputter_run_id.trim()) {
          throw new Error("请先填写样品编号或溅射 Run ID");
        }
        return compactObject({
          deposition_sample_id: draft.deposition_sample_id.trim(),
          sputter_run_id: draft.sputter_run_id.trim(),
          deposition_completed: true,
          deposition_note: draft.note.trim(),
        });
      }

      if (instrumentCode === "xrd") {
        if (!draft.xrd_phase.trim() && !draft.xrd_note.trim()) {
          throw new Error("请先填写 XRD 相结构或备注");
        }
        return compactObject({
          xrd_phase: draft.xrd_phase.trim(),
          xrd_note: draft.xrd_note.trim(),
        });
      }

      if (instrumentCode === "eds") {
        const measured = this.buildMeasuredComposition(draft);
        if (!measured) {
          throw new Error("请先填写完整的 EDS Ti/Zr/Nb/V 成分");
        }
        return compactObject({
          measured_composition_at_percent: measured,
          eds_ti_at: measured.ti_at,
          eds_zr_at: measured.zr_at,
          eds_nb_at: measured.nb_at,
          eds_v_at: measured.v_at,
          eds_note: draft.note.trim(),
        });
      }

      if (instrumentCode === "nanoindentation") {
        if (draft.hardness_gpa === "" || draft.hardness_gpa === null || draft.hardness_gpa === undefined) {
          throw new Error("请先填写纳米压痕硬度");
        }
        return compactObject({
          hardness_gpa: toOptionalNumber(draft.hardness_gpa),
          indentation_note: draft.note.trim(),
        });
      }

      throw new Error(`未知仪器类型: ${instrumentCode}`);
    },

    syncFeedbackDrafts() {
      const nextDrafts = {};
      for (const execution of this.pendingExecutions) {
        const schedule = this.findSchedule(execution.schedule_id);
        nextDrafts[execution.id] = {
          ...feedbackDraftDefaults(execution, schedule),
          ...(this.feedbackDrafts[execution.id] || {}),
        };
      }
      this.feedbackDrafts = nextDrafts;
    },

    syncAutoRefresh() {
      if (this.refreshTimer) {
        clearInterval(this.refreshTimer);
        this.refreshTimer = null;
      }
      if (this.autoRefreshEnabled && !this.isWorkflowBuilderMode) {
        this.refreshTimer = setInterval(() => {
          if (this.isExecutionDetailMode) {
            void this.refreshExecutionDetail();
          } else {
            void this.refreshDashboard();
          }
        }, 12000);
      }
    },

    async api(url, options = {}) {
      const response = await fetch(url, {
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
        ...options,
      });

      if (!response.ok) {
        let message = `请求失败: ${response.status}`;
        try {
          const detail = await response.json();
          message = detail.detail || message;
        } catch (error) {
          message = response.statusText || message;
        }
        throw new Error(message);
      }

      return response.json();
    },

    async refreshDashboard() {
      try {
        const [schedules, executions, pendingExecutions, recommendations, modelSpace] =
          await Promise.all([
            this.api("/api/v1/schedules"),
            this.api("/api/v1/executions?limit=24"),
            this.api("/api/v1/executions?status=awaiting_feedback&limit=12"),
            this.api("/api/v1/recommendations?limit=12"),
            this.api("/api/v1/mat-model/space").catch(() => null),
          ]);

        this.schedules = schedules;
        this.executions = executions;
        this.pendingExecutions = pendingExecutions;
        this.recommendations = recommendations;
        if (executions.some(execution => execution.workflow_runtime || execution.dispatch_payload?.workflow_version_id)) {
          this.workflowHealth = await workflowApi.health().catch(() => null);
        }
        if (modelSpace) {
          this.applyModelSpace(modelSpace);
        } else {
          this.modelSpace = null;
        }
        this.syncActiveTargetHardnessDraft();
        this.syncFeedbackDrafts();
        this.lastUpdatedAt = new Date();
      } catch (error) {
        this.setMessage(error.message, "error");
      }
    },

    async refreshExecutionDetail() {
      if (!this.detailRunId) {
        return;
      }

      this.detailLoading = true;
      try {
        const [detail, samplePlan, schedules] = await Promise.all([
          this.api(`/api/v1/runs/${this.detailRunId}`),
          this.api(`/api/v1/executions/${this.detailRunId}/sample-plan`).catch(() => null),
          this.api("/api/v1/schedules").catch(() => []),
        ]);
        this.executionDetail = detail;
        if (detail.execution?.workflow_runtime || detail.execution?.dispatch_payload?.workflow_version_id) {
          this.workflowHealth = await workflowApi.health().catch(() => null);
        }
        this.executionSamplePlan = samplePlan;
        this.schedules = schedules;
        this.lastUpdatedAt = new Date();
      } catch (error) {
        this.setMessage(error.message, "error");
      } finally {
        this.detailLoading = false;
      }
    },

    openExecutionDetail(execution) {
      if (!execution?.id) {
        return;
      }
      const url = new URL(window.location.href);
      url.searchParams.set("run_id", execution.id);
      window.open(url.toString(), "_blank", "noopener,noreferrer");
    },

    goBack() {
      window.history.back();
    },

    async createSchedule() {
      await this.runWithAction("schedule:create", async () => {
        try {
          const requestBody = this.buildScheduleRequestBody();
          const shouldTriggerImmediately = Number(requestBody.start_in_seconds || 0) === 0;
          const createdSchedule = await this.api("/api/v1/schedules", {
            method: "POST",
            body: JSON.stringify(requestBody),
          });

          if (shouldTriggerImmediately && createdSchedule?.id) {
            try {
              await this.api(`/api/v1/schedules/${createdSchedule.id}/trigger`, {
                method: "POST",
              });
            } catch (error) {
              const message = String(error?.message || "");
              if (!message.includes("is waiting for feedback")) {
                throw error;
              }
            }
          }

          this.scheduleForm.target_hardness_gpa = "";
          this.scheduleForm.sample_batch = "";
          await this.refreshDashboard();
          this.setMessage(
            shouldTriggerImmediately
              ? "Ti-Zr-Nb-V 调度已创建并进入首轮执行。"
              : "Ti-Zr-Nb-V 调度已创建，等待到达派发时间。",
            "success"
          );
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },

    async saveActiveTargetHardness() {
      const schedule = this.editableTargetSchedule;
      if (!schedule) {
        this.setMessage("当前没有可更新的活跃调度。", "error");
        return;
      }

      await this.runWithAction(this.actionKey("target", schedule.id), async () => {
        try {
          const targetHardness = toOptionalNumber(this.activeTargetHardnessDraft);
          if (
            targetHardness === null ||
            !Number.isFinite(targetHardness) ||
            targetHardness <= 0
          ) {
            throw new Error("目标硬度必须是大于 0 的数值。");
          }

          const payload = {
            ...this.schedulePayload(schedule),
            target_hardness_gpa: targetHardness,
            target_metric_value: targetHardness,
          };

          await this.api(`/api/v1/schedules/${schedule.id}`, {
            method: "PATCH",
            body: JSON.stringify({ payload }),
          });
          this.activeTargetHardnessDirty = false;
          await this.refreshDashboard();
          this.setMessage("目标硬度已更新，后续推荐和下一轮都会沿用这个值。", "success");
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },

    async uploadInstrumentData(executionId, instrumentCode) {
      const draft = this.feedbackDrafts[executionId];
      if (!draft) {
        this.setMessage("未找到反馈草稿。", "error");
        return;
      }
      const execution = this.pendingExecutions.find((item) => item.id === executionId);
      if (!execution) {
        this.setMessage("未找到待上传执行。", "error");
        return;
      }

      await this.runWithAction(this.actionKey(`upload:${instrumentCode}`, executionId), async () => {
        try {
          const observation = this.buildInstrumentUploadObservation(execution, instrumentCode, draft);
          const result = await this.api("/api/v1/instrument-uploads", {
            method: "POST",
            body: JSON.stringify({
              instrument_code: instrumentCode,
              component_id: this.instrumentComponentId(execution, instrumentCode),
              experiment_batch_id: this.executionBatch(execution),
              collected_at: new Date().toISOString(),
              observation,
              note: draft.note.trim(),
            }),
          });
          this.setMessage(
            result.round_completed
              ? "所有仪器数据已就绪，本轮实验已自动完成。"
              : `${instrumentCode} 数据已上传，等待其余仪器数据。`,
            "success"
          );
          await this.refreshDashboard();
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },

    async runTick() {
      await this.runWithAction("tick", async () => {
        try {
          const result = await this.api("/api/v1/engine/tick", { method: "POST" });
          this.setMessage(
            `完成一次调度扫描，派发 ${result.dispatched_count} 条执行，自动补超时 ${result.timed_out_count} 条。`,
            "success"
          );
          await this.refreshDashboard();
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },

    async triggerSchedule(scheduleId) {
      await this.runWithAction(this.actionKey("trigger", scheduleId), async () => {
        try {
          await this.api(`/api/v1/schedules/${scheduleId}/trigger`, { method: "POST" });
          this.setMessage("已手动触发实验。", "success");
          await this.refreshDashboard();
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },

    async toggleSchedule(scheduleId, action) {
      await this.runWithAction(this.actionKey(action, scheduleId), async () => {
        try {
          await this.api(`/api/v1/schedules/${scheduleId}/${action}`, { method: "POST" });
          this.setMessage(
            action === "pause" ? "实验调度已暂停。" : "实验调度已恢复。",
            "success"
          );
          await this.refreshDashboard();
        } catch (error) {
          this.setMessage(error.message, "error");
        }
      });
    },
  },
};

export default appOptions;
