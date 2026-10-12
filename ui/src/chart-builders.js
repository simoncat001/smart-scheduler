const QUATERNARY_KEYS = ["ti_at", "zr_at", "nb_at", "v_at"];
const QUATERNARY_LABELS = {
  ti_at: "Ti",
  zr_at: "Zr",
  nb_at: "Nb",
  v_at: "V",
};

const TOKENS = {
  text: "#1d1a16",
  muted: "#78726a",
  grid: "rgba(29, 26, 22, 0.08)",
  line: "rgba(29, 26, 22, 0.14)",
  warm: "#c2410c",
  warmSoft: "rgba(194, 65, 12, 0.14)",
  warmGlow: "rgba(194, 65, 12, 0.10)",
  teal: "#0e7490",
  tealSoft: "rgba(14, 116, 144, 0.14)",
  tealGlow: "rgba(14, 116, 144, 0.10)",
  gold: "#b45309",
  goldSoft: "rgba(180, 83, 9, 0.14)",
  danger: "#b91c1c",
  dangerSoft: "rgba(185, 28, 28, 0.14)",
  dangerGlow: "rgba(185, 28, 28, 0.10)",
  success: "#15803d",
  successSoft: "rgba(21, 128, 61, 0.14)",
  successGlow: "rgba(21, 128, 61, 0.10)",
  goldGlow: "rgba(180, 83, 9, 0.10)",
  pending: "#a9a69f",
  pendingSoft: "rgba(169, 166, 159, 0.22)",
  panel: "rgba(255, 255, 255, 0.98)",
  panelStrong: "#ffffff",
};

const RANGE_THEMES = {
  warm: {
    p10: "rgba(14, 116, 144, 0.56)",
    p50: "rgba(194, 65, 12, 0.72)",
    p90: "rgba(180, 83, 9, 0.60)",
    p10Glow: TOKENS.tealGlow,
    p50Glow: TOKENS.warmGlow,
    p90Glow: TOKENS.goldGlow,
  },
  sage: {
    p10: "rgba(21, 128, 61, 0.56)",
    p50: "rgba(14, 116, 144, 0.68)",
    p90: "rgba(194, 65, 12, 0.60)",
    p10Glow: TOKENS.successGlow,
    p50Glow: TOKENS.tealGlow,
    p90Glow: TOKENS.warmGlow,
  },
};

const MODEL_INFERENCE_SYMBOL =
  "path://M0,-42 L34,-24 L34,24 L0,42 L-34,24 L-34,-24 Z";

function formatNumber(value, digits = 1) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toFixed(digits) : "-";
}

function niceCeil(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 10;
  }
  return Math.ceil(value / 5) * 5;
}

function titleBlock(title, subtitle = "") {
  if (!title && !subtitle) {
    return undefined;
  }
  return {
    text: title || "",
    subtext: subtitle || "",
    left: 10,
    top: 6,
    textStyle: {
      color: TOKENS.text,
      fontSize: 15,
      fontWeight: 700,
      fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
      textShadowColor: TOKENS.tealGlow,
      textShadowBlur: 10,
    },
    subtextStyle: {
      color: TOKENS.muted,
      fontSize: 11,
      lineHeight: 17,
      fontFamily: "IBM Plex Mono, SFMono-Regular, Consolas, Liberation Mono, monospace",
      textShadowColor: TOKENS.tealGlow,
      textShadowBlur: 6,
    },
  };
}

function statusTone(status) {
  if (status === "failed") {
    return {
      fill: "rgba(254, 242, 242, 0.98)",
      glow: TOKENS.dangerGlow,
      label: "#7f1d1d",
      border: "rgba(185, 28, 28, 0.55)",
    };
  }
  if (status === "triggered") {
    return {
      fill: "rgba(254, 243, 224, 0.98)",
      glow: TOKENS.goldGlow,
      label: "#7c2d12",
      border: "rgba(180, 83, 9, 0.55)",
    };
  }
  if (status === "completed") {
    return {
      fill: "rgba(236, 253, 243, 0.98)",
      glow: TOKENS.successGlow,
      label: "#166534",
      border: "rgba(21, 128, 61, 0.55)",
    };
  }
  if (status === "current") {
    return {
      fill: "rgba(255, 241, 230, 0.98)",
      glow: TOKENS.warmGlow,
      label: "#7c2d12",
      border: "rgba(194, 65, 12, 0.55)",
    };
  }
  return {
    fill: "rgba(246, 243, 234, 0.98)",
    glow: TOKENS.tealGlow,
    label: "#3a352e",
    border: "rgba(29, 26, 22, 0.22)",
  };
}

function edgeStyle(status) {
  if (status === "failed") {
    return {
      color: TOKENS.danger,
      width: 4,
      type: "solid",
      opacity: 0.94,
      shadowBlur: 14,
      shadowColor: TOKENS.dangerGlow,
    };
  }
  if (status === "triggered") {
    return {
      color: TOKENS.gold,
      width: 4,
      type: "solid",
      opacity: 0.95,
      shadowBlur: 14,
      shadowColor: TOKENS.goldGlow,
    };
  }
  if (status === "completed") {
    return {
      color: TOKENS.success,
      width: 4,
      type: "solid",
      opacity: 0.92,
      shadowBlur: 10,
      shadowColor: TOKENS.successGlow,
    };
  }
  if (status === "current") {
    return {
      color: TOKENS.warm,
      width: 3,
      type: "dashed",
      opacity: 0.9,
      shadowBlur: 14,
      shadowColor: TOKENS.warmGlow,
    };
  }
  return {
    color: TOKENS.pending,
    width: 2,
    type: "dashed",
    opacity: 0.7,
    shadowBlur: 6,
    shadowColor: TOKENS.tealGlow,
  };
}

function richNodeLabel(short, stateLabel, tone) {
  return `{chip|${short}}\n{state|${stateLabel}}`;
}

function routerNode(name, x, y) {
  return {
    name,
    router: true,
    x,
    y,
    symbol: "circle",
    symbolSize: 2,
    itemStyle: {
      color: "rgba(0,0,0,0)",
      borderColor: "rgba(0,0,0,0)",
      opacity: 0,
    },
    label: {
      show: false,
    },
    emphasis: {
      disabled: true,
    },
    tooltip: {
      show: false,
    },
    silent: true,
  };
}

function headerNode(name, x, y, width, label, fill, stroke, glow) {
  return {
    name,
    header: true,
    x,
    y,
    symbol: "rect",
    symbolSize: [width, 22],
    itemStyle: {
      color: fill,
      borderColor: stroke,
      borderWidth: 1,
      shadowBlur: 10,
      shadowColor: glow,
    },
    label: {
      show: true,
      formatter: label,
      color: stroke,
      fontSize: 14,
      fontWeight: 700,
      fontFamily: "IBM Plex Mono, SFMono-Regular, Consolas, Liberation Mono, monospace",
      textShadowColor: glow,
      textShadowBlur: 8,
    },
    emphasis: {
      disabled: true,
    },
    tooltip: {
      show: false,
    },
    silent: true,
  };
}

export function buildEmptyChartOption(title, subtitle = "") {
  return {
    animation: false,
    xAxis: {
      show: false,
      min: 0,
      max: 1,
    },
    yAxis: {
      show: false,
      min: 0,
      max: 1,
    },
    series: [],
    graphic: [
      {
        type: "text",
        left: "center",
        top: "42%",
        style: {
          text: title,
          fill: TOKENS.text,
          fontSize: 18,
          fontWeight: 700,
          textAlign: "center",
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 12,
        },
      },
      {
        type: "text",
        left: "center",
        top: "56%",
        style: {
          text: subtitle,
          fill: TOKENS.muted,
          fontSize: 12,
          textAlign: "center",
          fontFamily: "IBM Plex Mono, SFMono-Regular, Consolas, Liberation Mono, monospace",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 6,
        },
      },
    ],
  };
}

export function buildHeroGaugeOption({
  execution,
  monitor,
  scheduleName,
  roundLabel,
  targetHardness,
}) {
  if (!execution || !monitor) {
    return buildEmptyChartOption("等待实验轮次", "调度派发后这里会显示当前轮次态势");
  }

  const progressScore = monitor.nodes.reduce((sum, node) => {
    if (node.status === "failed") {
      return sum + 0.08;
    }
    if (node.status === "triggered") {
      return sum + (node.isInference ? 1 : 0.36);
    }
    if (node.status === "completed") {
      return sum + 1;
    }
    if (node.status === "current") {
      return sum + 0.55;
    }
    return sum + 0.12;
  }, 0);
  const progress = Math.max(8, Math.min(100, Math.round((progressScore / monitor.nodes.length) * 100)));

  const gateColor =
    monitor.gateStatus === "triggered"
      ? TOKENS.success
      : monitor.gateStatus === "open"
        ? TOKENS.gold
        : TOKENS.teal;

  const subtitle = targetHardness ? `目标硬度 ${targetHardness} GPa` : "等待表征结果汇聚";

  return {
    animationDuration: 600,
    series: [
      {
        type: "gauge",
        center: ["50%", "56%"],
        radius: "88%",
        startAngle: 210,
        endAngle: -30,
        min: 0,
        max: 100,
        pointer: {
          show: false,
        },
        axisLine: {
          lineStyle: {
            width: 20,
            color: [[1, "rgba(29, 26, 22, 0.08)"]],
            cap: "butt",
          },
        },
        progress: {
          show: true,
          width: 20,
          roundCap: false,
          itemStyle: {
            color: {
              type: "linear",
              x: 0,
              y: 0,
              x2: 1,
              y2: 0,
              colorStops: [
                { offset: 0, color: TOKENS.warm },
                { offset: 1, color: gateColor },
              ],
            },
            shadowBlur: 28,
            shadowColor:
              monitor.gateStatus === "triggered"
                ? TOKENS.successGlow
                : monitor.gateStatus === "open"
                  ? TOKENS.goldGlow
                  : TOKENS.warmGlow,
          },
        },
        splitLine: { show: false },
        axisTick: { show: false },
        axisLabel: { show: false },
        detail: { show: false },
        data: [{ value: progress }],
      },
    ],
    graphic: [
      {
        type: "text",
        left: "center",
        top: "30%",
        style: {
          text: `${progress}%`,
          fill: TOKENS.text,
          fontSize: 32,
          fontWeight: 800,
          textAlign: "center",
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 18,
        },
      },
      {
        type: "text",
        left: "center",
        top: "48%",
        style: {
          text: scheduleName,
          fill: TOKENS.text,
          fontSize: 15,
          fontWeight: 700,
          textAlign: "center",
          width: 220,
          overflow: "truncate",
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 10,
        },
      },
      {
        type: "text",
        left: "center",
        top: "60%",
        style: {
          text: `${roundLabel} · ${monitor.gateLabel}`,
          fill: gateColor,
          fontSize: 12,
          fontWeight: 700,
          textAlign: "center",
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor:
            monitor.gateStatus === "triggered"
              ? TOKENS.successGlow
              : monitor.gateStatus === "open"
                ? TOKENS.goldGlow
                : TOKENS.tealGlow,
          textShadowBlur: 10,
        },
      },
      {
        type: "text",
        left: "center",
        top: "71%",
        style: {
          text: subtitle,
          fill: TOKENS.muted,
          fontSize: 12,
          textAlign: "center",
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 6,
        },
      },
    ],
  };
}

export function buildFlowMonitorOption({
  monitor,
  scheduleName,
  batchLabel,
}) {
  if (!monitor) {
    return buildEmptyChartOption("暂无流程监控", "等待调度创建并派发首轮执行");
  }

  const nodePositions = {
    magnetron_sputtering: [112, 310],
    xrd: [292, 190],
    eds: [292, 310],
    nanoindentation: [292, 430],
    gate: [522, 310],
    model_inference: [728, 310],
  };
  const routePositions = {
    start_bus: [188, 310],
    branch_xrd: [188, 190],
    branch_nano: [188, 430],
    xrd_gate: [430, 190],
    merge_gate: [430, 310],
    nano_gate: [430, 430],
  };
  const headerY = 88;

  const nodeSymbol = {
    magnetron_sputtering: "roundRect",
    xrd: "rect",
    eds: "rect",
    nanoindentation: "rect",
    gate: "diamond",
    model_inference: MODEL_INFERENCE_SYMBOL,
  };

  const nodeSize = {
    magnetron_sputtering: [118, 70],
    xrd: [110, 64],
    eds: [110, 64],
    nanoindentation: [110, 64],
    gate: [116, 86],
    model_inference: [120, 70],
  };

  const gateStatus =
    monitor.gateStatus === "triggered"
      ? "completed"
      : monitor.gateStatus === "open"
        ? "current"
        : "pending";

  const coreNodes = monitor.nodes.map((node) => {
    const tone = statusTone(node.status);
    return {
      name: node.code,
      title: node.title,
      detail: node.detail,
      componentId: node.componentId || "",
      x: nodePositions[node.code][0],
      y: nodePositions[node.code][1],
      symbol: nodeSymbol[node.code],
      symbolSize: nodeSize[node.code],
      itemStyle: {
        color: tone.fill,
        borderColor: tone.border,
        borderWidth: node.code === "model_inference" ? 3 : 2,
        shadowBlur: 24,
        shadowColor: tone.glow,
      },
      label: {
        show: true,
        formatter: richNodeLabel(node.short, node.stateLabel, tone),
        color: tone.label,
        lineHeight: 18,
        rich: {
          chip: {
            fontSize: node.code === "model_inference" ? 16 : 15,
            fontWeight: 800,
            lineHeight: 22,
            align: "center",
            textShadowColor: tone.glow,
            textShadowBlur: 10,
          },
          state: {
            fontSize: 13,
            color: tone.label,
            opacity: 0.92,
            align: "center",
            textShadowColor: tone.glow,
            textShadowBlur: 6,
          },
        },
      },
    };
  });

  const gateTone = statusTone(gateStatus);
  const graphNodes = [
    ...coreNodes,
    routerNode("route_start_bus", routePositions.start_bus[0], routePositions.start_bus[1]),
    routerNode("route_branch_xrd", routePositions.branch_xrd[0], routePositions.branch_xrd[1]),
    routerNode("route_branch_nano", routePositions.branch_nano[0], routePositions.branch_nano[1]),
    routerNode("route_xrd_gate", routePositions.xrd_gate[0], routePositions.xrd_gate[1]),
    routerNode("route_merge_gate", routePositions.merge_gate[0], routePositions.merge_gate[1]),
    routerNode("route_nano_gate", routePositions.nano_gate[0], routePositions.nano_gate[1]),
    routerNode("extent_left", 18, 310),
    routerNode("extent_right", 842, 310),
    routerNode("extent_top", 430, 74),
    routerNode("extent_bottom", 430, 502),
    headerNode(
      "header_start",
      nodePositions.magnetron_sputtering[0],
      headerY,
      126,
      "START",
      "rgba(194, 65, 12, 0.08)",
      "rgba(194, 65, 12, 0.9)",
      TOKENS.warmGlow
    ),
    headerNode(
      "header_parallel",
      340,
      headerY,
      256,
      "PARALLEL CHARACTERIZATION",
      "rgba(14, 116, 144, 0.08)",
      TOKENS.teal,
      TOKENS.tealGlow
    ),
    headerNode(
      "header_gate",
      nodePositions.gate[0],
      headerY,
      132,
      "DECISION GATE",
      monitor.gateStatus === "triggered"
        ? "rgba(21, 128, 61, 0.10)"
        : monitor.gateStatus === "open"
          ? "rgba(180, 83, 9, 0.10)"
          : "rgba(29, 26, 22, 0.05)",
      monitor.gateStatus === "triggered"
        ? TOKENS.success
        : monitor.gateStatus === "open"
          ? TOKENS.gold
          : TOKENS.muted,
      monitor.gateStatus === "triggered"
        ? TOKENS.successGlow
        : monitor.gateStatus === "open"
          ? TOKENS.goldGlow
          : TOKENS.tealGlow
    ),
    headerNode(
      "header_mat",
      nodePositions.model_inference[0],
      headerY,
      150,
      "MAT INFERENCE",
      "rgba(180, 83, 9, 0.08)",
      TOKENS.gold,
      TOKENS.goldGlow
    ),
    {
      name: "gate",
      title: "并行门控",
      detail: monitor.gateLabel,
      componentId: "",
      x: nodePositions.gate[0],
      y: nodePositions.gate[1],
      symbol: nodeSymbol.gate,
      symbolSize: nodeSize.gate,
      itemStyle: {
        color: gateTone.fill,
        borderColor: gateTone.border,
        borderWidth: 2,
        shadowBlur: 32,
        shadowColor: gateTone.glow,
      },
      label: {
        show: true,
        formatter: `{chip|推理门}\n{state|${monitor.gateLabel}}`,
        color: gateTone.label,
        rich: {
          chip: {
            fontSize: 16,
            fontWeight: 800,
            lineHeight: 24,
            align: "center",
            textShadowColor: gateTone.glow,
            textShadowBlur: 10,
          },
          state: {
            fontSize: 14,
            color: gateTone.label,
            lineHeight: 18,
            align: "center",
            textShadowColor: gateTone.glow,
            textShadowBlur: 6,
          },
        },
      },
    },
  ];

  const linkState = Object.fromEntries(monitor.links.map((item) => [item.key, item.status]));
  const prepLinkStatus = monitor.nodes.find((node) => node.code === "magnetron_sputtering")?.status === "completed"
    ? "completed"
    : "current";
  const gateOutputStatus =
    monitor.gateStatus === "triggered"
      ? "completed"
      : monitor.gateStatus === "open"
        ? "current"
        : "pending";
  const graphLinks = [
    {
      source: "magnetron_sputtering",
      target: "route_start_bus",
      lineStyle: { ...edgeStyle(prepLinkStatus), curveness: 0 },
    },
    {
      source: "route_start_bus",
      target: "route_branch_xrd",
      lineStyle: { ...edgeStyle(linkState["start-xrd"]), curveness: 0 },
    },
    {
      source: "route_branch_xrd",
      target: "xrd",
      lineStyle: { ...edgeStyle(linkState["start-xrd"]), curveness: 0 },
    },
    {
      source: "route_start_bus",
      target: "eds",
      lineStyle: { ...edgeStyle(linkState["start-eds"]), curveness: 0 },
    },
    {
      source: "route_start_bus",
      target: "route_branch_nano",
      lineStyle: { ...edgeStyle(linkState["start-nano"]), curveness: 0 },
    },
    {
      source: "route_branch_nano",
      target: "nanoindentation",
      lineStyle: { ...edgeStyle(linkState["start-nano"]), curveness: 0 },
    },
    {
      source: "xrd",
      target: "route_xrd_gate",
      lineStyle: {
        ...edgeStyle(linkState["xrd-inference"]),
        curveness: 0,
      },
    },
    {
      source: "route_xrd_gate",
      target: "route_merge_gate",
      lineStyle: { ...edgeStyle(linkState["xrd-inference"]), curveness: 0 },
    },
    {
      source: "eds",
      target: "route_merge_gate",
      lineStyle: { ...edgeStyle(linkState["eds-inference"]), curveness: 0 },
    },
    {
      source: "nanoindentation",
      target: "route_nano_gate",
      lineStyle: { ...edgeStyle(linkState["nano-inference"]), curveness: 0 },
    },
    {
      source: "route_nano_gate",
      target: "route_merge_gate",
      lineStyle: { ...edgeStyle(linkState["nano-inference"]), curveness: 0 },
    },
    {
      source: "route_merge_gate",
      target: "gate",
      lineStyle: { ...edgeStyle(gateStatus), curveness: 0 },
    },
    {
      source: "gate",
      target: "model_inference",
      lineStyle: { ...edgeStyle(gateOutputStatus), curveness: 0 },
    },
  ];

  return {
    animationDuration: 700,
    animationDurationUpdate: 400,
    tooltip: {
      trigger: "item",
      backgroundColor: TOKENS.panelStrong,
      borderColor: "rgba(29, 26, 22, 0.14)",
      borderWidth: 1,
      textStyle: {
        color: TOKENS.text,
        fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
      },
      formatter(params) {
        if (params.dataType !== "node" || params.data?.router || params.data?.header) {
          return "工艺链路";
        }
        const { title, detail, componentId } = params.data;
        return [
          `<strong style="color:${TOKENS.text}">${title}</strong>`,
          detail || "",
          componentId ? `<span style="color:${TOKENS.muted}">${componentId}</span>` : "",
        ]
          .filter(Boolean)
          .join("<br/>");
      },
    },
    series: [
      {
        type: "graph",
        layout: "none",
        coordinateSystem: null,
        left: 34,
        top: 72,
        right: 48,
        bottom: 38,
        roam: false,
        draggable: false,
        edgeSymbol: ["none", "arrow"],
        edgeSymbolSize: [0, 11],
        emphasis: {
          focus: "adjacency",
        },
        data: graphNodes,
        links: graphLinks,
        clip: false,
      },
    ],
    graphic: [
      {
        type: "text",
        left: 14,
        top: 10,
        style: {
          text: `${scheduleName} · ${batchLabel}`,
          fill: TOKENS.muted,
          fontSize: 12,
          fontWeight: 600,
          fontFamily: "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif",
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 6,
        },
        silent: true,
      },
    ],
  };
}

export function buildCompositionRangeOption({
  space,
  title = "",
  subtitle = "",
  theme = "warm",
}) {
  if (!space?.elements) {
    return buildEmptyChartOption("等待成分空间", "mat_model 返回后，这里会显示下一轮区间带");
  }

  const colors = RANGE_THEMES[theme] || RANGE_THEMES.warm;
  const labels = QUATERNARY_KEYS.map((key) => QUATERNARY_LABELS[key]);
  const stats = QUATERNARY_KEYS.map((key) => space.elements[key] || {});
  const maxValues = stats.map((item) => Number(item.max || 0));
  const p10Values = stats.map((item) => Number(item.p10 ?? item.min ?? 0));
  const p90Values = stats.map((item) => Number(item.p90 ?? item.max ?? 0));
  const medianValues = stats.map((item) => Number(item.median ?? item.p50 ?? item.min ?? 0));
  const yMax = niceCeil(Math.max(...maxValues, 30));

  return {
    animationDuration: 700,
    title: titleBlock(title, subtitle),
    legend: {
      top: title || subtitle ? 10 : 10,
      left: 16,
      itemWidth: 12,
      itemHeight: 12,
      itemGap: 16,
      textStyle: {
        color: TOKENS.muted,
        fontSize: 12,
        fontFamily: "IBM Plex Mono, SFMono-Regular, Consolas, Liberation Mono, monospace",
        textShadowColor: TOKENS.tealGlow,
        textShadowBlur: 6,
      },
      data: ["P10", "P50", "P90"],
    },
    grid: {
      left: 44,
      right: 24,
      top: title || subtitle ? 72 : 50,
      bottom: 42,
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "shadow",
        shadowStyle: {
          color: "rgba(14, 116, 144, 0.08)",
        },
      },
      backgroundColor: TOKENS.panelStrong,
      borderColor: "rgba(29, 26, 22, 0.14)",
      borderWidth: 1,
      textStyle: {
        color: TOKENS.text,
      },
      formatter(params) {
        const index = params?.[0]?.dataIndex ?? 0;
        const item = stats[index] || {};
        return [
          `<strong>${labels[index]}</strong>`,
          `Min-Max: ${formatNumber(item.min)} - ${formatNumber(item.max)} at.%`,
          `P10-P90: ${formatNumber(item.p10)} - ${formatNumber(item.p90)} at.%`,
          `P50: ${formatNumber(item.median)} at.%`,
        ].join("<br/>");
      },
    },
    xAxis: {
      type: "category",
      data: labels,
      axisTick: { show: false },
      axisLine: {
        lineStyle: {
          color: TOKENS.line,
        },
      },
      axisLabel: {
        color: TOKENS.muted,
        fontWeight: 700,
        fontSize: 12,
        margin: 12,
        textShadowColor: TOKENS.tealGlow,
        textShadowBlur: 4,
      },
    },
    yAxis: {
      type: "value",
      min: 0,
      max: yMax,
      splitNumber: 5,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        color: TOKENS.muted,
        fontSize: 12,
        margin: 10,
        textShadowColor: TOKENS.tealGlow,
        textShadowBlur: 4,
      },
      splitLine: {
        lineStyle: {
          color: TOKENS.grid,
        },
      },
    },
    series: [
      {
        name: "P10",
        type: "bar",
        data: p10Values,
        barWidth: 18,
        itemStyle: {
          color: colors.p10,
          borderRadius: [6, 6, 0, 0],
          shadowBlur: 14,
          shadowColor: colors.p10Glow,
        },
      },
      {
        name: "P50",
        type: "bar",
        data: medianValues,
        barWidth: 18,
        itemStyle: {
          color: colors.p50,
          borderRadius: [6, 6, 0, 0],
          shadowBlur: 18,
          shadowColor: colors.p50Glow,
        },
      },
      {
        name: "P90",
        type: "bar",
        data: p90Values,
        barWidth: 18,
        itemStyle: {
          color: colors.p90,
          borderRadius: [6, 6, 0, 0],
          shadowBlur: 14,
          shadowColor: colors.p90Glow,
        },
      },
    ],
  };
}

export function buildBatchHeatmapOption({
  plan,
  title = "",
  subtitle = "",
}) {
  if (!plan?.samples?.length) {
    return buildEmptyChartOption("等待批次布局", "预测空间载入后会生成 13×13 样本矩阵");
  }

  const cols = Number(plan.cols || 0);
  const rows = Number(plan.rows || 0);
  const samples = Array.isArray(plan.samples) ? plan.samples : [];
  const xLabels = Array.from({ length: cols }, (_, index) => `C${index + 1}`);
  const yLabels = Array.from({ length: rows }, (_, index) => `R${rows - index}`);

  const data = samples.map((sample) => ({
    value: [
      Number(sample.grid_col) - 1,
      rows - Number(sample.grid_row),
      Number(sample.snake_index) || 0,
    ],
    sample,
  }));

  const preview = (plan.preview || []).map((sample) => ({
    value: [
      Number(sample.grid_col) - 1,
      rows - Number(sample.grid_row),
      Number(sample.snake_index) || 0,
    ],
    sample,
  }));

  return {
    animationDuration: 700,
    title: titleBlock(title, subtitle),
    grid: {
      left: 52,
      right: 20,
      top: title || subtitle ? 68 : 28,
      bottom: 44,
    },
    tooltip: {
      position: "top",
      backgroundColor: TOKENS.panelStrong,
      borderColor: "rgba(29, 26, 22, 0.14)",
      borderWidth: 1,
      textStyle: {
        color: TOKENS.text,
      },
      formatter(params) {
        const sample = params.data?.sample;
        if (!sample) {
          return "批次点位";
        }
        return [
          `<strong>${sample.sample_id}</strong>`,
          `蛇形序号 ${sample.snake_index}`,
          `行 ${sample.grid_row} · 列 ${sample.grid_col}`,
          `Ti ${formatNumber(sample.ti_at)} / Zr ${formatNumber(sample.zr_at)} / Nb ${formatNumber(sample.nb_at)} / V ${formatNumber(sample.v_at)} at.%`,
        ].join("<br/>");
      },
    },
    xAxis: {
      type: "category",
      data: xLabels,
      splitArea: { show: false },
      axisTick: { show: false },
      axisLine: {
        lineStyle: {
          color: TOKENS.line,
        },
      },
      axisLabel: {
        color: TOKENS.muted,
        fontSize: 11,
        textShadowColor: TOKENS.tealGlow,
        textShadowBlur: 4,
      },
    },
    yAxis: {
      type: "category",
      data: yLabels,
      splitArea: { show: false },
      axisTick: { show: false },
      axisLine: {
        lineStyle: {
          color: TOKENS.line,
        },
      },
      axisLabel: {
        color: TOKENS.muted,
        fontSize: 11,
        textShadowColor: TOKENS.tealGlow,
        textShadowBlur: 4,
      },
    },
    visualMap: {
      min: 1,
      max: Number(plan.total_samples || samples.length || 1),
      show: false,
      calculable: false,
      inRange: {
        color: ["#e6efee", "#9fd0da", "#f0a776", "#c2410c"],
      },
    },
    series: [
      {
        name: "批次矩阵",
        type: "heatmap",
        data,
        progressive: 500,
        itemStyle: {
          borderColor: "rgba(29, 26, 22, 0.10)",
          borderWidth: 1,
          borderRadius: 0,
        },
        emphasis: {
          itemStyle: {
            shadowBlur: 24,
            shadowColor: TOKENS.warmGlow,
          },
        },
      },
      {
        name: "预览点位",
        type: "scatter",
        data: preview,
        symbolSize: 16,
        itemStyle: {
          color: TOKENS.teal,
          borderColor: "#ffffff",
          borderWidth: 2,
          shadowBlur: 18,
          shadowColor: TOKENS.tealGlow,
        },
        label: {
          show: true,
          formatter(params) {
            return params.data?.sample?.sample_id || "";
          },
          position: "top",
          color: TOKENS.text,
          fontSize: 10,
          fontWeight: 700,
          textShadowColor: TOKENS.tealGlow,
          textShadowBlur: 8,
        },
        tooltip: {
          show: false,
        },
      },
    ],
  };
}
