import { select, zoom, zoomIdentity } from "d3";

const VIEW_WIDTH = 1560;
const VIEW_HEIGHT = 640;

const TOKENS = {
  text: "#102433",
  muted: "#668393",
  bg: "#ffffff",
  panel: "#f8fcff",
  line: "rgba(19, 82, 113, 0.14)",
  pending: "#98aeba",
  pendingFill: "#f0f6fa",
  warm: "#e85d2a",
  warmFill: "#fff0e8",
  gold: "#c78615",
  goldFill: "#fff7df",
  teal: "#0788a8",
  tealFill: "#eaf8fc",
  success: "#159356",
  successFill: "#ebf9f2",
  danger: "#d12f4d",
  dangerFill: "#fff0f3",
};

const HEADER_THEME = {
  start: {
    fill: "rgba(232, 93, 42, 0.10)",
    stroke: "rgba(232, 93, 42, 0.88)",
  },
  parallel: {
    fill: "rgba(7, 136, 168, 0.10)",
    stroke: TOKENS.teal,
  },
  gate: {
    fill: "rgba(19, 82, 113, 0.06)",
    stroke: "#2f5567",
  },
  inference: {
    fill: "rgba(199, 134, 21, 0.10)",
    stroke: TOKENS.gold,
  },
};

const COLUMN_CENTERS = {
  start: 104,
  parallel: 576,
  gate: 1042,
  inference: 1456,
};

const ROUTE_ANCHORS = {
  startBusX: (COLUMN_CENTERS.start + COLUMN_CENTERS.parallel) / 2,
  mergeBusX: (COLUMN_CENTERS.parallel + COLUMN_CENTERS.gate) / 2,
};

const LAYOUT = {
  caption: { x: 24, y: 28 },
  headers: {
    start: { x: COLUMN_CENTERS.start, y: 92, width: 150, label: "START" },
    parallel: { x: COLUMN_CENTERS.parallel, y: 92, width: 430, label: "PARALLEL CHARACTERIZATION" },
    gate: { x: COLUMN_CENTERS.gate, y: 92, width: 204, label: "DECISION GATE" },
    inference: { x: COLUMN_CENTERS.inference, y: 92, width: 210, label: "MAT INFERENCE" },
  },
  nodes: {
    magnetron_sputtering: { x: COLUMN_CENTERS.start, y: 320, width: 176, height: 96 },
    xrd: { x: COLUMN_CENTERS.parallel, y: 180, width: 176, height: 96 },
    eds: { x: COLUMN_CENTERS.parallel, y: 320, width: 176, height: 96 },
    nanoindentation: { x: COLUMN_CENTERS.parallel, y: 460, width: 176, height: 96 },
    gate: { x: COLUMN_CENTERS.gate, y: 320, width: 210, height: 132 },
    model_inference: { x: COLUMN_CENTERS.inference, y: 320, width: 220, height: 112 },
  },
  routes: ROUTE_ANCHORS,
};

function roundedRectPath(cx, cy, width, height, radius = 22) {
  const x = cx - width / 2;
  const y = cy - height / 2;
  const r = Math.min(radius, width / 2, height / 2);
  return [
    `M${x + r},${y}`,
    `H${x + width - r}`,
    `Q${x + width},${y} ${x + width},${y + r}`,
    `V${y + height - r}`,
    `Q${x + width},${y + height} ${x + width - r},${y + height}`,
    `H${x + r}`,
    `Q${x},${y + height} ${x},${y + height - r}`,
    `V${y + r}`,
    `Q${x},${y} ${x + r},${y}`,
    "Z",
  ].join(" ");
}

function diamondPath(cx, cy, width, height) {
  return [
    `M${cx},${cy - height / 2}`,
    `L${cx + width / 2},${cy}`,
    `L${cx},${cy + height / 2}`,
    `L${cx - width / 2},${cy}`,
    "Z",
  ].join(" ");
}

function hexagonPath(cx, cy, width, height) {
  const shoulder = width * 0.24;
  return [
    `M${cx - width / 2 + shoulder},${cy - height / 2}`,
    `L${cx + width / 2 - shoulder},${cy - height / 2}`,
    `L${cx + width / 2},${cy}`,
    `L${cx + width / 2 - shoulder},${cy + height / 2}`,
    `L${cx - width / 2 + shoulder},${cy + height / 2}`,
    `L${cx - width / 2},${cy}`,
    "Z",
  ].join(" ");
}

function orthogonalPath(points) {
  if (!points.length) {
    return "";
  }
  const [first, ...rest] = points;
  return `M${first[0]},${first[1]} ` + rest.map((point) => `L${point[0]},${point[1]}`).join(" ");
}

function nodePalette(node) {
  if (node.status === "failed") {
    return { stroke: TOKENS.danger, fill: TOKENS.dangerFill, text: "#7f1d1d" };
  }
  if (node.code === "model_inference") {
    if (node.status === "completed") {
      return { stroke: TOKENS.gold, fill: TOKENS.goldFill, text: "#7c2d12" };
    }
    if (node.status === "current") {
      return { stroke: TOKENS.warm, fill: TOKENS.warmFill, text: "#7c2d12" };
    }
    return { stroke: TOKENS.pending, fill: TOKENS.pendingFill, text: "#3a352e" };
  }
  if (node.code === "gate") {
    if (node.status === "completed") {
      return { stroke: TOKENS.success, fill: TOKENS.successFill, text: "#166534" };
    }
    if (node.status === "current") {
      return { stroke: TOKENS.warm, fill: TOKENS.warmFill, text: "#7c2d12" };
    }
    return { stroke: TOKENS.pending, fill: TOKENS.pendingFill, text: "#3a352e" };
  }
  if (node.code === "magnetron_sputtering") {
    if (node.status === "completed") {
      return { stroke: TOKENS.success, fill: TOKENS.successFill, text: "#166534" };
    }
    if (node.status === "current") {
      return { stroke: TOKENS.warm, fill: TOKENS.warmFill, text: "#7c2d12" };
    }
    return { stroke: TOKENS.pending, fill: TOKENS.pendingFill, text: "#3a352e" };
  }
  if (node.status === "completed") {
    return { stroke: TOKENS.success, fill: TOKENS.successFill, text: "#166534" };
  }
  if (node.status === "uploaded") {
    return { stroke: "#0891b2", fill: "#ecf9fb", text: "#155e75" };
  }
  if (node.status === "current") {
    return { stroke: TOKENS.teal, fill: TOKENS.tealFill, text: "#155e75" };
  }
  if (node.status === "triggered") {
    return { stroke: TOKENS.gold, fill: TOKENS.goldFill, text: "#7c2d12" };
  }
  return { stroke: TOKENS.pending, fill: TOKENS.pendingFill, text: "#3a352e" };
}

function nodeLighting(node) {
  if (node.status === "failed") {
    return { glowOpacity: 0.16, glowSpread: 11, highlightOpacity: 0.14 };
  }
  if (node.status === "completed") {
    return { glowOpacity: 0.24, glowSpread: 15, highlightOpacity: 0.2 };
  }
  if (node.status === "uploaded") {
    return { glowOpacity: 0.3, glowSpread: 17, highlightOpacity: 0.24 };
  }
  if (node.status === "current") {
    return { glowOpacity: 0.26, glowSpread: 16, highlightOpacity: 0.22 };
  }
  if (node.status === "triggered") {
    return { glowOpacity: 0.23, glowSpread: 14, highlightOpacity: 0.18 };
  }
  return { glowOpacity: 0.08, glowSpread: 9, highlightOpacity: 0.08 };
}

function edgePalette(status) {
  if (status === "failed") {
    return {
      stroke: TOKENS.danger,
      dash: null,
      opacity: 0.94,
      width: 3.8,
      glowOpacity: 0.18,
      animated: false,
      animatedDash: null,
      animatedOpacity: 0,
      markerWidth: 16,
      markerHeight: 16,
      markerRefX: 13.2,
      markerViewBox: "0 0 16 12",
      markerRefY: 6,
      markerPath: "M0.8 0.8 L15 6 L0.8 11.2 L5.4 6 Z",
      lineCap: "round",
      lineJoin: "round",
    };
  }
  if (status === "triggered") {
    return {
      stroke: TOKENS.gold,
      dash: null,
      opacity: 0.95,
      width: 3.8,
      glowOpacity: 0.18,
      animated: false,
      animatedDash: null,
      animatedOpacity: 0,
      markerWidth: 16,
      markerHeight: 16,
      markerRefX: 13.2,
      markerViewBox: "0 0 16 12",
      markerRefY: 6,
      markerPath: "M0.8 0.8 L15 6 L0.8 11.2 L5.4 6 Z",
      lineCap: "round",
      lineJoin: "round",
    };
  }
  if (status === "uploaded") {
    return {
      stroke: "#0891b2",
      dash: null,
      opacity: 0.98,
      width: 4.1,
      glowOpacity: 0.26,
      glowWidthExtra: 7,
      animated: true,
      animatedDash: "8 14",
      animatedOpacity: 0.86,
      animatedWidth: 3.2,
      markerWidth: 16,
      markerHeight: 16,
      markerRefX: 13.2,
      markerViewBox: "0 0 16 12",
      markerRefY: 6,
      markerPath: "M0.8 0.8 L15 6 L0.8 11.2 L5.4 6 Z",
      lineCap: "round",
      lineJoin: "round",
    };
  }
  if (status === "completed") {
    return {
      stroke: TOKENS.success,
      dash: null,
      opacity: 0.98,
      width: 4.8,
      glowOpacity: 0.1,
      animated: false,
      animatedDash: null,
      animatedOpacity: 0,
      markerWidth: 24,
      markerHeight: 24,
      markerRefX: 20.2,
      markerViewBox: "0 0 22 16",
      markerRefY: 8,
      markerPath: "M0.8 0.8 L21 8 L0.8 15.2 L7.6 8 Z",
      lineCap: "square",
      lineJoin: "miter",
    };
  }
  if (status === "current") {
    return {
      stroke: TOKENS.warm,
      dash: "12 10",
      opacity: 0.98,
      width: 4.2,
      glowOpacity: 0.3,
      glowWidthExtra: 7.4,
      animated: true,
      animatedDash: "7 16",
      animatedOpacity: 0.98,
      animatedWidth: 3.5,
      markerWidth: 16,
      markerHeight: 16,
      markerRefX: 13.2,
      markerViewBox: "0 0 16 12",
      markerRefY: 6,
      markerPath: "M0.8 0.8 L15 6 L0.8 11.2 L5.4 6 Z",
      lineCap: "round",
      lineJoin: "round",
    };
  }
  return {
    stroke: TOKENS.pending,
    dash: "8 10",
    opacity: 0.9,
    width: 3.6,
    glowOpacity: 0.16,
    glowWidthExtra: 6.2,
    animated: true,
    animatedDash: "6 15",
    animatedOpacity: 0.68,
    animatedWidth: 2.9,
    markerWidth: 16,
    markerHeight: 16,
    markerRefX: 13.2,
    markerViewBox: "0 0 16 12",
    markerRefY: 6,
    markerPath: "M0.8 0.8 L15 6 L0.8 11.2 L5.4 6 Z",
    lineCap: "round",
    lineJoin: "round",
  };
}

function buildFlowScene(monitor, scheduleName, batchLabel) {
  if (!monitor) {
    return null;
  }

  const nodeMap = Object.fromEntries((monitor.nodes || []).map((node) => [node.code, node]));
  const linkState = Object.fromEntries((monitor.links || []).map((link) => [link.key, link.status]));
  const prep = nodeMap.magnetron_sputtering;
  const xrd = nodeMap.xrd;
  const eds = nodeMap.eds;
  const nano = nodeMap.nanoindentation;
  const inference = nodeMap.model_inference;
  const gateStatus =
    monitor.gateStatus === "triggered"
      ? "completed"
      : monitor.gateStatus === "open"
        ? "current"
        : "pending";
  const gateNode = {
    code: "gate",
    short: "表征实验未完成",
    title: "表征实验未完成",
    status: gateStatus,
    stateLabel: "",
    detail: monitor.gateLabel,
  };

  const nodes = [prep, xrd, eds, nano, gateNode, inference]
    .filter(Boolean)
    .map((node) => ({
      ...node,
      ...LAYOUT.nodes[node.code],
      shape:
        node.code === "magnetron_sputtering"
          ? "roundRect"
          : node.code === "gate"
            ? "diamond"
            : node.code === "model_inference"
              ? "hexagon"
              : "rect",
    }));

  const startRight = LAYOUT.nodes.magnetron_sputtering.x + LAYOUT.nodes.magnetron_sputtering.width / 2;
  const xrdLeft = LAYOUT.nodes.xrd.x - LAYOUT.nodes.xrd.width / 2;
  const edsLeft = LAYOUT.nodes.eds.x - LAYOUT.nodes.eds.width / 2;
  const nanoLeft = LAYOUT.nodes.nanoindentation.x - LAYOUT.nodes.nanoindentation.width / 2;
  const xrdRight = LAYOUT.nodes.xrd.x + LAYOUT.nodes.xrd.width / 2;
  const edsRight = LAYOUT.nodes.eds.x + LAYOUT.nodes.eds.width / 2;
  const nanoRight = LAYOUT.nodes.nanoindentation.x + LAYOUT.nodes.nanoindentation.width / 2;
  const gateLeft = LAYOUT.nodes.gate.x - LAYOUT.nodes.gate.width / 2;
  const gateRight = LAYOUT.nodes.gate.x + LAYOUT.nodes.gate.width / 2;
  const modelLeft = LAYOUT.nodes.model_inference.x - LAYOUT.nodes.model_inference.width / 2;
  const startBusX = LAYOUT.routes.startBusX;
  const mergeBusX = LAYOUT.routes.mergeBusX;

  const edges = [
    {
      key: "start-xrd",
      status: linkState["start-xrd"] || "pending",
      d: orthogonalPath([
        [startRight, LAYOUT.nodes.magnetron_sputtering.y],
        [startBusX, LAYOUT.nodes.magnetron_sputtering.y],
        [startBusX, LAYOUT.nodes.xrd.y],
        [xrdLeft, LAYOUT.nodes.xrd.y],
      ]),
    },
    {
      key: "start-eds",
      status: linkState["start-eds"] || "pending",
      d: orthogonalPath([
        [startRight, LAYOUT.nodes.magnetron_sputtering.y],
        [startBusX, LAYOUT.nodes.magnetron_sputtering.y],
        [edsLeft, LAYOUT.nodes.eds.y],
      ]),
    },
    {
      key: "start-nano",
      status: linkState["start-nano"] || "pending",
      d: orthogonalPath([
        [startRight, LAYOUT.nodes.magnetron_sputtering.y],
        [startBusX, LAYOUT.nodes.magnetron_sputtering.y],
        [startBusX, LAYOUT.nodes.nanoindentation.y],
        [nanoLeft, LAYOUT.nodes.nanoindentation.y],
      ]),
    },
    {
      key: "xrd-inference",
      status: linkState["xrd-inference"] || "pending",
      d: orthogonalPath([
        [xrdRight, LAYOUT.nodes.xrd.y],
        [mergeBusX, LAYOUT.nodes.xrd.y],
        [mergeBusX, LAYOUT.nodes.gate.y],
        [gateLeft, LAYOUT.nodes.gate.y],
      ]),
    },
    {
      key: "eds-inference",
      status: linkState["eds-inference"] || "pending",
      d: orthogonalPath([
        [edsRight, LAYOUT.nodes.eds.y],
        [mergeBusX, LAYOUT.nodes.eds.y],
        [gateLeft, LAYOUT.nodes.gate.y],
      ]),
    },
    {
      key: "nano-inference",
      status: linkState["nano-inference"] || "pending",
      d: orthogonalPath([
        [nanoRight, LAYOUT.nodes.nanoindentation.y],
        [mergeBusX, LAYOUT.nodes.nanoindentation.y],
        [mergeBusX, LAYOUT.nodes.gate.y],
        [gateLeft, LAYOUT.nodes.gate.y],
      ]),
    },
    {
      key: "gate-model",
      status:
        monitor.gateStatus === "triggered"
          ? "completed"
          : monitor.gateStatus === "open"
            ? "current"
            : "pending",
      d: orthogonalPath([
        [gateRight, LAYOUT.nodes.gate.y],
        [modelLeft, LAYOUT.nodes.model_inference.y],
      ]),
    },
  ];

  return {
    caption: `${scheduleName} · ${batchLabel}`,
    headers: [
      { ...LAYOUT.headers.start, ...HEADER_THEME.start },
      { ...LAYOUT.headers.parallel, ...HEADER_THEME.parallel },
      { ...LAYOUT.headers.gate, ...HEADER_THEME.gate },
      { ...LAYOUT.headers.inference, ...HEADER_THEME.inference },
    ],
    nodes,
    edges,
  };
}

export default {
  name: "FlowMonitorPanel",

  props: {
    monitor: {
      type: Object,
      default: null,
    },
    scheduleName: {
      type: String,
      default: "",
    },
    batchLabel: {
      type: String,
      default: "",
    },
    height: {
      type: String,
      default: "640px",
    },
    initialAlign: {
      type: String,
      default: "center",
      validator: (value) => ["center", "left"].includes(value),
    },
  },

  data() {
    return {
      resizeObserver: null,
      renderFrame: 0,
      zoomBehavior: null,
      svg: null,
      stage: null,
      currentTransform: null,
    };
  },

  watch: {
    monitor: {
      deep: true,
      handler() {
        this.scheduleRender();
      },
    },
    scheduleName() {
      this.scheduleRender();
    },
    batchLabel() {
      this.scheduleRender();
    },
    height() {
      this.scheduleRender();
    },
    initialAlign() {
      this.scheduleRender();
    },
  },

  mounted() {
    if (typeof ResizeObserver !== "undefined") {
      this.resizeObserver = new ResizeObserver(() => {
        this.scheduleRender();
      });
      this.resizeObserver.observe(this.$refs.host);
    } else {
      window.addEventListener("resize", this.handleWindowResize);
    }
    this.scheduleRender();
  },

  beforeUnmount() {
    if (this.renderFrame) {
      cancelAnimationFrame(this.renderFrame);
      this.renderFrame = 0;
    }
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    } else {
      window.removeEventListener("resize", this.handleWindowResize);
    }
  },

  methods: {
    handleWindowResize() {
      this.scheduleRender();
    },

    getHostSize() {
      const host = this.$refs.host;
      return {
        width: host?.clientWidth || 0,
        height: host?.clientHeight || 0,
      };
    },

    scheduleRender() {
      if (this.renderFrame) {
        cancelAnimationFrame(this.renderFrame);
      }
      this.renderFrame = requestAnimationFrame(() => {
        this.renderFrame = 0;
        this.renderGraph();
      });
    },

    initialTransform(width, height) {
      const pad = 4;
      const scale = Math.max(0.56, Math.min((width - pad * 2) / VIEW_WIDTH, (height - pad * 2) / VIEW_HEIGHT));
      const tx = this.initialAlign === "left" ? pad : (width - VIEW_WIDTH * scale) / 2;
      const ty = (height - VIEW_HEIGHT * scale) / 2;
      return zoomIdentity.translate(tx, ty).scale(scale);
    },

    zoomBy(factor) {
      if (!this.svg || !this.zoomBehavior) {
        return;
      }
      this.svg.transition().duration(180).call(this.zoomBehavior.scaleBy, factor);
    },

    resetZoom() {
      if (!this.svg || !this.zoomBehavior) {
        return;
      }
      const { width, height } = this.getHostSize();
      const transform = this.initialTransform(width, height);
      this.svg.transition().duration(220).call(this.zoomBehavior.transform, transform);
    },

    renderEmptyState(svg, width, height) {
      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height / 2 - 8)
        .attr("fill", TOKENS.text)
        .attr("text-anchor", "middle")
        .attr("font-size", 20)
        .attr("font-weight", 700)
        .text("暂无流程监控");

      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height / 2 + 20)
        .attr("fill", TOKENS.muted)
        .attr("text-anchor", "middle")
        .attr("font-size", 12)
        .text("等待调度创建并派发首轮执行");
    },

    renderGraph() {
      const { width, height } = this.getHostSize();
      if (!width || !height) {
        return;
      }

      const scene = buildFlowScene(this.monitor, this.scheduleName, this.batchLabel);
      const host = select(this.$refs.host);
      host.selectAll("*").remove();

      const svg = host
        .append("svg")
        .attr("class", "flow-graph-svg")
        .attr("width", width)
        .attr("height", height)
        .attr("viewBox", `0 0 ${width} ${height}`);

      if (!scene) {
        this.svg = svg;
        this.stage = null;
        this.zoomBehavior = null;
        this.currentTransform = null;
        this.renderEmptyState(svg, width, height);
        return;
      }

      const defs = svg.append("defs");
      defs
        .append("style")
        .text(`
          .flow-graph-edge-glow,
          .flow-graph-edge-main,
          .flow-graph-edge-animated {
            fill: none;
            stroke-linecap: round;
            stroke-linejoin: round;
            vector-effect: non-scaling-stroke;
          }

          .flow-edge-animated-current {
            animation: flow-edge-dash-current 1.15s linear infinite;
          }

          .flow-edge-animated-pending {
            animation: flow-edge-dash-pending 1.9s linear infinite;
          }

          @keyframes flow-edge-dash-current {
            from { stroke-dashoffset: 0; }
            to { stroke-dashoffset: -46; }
          }

          @keyframes flow-edge-dash-pending {
            from { stroke-dashoffset: 0; }
            to { stroke-dashoffset: -34; }
          }
        `);
      const nodeGlowFilter = defs
        .append("filter")
        .attr("id", "flow-node-glow")
        .attr("x", "-90%")
        .attr("y", "-90%")
        .attr("width", "280%")
        .attr("height", "280%");
      nodeGlowFilter.append("feGaussianBlur").attr("stdDeviation", 7.5).attr("result", "blur");
      nodeGlowFilter.append("feMerge").selectAll("feMergeNode").data(["blur"]).join("feMergeNode").attr("in", (d) => d);
      const headerGlowFilter = defs
        .append("filter")
        .attr("id", "flow-header-glow")
        .attr("x", "-80%")
        .attr("y", "-220%")
        .attr("width", "260%")
        .attr("height", "520%");
      headerGlowFilter.append("feGaussianBlur").attr("stdDeviation", 2.4).attr("result", "blur");
      headerGlowFilter.append("feMerge").selectAll("feMergeNode").data(["blur"]).join("feMergeNode").attr("in", (d) => d);
      const statuses = ["pending", "current", "uploaded", "completed", "triggered", "failed"];
      for (const status of statuses) {
        const palette = edgePalette(status);
        const marker = defs
          .append("marker")
          .attr("id", `flow-arrow-${status}`)
          .attr("viewBox", palette.markerViewBox)
          .attr("refX", palette.markerRefX)
          .attr("refY", palette.markerRefY)
          .attr("markerWidth", palette.markerWidth)
          .attr("markerHeight", palette.markerHeight)
          .attr("markerUnits", "userSpaceOnUse")
          .attr("orient", "auto");
        marker
          .append("path")
          .attr("d", palette.markerPath)
          .attr("fill", palette.stroke)
          .attr("opacity", palette.opacity);
      }

      const stage = svg.append("g").attr("class", "flow-graph-stage");

      stage
        .append("text")
        .attr("x", LAYOUT.caption.x)
        .attr("y", LAYOUT.caption.y)
        .attr("fill", TOKENS.muted)
        .attr("font-size", 20)
        .attr("font-weight", 700)
        .text(scene.caption);

      const headers = stage.append("g").attr("class", "flow-graph-headers");
      headers
        .selectAll("g.flow-graph-header")
        .data(scene.headers)
        .join("g")
        .attr("class", "flow-graph-header")
        .each(function renderHeader(header) {
          const group = select(this);
          group
            .append("rect")
            .attr("x", header.x - header.width / 2)
            .attr("y", header.y - 15)
            .attr("width", header.width)
            .attr("height", 30)
            .attr("fill", "none")
            .attr("stroke", header.stroke)
            .attr("stroke-width", 9)
            .attr("opacity", 0.22)
            .attr("filter", "url(#flow-header-glow)");
          group
            .append("rect")
            .attr("x", header.x - header.width / 2)
            .attr("y", header.y - 15)
            .attr("width", header.width)
            .attr("height", 30)
            .attr("fill", header.fill)
            .attr("stroke", header.stroke)
            .attr("stroke-width", 1.8);
          group
            .append("text")
            .attr("x", header.x)
            .attr("y", header.y + 5)
            .attr("fill", header.stroke)
            .attr("font-size", 18)
            .attr("font-weight", 800)
            .attr("letter-spacing", "0.02em")
            .attr("text-anchor", "middle")
            .text(header.label);
        });

      const edgeLayer = stage.append("g").attr("class", "flow-graph-edges");
      edgeLayer
        .selectAll("g.flow-graph-edge-group")
        .data(scene.edges)
        .join((enter) => {
          const group = enter.append("g").attr("class", "flow-graph-edge-group");
          group.append("path").attr("class", "flow-graph-edge-glow");
          group.append("path").attr("class", "flow-graph-edge-main");
          group.append("path").attr("class", "flow-graph-edge-animated");
          return group;
        })
        .attr("class", (edge) => `flow-graph-edge-group edge-status-${edge.status}`)
        .each(function renderEdge(edge) {
          const group = select(this);
          const palette = edgePalette(edge.status);
          const mainWidth = palette.width * 1.18;

          group
            .select(".flow-graph-edge-glow")
            .attr("d", edge.d)
            .attr("stroke", palette.stroke)
            .attr("stroke-width", mainWidth + (palette.glowWidthExtra || 6.4))
            .attr("opacity", Math.min(0.42, palette.glowOpacity + 0.05));

          group
            .select(".flow-graph-edge-main")
            .attr("d", edge.d)
            .attr("stroke", palette.stroke)
            .attr("stroke-width", mainWidth)
            .attr("stroke-dasharray", palette.dash || null)
            .attr("stroke-linecap", palette.lineCap)
            .attr("stroke-linejoin", palette.lineJoin)
            .attr("opacity", palette.opacity)
            .attr("marker-end", `url(#flow-arrow-${edge.status})`);

          group
            .select(".flow-graph-edge-animated")
            .attr("d", edge.d)
            .attr("stroke", palette.stroke)
            .attr("stroke-width", palette.animatedWidth ? palette.animatedWidth * 1.18 : Math.max(1.6, mainWidth - 1.2))
            .attr("stroke-dasharray", palette.animated ? palette.animatedDash : null)
            .attr("opacity", palette.animated ? palette.animatedOpacity : 0)
            .attr("class", `flow-graph-edge-animated ${palette.animated ? `flow-edge-animated-${edge.status}` : ""}`);
        });

      const nodeLayer = stage.append("g").attr("class", "flow-graph-nodes");
      nodeLayer
        .selectAll("g.flow-graph-node")
        .data(scene.nodes)
        .join("g")
        .attr("class", (node) => `flow-graph-node node-${node.code} node-status-${node.status}`)
        .attr("transform", (node) => `translate(${node.x},${node.y})`)
        .each(function renderNode(node) {
          const group = select(this);
          const palette = nodePalette(node);
          const lighting = nodeLighting(node);
          let d = "";
          if (node.shape === "roundRect") {
            d = roundedRectPath(0, 0, node.width, node.height, 22);
          } else if (node.shape === "diamond") {
            d = diamondPath(0, 0, node.width, node.height);
          } else if (node.shape === "hexagon") {
            d = hexagonPath(0, 0, node.width, node.height);
          } else {
            d = roundedRectPath(0, 0, node.width, node.height, 4);
          }

          group
            .append("path")
            .attr("d", d)
            .attr("fill", "none")
            .attr("stroke", palette.stroke)
            .attr("stroke-width", (node.code === "model_inference" ? 2.6 : 2.2) + lighting.glowSpread)
            .attr("opacity", lighting.glowOpacity)
            .attr("filter", "url(#flow-node-glow)")
            .attr("stroke-linejoin", "round")
            .attr("stroke-linecap", "round");

          group
            .append("path")
            .attr("d", d)
            .attr("fill", palette.fill)
            .attr("stroke", palette.stroke)
            .attr("stroke-width", node.code === "model_inference" ? 3.2 : 2.8);

          group
            .append("path")
            .attr("d", d)
            .attr("fill", "none")
            .attr("stroke", "rgba(29, 26, 22, 0.12)")
            .attr("stroke-width", node.code === "model_inference" ? 1.6 : 1.4)
            .attr("opacity", lighting.highlightOpacity)
            .attr("stroke-linejoin", "round")
            .attr("stroke-linecap", "round");

          group
            .append("title")
            .text([node.title, node.detail, node.componentId].filter(Boolean).join("\n"));

          const text = group
            .append("text")
            .attr("fill", palette.text)
            .attr("text-anchor", "middle")
            .attr("font-family", "IBM Plex Sans, Segoe UI, PingFang SC, Microsoft YaHei, sans-serif");

          const hasState = Boolean(node.stateLabel);
          text
            .append("tspan")
            .attr("x", 0)
            .attr("y", hasState ? -7 : 7)
            .attr("font-size", node.code === "model_inference" ? 28 : 25)
            .attr("font-weight", 800)
            .text(node.short);

          if (hasState) {
            text
              .append("tspan")
              .attr("x", 0)
              .attr("dy", 34)
              .attr("font-size", node.code === "model_inference" ? 18 : 18)
              .attr("font-weight", 800)
              .attr("fill-opacity", 0.92)
              .text(node.stateLabel);
          }
        });

      this.zoomBehavior = zoom()
        .scaleExtent([0.62, 2.4])
        .on("zoom", (event) => {
          stage.attr("transform", event.transform);
          this.currentTransform = event.transform;
        });

      svg.call(this.zoomBehavior).on("dblclick.zoom", null);
      const transform = this.currentTransform || this.initialTransform(width, height);
      svg.call(this.zoomBehavior.transform, transform);

      this.svg = svg;
      this.stage = stage;
    },
  },

  template: `
    <div class="flow-graph-root" :style="{ height }">
      <div class="flow-graph-toolbar">
        <button class="flow-graph-tool" type="button" @click="zoomBy(1.18)">+</button>
        <button class="flow-graph-tool" type="button" @click="zoomBy(1 / 1.18)">-</button>
        <button class="flow-graph-tool flow-graph-tool-reset" type="button" @click="resetZoom">重置</button>
      </div>
      <div ref="host" class="flow-graph-host"></div>
    </div>
  `,
};
