import { max, scaleBand, scaleLinear, select } from "d3";

const KEYS = ["ti_at", "zr_at", "nb_at", "v_at"];
const LABELS = {
  ti_at: "Ti",
  zr_at: "Zr",
  nb_at: "Nb",
  v_at: "V",
};

const THEMES = {
  warm: {
    p10: "rgba(7, 136, 168, 0.62)",
    p50: "rgba(232, 93, 42, 0.78)",
    p90: "rgba(199, 134, 21, 0.66)",
    grid: "rgba(19, 82, 113, 0.08)",
    axis: "rgba(19, 82, 113, 0.16)",
    text: "#668393",
    title: "#102433",
  },
  sage: {
    p10: "rgba(21, 147, 86, 0.58)",
    p50: "rgba(7, 136, 168, 0.72)",
    p90: "rgba(232, 93, 42, 0.66)",
    grid: "rgba(19, 82, 113, 0.08)",
    axis: "rgba(19, 82, 113, 0.16)",
    text: "#668393",
    title: "#102433",
  },
};

function niceCeil(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 10;
  }
  return Math.ceil(value / 5) * 5;
}

function statValue(stats, key, fallback = 0) {
  const value = stats?.[key];
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function buildRows(space) {
  return KEYS.map((key) => {
    const stats = space?.elements?.[key] || {};
    return {
      key,
      label: LABELS[key],
      min: statValue(stats, "min"),
      max: statValue(stats, "max"),
      p10: statValue(stats, "p10", statValue(stats, "min")),
      p50: statValue(stats, "median", statValue(stats, "p50")),
      p90: statValue(stats, "p90", statValue(stats, "max")),
    };
  });
}

export default {
  name: "CompositionRangePanel",

  props: {
    space: {
      type: Object,
      default: null,
    },
    height: {
      type: String,
      default: "260px",
    },
    theme: {
      type: String,
      default: "warm",
    },
  },

  data() {
    return {
      resizeObserver: null,
      renderFrame: 0,
    };
  },

  watch: {
    space: {
      deep: true,
      handler() {
        this.scheduleRender();
      },
    },
    height() {
      this.scheduleRender();
    },
    theme() {
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
        this.renderChart();
      });
    },

    renderEmptyState(svg, width, height, palette) {
      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height / 2 - 8)
        .attr("fill", palette.title)
        .attr("font-size", 18)
        .attr("font-weight", 700)
        .attr("text-anchor", "middle")
        .text("等待成分空间");

      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height / 2 + 18)
        .attr("fill", palette.text)
        .attr("font-size", 12)
        .attr("text-anchor", "middle")
        .text("mat_model 返回后，这里会显示下一轮区间带");
    },

    renderChart() {
      const { width, height } = this.getHostSize();
      if (!width || !height) {
        return;
      }

      const palette = THEMES[this.theme] || THEMES.warm;
      const host = select(this.$refs.host);
      host.selectAll("*").remove();

      const svg = host
        .append("svg")
        .attr("class", "range-graph-svg")
        .attr("width", width)
        .attr("height", height)
        .attr("viewBox", `0 0 ${width} ${height}`);

      if (!this.space?.elements) {
        this.renderEmptyState(svg, width, height, palette);
        return;
      }

      const rows = buildRows(this.space);
      const maxValue = max(rows, (row) => Math.max(row.max, row.p90, row.p50, row.p10)) || 30;
      const yMax = niceCeil(Math.max(30, maxValue));
      const margin = {
        top: 40,
        right: 22,
        bottom: 38,
        left: 44,
      };
      const innerWidth = width - margin.left - margin.right;
      const innerHeight = height - margin.top - margin.bottom;

      const x = scaleBand()
        .domain(rows.map((row) => row.label))
        .range([margin.left, margin.left + innerWidth])
        .paddingInner(0.32)
        .paddingOuter(0.08);

      const group = scaleBand()
        .domain(["p10", "p50", "p90"])
        .range([0, x.bandwidth()])
        .padding(0.16);

      const y = scaleLinear()
        .domain([0, yMax])
        .range([margin.top + innerHeight, margin.top]);

      const ticks = y.ticks(5);
      svg
        .append("g")
        .selectAll("line")
        .data(ticks)
        .join("line")
        .attr("x1", margin.left)
        .attr("x2", width - margin.right)
        .attr("y1", (tick) => y(tick))
        .attr("y2", (tick) => y(tick))
        .attr("stroke", palette.grid)
        .attr("stroke-width", 1);

      svg
        .append("line")
        .attr("x1", margin.left)
        .attr("x2", margin.left)
        .attr("y1", margin.top)
        .attr("y2", height - margin.bottom)
        .attr("stroke", palette.axis)
        .attr("stroke-width", 1);

      svg
        .append("line")
        .attr("x1", margin.left)
        .attr("x2", width - margin.right)
        .attr("y1", height - margin.bottom)
        .attr("y2", height - margin.bottom)
        .attr("stroke", palette.axis)
        .attr("stroke-width", 1);

      svg
        .append("g")
        .selectAll("text")
        .data(ticks)
        .join("text")
        .attr("x", margin.left - 10)
        .attr("y", (tick) => y(tick) + 4)
        .attr("fill", palette.text)
        .attr("font-size", 11)
        .attr("text-anchor", "end")
        .text((tick) => `${tick}`);

      svg
        .append("g")
        .selectAll("text")
        .data(rows)
        .join("text")
        .attr("x", (row) => x(row.label) + x.bandwidth() / 2)
        .attr("y", height - margin.bottom + 22)
        .attr("fill", palette.text)
        .attr("font-size", 12)
        .attr("font-weight", 700)
        .attr("text-anchor", "middle")
        .text((row) => row.label);

      const legend = [
        { key: "p10", label: "P10", color: palette.p10 },
        { key: "p50", label: "P50", color: palette.p50 },
        { key: "p90", label: "P90", color: palette.p90 },
      ];

      const legendGroup = svg.append("g").attr("transform", `translate(${margin.left},18)`);
      legend.forEach((item, index) => {
        const xOffset = index * 74;
        legendGroup
          .append("rect")
          .attr("x", xOffset)
          .attr("y", -8)
          .attr("width", 12)
          .attr("height", 12)
          .attr("fill", item.color);
        legendGroup
          .append("text")
          .attr("x", xOffset + 18)
          .attr("y", 2)
          .attr("fill", palette.text)
          .attr("font-size", 12)
          .text(item.label);
      });

      const bars = svg.append("g").attr("class", "range-graph-bars");
      for (const row of rows) {
        const bandX = x(row.label);
        for (const key of ["p10", "p50", "p90"]) {
          const value = row[key];
          const barX = bandX + group(key);
          const barY = y(value);
          const barHeight = height - margin.bottom - barY;
          const rect = bars
            .append("rect")
            .attr("x", barX)
            .attr("y", barY)
            .attr("width", group.bandwidth())
            .attr("height", barHeight)
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("fill", palette[key]);

          rect
            .append("title")
            .text(
              `${row.label}\nMin-Max ${row.min.toFixed(1)} - ${row.max.toFixed(1)} at.%\n${key.toUpperCase()} ${value.toFixed(1)} at.%`
            );
        }
      }
    },
  },

  template: `<div ref="host" class="range-graph-root" :style="{ height }"></div>`,
};
