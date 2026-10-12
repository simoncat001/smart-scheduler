import * as echarts from "echarts";

export default {
  name: "ChartPanel",

  props: {
    option: {
      type: Object,
      default: null,
    },
    height: {
      type: String,
      default: "320px",
    },
  },

  data() {
    return {
      chart: null,
      resizeObserver: null,
      renderFrame: 0,
    };
  },

  watch: {
    option: {
      deep: true,
      handler() {
        this.scheduleRender();
      },
    },
    height() {
      this.scheduleRender();
    },
  },

  mounted() {
    if (typeof ResizeObserver !== "undefined") {
      this.resizeObserver = new ResizeObserver(() => {
        if (!this.hasRenderableSize()) {
          return;
        }
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

    if (this.chart) {
      this.chart.dispose();
      this.chart = null;
    }
  },

  methods: {
    getHostSize() {
      const host = this.$refs.host;
      return {
        width: host?.clientWidth || 0,
        height: host?.clientHeight || 0,
      };
    },

    hasRenderableSize() {
      const { width, height } = this.getHostSize();
      return width > 0 && height > 0;
    },

    ensureChart() {
      if (this.chart || !this.hasRenderableSize()) {
        return this.chart;
      }
      const { width, height } = this.getHostSize();
      this.chart = echarts.init(this.$refs.host, null, {
        renderer: "canvas",
        width,
        height,
      });
      return this.chart;
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

    handleWindowResize() {
      this.scheduleRender();
    },

    renderChart() {
      if (!this.hasRenderableSize()) {
        return;
      }
      if (this.chart) {
        this.chart.dispose();
        this.chart = null;
      }
      const chart = this.ensureChart();
      if (!chart) {
        return;
      }
      chart.setOption(this.option || {}, {
        notMerge: true,
        lazyUpdate: false,
      });
    },
  },

  template: `<div ref="host" class="chart-panel-root" :style="{ height }"></div>`,
};
