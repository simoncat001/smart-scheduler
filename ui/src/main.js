import { createApp } from "vue";

import appOptions from "./app-options";
import ChartPanel from "./components/chart-panel";
import CompositionRangePanel from "./components/composition-range-panel";
import FlowMonitorPanel from "./components/flow-monitor-panel";
import WorkflowBuilder from "./components/workflow-builder.js";
import DynamicFlowPanel from "./components/dynamic-flow-panel.js";
import { GitBranch } from "lucide-vue-next";
import "@vue-flow/core/dist/style.css";
import "@vue-flow/core/dist/theme-default.css";
import "./styles.css";
import templateDocument from "./template.html?raw";

function resolveTemplate(documentText) {
  const parsed = new DOMParser().parseFromString(documentText, "text/html");
  const root = parsed.querySelector("#app");
  return root ? root.innerHTML.trim() : documentText;
}

const app = createApp({
  ...appOptions,
  template: resolveTemplate(templateDocument),
});

app.component("ChartPanel", ChartPanel);
app.component("CompositionRangePanel", CompositionRangePanel);
app.component("FlowMonitorPanel", FlowMonitorPanel);
app.component("WorkflowBuilder", WorkflowBuilder);
app.component("DynamicFlowPanel", DynamicFlowPanel);
app.component("GitBranch", GitBranch);
app.mount("#app");
