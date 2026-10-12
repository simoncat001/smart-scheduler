import { markRaw } from "vue";
import { VueFlow } from "@vue-flow/core";
import { ZoomIn, ZoomOut, Maximize, Radio, X, RefreshCw } from "lucide-vue-next";
import WorkflowNode from "./workflow-node.js";
import { toFlowGraph } from "../workflow-graph.js";
import { runtimeToMonitor, availableNodeOperations } from "../workflow-monitor.js";
import { workflowApi } from "../workflow-api.js";

export default {
  name: "DynamicFlowPanel",
  components: { VueFlow, WorkflowNode, ZoomIn, ZoomOut, Maximize, Radio, X, RefreshCw },
  props: { runtime: { type: Object, required: true }, health: Object, scheduleName: String, batchLabel: String, height: { type: String, default: "677px" } },
  emits: ["refresh"],
  data() {
    return { flow: null, flowNodes: [], flowEdges: [], selectedId: "", showHealth: false, error: "", busy: false,
      actor: "", reason: "", approvalJson: "{}", request: null, requestSignature: "", geometryKey: "", needsFit: true };
  },
  computed: {
    monitor() { return runtimeToMonitor(this.runtime); },
    selectedNode() { return this.monitor.nodes.find(node => node.code === this.selectedId); },
    canRetry() { return availableNodeOperations(this.selectedNode).includes("retry"); },
    canCancel() { return availableNodeOperations(this.selectedNode).includes("cancel"); },
  },
  watch: { runtime: { deep: true, immediate: true, handler() { this.syncFlow(); } } },
  methods: {
    syncFlow() {
      const monitor = runtimeToMonitor(this.runtime);
      const mapped = toFlowGraph(monitor.graph);
      const states = new Map(monitor.nodes.map(node => [node.code, node]));
      this.flowNodes = mapped.nodes.map(node => ({ ...node, data: { ...node.data, state: states.get(node.id) } }));
      this.flowEdges = mapped.edges.map(edge => ({ ...edge,
        animated: ["running", "queued", "accepted"].includes(states.get(edge.target)?.rawStatus),
        class: states.get(edge.source)?.rawStatus === "completed" ? "wf-edge-completed" : "wf-edge-pending",
      }));
      const key = JSON.stringify([this.runtime.run_id, this.runtime.workflow_version_id, monitor.graph.positions]);
      if (this.geometryKey !== key) { this.geometryKey = key; this.needsFit = true; this.selectedId = ""; }
      if (this.flow && this.needsFit) this.$nextTick(this.fit);
    },
    initialized(flow) { this.flow = markRaw(flow); if (this.needsFit) this.$nextTick(this.fit); },
    nodesInitialized() { if (this.needsFit) this.fit(); },
    async fit() {
      if (!this.flow || !this.flowNodes.length) return;
      const fitted = await this.flow.fitView({ padding: 0.14, minZoom: 0.01, maxZoom: 1, duration: 180 });
      if (fitted) this.needsFit = false;
    },
    selectNode(id) { this.selectedId = id; this.error = ""; this.actor = ""; this.reason = ""; this.approvalJson = "{}"; this.request = null; },
    async operate(action) {
      if (this.busy || !this.selectedNode) return;
      this.error = "";
      try {
        if (!this.actor.trim() || !this.reason.trim()) throw new Error("请填写操作人和原因");
        const operation = { action, actor: this.actor.trim(), reason: this.reason.trim() };
        if (action === "approve") {
          const outputs = JSON.parse(this.approvalJson);
          if (!outputs || Array.isArray(outputs) || typeof outputs !== "object") throw new Error("审批输出必须是 JSON 对象");
          operation.outputs = outputs;
        } else operation.attempt = this.selectedNode.attempt;
        const signature = JSON.stringify([this.selectedNode.nodeRunId, operation]);
        if (!this.request || signature !== this.requestSignature) {
          this.request = { ...operation, request_id: crypto.randomUUID() }; this.requestSignature = signature;
        }
        this.busy = true;
        await workflowApi.operate(this.selectedNode.nodeRunId, this.request);
        this.error = "操作已记录，等待状态同步"; this.$emit("refresh");
      } catch (error) { this.error = error.message; }
      finally { this.busy = false; }
    },
  },
  template: `
    <div class="flow-graph-root wf-runtime-root" :style="{height}">
      <div class="wf-runtime-caption">{{ scheduleName }} · {{ batchLabel }}</div>
      <div class="wf-runtime-viewport">
        <VueFlow :id="'workflow-monitor-' + runtime.run_id" :nodes="flowNodes" :edges="flowEdges" :nodes-draggable="false" :nodes-connectable="false" :elements-selectable="true" :delete-key-code="null" :min-zoom="0.01" :max-zoom="3" @init="initialized" @nodes-initialized="nodesInitialized" @node-click="selectNode($event.node.id)">
          <template #node-workflow="nodeProps"><WorkflowNode v-bind="nodeProps" /></template>
        </VueFlow>
      </div>
      <div class="wf-canvas-tools"><button class="wf-icon-button" title="放大" aria-label="放大流程图" @click="flow?.zoomIn()"><ZoomIn :size="18" /></button><button class="wf-icon-button" title="缩小" aria-label="缩小流程图" @click="flow?.zoomOut()"><ZoomOut :size="18" /></button><button class="wf-icon-button" title="适应画布" aria-label="适应流程图画布" @click="fit"><Maximize :size="18" /></button></div>
      <button class="wf-health-button" title="消息收发与 Worker 状态" @click="showHealth = true"><Radio :size="15" />Kafka {{ health?.broker?.status === 'online' ? '在线' : '未确认' }}</button>
      <div v-if="selectedNode || showHealth" class="wf-dialog-backdrop" @click.self="!busy && (selectedId = '', showHealth = false)">
        <section class="wf-dialog" role="dialog" aria-modal="true" :aria-label="showHealth ? '消息收发状态' : selectedNode.title">
          <header><h2>{{ showHealth ? '消息收发状态' : selectedNode.title }}</h2><button class="wf-icon-button" title="关闭" aria-label="关闭节点详情" :disabled="busy" @click="selectedId = ''; showHealth = false"><X :size="18" /></button></header>
          <template v-if="showHealth"><pre>{{ JSON.stringify(health || {status:'未加载'},null,2) }}</pre><button class="wf-command" @click="$emit('refresh')"><RefreshCw :size="16" />刷新</button></template>
          <template v-else>
            <p class="wf-node-id">{{ selectedNode.code }} · 尝试 {{ selectedNode.attempt }} · {{ selectedNode.statusLabel }}</p>
            <p v-if="selectedNode.error" class="wf-error">{{ selectedNode.error }}</p>
            <h3>输出</h3><pre>{{ JSON.stringify(selectedNode.outputs,null,2) }}</pre>
            <fieldset v-if="canRetry || canCancel || selectedNode.rawStatus === 'waiting_approval'" :disabled="busy">
              <label class="wf-field">操作人<input v-model="actor" maxlength="120" /></label><label class="wf-field">原因<textarea v-model="reason" rows="2" maxlength="1000"></textarea></label>
              <label v-if="selectedNode.rawStatus === 'waiting_approval'" class="wf-field">审批输出 JSON<textarea v-model="approvalJson" rows="4"></textarea></label>
              <div class="wf-operation-actions"><button v-if="selectedNode.rawStatus === 'waiting_approval'" class="wf-command wf-primary" @click="operate('approve')">确认审批</button><button v-if="canRetry" class="wf-command" @click="operate('retry')">重试计算</button><button v-if="canCancel" class="wf-command wf-danger" @click="operate('cancel')">请求取消</button></div>
            </fieldset>
            <p v-if="error" role="status" class="wf-error">{{ error }}</p>
          </template>
        </section>
      </div>
    </div>`,
};
