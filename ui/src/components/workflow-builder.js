import { markRaw } from "vue";
import { VueFlow } from "@vue-flow/core";
import { ArrowLeft, Plus, Copy, Trash2, Save, Check, Upload, Link, ZoomIn, ZoomOut, Maximize, Download } from "lucide-vue-next";
import WorkflowNode from "./workflow-node.js";
import { workflowApi, WorkflowDraftSession } from "../workflow-api.js";
import { cloneJson, normalizeWorkflowGraph, validateEditorGraph, deleteWorkflowNode, duplicateWorkflowNode, createWorkflowNode, connectWorkflowPorts, toFlowGraph, TYPE_LABELS } from "../workflow-graph.js";

function objectJson(text, label) {
  const value = JSON.parse(text);
  if (!value || Array.isArray(value) || typeof value !== "object") throw new Error(`${label} 必须是 JSON 对象`);
  return value;
}

export default {
  name: "WorkflowBuilder",
  components: { VueFlow, WorkflowNode, ArrowLeft, Plus, Copy, Trash2, Save, Check, Upload, Link, ZoomIn, ZoomOut, Maximize, Download },
  props: { theme: String }, emits: ["toggle-theme"],
  data() {
    return {
      session: new WorkflowDraftSession(), types: [], drafts: [], versions: [], schedules: [],
      draftKey: "", versionKey: "", scheduleKey: "", selectedId: "", selectedEdge: null,
      flowNodes: [], flowEdges: [], flow: null, busy: false, loading: true, errors: [], notice: "", noticeTone: "neutral",
      configJson: "{}", inputsJson: "{}", outputsJson: "{}", constantsJson: "{}", formDirty: false,
      connectForm: { source: "", target: "", source_port: "", target_port: "", route: "" },
      TYPE_LABELS, loadEpoch: 0, needsFit: true,
    };
  },
  computed: {
    readonly() { return this.session.readonly; },
    editable() { return !this.readonly && !this.busy && !this.loading; },
    selectedNode() { return this.session.displayGraph.nodes.find(node => node.id === this.selectedId) || null; },
    selectedType() { return this.types.find(type => type.type === this.selectedNode?.type); },
    pendingChanges() { return this.session.dirty || this.formDirty; },
    decisionNode: {
      get() { return this.session.displayGraph.decision_node_id; },
      set(value) { if (!this.readonly) this.session.graph.decision_node_id = value; },
    },
    publishReady() { return this.editable && this.session.id && !this.pendingChanges; },
    sourcePorts() { return Object.keys(this.session.displayGraph.nodes.find(n => n.id === this.connectForm.source)?.outputs || {}); },
    targetPorts() { return Object.keys(this.session.displayGraph.nodes.find(n => n.id === this.connectForm.target)?.inputs || {}); },
  },
  async mounted() {
    window.addEventListener("beforeunload", this.guardUnload);
    try {
      this.types = await workflowApi.types();
      this.drafts = await workflowApi.list();
      this.schedules = await workflowApi.schedules();
      if (this.drafts.length) await this.loadDraft(this.drafts[0].id, false);
      else await this.newDraft(false);
    } catch (error) { this.showError(error); }
    finally { this.loading = false; }
  },
  beforeUnmount() { this.loadEpoch += 1; window.removeEventListener("beforeunload", this.guardUnload); },
  methods: {
    guardUnload(event) { if (this.pendingChanges) { event.preventDefault(); event.returnValue = ""; } },
    discardAllowed() { return !this.pendingChanges || window.confirm("放弃当前未保存的编辑？"); },
    showError(error) { this.notice = error.message; this.noticeTone = "error"; },
    async action(callback) {
      if (this.busy) return;
      this.busy = true; this.notice = "";
      try { await callback(); } catch (error) { this.showError(error); }
      finally { this.busy = false; }
    },
    syncFlow(fit = false) {
      const mapped = toFlowGraph(this.session.displayGraph);
      this.flowNodes = mapped.nodes; this.flowEdges = mapped.edges;
      if (fit) this.needsFit = true;
      if (this.flow) this.$nextTick(() => { this.flow.updateNodeInternals(); if (this.needsFit) this.fit(); });
    },
    initialized(flow) { this.flow = markRaw(flow); if (this.needsFit) this.$nextTick(this.fit); },
    nodesInitialized() { if (this.needsFit) this.fit(); },
    async fit() {
      if (!this.flow || !this.flowNodes.length) return;
      const fitted = await this.flow.fitView({ padding: 0.15, minZoom: 0.01, maxZoom: 1, duration: 180 });
      if (fitted) this.needsFit = false;
    },
    selectNode(id) {
      if (this.formDirty && !this.applyNode()) return;
      this.selectedId = id; this.selectedEdge = null;
      const node = this.selectedNode;
      this.configJson = JSON.stringify(node?.config || {}, null, 2);
      this.inputsJson = JSON.stringify(node?.inputs || {}, null, 2);
      this.outputsJson = JSON.stringify(node?.outputs || {}, null, 2);
      this.constantsJson = JSON.stringify(node?.input_values || {}, null, 2);
      this.formDirty = false;
    },
    selectEdge(index) {
      if (this.formDirty && !this.applyNode()) return;
      this.selectedId = ""; this.selectedEdge = index;
    },
    applyNode() {
      if (!this.formDirty || !this.selectedNode || this.readonly) return true;
      try {
        const edits = { config: objectJson(this.configJson, "配置"), inputs: objectJson(this.inputsJson, "输入端口"), outputs: objectJson(this.outputsJson, "输出端口"), input_values: objectJson(this.constantsJson, "输入常量") };
        Object.assign(this.selectedNode, edits); this.formDirty = false; this.syncFlow();
        return true;
      } catch (error) { this.showError(error); return false; }
    },
    async loadDraft(key, guard = true) {
      if (guard && !this.discardAllowed()) { this.draftKey = this.session.id; return; }
      const epoch = ++this.loadEpoch;
      const draft = await workflowApi.read(key);
      const versions = await workflowApi.versions(key);
      if (epoch !== this.loadEpoch) return;
      this.session.load(draft); this.draftKey = key; this.versions = versions;
      this.versionKey = ""; this.formDirty = false; this.selectedId = ""; this.errors = [];
      this.selectNode(draft.graph.nodes[0]?.id || ""); this.syncFlow(true);
    },
    async newDraft(guard = true, template = true) {
      if (guard && !this.discardAllowed()) return;
      const source = template ? await workflowApi.template() : { name: "新流程", graph: normalizeWorkflowGraph() };
      this.loadEpoch += 1; this.session.load({ name: template ? "材料并行流程" : source.name, graph: source.graph });
      this.draftKey = ""; this.versionKey = ""; this.versions = []; this.formDirty = false;
      this.selectedId = ""; this.errors = []; this.selectNode(source.graph.nodes[0]?.id || ""); this.syncFlow(true);
    },
    switchVersion() {
      if (!this.applyNode()) { this.versionKey = this.session.published?.id || ""; return; }
      this.session.viewVersion(this.versions.find(version => version.id === this.versionKey) || null);
      this.selectedId = ""; this.selectNode(this.session.displayGraph.nodes[0]?.id || ""); this.syncFlow(true);
    },
    async save() {
      if (!this.applyNode()) return;
      this.errors = validateEditorGraph(this.session.graph);
      if (this.errors.length) throw new Error("请先修正流程结构错误");
      await this.session.save(workflowApi);
      this.draftKey = this.session.id; this.drafts = await workflowApi.list();
      this.notice = `草稿已保存 · 修订 ${this.session.revision}`; this.noticeTone = "success";
    },
    async validate() {
      if (!this.applyNode()) return;
      this.errors = validateEditorGraph(this.session.displayGraph);
      if (this.errors.length) return;
      if (!this.session.id || this.pendingChanges) throw new Error("请先保存草稿，再执行完整校验");
      await workflowApi.validate(this.session.id, this.session.revision);
      this.notice = "校验通过"; this.noticeTone = "success";
    },
    async publish() {
      if (!this.publishReady && !this.busy) return;
      if (this.pendingChanges) throw new Error("请先保存草稿");
      const version = await workflowApi.publish(this.session.id, this.session.revision);
      this.versions = await workflowApi.versions(this.session.id);
      this.versionKey = version.id; this.switchVersion();
      this.notice = `已发布 V${version.version} · 版本只读`; this.noticeTone = "success";
    },
    async bind() {
      if (!this.scheduleKey || !this.versionKey) throw new Error("请选择调度和已发布版本");
      const schedule = this.schedules.find(item => item.id === this.scheduleKey);
      if (!window.confirm(`将「${schedule?.name || this.scheduleKey}」绑定到 V${this.session.published.version}？后续执行将使用此版本。`)) return;
      await workflowApi.bind(this.scheduleKey, this.versionKey);
      this.notice = "版本绑定已保存；未触发执行"; this.noticeTone = "success";
    },
    async deleteDraft() {
      if (!window.confirm("删除此草稿？已发布的流程不可删除。")) return;
      await workflowApi.remove(this.session.id, this.session.revision);
      this.drafts = await workflowApi.list(); await this.newDraft(false, false);
    },
    forkDraft() {
      if (!this.applyNode()) return;
      if (!this.discardAllowed()) return;
      const graph = cloneJson(this.session.displayGraph); const name = `${this.session.name} 副本`;
      this.session.load({ name, graph }); this.draftKey = ""; this.versionKey = ""; this.versions = [];
      this.formDirty = false; this.selectedId = ""; this.selectNode(graph.nodes[0]?.id || ""); this.syncFlow(true);
    },
    addNode(entry) {
      if (!this.editable || !this.applyNode()) return;
      const node = createWorkflowNode(entry, this.session.graph.nodes);
      const index = this.session.graph.nodes.length;
      this.session.graph.nodes.push(node);
      this.session.graph.positions[node.id] = { x: 180 + (index % 4) * 280, y: 160 + Math.floor(index / 4) * 170 };
      this.syncFlow(); this.selectNode(node.id);
    },
    duplicateNode() {
      if (!this.selectedId || !this.applyNode()) return;
      this.session.graph = duplicateWorkflowNode(this.session.graph, this.selectedId);
      const id = this.session.graph.nodes.at(-1).id; this.syncFlow(); this.selectNode(id);
    },
    removeSelection() {
      if (!this.applyNode()) return;
      if (this.selectedEdge !== null) this.session.graph.edges.splice(this.selectedEdge, 1);
      else if (this.selectedId) this.session.graph = deleteWorkflowNode(this.session.graph, this.selectedId);
      this.selectedId = ""; this.selectedEdge = null; this.formDirty = false; this.syncFlow();
    },
    dragged(event) {
      if (!this.editable) return;
      for (const node of event.nodes || [event.node]) this.session.graph.positions[node.id] = { x: node.position.x + 100, y: node.position.y + 45 };
    },
    connect(connection) {
      if (!this.editable || !this.applyNode()) return;
      try {
        this.session.graph = connectWorkflowPorts(this.session.graph, connection);
        this.syncFlow();
      } catch (error) { this.showError(error); }
    },
    connected(params) {
      this.connect({ source: params.source, target: params.target,
        source_port: params.sourceHandle?.startsWith("output:") ? params.sourceHandle.slice(7) : null,
        target_port: params.targetHandle?.startsWith("input:") ? params.targetHandle.slice(6) : null });
    },
    connectSelected() { this.connect({ ...this.connectForm, source_port: this.connectForm.source_port || null, target_port: this.connectForm.target_port || null, route: this.connectForm.route || null }); },
    download() {
      const url = URL.createObjectURL(new Blob([JSON.stringify(this.session.displayGraph, null, 2)], { type: "application/json" }));
      const anchor = document.createElement("a"); anchor.href = url; anchor.download = `${this.session.id || "workflow"}.json`; anchor.click(); URL.revokeObjectURL(url);
    },
  },
  template: `
    <main class="workflow-builder" :data-theme="theme">
      <header class="wf-builder-header">
        <a class="wf-icon-button" href="/" title="返回调度台" aria-label="返回调度台"><ArrowLeft :size="20" /></a>
        <h1>流程编排</h1><span class="wf-revision">{{ session.readonly ? 'V' + session.published.version + ' · 只读' : '草稿 · 修订 ' + session.revision }}<span v-if="pendingChanges"> · 未保存</span></span>
        <button class="theme-toggle wf-theme-toggle" type="button" :aria-pressed="theme === 'dark'" aria-label="切换浅色和深色主题" @click="$emit('toggle-theme')"><span class="theme-toggle-label">浅色</span><span class="theme-toggle-track"><span class="theme-toggle-thumb"></span></span><span class="theme-toggle-label">深色</span></button>
      </header>
      <div class="wf-builder-toolbar">
        <label>流程<select v-model="draftKey" :disabled="busy || loading" aria-label="流程草稿" @change="action(() => loadDraft(draftKey))"><option value="" disabled>未保存流程</option><option v-for="draft in drafts" :key="draft.id" :value="draft.id">{{ draft.name }}</option></select></label>
        <label>版本<select v-model="versionKey" :disabled="busy || !session.id" aria-label="流程版本" @change="switchVersion"><option value="">当前草稿</option><option v-for="version in versions" :key="version.id" :value="version.id">V{{ version.version }} · {{ version.digest.slice(0,8) }}</option></select></label>
        <button class="wf-icon-button" title="新建空白流程" aria-label="新建空白流程" :disabled="busy || loading" @click="action(() => newDraft(true,false))"><Plus :size="18" /></button>
        <button class="wf-command" :disabled="busy || loading" @click="action(() => newDraft())">材料模板</button>
        <button class="wf-icon-button" title="复制为新草稿" aria-label="复制为新草稿" :disabled="busy || loading" @click="forkDraft"><Copy :size="18" /></button>
        <button class="wf-icon-button" title="删除草稿" aria-label="删除草稿" :disabled="!editable || !session.id || versions.length > 0" @click="action(deleteDraft)"><Trash2 :size="18" /></button>
        <span class="wf-toolbar-spacer"></span>
        <button class="wf-command" :disabled="!editable" @click="action(save)"><Save :size="17" />保存</button>
        <button class="wf-command" :disabled="!editable" @click="action(validate)"><Check :size="17" />校验</button>
        <button class="wf-command wf-primary" :disabled="!publishReady" @click="action(publish)"><Upload :size="17" />发布</button>
        <button class="wf-icon-button" title="导出当前流程" aria-label="导出当前流程" @click="download"><Download :size="18" /></button>
      </div>
      <div v-if="notice || errors.length || session.conflict" class="wf-notice" :class="'wf-notice-' + noticeTone" role="status">
        <strong v-if="session.conflict">保存冲突，当前编辑已保留。可导出或复制为新草稿。</strong><span>{{ notice }}</span>
        <ul v-if="errors.length"><li v-for="(error,index) in errors" :key="index">{{ error.message }}</li></ul>
      </div>
      <div class="wf-editor-layout">
        <aside class="wf-library">
          <h2>节点</h2><button v-for="entry in types" :key="entry.type" class="wf-library-item" :disabled="!editable" @click="addNode(entry)"><Plus :size="16" /><span>{{ TYPE_LABELS[entry.type] || entry.type }}<small>{{ entry.executor_kind || '内部节点' }}</small></span></button>
          <div class="wf-library-meta"><span>{{ session.displayGraph.nodes.length }} 节点</span><span>{{ session.displayGraph.edges.length }} 连线</span></div>
        </aside>
        <section class="wf-editor-canvas" aria-label="流程画布">
          <VueFlow id="workflow-builder" v-model:nodes="flowNodes" v-model:edges="flowEdges" :min-zoom="0.01" :max-zoom="2" :delete-key-code="null" :nodes-draggable="editable" :nodes-connectable="editable" :edges-updatable="false"
            @init="initialized" @nodes-initialized="nodesInitialized" @node-click="selectNode($event.node.id)" @edge-click="selectEdge($event.edge.data.index)" @node-drag-stop="dragged" @connect="connected">
            <template #node-workflow="nodeProps"><WorkflowNode v-bind="nodeProps" /></template>
          </VueFlow>
          <div class="wf-canvas-tools"><button class="wf-icon-button" title="放大" aria-label="放大" @click="flow?.zoomIn()"><ZoomIn :size="18" /></button><button class="wf-icon-button" title="缩小" aria-label="缩小" @click="flow?.zoomOut()"><ZoomOut :size="18" /></button><button class="wf-icon-button" title="适应画布" aria-label="适应画布" @click="fit"><Maximize :size="18" /></button></div>
        </section>
        <aside class="wf-inspector">
          <div class="wf-inspector-head"><h2>{{ selectedEdge !== null ? '连线' : '节点配置' }}</h2><button class="wf-icon-button" title="复制节点" aria-label="复制节点" :disabled="!editable || !selectedNode" @click="duplicateNode"><Copy :size="16" /></button><button class="wf-icon-button" title="删除选中项" aria-label="删除选中项" :disabled="!editable || (!selectedNode && selectedEdge === null)" @click="removeSelection"><Trash2 :size="16" /></button></div>
          <label class="wf-field">流程名称<input v-model="session.name" :disabled="busy || Boolean(session.id) || readonly" maxlength="120" aria-label="流程名称" /></label>
          <label class="wf-field">闭环决策节点<select v-model="decisionNode" :disabled="!editable"><option :value="null">不自动生成下一轮</option><option v-for="node in session.displayGraph.nodes.filter(n => n.type === 'model_inference')" :key="node.id" :value="node.id">{{ node.label }} · {{ node.id }}</option></select></label>
          <template v-if="selectedNode">
            <div class="wf-node-id">{{ selectedNode.id }} · {{ TYPE_LABELS[selectedNode.type] }} V{{ selectedNode.type_version }}</div>
            <fieldset :disabled="!editable">
              <label class="wf-field">节点名称<input v-model="selectedNode.label" maxlength="120" @input="syncFlow()" /></label>
              <template v-if="selectedNode.executor"><label class="wf-field">网关 / Worker ID<input v-model="selectedNode.executor.gateway_id" /></label><label class="wf-field">适配器<select v-model="selectedNode.executor.adapter_id"><option v-for="adapter in selectedType?.adapter_ids || []" :key="adapter" :value="adapter">{{ adapter }}</option></select></label></template>
              <label class="wf-field">配置 JSON<textarea v-model="configJson" aria-label="配置 JSON" rows="5" @input="formDirty = true"></textarea></label>
              <details class="wf-details"><summary>配置结构</summary><pre>{{ JSON.stringify(selectedType?.config_schema || {},null,2) }}</pre></details>
              <details class="wf-details" open><summary>端口与输入常量</summary><label class="wf-field">输入端口 JSON<textarea v-model="inputsJson" aria-label="输入端口 JSON" rows="4" @input="formDirty = true"></textarea></label><label class="wf-field">输出端口 JSON<textarea v-model="outputsJson" aria-label="输出端口 JSON" rows="4" @input="formDirty = true"></textarea></label><label class="wf-field">输入常量 JSON<textarea v-model="constantsJson" aria-label="输入常量 JSON" rows="3" @input="formDirty = true"></textarea></label></details>
              <button class="wf-command" :disabled="!formDirty" @click="applyNode"><Check :size="16" />应用配置</button>
              <details class="wf-details"><summary>重试与超时</summary><div class="wf-policy-grid"><label class="wf-field">最大尝试次数<input v-model.number="selectedNode.retry_policy.max_attempts" type="number" min="1" max="10" :disabled="selectedNode.executor?.kind === 'device'" /></label><label class="wf-field">重试延迟 / 秒<input v-model.number="selectedNode.retry_policy.delay_seconds" type="number" min="1" max="86400" /></label><label v-for="(value,key) in selectedNode.timeouts" :key="key" class="wf-field">{{ {delivery_seconds:'消息投递',acceptance_seconds:'接收确认',execution_seconds:'节点执行',validation_seconds:'结果校验'}[key] }} / 秒<input v-model.number="selectedNode.timeouts[key]" type="number" min="1" :max="key === 'execution_seconds' ? 604800 : 86400" /></label></div></details>
            </fieldset>
          </template>
          <template v-if="selectedEdge !== null"><pre>{{ JSON.stringify(session.displayGraph.edges[selectedEdge],null,2) }}</pre></template>
          <details class="wf-details"><summary>添加连线</summary><fieldset :disabled="!editable"><label class="wf-field">源节点<select v-model="connectForm.source" @change="connectForm.source_port = ''"><option value="">选择节点</option><option v-for="node in session.graph.nodes" :key="node.id" :value="node.id">{{ node.label }} · {{ node.id }}</option></select></label><label class="wf-field">输出端口<select v-model="connectForm.source_port"><option value="">控制流</option><option v-for="port in sourcePorts" :key="port" :value="port">{{ port }}</option></select></label><label class="wf-field">目标节点<select v-model="connectForm.target" @change="connectForm.target_port = ''"><option value="">选择节点</option><option v-for="node in session.graph.nodes" :key="node.id" :value="node.id">{{ node.label }} · {{ node.id }}</option></select></label><label class="wf-field">输入端口<select v-model="connectForm.target_port"><option value="">控制流</option><option v-for="port in targetPorts" :key="port" :value="port">{{ port }}</option></select></label><label class="wf-field">分支路由<input v-model="connectForm.route" /></label><button class="wf-command" @click="connectSelected"><Link :size="16" />连接</button></fieldset></details>
          <section class="wf-binding"><h2>调度绑定</h2><label class="wf-field">调度<select v-model="scheduleKey" aria-label="调度" :disabled="busy"><option value="">选择调度</option><option v-for="schedule in schedules" :key="schedule.id" :value="schedule.id">{{ schedule.name }}</option></select></label><button class="wf-command" :disabled="busy || !versionKey || !scheduleKey" @click="action(bind)"><Link :size="16" />绑定所选版本</button></section>
        </aside>
      </div>
    </main>`,
};
