import { Handle, Position } from "@vue-flow/core";
import { TYPE_LABELS } from "../workflow-graph.js";

export default {
  name: "WorkflowNode",
  components: { Handle },
  props: { data: { type: Object, required: true } },
  data: () => ({ Position, TYPE_LABELS }),
  emits: ["updateNodeInternals"],
  watch: {
    "data.definition.inputs": { deep: true, handler() { this.$nextTick(() => this.$emit("updateNodeInternals")); } },
    "data.definition.outputs": { deep: true, handler() { this.$nextTick(() => this.$emit("updateNodeInternals")); } },
  },
  methods: {
    handleTop(index, count) { return `${18 + (index + 1) * (62 / (count + 1))}%`; },
  },
  template: `
    <div :class="['wf-node', 'wf-type-' + data.definition.type, 'wf-' + (data.state?.rawStatus || 'draft')]" :title="data.definition.label">
      <Handle id="control-in" type="target" :position="Position.Left" :style="{top:'90%'}" class="wf-control-handle" title="控制流输入" />
      <Handle id="control-out" type="source" :position="Position.Right" :style="{top:'90%'}" class="wf-control-handle" title="控制流输出" />
      <Handle v-for="(port, name, index) in data.definition.inputs" :key="'in-' + name" :id="'input:' + name" type="target" :position="Position.Left"
        :style="{top:handleTop(index,Object.keys(data.definition.inputs).length)}" :title="'输入 ' + name + ' · ' + JSON.stringify(port.schema)" :aria-label="'输入 ' + name" />
      <Handle v-for="(port, name, index) in data.definition.outputs" :key="'out-' + name" :id="'output:' + name" type="source" :position="Position.Right"
        :style="{top:handleTop(index,Object.keys(data.definition.outputs).length)}" :title="'输出 ' + name + ' · ' + JSON.stringify(port.schema)" :aria-label="'输出 ' + name" />
      <strong class="wf-node-title">{{ data.definition.label }}</strong>
      <span v-if="data.state" class="wf-node-state">{{ data.state.statusLabel }}</span>
      <span v-else class="wf-node-kind">{{ TYPE_LABELS[data.definition.type] || data.definition.type }} · {{ data.definition.id }}</span>
    </div>`,
};
