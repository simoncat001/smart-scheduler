import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { BrowserFixture } from "./fixtures/workflow-browser.js";

const { chromium } = createRequire(import.meta.url)("playwright");
const url = process.env.WORKFLOW_PREVIEW_URL || "http://127.0.0.1:5174/";
const output = new URL("../../.superpowers/sdd/2026-10-10-kafka-workflow-implementation/", import.meta.url).pathname;
const browser = await chromium.launch({ headless: true, channel: process.env.PLAYWRIGHT_CHANNEL || "chrome" });
const context = await browser.newContext({ viewport: { width: 2048, height: 1230 } });
const page = await context.newPage();
const fixture = new BrowserFixture();
const errors = [];
page.on("pageerror", error => errors.push(error.message));
page.on("dialog", dialog => dialog.accept());
await fixture.install(page);

const node = id => page.locator(`.vue-flow__node[data-id="${id}"]`);
const command = name => page.getByRole("button", { name, exact: true });
const bounds = () => page.evaluate(() => Object.fromEntries([
  ".hero-dashboard", ".dashboard-sidebar", ".dashboard-center", ".dashboard-panel-prediction", ".dashboard-panel-composer", ".flow-graph-root",
].map(selector => { const r = document.querySelector(selector).getBoundingClientRect(); return [selector, { x: r.x, y: r.y, width: r.width, height: r.height }]; })));
const baseline = {
  ".hero-dashboard": { x: 16, y: 24, width: 2016, height: 77 },
  ".dashboard-sidebar": { x: 16, y: 119, width: 360, height: 680 },
  ".dashboard-center": { x: 376, y: 119, width: 1228.1875, height: 677 },
  ".dashboard-panel-prediction": { x: 1604.1875, y: 119, width: 427.796875, height: 677 },
  ".dashboard-panel-composer": { x: 16, y: 798.5, width: 2015.984375, height: 246 },
  ".flow-graph-root": { x: 376, y: 119, width: 1228.1875, height: 677 },
};
async function fitted(selector) {
  await page.waitForFunction(selector => {
    const canvas = document.querySelector(selector)?.getBoundingClientRect();
    const nodes = [...document.querySelectorAll(`${selector} .wf-node`)];
    return canvas && nodes.length && nodes.every(node => { const r = node.getBoundingClientRect(); return r.x >= canvas.x - 1 && r.right <= canvas.right + 1 && r.y >= canvas.y - 1 && r.bottom <= canvas.bottom + 1; });
  }, selector, { timeout: 8000 });
}
async function notice(text) { await page.waitForFunction(text => document.querySelector(".wf-notice")?.textContent.includes(text), text); }
async function overflowFree() { assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1), "page overflows horizontally"); }

try {
  await page.goto(url); await page.waitForSelector(".stage-card");
  assert.deepEqual(await bounds(), baseline);
  assert.equal(await page.getByRole("link", { name: "打开流程编排" }).locator("svg").count(), 1);
  await page.waitForFunction(() => [...document.querySelectorAll(".range-graph-bars rect")].filter(bar => Number(bar.getAttribute("height")) > 0).length === 12);
  assert.equal(await page.locator(".range-graph-bars rect").count(), 12, "prediction bars missing");
  await page.screenshot({ path: `${output}phase7-home-after.png`, fullPage: true });
  await page.getByRole("link", { name: "打开流程编排" }).click(); await page.waitForSelector(".wf-node");
  await fitted(".wf-editor-canvas");
  assert.equal(await page.evaluate(() => document.querySelector("#app").__vue_app__._instance.proxy.refreshTimer), null);
  assert.equal(fixture.calls.filter(call => call.path === "/executions").length, 2, "builder starts dashboard polling");
  await node("xrd").click(); await command("复制节点").click();
  assert.equal(await page.locator(".wf-node").count(), 7);
  await command("删除选中项").click(); assert.equal(await page.locator(".wf-node").count(), 6);
  await page.locator(".wf-library-item").filter({ hasText: "人工审批" }).click();
  await page.getByLabel("输入端口 JSON").fill('{"sample":{"schema":{"type":"object","minProperties":1},"required":true}}');
  await command("应用配置").click();
  await command("适应画布").click(); await fitted(".wf-editor-canvas");
  const draggable = await node("manual_approval").locator(".wf-node-title").boundingBox();
  await page.mouse.move(draggable.x + draggable.width / 2, draggable.y + draggable.height / 2); await page.mouse.down();
  await page.mouse.move(draggable.x + draggable.width / 2 + 60, draggable.y + draggable.height / 2 + 160, { steps: 12 }); await page.mouse.up();
  const start = await node("magnetron_sputtering").locator('[data-handleid="output:sample"]').boundingBox();
  const end = await node("manual_approval").locator('[data-handleid="input:sample"]').boundingBox();
  await page.mouse.move(start.x + start.width / 2, start.y + start.height / 2); await page.mouse.down();
  await page.mouse.move(end.x + end.width / 2, end.y + end.height / 2, { steps: 18 }); await page.mouse.up();
  await page.waitForFunction(() => document.querySelectorAll(".vue-flow__edge").length === 10);
  await command("保存").click(); await notice("草稿已保存");
  assert.equal(fixture.drafts.length, 1);
  assert.ok(fixture.drafts[0].graph.edges.some(edge => edge.target === "manual_approval" && edge.target_port === "sample" && edge.source_port === "sample"));
  assert.ok(fixture.drafts[0].graph.positions.manual_approval.x > 740 && fixture.drafts[0].graph.positions.manual_approval.y > 330, "node drag position was not persisted");
  const savedId = fixture.drafts[0].id;
  await page.reload(); await page.waitForSelector('.vue-flow__node[data-id="manual_approval"]');
  assert.equal(await page.locator(".wf-node").count(), 7);
  assert.equal(fixture.drafts[0].id, savedId);
  await node("manual_approval").click();
  await page.getByLabel("配置 JSON").fill("{");
  const requestCount = fixture.calls.filter(call => call.method !== "GET").length;
  await command("保存").click(); await notice("JSON");
  assert.equal(fixture.calls.filter(call => call.method !== "GET").length, requestCount, "invalid config was sent");
  await page.getByLabel("配置 JSON").fill("{}"); await command("应用配置").click();
  await command("保存").click(); await notice("草稿已保存");
  await command("校验").click(); await notice("校验通过");
  await command("发布").click(); await notice("已发布 V1");
  assert.ok(await command("保存").isDisabled());
  const published = JSON.stringify(fixture.versions[0].graph);
  await page.getByLabel("调度", { exact: true }).selectOption("preview-schedule");
  await command("绑定所选版本").click(); await notice("未触发执行");
  assert.equal(fixture.binding.workflow_version_id, fixture.versions[0].id);
  await page.getByLabel("流程版本").selectOption(""); await node("manual_approval").click();
  await page.getByLabel("节点名称", { exact: true }).fill("本地未保存审批");
  fixture.drafts[0].revision += 1;
  await command("保存").click(); await notice("保存冲突");
  assert.equal(await page.getByLabel("节点名称", { exact: true }).inputValue(), "本地未保存审批");
  assert.equal(JSON.stringify(fixture.versions[0].graph), published);
  await page.getByRole("button", { name: "切换浅色和深色主题" }).click();
  assert.equal(await page.locator("body").getAttribute("class"), "theme-dark");
  await page.screenshot({ path: `${output}phase7-builder-dark.png`, fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 }); await overflowFree();
  await command("适应画布").click(); await fitted(".wf-editor-canvas");
  await page.screenshot({ path: `${output}phase7-builder-mobile.png`, fullPage: true });

  fixture.mode = "workflow";
  const approval = JSON.parse(published).nodes.find(n => n.id === "manual_approval");
  fixture.graph.nodes.push(approval); fixture.graph.positions.manual_approval = { x: 1042, y: 540 };
  fixture.states.manual_approval = { status: "waiting_approval" };
  await page.setViewportSize({ width: 2048, height: 1230 }); await page.goto(url);
  await page.waitForSelector(".wf-runtime-root"); await fitted(".wf-runtime-viewport");
  assert.deepEqual(await bounds(), baseline);
  assert.equal(await page.locator(".stage-state-badge").filter({ hasText: /^待上传$/ }).count(), 3);
  assert.equal(await node("model_inference").locator(".wf-node-state").innerText(), "待预测开始");
  assert.ok(await page.evaluate(() => { const el = document.querySelector(".workflow-stages"); return el.scrollHeight > el.clientHeight && el.clientHeight === 680; }));
  await page.screenshot({ path: `${output}phase7-runtime-dark.png`, fullPage: true });
  await command("放大流程图").click();
  await page.waitForTimeout(300);
  const transform = await page.locator(".wf-runtime-root .vue-flow__transformationpane").getAttribute("style");
  fixture.states.xrd = { status: "completed", attempt: 1, outputs: { result: { phase: "bcc" } } };
  await page.evaluate(() => document.querySelector("#app").__vue_app__._instance.proxy.refreshDashboard());
  assert.equal(await node("xrd").locator(".wf-node-state").innerText(), "已完成");
  assert.equal(await node("eds").locator(".wf-node-state").innerText(), "待上传");
  assert.equal(await page.locator(".wf-runtime-root .vue-flow__transformationpane").getAttribute("style"), transform);
  await command("适应流程图画布").click(); await fitted(".wf-runtime-viewport");
  await node("manual_approval").click(); await page.getByLabel("操作人", { exact: true }).fill("browser-operator"); await page.getByLabel("原因", { exact: true }).fill("isolated UI test");
  fixture.failOperationOnce = true; await command("确认审批").click();
  await page.waitForFunction(() => document.querySelector(".wf-dialog")?.textContent.includes("temporary service failure"));
  await command("确认审批").click();
  await page.waitForFunction(() => document.querySelector(".wf-dialog")?.textContent.includes("操作已记录"));
  const operations = fixture.calls.filter(call => call.path.endsWith("/operations"));
  assert.equal(operations.length, 2); assert.deepEqual(operations[0].body, operations[1].body);
  assert.equal(operations[0].body.attempt, undefined);
  await command("关闭节点详情").click();
  fixture.states.manual_approval = { status: "skipped" };
  await page.evaluate(() => document.querySelector("#app").__vue_app__._instance.proxy.refreshDashboard());
  assert.equal(await page.evaluate(() => document.querySelector("#app").__vue_app__._instance.proxy.currentRoundProgress.percent), 33, "skipped nodes must not count as unfinished progress");
  await page.getByRole("button", { name: "切换浅色和深色主题" }).click();
  await page.screenshot({ path: `${output}phase7-runtime-light.png`, fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 }); await overflowFree();
  await command("适应流程图画布").click(); await fitted(".wf-runtime-viewport");
  assert.ok(await page.evaluate(() => {
    const heading = document.querySelector(".hero-copy h1"); const range = document.createRange(); range.selectNodeContents(heading);
    const h = range.getBoundingClientRect(), t = document.querySelector(".theme-toggle").getBoundingClientRect();
    return h.right <= t.left || h.bottom <= t.top || h.top >= t.bottom;
  }), "mobile heading overlaps theme switch");
  await page.screenshot({ path: `${output}phase7-runtime-mobile.png`, fullPage: true });
  await page.goto(`${url}?run_id=preview-run`); await page.waitForSelector(".wf-runtime-root");
  assert.equal(await page.locator(".wf-node").count(), 7);
  assert.deepEqual(errors, []);
  assert.ok(!fixture.calls.some(call => call.path.endsWith("/trigger") || call.path === "/engine/tick"));
  console.log("Browser checks passed: exact homepage geometry, typed drag/connect, CRUD/reload, publish/bind, conflict preservation, themes, mobile, runtime updates, operation idempotency, detail view, no hardware calls.");
} catch (error) {
  await page.screenshot({ path: `${output}phase7-browser-failure.png`, fullPage: true });
  console.error({ errors, body: (await page.locator("body").innerText()).slice(0,1800) });
  throw error;
} finally { await browser.close(); }
