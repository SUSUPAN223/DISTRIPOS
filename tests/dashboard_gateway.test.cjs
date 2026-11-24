const fs = require("fs");
const vm = require("vm");
const assert = require("assert");

const appJs = fs.readFileSync("dashboard/app.js", "utf8");
const elements = new Map();
function element(id = "") {
  return {id, dataset: {}, classList: {add(){}, remove(){}, toggle(){}, contains(){return false;}} ,
    textContent: "", innerHTML: "", disabled: false, style: {setProperty(){}, removeProperty(){}},
    appendChild(){}, addEventListener(){}, focus(){}, remove(){}, setAttribute(){},
    querySelector(){return element();}, querySelectorAll(){return [];}, getBoundingClientRect(){return {left:0,top:0,width:1,height:1};}};
}
const document = {
  querySelector(selector) { if (!elements.has(selector)) elements.set(selector, element(selector)); return elements.get(selector); },
  querySelectorAll() { return []; }, createElement() { return element(); }, createElementNS() { return element(); },
  addEventListener() {},
};
let calls = [];
const context = {
  document, location: {protocol: "http:", hostname: "ui-host"}, console,
  performance: {now: () => 0}, Intl, Date, Math, Number, String, Array, Object, Map, Set, Promise,
  URL, encodeURIComponent, crypto: {randomUUID: () => "test-id"},
  window: {setTimeout: () => 1, setInterval: () => 1, addEventListener(){}, requestAnimationFrame(fn){fn(1);}},
  requestAnimationFrame(fn) { fn(1); },
  fetch: async (url) => {
    calls.push(url);
    if (url.includes(":8001/rollback")) return {ok:false,status:502,headers:{},text:async()=>JSON.stringify({ok:false,error:{message:"Transfer gagal; rollback selesai"},trace:[{dari:2,ke:2,protokol:"ROLLBACK",aksi:"auto_rollback",ringkasan_data:"restored",status:"ok"}]})};
    return {ok:true,status:200,headers:{},text:async()=>JSON.stringify({ok:true,data:{}})};
  },
};
context.window.document = document;
vm.runInNewContext(appJs, context, {filename:"dashboard/app.js"});
context.window.DistriPOS.apiFetch("/rollback", {method:"POST", body:"{}"}, 1).then(() => {
  throw new Error("Expected HTTP rollback error");
}).catch(async (error) => {
  assert.strictEqual(error.status, 502);
  assert.strictEqual(error.message, "Transfer gagal; rollback selesai");
  assert.strictEqual(calls.filter((url) => url.includes("/rollback")).length, 1,
    "HTTP 502 must not replay through another gateway");
  assert.strictEqual(error.payload.trace[0].protokol, "ROLLBACK");
  console.log("Dashboard gateway HTTP 502: no replay, original message and trace preserved");
});