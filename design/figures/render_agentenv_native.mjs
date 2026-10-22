#!/usr/bin/env node
// Local artifact rendering only. Supply installed dependencies; no downloads.
// Node.js 20+: node render_agentenv_native.mjs /path/to/node_modules [/path/to/chrome]
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const modules = process.argv[2];
if (!modules) throw new Error('Supply a node_modules directory with playwright and mermaid.');
const { chromium } = await import(pathToFileURL(path.resolve(modules, 'playwright/index.mjs')));
const browser = await chromium.launch({
  headless: true,
  ...(process.argv[3] ? { executablePath: process.argv[3] } : {}),
});
try {
  const doc = await fs.readFile(path.join(root, '../16-agentenvadapter-native-integration-design.md'), 'utf8');
  const blocks = [...doc.matchAll(/```mermaid\n([\s\S]*?)\n```/g)].map(m => m[1]);
  const names = ['modules', 'calls', 'deployment'];
  if (blocks.length !== names.length) throw new Error('Unexpected Mermaid block count');
  const theme = {
    theme: 'dark',
    themeVariables: {
      background: '#111c27', primaryTextColor: '#d5e0ec', lineColor: '#91a9b9',
      clusterBkg: '#182633', clusterBorder: '#617d9b', actorBkg: '#44392c',
      actorBorder: '#aa9066', actorTextColor: '#e5d6bb',
      fontFamily: 'Arial, PingFang SC, sans-serif',
    },
    themeCSS: "rect.actor[name='C'],rect.actor[name='N']{fill:#244351!important;stroke:#739bad!important}rect.actor[name='K']{fill:#24364a!important;stroke:#617d9b!important}",
  };
  const init = '%%{init: ' + JSON.stringify(theme) + '}%%\n';
  const page = await browser.newPage({ viewport: { width: 1800, height: 1200 }, deviceScaleFactor: 1 });
  for (let i = 0; i < names.length; i++) {
    const stem = path.join(root, `agentenv-native-${names[i]}`);
    const source = init + blocks[i] + '\n';
    await fs.writeFile(stem + '.mmd', source);
    await page.setContent('<html><meta charset="utf-8"><style>body{margin:0;padding:24px;background:#111c27;color:#d5e0ec;font-family:Arial,"PingFang SC",sans-serif}h1{font-size:24px;margin:0 0 24px}#diagram{display:inline-block}svg{max-width:none!important}</style><h1></h1><div id="diagram"></div></html>');
    await page.addScriptTag({ path: path.resolve(modules, 'mermaid/dist/mermaid.min.js') });
    const titles = ['AgentENV 原生接入：关键模块', 'AgentENV 原生接入：正常创建调用', 'AgentENV 原生接入：部署归属'];
    const result = await page.evaluate(async ({ source, title, theme }) => {
      mermaid.initialize({ ...theme, startOnLoad: false, securityLevel: 'strict', flowchart: { htmlLabels: false }, sequence: { useMaxWidth: false } });
      await mermaid.parse(source);
      const { svg } = await mermaid.render('nativeSSF', source);
      document.querySelector('h1').textContent = title;
      document.querySelector('#diagram').innerHTML = svg;
      const el = document.querySelector('#diagram svg');
      const view = el.viewBox.baseVal;
      el.setAttribute('width', String(Math.ceil(view.width)));
      el.setAttribute('height', String(Math.ceil(view.height)));
      el.style.backgroundColor = '#111c27';
      await document.fonts.ready;
      return { svg: new XMLSerializer().serializeToString(el), width: Math.ceil(view.width), height: Math.ceil(view.height) };
    }, { source, title: titles[i], theme });
    await fs.writeFile(stem + '.svg', result.svg + '\n');
    await page.setViewportSize({ width: Math.max(900, result.width + 48), height: result.height + 130 });
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    await page.screenshot({ path: stem + '.png', fullPage: false });
    console.log(`${names[i]}: parsed and rendered ${result.width} x ${result.height}`);
  }
} finally {
  await browser.close();
}
