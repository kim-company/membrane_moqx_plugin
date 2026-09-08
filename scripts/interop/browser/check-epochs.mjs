// Run from the plugin checkout with Playwright resolvable, the pinned Vite
// harness serving epochs.html, and the same environment as publish_epochs.exs.
// MOQX_PROOF_RESULT is a new output file: failed runs are never overwritten.
import {chromium} from 'playwright';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {writeFile} from 'node:fs/promises';

for (const name of ['MOQX_BROWSER_RELAY', 'MOQX_INTEROP_BROADCAST', 'MOQX_PROOF_RESULT']) {
  if (!process.env[name]) throw new Error(`missing ${name}`);
}
const timeout = (ms, message) => new Promise((_, reject) => {
  const timer = setTimeout(() => reject(new Error(message)), ms); timer.unref();
});
const child = spawn('mise', ['exec', '--', 'mix', 'run', 'scripts/interop/publish_epochs.exs'], {
  cwd: process.env.MOQX_PLUGIN_DIR || process.cwd(), env: {...process.env, MIX_ENV: 'test'},
  stdio: ['pipe', 'pipe', 'inherit']
});
let resolveReady;
const ready = new Promise(resolve => resolveReady = resolve);
let publisherExited;
const childExit = new Promise(resolve => {
  child.on('exit', (code, signal) => { publisherExited = {code, signal}; resolve(publisherExited); });
  child.on('error', error => { publisherExited = {error: error.message}; resolve(publisherExited); });
});
const unexpectedExit = childExit.then(exit => { throw new Error(`publisher exited: ${JSON.stringify(exit)}`); });
// This promise is raced only during phases; avoid a late unhandled rejection.
unexpectedExit.catch(() => {});
const phases = new Set();
const phaseWaits = new Map();
createInterface({input: child.stdout}).on('line', line => {
  console.log('PUBLISHER', line);
  if (line === 'READY') resolveReady();
  if (line.startsWith('EPOCH ')) {
    const epoch = Number(line.split(' ')[1]); phases.add(epoch); phaseWaits.get(epoch)?.();
  }
});
let browser;
let page;
const pageErrors = [];
const networkErrors = [];
let failure;
try {
  browser = await chromium.launch({executablePath: process.env.CHROME_EXECUTABLE,
    headless: true, args: ['--autoplay-policy=no-user-gesture-required']});
  page = await browser.newPage();
  page.on('response', response => {
    if (response.status() >= 400) networkErrors.push({url: response.url(), status: response.status()});
  });
  page.on('pageerror', error => pageErrors.push(error.message));
  page.on('console', message => {
    if (message.type() === 'error') pageErrors.push(message.text());
  });
  await Promise.race([ready, unexpectedExit, timeout(30000, 'publisher readiness timeout')]);
  const url = new URL('epochs.html', process.env.MOQX_BROWSER_HARNESS || 'http://127.0.0.1:5179/');
  url.searchParams.set('relay', process.env.MOQX_BROWSER_RELAY);
  url.searchParams.set('broadcast', process.env.MOQX_INTEROP_BROADCAST);
  if (process.env.MOQX_CERT_HASH) url.searchParams.set('hash', process.env.MOQX_CERT_HASH);
  await page.goto(url.href);
  for (let epoch = 0; epoch < 3; epoch++) {
    const offset = [5000000, 10000000, 0][epoch];
    await Promise.race([unexpectedExit, page.waitForFunction(({epoch, offset}) => {
      const p = window.proof;
      return p && (p.errors.length || (p.resets.length === epoch && p.outputs.filter(
        x => x.resets === epoch && x.timestamp >= offset && x.timestamp < offset + 1000000 &&
          x.frames === 960 && x.peak > 0.001).length === 50));
    }, {epoch, offset}, {timeout: 25000})]);
    const errors = await page.evaluate(() => window.proof.errors);
    if (errors.length || pageErrors.length) throw new Error(`decoder/browser errors: ${JSON.stringify({errors, pageErrors, networkErrors})}`);
    if (!phases.has(epoch)) await Promise.race([unexpectedExit,
      new Promise(resolve => phaseWaits.set(epoch, resolve)), timeout(10000, 'phase completion timeout')]);
    console.log('EPOCH_DECODED', epoch);
    child.stdin.write('next\n');
  }
  const exit = await Promise.race([childExit, timeout(10000, 'publisher exit timeout')]);
  if (exit.code !== 0) throw new Error(`publisher exit: ${JSON.stringify(exit)}`);
  const proof = await page.evaluate(() => window.proof);
  if (proof.errors.length || pageErrors.length || proof.outputs.length !== 150 ||
      JSON.stringify(proof.resets) !== JSON.stringify([{afterOutputs: 50}, {afterOutputs: 100}]) ||
      JSON.stringify(proof.configurations) !== JSON.stringify(['opus', 'opus', 'opus'])) {
    throw new Error('final decoder epoch invariants failed');
  }
} catch (error) {
  failure = String(error);
  process.exitCode = 1;
} finally {
  const observed = page ? await page.evaluate(() => ({proof: window.proof,
    connection: window.player?.connection.status.peek()})).catch(() => null) : null;
  // Exclusive creation preserves both passing and failing traces across reruns.
  await writeFile(process.env.MOQX_PROOF_RESULT, JSON.stringify({
    browser: browser?.version(), failure, pageErrors, networkErrors, publisherExited, observed
  }, null, 2), {flag: 'wx'}).catch(error => { console.error(error); process.exitCode = 1; });
  if (failure) console.error('FAILED', failure);
  else console.log('DECODER_EPOCH_PROOF', process.env.MOQX_PROOF_RESULT);
  child.stdin.end();
  if (publisherExited === undefined) child.kill('SIGTERM');
  await browser?.close();
}
