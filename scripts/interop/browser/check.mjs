import {chromium} from 'playwright';
const browser = await chromium.launch({
  executablePath: process.env.CHROME_EXECUTABLE,
  headless: true,
  args: ['--autoplay-policy=no-user-gesture-required']
});
console.log('BROWSER', browser.version());
const page = await browser.newPage();
page.on('pageerror', error => console.log('PAGE_ERROR', error.message));
const url = new URL('http://127.0.0.1:5179/');
url.searchParams.set('relay', process.env.MOQX_BROWSER_RELAY);
url.searchParams.set('broadcast', process.env.MOQX_INTEROP_BROADCAST);
if (process.env.MOQX_CERT_HASH) url.searchParams.set('hash', process.env.MOQX_CERT_HASH);
try {
  await page.goto(url.href);
  await page.waitForFunction(() => {
    const samples = window.samples || [];
    return samples.some(s => s.decodedVideoFrames >= 20 && s.videoTimestamp > 1000) &&
      samples.some(s => s.audioTimestamp > 1000 && s.audioPeak > 0.001);
  }, undefined, {timeout: 30000});
  console.log('DECODE_PROOF', JSON.stringify(await page.evaluate(() => window.samples)));
} catch(error) {
  console.log('FAILED', error.message);
  console.log('OBSERVED', JSON.stringify(await page.evaluate(() => window.samples)));
  process.exitCode = 1;
} finally {
  await browser.close();
}
