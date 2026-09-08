// Run in the isolated Playwright tools directory; only synthetic fixture input.
import {chromium} from 'playwright';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile} from 'node:fs/promises';
import {join} from 'node:path';
const browser = await chromium.launch({
  executablePath: process.env.CHROME_EXECUTABLE,
  headless: true,
  args: ['--autoplay-policy=no-user-gesture-required']
});
try {
  console.log('BROWSER', browser.version());
  const page = await browser.newPage();
  page.on('pageerror', error => console.log('PAGE_ERROR', error.message));
  const url = new URL('http://127.0.0.1:5179/publish.html');
  url.searchParams.set('relay', process.env.MOQX_BROWSER_RELAY);
  url.searchParams.set('broadcast', process.env.MOQX_INTEROP_BROADCAST);
  if (process.env.MOQX_CERT_HASH) url.searchParams.set('hash', process.env.MOQX_CERT_HASH);
  await page.goto(url.href);
  await page.locator('#media').setInputFiles(process.env.MOQX_INTEROP_INPUT);
  await page.waitForFunction(() => window.publisher.video.out.catalog.peek() &&
    window.publisher.audio.out.catalog.peek(), undefined, {timeout: 20000});
  const {stdout} = await promisify(execFile)('mise', ['exec', '--', 'mix', 'run', 'scripts/interop/receive_legacy.exs'], {
    cwd: process.env.MOQX_PLUGIN_ROOT,
    env: {...process.env, MIX_ENV: 'test'},
    timeout: 45000,
    maxBuffer: 1024 * 1024
  });
  console.log(stdout.trim());
  const captured = JSON.parse(await readFile(join(process.env.MOQX_INTEROP_OUTPUT, 'legacy.json'), 'utf8'));
  const proof = await page.evaluate(async captured => {
    const bytes = base64 => Uint8Array.from(atob(base64), c => c.charCodeAt(0));
    const proof = {videoFrames: 0, audioSamples: 0, audioPeak: 0, reorderedPackets: {}, videoTimestamps: [], audioTimestamps: []};
    for (const role of ['video', 'audio']) {
      const track = captured[role];
      // Offline decoder-side ordering only: the plugin is not a jitter buffer.
      // Keep capture arrival order intact and report any changed positions.
      const packets = [...track.packets].sort((a, b) => a.group - b.group || a.object - b.object);
      proof.reorderedPackets[role] = packets.filter((p, i) => p !== track.packets[i]).length;
      const config = {...track.config};
      if (role === 'audio' ? config.codec !== 'opus' : !/^avc[13]\./.test(config.codec))
        throw new Error(`Unexpected ${role} codec: ${config.codec}`);
      if (role === 'video' && (!packets[0].key || !packets.some(p => !p.key)))
        throw new Error('Video proof requires an initial keyframe and later delta frames');
      if (track.description) config.description = bytes(track.description);
      const errors = [];
      const output = frame => {
        if (role === 'video') {
          proof.videoFrames++;
          proof.videoTimestamps.push(frame.timestamp);
        } else {
          proof.audioSamples += frame.numberOfFrames;
          proof.audioTimestamps.push(frame.timestamp);
          for (let channel = 0; channel < frame.numberOfChannels; channel++) {
            const samples = new Float32Array(frame.numberOfFrames);
            frame.copyTo(samples, {planeIndex: channel, format: 'f32-planar'});
            for (const sample of samples) proof.audioPeak = Math.max(proof.audioPeak, Math.abs(sample));
          }
        }
        frame.close();
      };
      const Decoder = role === 'video' ? VideoDecoder : AudioDecoder;
      const Chunk = role === 'video' ? EncodedVideoChunk : EncodedAudioChunk;
      const decoder = new Decoder({output, error: e => errors.push(e.message)});
      try {
        decoder.configure(config);
        for (const packet of packets) decoder.decode(new Chunk({
          type: packet.key ? 'key' : 'delta', timestamp: packet.timestamp, data: bytes(packet.data)
        }));
        await decoder.flush();
        if (errors.length) throw new Error(errors.join('; '));
      } finally {
        if (decoder.state !== 'closed') decoder.close();
      }
    }
    if (proof.videoFrames < 20 || proof.audioSamples < 48000 || proof.audioPeak < 0.001)
      throw new Error(`Insufficient decoded media: ${JSON.stringify(proof)}`);
    for (const timestamps of [proof.videoTimestamps, proof.audioTimestamps]) {
      if (timestamps.at(-1) <= timestamps[0] || timestamps.some((t, i) => i && t < timestamps[i - 1]))
        throw new Error('Decoded timestamps do not advance monotonically');
    }
    return {...proof,
      videoTimestamps: {first: proof.videoTimestamps[0], last: proof.videoTimestamps.at(-1)},
      audioTimestamps: {first: proof.audioTimestamps[0], last: proof.audioTimestamps.at(-1)}};
  }, captured);
  console.log('REVERSE_LEGACY_DECODE_PROOF', JSON.stringify(proof));
} finally {
  await browser.close();
}
