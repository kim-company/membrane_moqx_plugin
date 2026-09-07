// Place this directory at <pinned moq checkout>/interop; see README.md.
import '../js/watch/src/element';
const query = new URLSearchParams(location.search);
const player = document.createElement('moq-watch') as any;
if (query.has('hash')) {
  player.connection.webtransport = {serverCertificateHashes: [{value: query.get('hash')!}]};
}
player.connection.websocket = {enabled: false};
player.url = query.get('relay');
player.name = query.get('broadcast');
player.setAttribute('visible', 'always');
player.innerHTML = '<canvas width="320" height="180"></canvas>';
document.querySelector('#player')!.append(player);
(window as any).samples = [];
let decodedVideoFrames = 0;
let lastVideoTimestamp: number | undefined;
player.signals.run((effect: any) => {
  const frame = effect.get(player.video.out.frame);
  if (frame && frame.timestamp !== lastVideoTimestamp) {
    lastVideoTimestamp = frame.timestamp;
    decodedVideoFrames++;
  }
});
let analyser: AnalyserNode | undefined;
let observedRoot: AudioNode | undefined;
setInterval(() => {
  const context = player.audio.out.context.peek();
  const root = player.audio.out.root.peek();
  if (context && root && root !== observedRoot) {
    if (observedRoot && analyser) observedRoot.disconnect(analyser);
    analyser = context.createAnalyser();
    root.connect(analyser);
    observedRoot = root;
  }
  let peak = 0;
  if (analyser) {
    const samples = new Float32Array(analyser.fftSize);
    analyser.getFloatTimeDomainData(samples);
    peak = samples.reduce((max, value) => Math.max(max, Math.abs(value)), 0);
  }
  const stats = {
    connection: player.connection.status.peek(),
    decodedVideoFrames,
    videoInputFrames: player.video.out.stats.peek()?.frameCount,
    videoTimestamp: player.video.out.timestamp.peek(),
    audioTimestamp: player.audio.out.timestamp.peek(),
    audioState: context?.state,
    audioPeak: peak,
  };
  (window as any).samples.push(stats);
  document.querySelector('#stats')!.textContent = JSON.stringify(stats, null, 2);
}, 200);
