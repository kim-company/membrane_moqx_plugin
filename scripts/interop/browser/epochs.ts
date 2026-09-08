// Place with the other harness files in <pinned moq checkout>/interop.
// Instrument real WebCodecs calls and output, without synthesizing resets/frames.
const proof = (window as any).proof = {
  resets: [], configurations: [], outputs: [], errors: [], inputs: [], consumer: []
};
const NativeAudioDecoder = window.AudioDecoder;
window.AudioDecoder = class extends NativeAudioDecoder {
  constructor(init: AudioDecoderInit) {
    super({...init, output(data: AudioData) {
      const samples = new Float32Array(data.numberOfFrames);
      data.copyTo(samples, {planeIndex: 0, format: 'f32-planar'});
      const peak = samples.reduce((peak, sample) => Math.max(peak, Math.abs(sample)), 0);
      proof.outputs.push({timestamp: data.timestamp, frames: data.numberOfFrames, peak, resets: proof.resets.length});
      init.output(data);
    }, error(error: DOMException) { proof.errors.push(error.message); init.error(error); }});
  }
  reset() { proof.resets.push({afterOutputs: proof.outputs.length}); return super.reset(); }
  configure(config: AudioDecoderConfig) { proof.configurations.push(config.codec); return super.configure(config); }
  decode(chunk: EncodedAudioChunk) {
    const input = {timestamp: chunk.timestamp, type: chunk.type, resets: proof.resets.length};
    proof.inputs.push(input);
    try { return super.decode(chunk); }
    catch (error) { proof.errors.push({...input, message: String(error)}); throw error; }
  }
};
window.addEventListener('error', event => proof.errors.push(event.message));
window.addEventListener('unhandledrejection', event => proof.errors.push(String(event.reason)));
const {Consumer} = await import('../js/hang/src/container/consumer');
const next = Consumer.prototype.next;
Consumer.prototype.next = async function(...args: any[]) {
  const result = await next.apply(this, args);
  if (result) proof.consumer.push({discontinuity: result.discontinuity,
    timestamp: result.frame?.timestamp, keyframe: result.frame?.keyframe});
  return result;
};
await import('../js/watch/src/element');
const query = new URLSearchParams(location.search);
const player = document.createElement('moq-watch') as any;
if (query.has('hash')) player.connection.webtransport = {serverCertificateHashes: [{value: query.get('hash')!}]};
player.connection.websocket = {enabled: false};
player.url = query.get('relay');
player.name = query.get('broadcast');
player.setAttribute('visible', 'always');
player.innerHTML = '<canvas width="320" height="180"></canvas>';
document.querySelector('#player')!.append(player);
(window as any).player = player;
