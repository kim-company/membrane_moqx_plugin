import '../js/publish/src/element';
const query = new URLSearchParams(location.search);
const publisher = document.createElement('moq-publish') as any;
if (query.has('hash')) {
  publisher.connection.webtransport = {serverCertificateHashes: [{value: query.get('hash')!}]};
}
publisher.connection.websocket = {enabled: false};
publisher.url = query.get('relay');
publisher.name = query.get('broadcast');
publisher.video.config.set({codec: 'avc1', keyframeInterval: 1000, frameRate: 10});
publisher.controls.preview.set('none');
document.body.append(publisher);
document.querySelector<HTMLInputElement>('#media')!.onchange = event => {
  publisher.controls.source.set((event.target as HTMLInputElement).files![0]);
};
(window as any).publisher = publisher;
