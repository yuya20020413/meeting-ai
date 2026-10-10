export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname !== '/ws') return env.ASSETS.fetch(request);
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket required', {status:426});
    if (request.headers.get('Origin') !== u.origin) return new Response('Origin denied', {status:403});
    if (!/^[a-f0-9]{32}$/.test(u.searchParams.get('key') || '') || !['pad','mic'].includes(u.searchParams.get('role'))) return new Response('Invalid room', {status:400});
    return env.ROOMS.get(env.ROOMS.idFromName(u.searchParams.get('key'))).fetch(request);
  }
};
export class Room {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const role = new URL(request.url).searchParams.get('role');
    if (this.ctx.getWebSockets().some(s=>s.deserializeAttachment()?.role===role)) return new Response('Role already connected', {status:409});
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    pair[1].serializeAttachment({role,stamp:0,count:0});
    this.presence();
    return new Response(null,{status:101,webSocket:pair[0]});
  }
  presence() {
    const sockets=this.ctx.getWebSockets();
    const message=JSON.stringify({type:'presence',roles:sockets.map(s=>s.deserializeAttachment().role)});
    for(const s of sockets) { try{s.send(message);}catch{} }
  }
  webSocketMessage(ws, message) {
    if(typeof message!=='string'||message.length>20000){ws.close(1008,'Message too large');return;}
    const a=ws.deserializeAttachment(),now=Date.now();
    if(now-a.stamp>1000){a.stamp=now;a.count=0;}
    if(++a.count>30){ws.close(1008,'Rate limit');return;}ws.serializeAttachment(a);
    let o;try{o=JSON.parse(message);}catch{return;}
    if(o.type==='ping'){ws.send('{"type":"pong"}');return;}
    if(typeof o.attempt!=='string'||!/^[a-f0-9]{32}$/.test(o.attempt))return;
    let out;
    if(a.role==='pad'&&o.type==='complete')out={type:'complete',attempt:o.attempt};
    if(a.role==='mic'&&o.type==='command'&&['play','pause','prev','next','reset','finish','mode','threshold'].includes(o.action)&&['shadow','repeat'].includes(o.mode)&&[70,80,90,100].includes(o.threshold))out={type:'command',action:o.action,mode:o.mode,threshold:o.threshold,attempt:o.attempt};
    if(a.role==='pad'&&o.type==='state'&&['shadow','repeat'].includes(o.mode))out={type:'state',mode:o.mode,attempt:o.attempt,threshold:[70,80,90,100].includes(o.threshold)?o.threshold:80,stage:['idle','ready','listening','speaking','done'].includes(o.stage)?o.stage:'idle',running:o.running===true,counter:String(o.counter||'').slice(0,60),phase:String(o.phase||'').slice(0,200),playing:o.playing===true};
    if(a.role==='pad'&&o.type==='target'&&(o.target===null||(typeof o.target?.english==='string'&&o.target.english.length<=12000)))out={type:'target',attempt:o.attempt,target:o.target?{english:o.target.english}:null,mode:['shadow','repeat'].includes(o.mode)?o.mode:'repeat',threshold:[70,80,90,100].includes(o.threshold)?o.threshold:80,stage:['idle','ready','listening','speaking','done'].includes(o.stage)?o.stage:'ready',running:o.running===true,counter:String(o.counter||'').slice(0,60),phase:String(o.phase||'').slice(0,200)};
    if(a.role==='pad'&&o.type==='phase')out={type:'phase',attempt:o.attempt,phase:String(o.phase||'').slice(0,200)};
    if(a.role==='mic'&&o.type==='text'&&typeof o.text==='string'&&o.text.length<=8000)out={type:'text',attempt:o.attempt,text:o.text,final:o.final===true};
    if(out)for(const s of this.ctx.getWebSockets())if(s!==ws){try{s.send(JSON.stringify(out));}catch{}}
  }
  webSocketClose(ws,code,reason) {ws.close(code,reason);this.presence();}
  webSocketError(ws) {ws.close(1011,'Socket error');this.presence();}
}
