// TCB v4.0 — Deno Deploy VLESS Worker
const TOKEN="5bf46be7-ffa1-4f5b-8703-69a397f37aab";
const TOKEN_HEX=TOKEN.replace(/-/g,"").toLowerCase();
Deno.serve(async(req)=>{
if((req.headers.get("upgrade")||"").toLowerCase()!=="websocket")return new Response("",{status:200});
const{socket:ws,response}=Deno.upgradeWebSocket(req);
let tcp=null,state=0,buf=[];
ws.onmessage=async(ev)=>{try{
let d;if(ev.data instanceof ArrayBuffer)d=new Uint8Array(ev.data);
else if(ev.data instanceof Blob)d=new Uint8Array(await ev.data.arrayBuffer());else return;
if(state===0){
if(d.length<19){ws.close(1002);return}
const uuid=[...d.slice(1,17)].map(v=>v.toString(16).padStart(2,"0")).join("");
if(uuid!==TOKEN_HEX){ws.close(1008);return}
const ol=d[17];let o=18+ol+1;const port=(d[o]<<8)|d[o+1];o+=2;const at=d[o++];let host="";
if(at===1){host=Array.from(d.slice(o,o+4)).join(".");o+=4}
else if(at===2){const len=d[o++];host=new TextDecoder().decode(d.slice(o,o+len));o+=len}
else if(at===3){const v6=d.slice(o,o+16);host=[...Array(8)].map((_,i)=>((v6[i*2]<<8)|v6[i*2+1]).toString(16)).join(":");o+=16}
else{ws.close(1002);return}
const payload=d.slice(o);state=1;ws.send(new Uint8Array(2));
try{tcp=await Deno.connect({hostname:host,port,transport:"tcp"});
const wa=async(c,dd)=>{let off=0;while(off<dd.length){off+=await c.write(dd.slice(off))}};
if(payload.length>0)await wa(tcp,payload);for(const b of buf)await wa(tcp,b);buf=[];
(async()=>{const ch=new Uint8Array(16384);try{while(true){const n=await tcp.read(ch);if(n===null)break;if(ws.readyState===WebSocket.OPEN)ws.send(ch.slice(0,n));else break}}catch(_){}
try{ws.close()}catch(_){}try{tcp?.close()}catch(_){}})()}catch(_){ws.close(1011)}}
else if(tcp){const wa=async(c,dd)=>{let off=0;while(off<dd.length){off+=await c.write(dd.slice(off))}};try{await wa(tcp,d)}catch(_){ws.close(1011)}}
else buf.push(d)}catch(_){try{ws.close(1011)}catch(_){}}};
ws.onerror=()=>{try{tcp?.close()}catch(_){}};
ws.onclose=()=>{try{tcp?.close()}catch(_){}};
return response});
