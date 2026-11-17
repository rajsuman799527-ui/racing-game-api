// APEX RUSH API: guest accounts + friends + invites (KV) aur lobby realtime (Durable Object)
const CORS={'access-control-allow-origin':'*','access-control-allow-headers':'content-type,authorization','access-control-allow-methods':'GET,POST,OPTIONS'};
const json=(o,s=200)=>new Response(JSON.stringify(o),{status:s,headers:{...CORS,'content-type':'application/json'}});
const err=(m,s=400)=>json({error:m},s);
const rid=n=>{const a='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';return[...crypto.getRandomValues(new Uint8Array(n))].map(x=>a[x%a.length]).join('')};
const getU=(env,uid)=>env.USERS.get('u:'+uid,'json');
const putU=(env,u)=>env.USERS.put('u:'+u.uid,JSON.stringify(u));
async function auth(req,env,q){const t=q||(req.headers.get('authorization')||'').replace('Bearer ','');if(!t)return null;const uid=await env.USERS.get('t:'+t);return uid?getU(env,uid):null}
const hyd=(env,ids)=>Promise.all(ids.map(async id=>{const u=await getU(env,id);return u?{uid:u.uid,name:u.name}:null})).then(a=>a.filter(Boolean));

export default{
  async fetch(req,env){
    if(req.method==='OPTIONS')return new Response(null,{headers:CORS});
    const url=new URL(req.url),p=url.pathname;
    try{
      if(p==='/api/register'&&req.method==='POST'){
        const b=await req.json().catch(()=>({})),name=String(b.name||'').replace(/[^\p{L}\p{N} _.-]/gu,'').trim().slice(0,14);
        if(name.length<2)return err('Naam kam se kam 2 letter ka rakho');
        let uid;for(let i=0;i<8;i++){const c=String(10000000+Math.floor(Math.random()*89999999));if(!await getU(env,c)){uid=c;break}}
        if(!uid)return err('Dobara try karo',500);
        const token=crypto.randomUUID();
        await putU(env,{uid,name,friends:[],reqIn:[],invites:[],created:Date.now()});await env.USERS.put('t:'+token,uid);
        return json({uid,name,token});
      }
      if(p==='/api/lobby/create'&&req.method==='POST'){const u=await auth(req,env);if(!u)return err('Login karo',401);return json({code:rid(6)})}
      const m=p.match(/^\/api\/lobby\/([A-Z0-9]{6})\/ws$/);
      if(m){
        const u=await auth(req,env,url.searchParams.get('token'));if(!u)return err('Login karo',401);
        if(req.headers.get('upgrade')!=='websocket')return err('WebSocket chahiye',426);
        const h=new Headers(req.headers);h.set('x-uid',u.uid);h.set('x-name',encodeURIComponent(u.name));h.set('x-create',url.searchParams.get('create')==='1'?'1':'0');
        return env.LOBBY.get(env.LOBBY.idFromName(m[1])).fetch(new Request(req,{headers:h}));
      }
      const u=await auth(req,env);if(!u)return err('Login karo',401);
      const b=req.method==='POST'?await req.json().catch(()=>({})):{};
      if(p==='/api/me'){
        const fresh=Date.now()-600000;
        return json({uid:u.uid,name:u.name,friends:await hyd(env,u.friends),requests:await hyd(env,u.reqIn),invites:u.invites.filter(i=>i.ts>fresh)});
      }
      if(p==='/api/friend/request'){
        const t=String(b.uid||'');if(t===u.uid)return err('Ye tumhara hi UID hai');
        const o=await getU(env,t);if(!o)return err('Is UID ka koi racer nahi mila');
        if(u.friends.includes(t))return err('Pehle se dost hai');
        if(u.reqIn.includes(t)){u.reqIn=u.reqIn.filter(x=>x!==t);u.friends.push(t);o.friends.push(u.uid);await putU(env,u);await putU(env,o);return json({ok:1,accepted:true})}
        if(!o.reqIn.includes(u.uid)){o.reqIn.push(u.uid);await putU(env,o)}
        return json({ok:1});
      }
      if(p==='/api/friend/accept'){
        const t=String(b.uid||'');if(!u.reqIn.includes(t))return err('Request nahi mili');
        const o=await getU(env,t);u.reqIn=u.reqIn.filter(x=>x!==t);if(!u.friends.includes(t))u.friends.push(t);
        if(o&&!o.friends.includes(u.uid)){o.friends.push(u.uid);await putU(env,o)}await putU(env,u);return json({ok:1});
      }
      if(p==='/api/friend/decline'){u.reqIn=u.reqIn.filter(x=>x!==String(b.uid));await putU(env,u);return json({ok:1})}
      if(p==='/api/invite'){
        const t=String(b.uid||''),code=String(b.code||'');if(!u.friends.includes(t)||!/^[A-Z0-9]{6}$/.test(code))return err('Invite nahi ja saka');
        const o=await getU(env,t);if(!o)return err('Racer nahi mila');
        o.invites=o.invites.filter(i=>i.from!==u.uid).concat({from:u.uid,fromName:u.name,code,ts:Date.now()}).slice(-5);await putU(env,o);return json({ok:1});
      }
      if(p==='/api/invite/clear'){u.invites=u.invites.filter(i=>i.code!==b.code);await putU(env,u);return json({ok:1})}
      return err('Not found',404);
    }catch(e){return err('Server error: '+e.message,500)}
  }
};

// Ek Durable Object = ek lobby (max 6 racers)
export class Lobby{
  constructor(){this.peers=new Map();this.host=null;this.phase='wait';this.made=false}
  bc(o,skip){const s=JSON.stringify(o);for(const [u,p] of this.peers)if(u!==skip)try{p.ws.send(s)}catch{}}
  sync(){this.bc({t:'lobby',host:this.host,phase:this.phase,players:[...this.peers].map(([uid,p])=>({uid,name:p.name,car:p.car,ready:p.ready}))})}
  async fetch(req){
    const uid=req.headers.get('x-uid'),name=decodeURIComponent(req.headers.get('x-name')||'Racer'),create=req.headers.get('x-create')==='1';
    const [cl,sv]=Object.values(new WebSocketPair());sv.accept();
    const bad=m=>{sv.send(JSON.stringify({t:'err',m}));sv.close(1000,'x');return new Response(null,{status:101,webSocket:cl})};
    if(create&&!this.made)this.made=true;
    if(!this.made)return bad('Lobby nahi mili. Code check karo');
    if(!this.peers.has(uid)){
      if(this.peers.size>=6)return bad('Lobby full hai');
      if(this.phase!=='wait')return bad('Race shuru ho chuki hai');
    }else{try{this.peers.get(uid).ws.close(1000,'dup')}catch{}}
    this.peers.set(uid,{ws:sv,name,car:0,ready:false});if(!this.host||!this.peers.has(this.host))this.host=uid;
    sv.addEventListener('message',e=>{
      let m;try{m=JSON.parse(e.data)}catch{return}const p=this.peers.get(uid);if(!p||p.ws!==sv)return;
      if(m.t==='car'){p.car=Math.max(0,Math.min(15,m.car|0));this.sync()}
      else if(m.t==='ready'){p.ready=!!m.v;this.sync()}
      else if(m.t==='start'&&uid===this.host&&this.phase==='wait'){
        if(![...this.peers].every(([k,x])=>k===this.host||x.ready))return sv.send(JSON.stringify({t:'err',m:'Sab ready nahi hain'}));
        this.phase='race';this.bc({t:'go',at:Date.now()+3000,order:[...this.peers.keys()]});this.sync();
      }
      else if(m.t==='s')this.bc({t:'s',u:uid,d:m.d},uid); // race position relay (phase 2)
      else if(m.t==='back'&&this.phase==='race'){this.phase='wait';this.peers.forEach(x=>x.ready=false);this.sync()}
    });
    sv.addEventListener('close',()=>{
      if(this.peers.get(uid)?.ws!==sv)return;this.peers.delete(uid);
      if(uid===this.host)this.host=[...this.peers.keys()][0]||null;
      if(!this.peers.size){this.phase='wait'}this.sync();
    });
    this.sync();return new Response(null,{status:101,webSocket:cl});
  }
}
