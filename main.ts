// TCB v5.0 - Optimized for Deno Deploy
import { copy } from "https://deno.land/std@0.190.0/streams/mod.ts";

const UUID = "93125f15-f330-4db8-9ad5-e1053764ebfb"; // شناسه خود را اینجا بگذارید
const WS_PATH = "/proxy"; 

Deno.serve(async (req) => {
  if (req.headers.get("upgrade") !== "websocket") {
    return new Response("Service Running", { status: 200 });
  }

  const url = new URL(req.url);
  if (url.pathname !== WS_PATH) {
    return new Response("Forbidden", { status: 403 });
  }

  const { socket: ws, response } = Deno.upgradeWebSocket(req);

  ws.onopen = () => console.log("Connected to Client");

  ws.onmessage = async (e) => {
    const vlessBuffer = new Uint8Array(await e.data.arrayBuffer());
    
    // شناسایی پروتکل VLESS و استخراج آدرس مقصد
    if (vlessBuffer.length < 24) return;
    
    const version = vlessBuffer[0];
    const targetPort = (vlessBuffer[22] << 8) | vlessBuffer[23];
    const addressType = vlessBuffer[21]; // 1: IPv4, 2: Domain, 3: IPv6
    
    let address = "";
    if (addressType === 1) address = vlessBuffer.slice(24, 28).join(".");
    else if (addressType === 2) {
      const len = vlessBuffer[24];
      address = new TextDecoder().decode(vlessBuffer.slice(25, 25 + len));
    }

    try {
      const conn = await Deno.connect({ hostname: address, port: targetPort });
      ws.send(new Uint8Array([version, 0])); // تایید پروتکل به کلاینت

      // پایپ کردن دو طرفه ترافیک
      const promise1 = copy(conn, { write: (data) => ws.send(data) });
      const promise2 = (async () => {
        try {
          // دریافت داده از WS و ارسال به مقصد
          // در اینجا برای سادگی و کارایی از سیستم رویداد استفاده میکنیم
          ws.onmessage = async (event) => {
            const data = new Uint8Array(await event.data.arrayBuffer());
            await conn.write(data);
          };
        } catch (err) { conn.close(); }
      })();

      await Promise.all([promise1, promise2]);
    } catch (err) {
      console.error(`Failed to connect to ${address}:${targetPort}`);
      ws.close();
    }
  };

  return response;
});
