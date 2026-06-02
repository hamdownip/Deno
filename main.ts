const UUID = "93125f15-f330-4db8-9ad5-e1053764ebfb"; // حتما یک UUID معتبر بذار

Deno.serve(async (req) => {
  if (req.headers.get("upgrade") !== "websocket") {
    return new Response("Deno Server is Up", { status: 200 });
  }

  const { socket: ws, response } = Deno.upgradeWebSocket(req);
  let conn: Deno.TcpConn | null = null;

  ws.onmessage = async (e) => {
    const data = new Uint8Array(await e.data.arrayBuffer());
    if (!conn) {
      // VLESS Header Parsing
      const port = (data[22] << 8) | data[23];
      const addrType = data[21];
      let host = "";
      if (addrType === 1) host = data.slice(24, 28).join(".");
      else if (addrType === 2) host = new TextDecoder().decode(data.slice(25, 25 + data[24]));
      
      try {
        conn = await Deno.connect({ hostname: host, port });
        ws.send(new Uint8Array([data[0], 0])); // تایید نسخه VLESS
        
        // ارسال داده از سرور به کلاینت
        (async () => {
          try {
            const b = new Uint8Array(16384);
            while (true) {
              const n = await conn!.read(b);
              if (n === null) break;
              ws.send(b.slice(0, n));
            }
          } catch (_) {}
        })();
      } catch (_) { ws.close(); }
    } else {
      await conn.write(data);
    }
  };

  ws.onclose = () => conn?.close();
  ws.onerror = () => conn?.close();
  return response;
});
