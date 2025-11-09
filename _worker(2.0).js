/*
纯手搓节点使用说明如下：
    一、本程序预设：
      FIXED_UUID=6965d115-0236-44fd-b0ca-eefa7872c98e（强烈建议部署时更换）
    二、v2rayN客户端的单节点路径设置代理ip，通过代理客户端路径传递
      1、socks5或者http代理所有网站(即：全局代理),格式：s5all=xxx或者httpall=xxx,二者任选其一
      2、socks5代理cf相关的网站，非cf相关的网站走直连,格式：socks5=xxx或者socks5://xxx
      3、http代理cf相关的网站，非cf相关的网站走直连,格式：http=xxx或者http://xxx
      4、proxyip代理cf相关的网站，非cf相关的网站走直连,格式：pyip=xxx或者proxyip=xxx
      5、如果path路径不设置留空，cf相关的网站无法访问
      以上五种任选其一即可
    注意：
      1、workers、pages、snippets都可以部署，纯手搓443系6个端口节点vless+ws+tls
      2、snippets部署的，william的proxyip域名"不支持"
*/
import { connect } from "cloudflare:sockets";
const FIXED_UUID = "6965d115-0236-44fd-b0ca-eefa7872c98e";
const MAX_PENDING = 2097152, KEEPALIVE = 15000, STALL_TIMEOUT = 8000, MAX_STALL = 12, MAX_RECONNECT = 24;

export default {
    async fetch(request) {
        const upgradeHeader = request.headers.get('Upgrade');
        if (!upgradeHeader || upgradeHeader !== 'websocket') {
            return new Response(await nginx(), { status: 200, headers: { 'Content-Type': 'text/html; charset=UTF-8' } });
        } else {// ws代理
            const { 0: client, 1: server } = new WebSocketPair();
            server.accept();
            handleConnection(server, request);
            return new Response(null, { status: 101, webSocket: client });
        }
    }
};
// 内存池类 - 优化内存分配和回收
class Pool {
    constructor() {
        this.buf = new ArrayBuffer(16384);
        this.ptr = 0;
        this.pool = [];
        this.max = 8;
        this.large = false;
    }
    alloc = s => {
        if (s <= 4096 && s <= 16384 - this.ptr) {
            const v = new Uint8Array(this.buf, this.ptr, s);
            this.ptr += s;
            return v;
        }
        const r = this.pool.pop();
        if (r && r.byteLength >= s) return new Uint8Array(r.buffer, 0, s);
        return new Uint8Array(s);
    };
    free = b => {
        if (b.buffer === this.buf) {
            this.ptr = Math.max(0, this.ptr - b.length);
            return;
        }
        if (this.pool.length < this.max && b.byteLength >= 1024) this.pool.push(b);
    };
    enableLarge = () => { this.large = true; };
    reset = () => { this.ptr = 0; this.pool.length = 0; this.large = false; };
}

function handleConnection(ws, request) {
    const url = new URL(request.url);
    const tempPath = decodeURIComponent(url.pathname + url.search);
    const pool = new Pool();
    let socket, writer, reader, info;
    let isFirstMsg = true, bytesReceived = 0, stallCount = 0, reconnectCount = 0;
    let lastData = Date.now();
    let isDns = false, udpStreamWrite = null;
    const timers = {};
    const dataBuffer = [];
    let dataBufferBytes = 0;
    const earlyDataHeader = request.headers.get("sec-websocket-protocol") || "";
    // 新增: 连接状态和性能监控变量
    let isConnecting = false, isReading = false;
    let score = 1.0, lastCheck = Date.now(), lastRxBytes = 0, successCount = 0, failCount = 0;
    let stats = { total: 0, count: 0, bigChunks: 0, window: 0, timestamp: Date.now() };
    let mode = 'direct', avgSize = 0, throughputs = [];
    // 动态调整传输模式
    const updateMode = size => {
        stats.total += size;
        stats.count++;
        if (size > 8192) stats.bigChunks++;
        avgSize = avgSize * 0.9 + size * 0.1;
        const now = Date.now();
        if (now - stats.timestamp > 1000) {
            const rate = stats.window;
            throughputs.push(rate);
            if (throughputs.length > 5) throughputs.shift();
            stats.window = size;
            stats.timestamp = now;
            const avg = throughputs.reduce((a, b) => a + b, 0) / throughputs.length;
            if (stats.count >= 20) {
                if (avg > 20971520 && avgSize > 16384) {
                    if (mode !== 'buffered') {
                        mode = 'buffered';
                        pool.enableLarge();
                    }
                } else if (avg < 10485760 || avgSize < 8192) {
                    if (mode !== 'direct') mode = 'direct';
                } else {
                    if (mode !== 'adaptive') mode = 'adaptive';
                }
            }
        } else {
            stats.window += size;
        }
    };
    async function 处理魏烈思握手(data) {
        const bytes = new Uint8Array(data);
        ws.send(new Uint8Array([bytes[0], 0]));
        if (Array.from(bytes.slice(1, 17)).map(n => n.toString(16).padStart(2, '0')).join('').replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5') !== FIXED_UUID) throw new Error('Auth failed');
        const offset1 = 18 + bytes[17] + 1;
        const command = bytes[offset1 - 1]; // 获取命令字节: 0x01=TCP, 0x02=UDP, 0x03=MUX
        const port = (bytes[offset1] << 8) | bytes[offset1 + 1];
        const addrType = bytes[offset1 + 2];
        const offset2 = offset1 + 3;
        const addressType = addrType === 3 ? 4 : addrType === 2 ? 3 : 1;
        const { host, length } = parseAddress(bytes, offset2, addressType);
        const payload = bytes.slice(length);
        // 处理 UDP 请求
        if (command === 2) { // 0x02 = UDP
            if (port === 53) {
                isDns = true;
                const 魏烈思响应头 = new Uint8Array([bytes[0], 0]);
                const { write } = await handleUDPOutBound(ws, 魏烈思响应头);
                udpStreamWrite = write;
                if (payload.length) udpStreamWrite(payload);
                return null; // UDP 不需要返回 socket
            } else {
                throw new Error('UDP proxy only enable for DNS which is port 53');
            }
        }
        //speed.cloudflare.com
        if (host.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access');
        const sock = await createConnection(addressType, host, port);
        await sock.opened;
        const w = sock.writable.getWriter();
        if (payload.length) await w.write(payload);
        return { socket: sock, writer: w, reader: sock.readable.getReader(), info: { host, port } };
    }

    async function 处理木马握手(data) {
        const bytes = new Uint8Array(data);
        if (bytes.byteLength < 56 || bytes[56] !== 0x0d || bytes[57] !== 0x0a) throw new Error("invalid data or header format");
        if (new TextDecoder().decode(bytes.slice(0, 56)) !== sha224(FIXED_UUID)) throw new Error("invalid password");
        const socks5Data = bytes.slice(58);
        if (socks5Data.byteLength < 6) throw new Error("invalid SOCKS5 request data");
        if (socks5Data[0] !== 1) throw new Error("unsupported command, only TCP (CONNECT) is allowed");
        const addressType = socks5Data[1]
        const { host, length } = parseAddress(socks5Data, 2, addressType);
        if (!host) throw new Error(`address is empty, addressType is ${addressType}`);
        if (host.includes(atob('c3BlZWQuY2xvdWRmbGFyZS5jb20='))) throw new Error('Access');
        const port = (socks5Data[length] << 8) | socks5Data[length + 1];
        const sock = await createConnection(addressType, host, port);
        await sock.opened;
        const w = sock.writable.getWriter();
        const payload = socks5Data.slice(length + 4);
        if (payload.length) await w.write(payload);
        return { socket: sock, writer: w, reader: sock.readable.getReader(), info: { host, port } };
    }

    async function createConnection(addressType, host, port) {
        let sock;
        const socksAllMatch = tempPath.match(/(http|s5)all\s*=\s*([^&]+(?:\d+)?)/i);
        if (socksAllMatch != null && socksAllMatch[1] === 's5') {
            sock = await socks5Connect(host, port, socksAllMatch[2], addressType);
        } else if (socksAllMatch != null && socksAllMatch[1] === 'http') {
            sock = await httpConnect(host, port, socksAllMatch[2]);
        } else {
            try {
                sock = connect({ hostname: host, port });
                await sock.opened;
            } catch {
                const pyipMatch = tempPath.match(/p(?:rox)?yip\s*=\s*([^&]+(?:\d+)?)/i)?.[1];
                const socksMatch = tempPath.match(/socks5\s*(?:=|(?::\/\/))\s*([^&]+(?:\d+)?)/i)?.[1];
                const httpMatch = tempPath.match(/http\s*(?:=|(?::\/\/))\s*([^&]+(?:\d+)?)/i)?.[1];
                if (socksMatch) {
                    sock = await socks5Connect(host, port, socksMatch, addressType);
                } else if (httpMatch) {
                    sock = await httpConnect(host, port, httpMatch);
                } else if (pyipMatch) {
                    const [反代IP地址, 反代IP端口] = await 解析地址端口(pyipMatch);
                    try {
                        sock = connect({ hostname: 反代IP地址, port: 反代IP端口 });
                    } catch {
                        sock = connect({ hostname: atob('UFJPWFlJUC50cDEuMDkwMjI3Lnh5eg=='), port: 1 });
                    }
                }
            }
        }
        return sock;
    }

    async function readLoop() {
        if (isReading) return;
        isReading = true;
        let batch = [], batchSize = 0, batchTimer = null;
        // 批处理发送函数
        const flush = () => {
            if (!batchSize) return;
            const merged = new Uint8Array(batchSize);
            let pos = 0;
            for (const chunk of batch) {
                merged.set(chunk, pos);
                pos += chunk.length;
            }
            if (ws.readyState === 1) ws.send(merged);
            batch = [];
            batchSize = 0;
            if (batchTimer) {
                clearTimeout(batchTimer);
                batchTimer = null;
            }
        };
        try {
            while (true) {
                // 背压控制
                if (dataBufferBytes > MAX_PENDING) {
                    await new Promise(res => setTimeout(res, 100));
                    continue;
                }
                const { done, value } = await reader.read();
                if (value?.length) {
                    bytesReceived += value.length;
                    lastData = Date.now();
                    stallCount = 0;
                    updateMode(value.length);
                    // 定期更新网络评分
                    const now = Date.now();
                    if (now - lastCheck > 5000) {
                        const elapsed = now - lastCheck;
                        const bytes = bytesReceived - lastRxBytes;
                        const throughput = bytes / elapsed;

                        if (throughput > 500) score = Math.min(1.0, score + 0.05);
                        else if (throughput < 50) score = Math.max(0.1, score - 0.05);

                        lastCheck = now;
                        lastRxBytes = bytesReceived;
                    }
                    // 根据模式选择发送策略
                    if (mode === 'buffered') {
                        if (value.length < 32768) {
                            batch.push(value);
                            batchSize += value.length;
                            if (batchSize >= 131072) flush();
                            else if (!batchTimer) batchTimer = setTimeout(flush, avgSize > 16384 ? 5 : 20);
                        } else {
                            flush();
                            if (ws.readyState === 1) ws.send(value);
                        }
                    } else if (mode === 'adaptive') {
                        if (value.length < 4096) {
                            batch.push(value);
                            batchSize += value.length;
                            if (batchSize >= 32768) flush();
                            else if (!batchTimer) batchTimer = setTimeout(flush, 15);
                        } else {
                            flush();
                            if (ws.readyState === 1) ws.send(value);
                        }
                    } else {
                        flush();
                        if (ws.readyState === 1) ws.send(value);
                    }
                }
                if (done) {
                    flush();
                    isReading = false;
                    reconnect();
                    break;
                }
            }
        } catch (err) {
            flush();
            if (batchTimer) clearTimeout(batchTimer);
            isReading = false;
            failCount++;
            reconnect();
        }
    }

    async function reconnect() {
        if (!info || ws.readyState !== 1) {
            cleanup();
            ws.close(1011, 'Invalid.');
            return;
        }
        if (reconnectCount >= MAX_RECONNECT) {
            cleanup();
            ws.close(1011, 'Max reconnect.');
            return;
        }
        // 基于网络质量评分的随机退出机制
        if (score < 0.3 && reconnectCount > 5 && Math.random() > 0.6) {
            cleanup();
            ws.close(1011, 'Poor network.');
            return;
        }
        if (isConnecting) return;
        reconnectCount++;
        // 动态计算重连延迟
        let delay = Math.min(50 * Math.pow(1.5, reconnectCount - 1), 3000);
        delay *= (1.5 - score * 0.5);
        delay += (Math.random() - 0.5) * delay * 0.2;
        delay = Math.max(50, Math.floor(delay));
        console.log(`Reconnecting (attempt ${reconnectCount})...`);
        try {
            cleanupSocket();
            // 背压控制: 清理过多缓冲数据
            if (dataBufferBytes > MAX_PENDING * 2) {
                while (dataBufferBytes > MAX_PENDING && dataBuffer.length > 5) {
                    const drop = dataBuffer.shift();
                    dataBufferBytes -= drop.length;
                    pool.free(drop);
                }
            }
            await new Promise(res => setTimeout(res, delay));
            isConnecting = true;
            socket = connect({ hostname: info.host, port: info.port });
            await socket.opened;

            writer = socket.writable.getWriter();
            reader = socket.readable.getReader();
            // 发送缓冲数据 (限制数量防止阻塞)
            const buffersToSend = dataBuffer.splice(0, 10);
            for (const buf of buffersToSend) {
                await writer.write(buf);
                dataBufferBytes -= buf.length;
                pool.free(buf);
            }
            isConnecting = false;
            reconnectCount = 0;
            score = Math.min(1.0, score + 0.15);
            successCount++;
            stallCount = 0;
            lastData = Date.now();
            readLoop();
        } catch (err) {
            isConnecting = false;
            failCount++;
            score = Math.max(0.1, score - 0.2);
            if (reconnectCount < MAX_RECONNECT && ws.readyState === 1) setTimeout(reconnect, 500);
            else {
                cleanup();
                ws.close(1011, 'Exhausted.');
            }
        }
    }

    function startTimers() {
        timers.keepalive = setInterval(async () => {
            if (!isConnecting && writer && Date.now() - lastData > KEEPALIVE) {
                try {
                    await writer.write(new Uint8Array(0));
                    lastData = Date.now();
                } catch (e) {
                    reconnect();
                }
            }
        }, KEEPALIVE / 3);
        timers.health = setInterval(() => {
            if (!isConnecting && stats.total > 0 && Date.now() - lastData > STALL_TIMEOUT) {
                stallCount++;
                if (stallCount >= MAX_STALL) {
                    if (reconnectCount < MAX_RECONNECT) {
                        stallCount = 0;
                        reconnect();
                    } else {
                        cleanup();
                        ws.close(1011, 'Stall.');
                    }
                }
            }
        }, STALL_TIMEOUT / 2);
    }

    function cleanupSocket() {
        isReading = false;
        try {
            writer?.releaseLock();
            reader?.releaseLock();
            socket?.close();
        } catch { }
    }

    function cleanup() {
        Object.values(timers).forEach(clearInterval);
        cleanupSocket();
        while (dataBuffer.length) pool.free(dataBuffer.shift());
        dataBufferBytes = 0;
        stats = { total: 0, count: 0, bigChunks: 0, window: 0, timestamp: Date.now() };
        mode = 'direct';
        avgSize = 0;
        throughputs = [];
        pool.reset();
    }
    // 处理 early data
    function processEarlyData(earlyDataHeader) {
        if (!earlyDataHeader) return null;
        try {
            const base64Str = earlyDataHeader.replace(/-/g, "+").replace(/_/g, "/");
            const decode = atob(base64Str);
            const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
            return arryBuffer;
        } catch (error) {
            return null;
        }
    }

    ws.addEventListener('message', async evt => {
        try {
            if (isFirstMsg) {
                isFirstMsg = false;
                // 合并 early data 和第一条消息
                let firstData = evt.data;
                const earlyData = processEarlyData(earlyDataHeader);
                if (earlyData) {
                    const combined = new Uint8Array(earlyData.length + firstData.byteLength);
                    combined.set(earlyData);
                    combined.set(new Uint8Array(firstData), earlyData.length);
                    firstData = combined.buffer;
                }
                const bytes = new Uint8Array(firstData);
                let result;
                if (bytes.byteLength >= 58 && bytes[56] === 0x0d && bytes[57] === 0x0a) {
                    result = await 处理木马握手(firstData);
                } else {
                    result = await 处理魏烈思握手(firstData);
                }
                // 如果是 UDP DNS,result 为 null,不需要启动 TCP 相关逻辑
                if (result) {
                    ({ socket, writer, reader, info } = result);
                    startTimers();
                    readLoop();
                }
            } else {
                lastData = Date.now();
                if (isDns && udpStreamWrite) {
                    udpStreamWrite(evt.data);
                } else if (isConnecting || !writer) {
                    // 使用内存池分配缓冲区
                    const buf = pool.alloc(evt.data.byteLength);
                    buf.set(new Uint8Array(evt.data));
                    dataBuffer.push(buf);
                    dataBufferBytes += buf.length;
                } else {
                    await writer.write(evt.data);
                }
            }
        } catch (err) {
            cleanup();
            ws.close(1006, 'Error.');
        }
    });

    ws.addEventListener('close', cleanup);
    ws.addEventListener('error', cleanup);
}

function parseAddress(bytes, offset, addrType) {
    let host, length, endOffset;
    switch (addrType) {
        case 1: // IPv4
            length = 4;
            host = Array.from(bytes.slice(offset, offset + length)).join('.');
            endOffset = offset + length;
            break;
        case 3: // Domain name
            length = bytes[offset];
            host = new TextDecoder().decode(bytes.slice(offset + 1, offset + 1 + length));
            endOffset = offset + 1 + length;
            break;
        case 4: // IPv6
            length = 16;
            const ipv6 = [];
            for (let i = 0; i < 8; i++) {
                ipv6.push(((bytes[offset + i * 2] << 8) | bytes[offset + i * 2 + 1]).toString(16));
            }
            host = ipv6.join(':');
            endOffset = offset + length;
            break;
        default:
            throw new Error(`Invalid address type: ${addrType}`);
    }
    return { host, length: endOffset };
}

async function handleUDPOutBound(webSocket, 魏烈思响应头) {
    let 是否已发送魏烈思响应头 = false;
    const transformStream = new TransformStream({
        start(controller) { },
        transform(chunk, controller) {
            // 确保 chunk 是 Uint8Array
            if (!(chunk instanceof Uint8Array)) {
                chunk = new Uint8Array(chunk);
            }
            // UDP 消息前 2 字节是 UDP 数据长度
            for (let index = 0; index < chunk.byteLength;) {
                // 直接从字节中读取长度，避免使用 DataView
                const udpPacketLength = (chunk[index] << 8) | chunk[index + 1];
                const udpData = new Uint8Array(
                    chunk.slice(index + 2, index + 2 + udpPacketLength)
                );
                index = index + 2 + udpPacketLength;
                controller.enqueue(udpData);
            }
        },
        flush(controller) { }
    });
    // 只处理 DNS UDP 请求
    transformStream.readable.pipeTo(new WritableStream({
        async write(chunk) {
            try {
                const startTime = performance.now();
                // 解析 DNS 查询域名
                const dnsQuery = parseDNSQuery(chunk);
                console.log(`[UDP DNS] 查询域名: ${dnsQuery.domain || '未知'}, 类型: ${dnsQuery.type}, 处理时间: ${(performance.now() - startTime).toFixed(2)}ms`);
                const resp = await fetch('https://1.1.1.1/dns-query', {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/dns-message',
                    },
                    body: chunk,
                });
                const dnsQueryResult = await resp.arrayBuffer();
                const udpSize = dnsQueryResult.byteLength;
                const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
                // 解析 DNS 响应内容
                const dnsResponse = parseDNSResponse(new Uint8Array(dnsQueryResult));
                const answers = dnsResponse.answers.length > 0 ? dnsResponse.answers.join(', ') : '无记录';
                console.log(`[UDP DNS] 响应域名: ${dnsQuery.domain || '未知'}, 答案: ${answers}, 响应时间: ${(performance.now() - startTime).toFixed(2)}ms`);
                if (webSocket.readyState === 1) { // WebSocket.OPEN
                    if (是否已发送魏烈思响应头) {
                        webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                    } else {
                        webSocket.send(await new Blob([魏烈思响应头, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                        是否已发送魏烈思响应头 = true;
                    }
                    // DNS 查询完成后关闭 WebSocket 连接
                    setTimeout(() => {
                        if (webSocket.readyState === 1) {
                            webSocket.close(1000, 'DNS query completed');
                            console.log(`[UDP DNS] 连接已关闭: ${dnsQuery.domain || '未知'}`);
                        }
                    }, 10); // 给一点时间让数据发送完成
                }
            } catch (error) {
                console.error('DoH request failed:', error);
                // 出错时也关闭连接
                if (webSocket.readyState === 1) {
                    webSocket.close(1000, 'DNS query failed');
                }
            }
        }
    })).catch((error) => {
        console.error('DNS UDP error:', error);
    });
    const writer = transformStream.writable.getWriter();
    return {
        write(chunk) {
            writer.write(chunk);
        }
    };
}

function parseDNSQuery(dnsPacket) {
    try {
        // 确保 dnsPacket 有 byteLength 属性
        if (!dnsPacket || !dnsPacket.byteLength) {
            return { domain: null, type: 'Invalid' };
        }
        // DNS 头部是 12 字节
        if (dnsPacket.byteLength < 12) return { domain: null, type: 'Invalid' };
        // 从第 12 字节开始是查询部分
        let offset = 12;
        const labels = [];
        // 解析域名标签
        while (offset < dnsPacket.byteLength) {
            const length = dnsPacket[offset];
            if (length === 0) {
                offset++;
                break;
            }
            // 检查是否是指针 (压缩格式)
            if ((length & 0xC0) === 0xC0) {
                offset += 2;
                break;
            }
            offset++;
            if (offset + length > dnsPacket.byteLength) break;
            const label = new TextDecoder().decode(dnsPacket.slice(offset, offset + length));
            labels.push(label);
            offset += length;
        }
        const domain = labels.join('.');
        // 查询类型在域名之后的 2 字节 (TYPE)
        let queryType = 'Unknown';
        if (offset + 2 <= dnsPacket.byteLength) {
            const type = (dnsPacket[offset] << 8) | dnsPacket[offset + 1];
            const types = { 1: 'A', 2: 'NS', 5: 'CNAME', 6: 'SOA', 12: 'PTR', 15: 'MX', 16: 'TXT', 28: 'AAAA', 33: 'SRV', 65: 'HTTPS' };
            queryType = types[type] || `TYPE${type}`;
        }
        return { domain: domain || null, type: queryType };
    } catch (error) {
        console.error('[UDP DNS] 解析 DNS 查询失败:', error);
        return { domain: null, type: 'Error' };
    }
}

function parseDNSResponse(dnsPacket) {
    try {
        if (!dnsPacket || dnsPacket.byteLength < 12) return { answers: [] };
        const answerCount = (dnsPacket[6] << 8) | dnsPacket[7];
        if (answerCount === 0) return { answers: [] };
        let offset = 12;
        // 跳过查询部分
        while (offset < dnsPacket.byteLength) {
            const length = dnsPacket[offset];
            if (length === 0) { offset += 5; break; }
            if ((length & 0xC0) === 0xC0) { offset += 6; break; }
            offset += 1 + length;
        }
        const answers = [];
        for (let i = 0; i < answerCount && offset < dnsPacket.byteLength; i++) {
            try {
                // 跳过 NAME
                if ((dnsPacket[offset] & 0xC0) === 0xC0) offset += 2;
                else { while (offset < dnsPacket.byteLength && dnsPacket[offset] !== 0) offset += 1 + dnsPacket[offset]; offset += 1; }
                if (offset + 10 > dnsPacket.byteLength) break;
                const type = (dnsPacket[offset] << 8) | dnsPacket[offset + 1];
                const dataLength = (dnsPacket[offset + 8] << 8) | dnsPacket[offset + 9];
                offset += 10;
                if (offset + dataLength > dnsPacket.byteLength) break;
                let answer = '';
                if (type === 1 && dataLength === 4) answer = `${dnsPacket[offset]}.${dnsPacket[offset + 1]}.${dnsPacket[offset + 2]}.${dnsPacket[offset + 3]}`;
                else if (type === 28 && dataLength === 16) answer = Array.from({ length: 8 }, (_, j) => ((dnsPacket[offset + j * 2] << 8) | dnsPacket[offset + j * 2 + 1]).toString(16)).join(':');
                else if (type === 5 || type === 2 || type === 12) answer = parseDNSName(dnsPacket, offset);
                else if (type === 16) answer = new TextDecoder().decode(dnsPacket.slice(offset + 1, offset + 1 + dnsPacket[offset]));
                else answer = `TYPE${type}`;
                if (answer) answers.push(answer);
                offset += dataLength;
            } catch (e) { break; }
        }
        return { answers };
    } catch (error) {
        console.error('[UDP DNS] 解析 DNS 响应失败:', error);
        return { answers: [] };
    }
}

function parseDNSName(packet, offset) {
    const labels = [];
    let maxJumps = 5;
    while (offset < packet.byteLength && maxJumps > 0) {
        const length = packet[offset];
        if (length === 0) break;
        if ((length & 0xC0) === 0xC0) { offset = ((length & 0x3F) << 8) | packet[offset + 1]; maxJumps--; continue; }
        offset++;
        if (offset + length > packet.byteLength) break;
        labels.push(new TextDecoder().decode(packet.slice(offset, offset + length)));
        offset += length;
    }
    return labels.join('.');
}

function sha224(s) {
    const K = [0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2];
    const r = (n, b) => ((n >>> b) | (n << (32 - b))) >>> 0;
    s = unescape(encodeURIComponent(s));
    const l = s.length * 8; s += String.fromCharCode(0x80);
    while ((s.length * 8) % 512 !== 448) s += String.fromCharCode(0);
    const h = [0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4];
    const hi = Math.floor(l / 0x100000000), lo = l & 0xFFFFFFFF;
    s += String.fromCharCode((hi >>> 24) & 0xFF, (hi >>> 16) & 0xFF, (hi >>> 8) & 0xFF, hi & 0xFF, (lo >>> 24) & 0xFF, (lo >>> 16) & 0xFF, (lo >>> 8) & 0xFF, lo & 0xFF);
    const w = []; for (let i = 0; i < s.length; i += 4)w.push((s.charCodeAt(i) << 24) | (s.charCodeAt(i + 1) << 16) | (s.charCodeAt(i + 2) << 8) | s.charCodeAt(i + 3));
    for (let i = 0; i < w.length; i += 16) {
        const x = new Array(64).fill(0);
        for (let j = 0; j < 16; j++)x[j] = w[i + j];
        for (let j = 16; j < 64; j++) {
            const s0 = r(x[j - 15], 7) ^ r(x[j - 15], 18) ^ (x[j - 15] >>> 3);
            const s1 = r(x[j - 2], 17) ^ r(x[j - 2], 19) ^ (x[j - 2] >>> 10);
            x[j] = (x[j - 16] + s0 + x[j - 7] + s1) >>> 0;
        }
        let [a, b, c, d, e, f, g, h0] = h;
        for (let j = 0; j < 64; j++) {
            const S1 = r(e, 6) ^ r(e, 11) ^ r(e, 25), ch = (e & f) ^ (~e & g), t1 = (h0 + S1 + ch + K[j] + x[j]) >>> 0;
            const S0 = r(a, 2) ^ r(a, 13) ^ r(a, 22), maj = (a & b) ^ (a & c) ^ (b & c), t2 = (S0 + maj) >>> 0;
            h0 = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + t2) >>> 0;
        }
        for (let j = 0; j < 8; j++)h[j] = (h[j] + (j === 0 ? a : j === 1 ? b : j === 2 ? c : j === 3 ? d : j === 4 ? e : j === 5 ? f : j === 6 ? g : h0)) >>> 0;
    }
    let hex = '';
    for (let i = 0; i < 7; i++) {
        for (let j = 24; j >= 0; j -= 8)hex += ((h[i] >>> j) & 0xFF).toString(16).padStart(2, '0');
    }
    return hex;
}

async function 解析地址端口(hostSeg) {
    let host, ipv6, port;
    if (/\.william/i.test(hostSeg)) {
        const williamResult = await (async function (william) {
            try {
                const response = await fetch(`https://doh.pub/dns-query?name=${william}&type=TXT`, { headers: { 'Accept': 'application/dns-json' } });
                if (!response.ok) return null;
                const data = await response.json();
                const txtRecords = (data.Answer || []).filter(record => record.type === 16).map(record => record.data);
                if (txtRecords.length === 0) return null;
                let txtData = txtRecords[0];
                if (txtData.startsWith('"') && txtData.endsWith('"')) txtData = txtData.slice(1, -1);
                const prefixes = txtData.replace(/\\010/g, ',').replace(/\n/g, ',').split(',').map(s => s.trim()).filter(Boolean);
                console.log(william, '域名绑定的ProxyIP有:', prefixes);
                if (prefixes.length === 0) return null;
                return prefixes[Math.floor(Math.random() * prefixes.length)];
            } catch (error) {
                console.error('Failed to resolve ProxyIP:', error);
                return null;
            }
        })(hostSeg);
        hostSeg = williamResult || hostSeg;
    }
    if (hostSeg.startsWith('[') && hostSeg.includes(']')) {
        [ipv6, port = 443] = hostSeg.split(']:');
        host = ipv6.endsWith(']') ? `${ipv6}` : `${ipv6}]`;
    } else {
        [host, port = 443] = hostSeg.split(/[:,;]/);
    }
    return [host, Number(port)];
}

async function socks5Connect(addressRemote, portRemote, socks5Spec, addressType = 3) {
    const [latter, former] = socks5Spec.split(/@?([\d\[\]a-z.:]+(?::\d+)?)$/i);
    let [username, password] = latter.split(':');
    if (!password) { password = '' };
    const [hostname, port] = await 解析地址端口(former);
    const socket = connect({ hostname, port });
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    const encoder = new TextEncoder();
    // SOCKS5 握手: VER(5) + NMETHODS(2) + METHODS(0x00,0x02)
    await writer.write(new Uint8Array([5, 2, 0, 2]));
    let res = (await reader.read()).value;
    if (res[0] !== 0x05 || res[1] === 0xff) return;
    // 如果需要用户名密码认证
    if (res[1] === 0x02) {
        if (!username || !password) return;
        await writer.write(new Uint8Array([1, username.length, ...encoder.encode(username), password.length, ...encoder.encode(password)]));
        res = (await reader.read()).value;
        if (res[0] !== 0x01 || res[1] !== 0x00) return;
    }
    // 构建目标地址 (ATYP + DST.ADDR)
    const DSTADDR = addressType === 1 ? new Uint8Array([1, ...addressRemote.split('.').map(Number)])
        : addressType === 3 ? new Uint8Array([3, addressRemote.length, ...encoder.encode(addressRemote)])
            : new Uint8Array([4, ...addressRemote.split(':').flatMap(x => [parseInt(x.slice(0, 2), 16), parseInt(x.slice(2), 16)])]);

    // 发送连接请求: VER(5) + CMD(1=CONNECT) + RSV(0) + DSTADDR + DST.PORT
    await writer.write(new Uint8Array([5, 1, 0, ...DSTADDR, portRemote >> 8, portRemote & 0xff]));
    res = (await reader.read()).value;
    if (res[1] !== 0x00) return;
    writer.releaseLock();
    reader.releaseLock();
    return socket;
}

async function httpConnect(addressRemote, portRemote, httpSpec) {
    const [latter, former] = httpSpec.split(/@?([\d\[\]a-z.:]+(?::\d+)?)$/i);
    let [username, password] = latter.split(':');
    if (!password) { password = '' };
    const [hostname, port] = await 解析地址端口(former);
    const sock = await connect({
        hostname: hostname,
        port: port
    });
    let connectRequest = `CONNECT ${addressRemote}:${portRemote} HTTP/1.1\r\n`;
    connectRequest += `Host: ${addressRemote}:${portRemote}\r\n`;
    if (username && password) {
        const authString = `${username}:${password}`;
        const base64Auth = btoa(authString);
        connectRequest += `Proxy-Authorization: Basic ${base64Auth}\r\n`;
    }
    connectRequest += `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n`;
    connectRequest += `Proxy-Connection: Keep-Alive\r\n`;
    connectRequest += `Connection: Keep-Alive\r\n`;
    connectRequest += `\r\n`;
    try {
        const writer = sock.writable.getWriter();
        await writer.write(new TextEncoder().encode(connectRequest));
        writer.releaseLock();
    } catch (err) {
        console.error('The HTTP CONNECT request failed to send:', err);
        throw new Error(`The HTTP CONNECT request failed to send: ${err.message}`);
    }
    const reader = sock.readable.getReader();
    let respText = '';
    let connected = false;
    let responseBuffer = new Uint8Array(0);
    try {
        while (true) {
            const { value, done } = await reader.read();
            if (done) {
                console.error('HTTP proxy connection interrupted');
                throw new Error('HTTP proxy connection interrupted');
            }
            const newBuffer = new Uint8Array(responseBuffer.length + value.length);
            newBuffer.set(responseBuffer);
            newBuffer.set(value, responseBuffer.length);
            responseBuffer = newBuffer;
            respText = new TextDecoder().decode(responseBuffer);
            if (respText.includes('\r\n\r\n')) {
                const headersEndPos = respText.indexOf('\r\n\r\n') + 4;
                const headers = respText.substring(0, headersEndPos);
                if (headers.startsWith('HTTP/1.1 200') || headers.startsWith('HTTP/1.0 200')) {
                    connected = true;
                    if (headersEndPos < responseBuffer.length) {
                        const remainingData = responseBuffer.slice(headersEndPos);
                        const dataStream = new ReadableStream({
                            start(controller) {
                                controller.enqueue(remainingData);
                            }
                        });
                        const { readable, writable } = new TransformStream();
                        dataStream.pipeTo(writable).catch(err => console.error('Error processing remaining data:', err));
                        // @ts-ignore
                        sock.readable = readable;
                    }
                } else {
                    const errorMsg = `HTTP proxy connection failed: ${headers.split('\r\n')[0]}`;
                    console.error(errorMsg);
                    throw new Error(errorMsg);
                }
                break;
            }
        }
    } catch (err) {
        reader.releaseLock();
        throw new Error(`Failed to process HTTP proxy response: ${err.message}`);
    }
    reader.releaseLock();
    if (!connected) {
        throw new Error('HTTP proxy connection failed: No successful response received');
    }
    return sock;
}

async function nginx() {
    return `
	<!DOCTYPE html>
	<html>
	<head>
	<title>Welcome to nginx!</title>
	<style>
		body {
			width: 35em;
			margin: 0 auto;
			font-family: Tahoma, Verdana, Arial, sans-serif;
		}
	</style>
	</head>
	<body>
	<h1>Welcome to nginx!</h1>
	<p>If you see this page, the nginx web server is successfully installed and
	working. Further configuration is required.</p>
	
	<p>For online documentation and support please refer to
	<a href="http://nginx.org/">nginx.org</a>.<br/>
	Commercial support is available at
	<a href="http://nginx.com/">nginx.com</a>.</p>
	
	<p><em>Thank you for using nginx.</em></p>
	</body>
	</html>
	`
}