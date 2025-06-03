const net = require('net');
const http = require('http');
const { Buffer } = require('buffer');
const { execSync } = require('child_process');

const UUID = process.env.UUID || '01388606-8378-436c-84c4-198a464b4779';
const SUB_PATH = process.env.SUB_PATH || 'sub';
const XPATH = process.env.XPATH || UUID.slice(0, 8);
const DOMAIN = process.env.DOMAIN || 'jiushipipe-gemininono.hf.space';
const NAME = process.env.NAME || 'Hf';
const PORT = process.env.PORT || 7860;

const SETTINGS = {
    ['UUID']: UUID,
    ['LOG_LEVEL']: 'none',
    ['BUFFER_SIZE']: '1024',
    ['XPATH']: `%2F${XPATH}`,
    ['MAX_BUFFERED_POSTS']: 30,
    ['MAX_POST_SIZE']: 1000000,
    ['SESSION_TIMEOUT']: 30000,
    ['CHUNK_SIZE']: 1024 * 1024,
    ['TCP_NODELAY']: true,
    ['TCP_KEEPALIVE']: true,
};

function validate_uuid(left, right) {
    for (let i = 0; i < 16; i++) {
        if (left[i] !== right[i]) return false;
    }
    return true;
}

function concat_typed_arrays(a, b) {
    const tmp = new Uint8Array(a.length + b.length);
    tmp.set(a, 0);
    tmp.set(b, a.length);
    return tmp;
}

async function read_atleast(reader, n) {
    let r = await reader.read();
    if (r.done && (r.value == null || r.value?.length < n)) {
        return { value: new Uint8Array(0), done: true };
    }
    return r;
}

async function read_vless_header(reader, first) {
    const HOST_LEN = 1;
    const PORT_LEN = 2;
    const ATYP_LEN = 1;
    const PORT_OFFSET = 1;
    const ADDR_TYPE_DOMAIN = 3;
    const ADDR_TYPE_IPV4 = 1;
    const ADDR_TYPE_IPV6 = 4;
    let header = first;
    let readed_len = first.length;
    async function inner_read_until(offset) {
        if (header.length >= offset) return;
        const len = offset - header.length;
        const r = await read_atleast(reader, len);
        if (r.done) throw new Error('header length too short');
        header = concat_typed_arrays(header, r.value);
    }
    await inner_read_until(ATYP_LEN + HOST_LEN);
    const atype = header[0];
    let hostname;
    let idx = ATYP_LEN;
    if (atype === ADDR_TYPE_IPV4) {
        await inner_read_until(idx + 4 + PORT_LEN);
        hostname = Array.from(header.slice(idx, idx + 4)).join('.');
        idx += 4;
    } else if (atype === ADDR_TYPE_DOMAIN) {
        const domainLen = header[idx];
        await inner_read_until(idx + 1 + domainLen + PORT_LEN);
        hostname = new TextDecoder().decode(header.slice(idx + 1, idx + 1 + domainLen));
        idx += 1 + domainLen;
    } else if (atype === ADDR_TYPE_IPV6) {
        await inner_read_until(idx + 16 + PORT_LEN);
        hostname = header
            .slice(idx, idx + 16)
            .reduce((s, b2, i2, a) => (i2 % 2 ? s.concat(((a[i2 - 1] << 8) + b2).toString(16)) : s), [])
            .join(':');
        idx += 16;
    } else {
        throw new Error('unsupported address type');
    }
    const port = header.slice(idx, idx + PORT_LEN).readUInt16BE(0);
    const resp = Buffer.alloc(1 + PORT_LEN + 1 + hostname.length + PORT_LEN);
    resp[0] = ATYP_LEN;
    resp.writeUInt16BE(PortMapping.PORT, 1);
    resp[3] = OFFSET_TYPE.FQDN;
    resp[4] = hostname.length;
    resp.write(hostname, 5);
    resp.writeUInt16BE(port, 5 + hostname.length);
    const data = header.slice(idx + PORT_LEN);
    return { hostname, port, resp, data };
}

async function connect_remote(host, port) {
    return new Promise((resolve, reject) => {
        const remote = net.connect({ host, port }, () => resolve(remote));
        remote.on('error', (e) => reject(e));
    });
}

async function pipe_stream(src, dst) {
    try {
        for await (const chunk of src) {
            if (!dst.write(chunk)) break;
        }
    } catch (e) {}
}

async function handle_client(client) {
    const reader = client[Symbol.asyncIterator]();
    let first;
    try {
        first = (await reader.next()).value;
    } catch (e) {
        client.end();
        return;
    }
    let vlessHeader;
    try {
        vlessHeader = await read_vless_header(reader, first);
    } catch (e) {
        client.end();
        return;
    }
    const remoteSocket = await connect_remote(vlessHeader.hostname, vlessHeader.port);
    remoteSocket.write(vlessHeader.resp);
    remoteSocket.write(vlessHeader.data);
    pipe_stream(reader, remoteSocket);
    remoteSocket.on('data', (chunk) => {
        client.write(chunk);
    });
    remoteSocket.on('end', () => {
        client.end();
    });
    client.on('end', () => {
        remoteSocket.end();
    });
}

async function get_public_ip() {
    let ip = DOMAIN;
    if (!DOMAIN) {
        try {
            ip = execSync('curl -s --max-time 2 ipv4.ip.sb', { encoding: 'utf-8' }).trim();
        } catch (_) {
            try {
                ip = `[${execSync('curl -s --max-time 1 ipv6.ip.sb', { encoding: 'utf-8' }).trim()}]`;
            } catch (_) {
                ip = 'localhost';
            }
        }
    }
    return ip;
}

(async () => {
    const ISP = (() => {
        try {
            const metaInfo = execSync('curl -s --max-time 1 ipapi.co/org', { encoding: 'utf-8' });
            return metaInfo.trim();
        } catch (e) {
            return 'Unknown';
        }
    })();
    const IP = await get_public_ip();

    const server = http.createServer(async (req, res) => {
        if (req.url === '/') {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('Hello, World\n');
            return;
        }

        if (req.url === `/${SUB_PATH}`) {
            const proto = String.fromCharCode(118, 108, 101, 115, 115) + '://';
            const vlessURL = proto + `${UUID}@${IP}:443?encryption=none&security=tls&flow=xtls-rprx-vision&fp=firefox&headerType=none&headerValue=&sni=${IP}&type=http&alpn=h2&path=${SETTINGS.XPATH}&mode=packet-up#${NAME}-${ISP}`;
            const base64Content = Buffer.from(vlessURL).toString('base64');
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end(base64Content + '\n');
            return;
        }

        const pathMatch = req.url.match(new RegExp(`${XPATH}/([^/]+)(?:/([0-9]+))?$`));
        if (!pathMatch) {
            res.writeHead(404);
            res.end();
            return;
        }
        const id = pathMatch[1];
        const count = Number(pathMatch[2] || '0');

        const dataBuffer = [];
        let totalSize = 0;
        for await (const chunk of req) {
            dataBuffer.push(chunk);
            totalSize += chunk.length;
            if (totalSize > SETTINGS.MAX_POST_SIZE) {
                res.writeHead(413);
                res.end();
                return;
            }
        }
        const body = Buffer.concat(dataBuffer).toString();
        let bufferMap = new Map();
        if (!global.postBuffers) global.postBuffers = {};
        if (!global.postBuffers[id]) {
            global.postBuffers[id] = [];
        }
        global.postBuffers[id].push(body);
        if (global.postBuffers[id].length > SETTINGS.MAX_BUFFERED_POSTS) {
            global.postBuffers[id].shift();
        }

        const posts = global.postBuffers[id];
        const index = count % posts.length;
        const selected = posts[index];
        const responseBuffer = Buffer.from(Array(Buffer.byteLength(selected)).fill('X').join('')).toString('base64');
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(responseBuffer);
    });

    server.keepAliveTimeout = 620000;
    server.headersTimeout = 625000;

    server.on('error', () => { /*  */ });

    server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });

    const tcpServer = net.createServer((socket) => {
        handle_client([socket]);
    });

    tcpServer.listen(443, () => {
        console.log('TCP server listening on port 443');
    });
})();
