const net = require('net');
const http = require('http');
const { Buffer } = require('buffer');
const { exec, execSync } = require('child_process');

// Constants
const CORESERVICE_COMMAND_TCP = 1; // 原 VLESS_COMMAND_TCP
const ADDRESS_TYPE_IPV4 = 1;
const ADDRESS_TYPE_DOMAIN = 2;
const ADDRESS_TYPE_IPV6 = 3;

const HTTP_STATUS_OK = 200;
const HTTP_STATUS_NOT_FOUND = 404;
const HTTP_STATUS_PAYLOAD_TOO_LARGE = 413;
const HTTP_STATUS_INTERNAL_SERVER_ERROR = 500;

const H2_CHUNK_SIZE = 16384;

const UUID = process.env.UUID || '7ef3421d-0ee1-4866-86a1-8087b61510c8';
const SUB_PATH = process.env.SUB_PATH || 'sub';
const XPATH_BASE = process.env.XPATH || UUID.slice(0, 8);
const DOMAIN = process.env.DOMAIN || 'jiushipipe-newbeexp.hf.space';
const NAME = process.env.NAME || 'Hf';
const PORT = process.env.PORT || 7860;

const SETTINGS = {
    UUID: UUID,
    LOG_LEVEL: 'none',
    BUFFER_SIZE: '1024',
    XPATH: `%2F${XPATH_BASE}`,
    MAX_BUFFERED_POSTS: 30,
    MAX_POST_SIZE: 1000000,
    SESSION_TIMEOUT: 30000,
    CHUNK_SIZE: 1024 * 1024,
    TCP_NODELAY: true,
    TCP_KEEPALIVE: true,
};

const CORESERVICE_PATH_REGEX = new RegExp(`^${SETTINGS.XPATH}/([^/]+)(?:/([0-9]+))?$`); // 原 VLESS_PATH_REGEX

function validate_uuid(left, right) {
    if (left.length !== 16 || right.length !== 16) return false;
    for (let i = 0; i < 16; i++) {
        if (left[i] !== right[i]) return false;
    }
    return true;
}

function concat_typed_arrays(first, ...args) {
    if (!args || args.length < 1) return first;
    let totalLength = first.length;
    for (const arr of args) totalLength += arr.length;
    
    const result = new first.constructor(totalLength);
    result.set(first, 0);
    let offset = first.length;
    for (const arr of args) {
        result.set(arr, offset);
        offset += arr.length;
    }
    return result;
}

function log(type, ...args) {
    if (SETTINGS.LOG_LEVEL === 'none') return;

    const levels = { debug: 0, info: 1, warn: 2, error: 3 };
    const colors = {
        debug: '\x1b[36m', info: '\x1b[32m', warn: '\x1b[33m',
        error: '\x1b[31m', reset: '\x1b[0m'
    };

    const configLevel = levels[SETTINGS.LOG_LEVEL] || levels.info;
    const messageLevel = levels[type] || levels.debug;

    if (messageLevel >= configLevel) {
        const time = new Date().toISOString();
        const color = colors[type] || colors.reset;
        console.log(`${color}[${time}] [${type.toUpperCase()}]`, ...args, colors.reset);
    }
}

function parse_uuid(uuidStr) {
    const cleanedUuid = uuidStr.replace(/-/g, '');
    if (cleanedUuid.length !== 32) throw new Error('Invalid UUID string length for parsing');
    const result = [];
    for (let i = 0; i < 16; i++) {
        result.push(parseInt(cleanedUuid.substring(i * 2, i * 2 + 2), 16));
    }
    return new Uint8Array(result);
}

async function read_atleast(reader, n) {
    const buffs = [];
    let bytesRead = 0;
    let done = false;
    while (bytesRead < n && !done) {
        const { value, done: streamDone } = await reader.read();
        done = streamDone;
        if (value) {
            const bufferChunk = new Uint8Array(value);
            buffs.push(bufferChunk);
            bytesRead += bufferChunk.length;
        } else if (done && bytesRead < n) {
            throw new Error(`Stream ended prematurely, expected ${n} bytes, got ${bytesRead}`);
        }
    }
    if (bytesRead < n) {
        throw new Error(`Not enough data to read, expected ${n} bytes, got ${bytesRead}`);
    }
    return {
        value: concat_typed_arrays(...buffs),
        done,
    };
}

async function read_coreservice_protocol_header(reader, cfg_uuid_str) { // Renamed from read_vless_header
    let accumulatedHeader = new Uint8Array();
    let totalReadLength = 0;

    async function ensure_bytes(count) {
        if (totalReadLength >= count) return;
        const needed = count - totalReadLength;
        const { value, done } = await read_atleast(reader, needed);
        if (done && value.length < needed) throw new Error('CoreService protocol header too short while reading.'); // Text changed
        accumulatedHeader = concat_typed_arrays(accumulatedHeader, value);
        totalReadLength += value.length;
    }

    await ensure_bytes(1 + 16 + 1); 

    const version = accumulatedHeader[0];
    const receivedUuidBytes = accumulatedHeader.slice(1, 1 + 16);
    const configuredUuidBytes = parse_uuid(cfg_uuid_str);

    if (!validate_uuid(receivedUuidBytes, configuredUuidBytes)) {
        throw new Error('Invalid UUID');
    }

    const addonsLength = accumulatedHeader[1 + 16];
    await ensure_bytes(1 + 16 + 1 + addonsLength + 1 + 2 + 1); 

    const command = accumulatedHeader[1 + 16 + 1 + addonsLength];
    if (command !== CORESERVICE_COMMAND_TCP) { // Constant changed
        throw new Error(`Unsupported command: ${command}`);
    }

    const portStartIndex = 1 + 16 + 1 + addonsLength + 1;
    const port = (accumulatedHeader[portStartIndex] << 8) + accumulatedHeader[portStartIndex + 1];
    const addressType = accumulatedHeader[portStartIndex + 2];
    
    let addressLength = 0;
    let addressStartIndex = portStartIndex + 2 + 1;

    if (addressType === ADDRESS_TYPE_IPV4) addressLength = 4;
    else if (addressType === ADDRESS_TYPE_IPV6) addressLength = 16;
    else if (addressType === ADDRESS_TYPE_DOMAIN) {
        await ensure_bytes(addressStartIndex + 1); 
        addressLength = accumulatedHeader[addressStartIndex];
        addressStartIndex += 1; 
    } else {
        throw new Error(`Unsupported address type: ${addressType}`);
    }

    await ensure_bytes(addressStartIndex + addressLength);
    const addressBytes = accumulatedHeader.slice(addressStartIndex, addressStartIndex + addressLength);
    let hostname = '';

    if (addressType === ADDRESS_TYPE_IPV4) {
        hostname = addressBytes.join('.');
    } else if (addressType === ADDRESS_TYPE_DOMAIN) {
        hostname = new TextDecoder().decode(addressBytes);
    } else if (addressType === ADDRESS_TYPE_IPV6) {
        const parts = [];
        for (let i = 0; i < 16; i += 2) {
            parts.push(((addressBytes[i] << 8) + addressBytes[i + 1]).toString(16));
        }
        hostname = parts.join(':');
    }

    if (!hostname) {
        log('error', 'Failed to parse hostname');
        throw new Error('Parse hostname failed');
    }
    
    log('info', `CoreService connection to ${hostname}:${port}`); // Text changed
    const headerTotalLength = addressStartIndex + addressLength;
    return {
        hostname,
        port,
        data: accumulatedHeader.slice(headerTotalLength),
        responseBytes: new Uint8Array([version, 0]), 
    };
}


async function parse_coreservice_client_header(clientStreamReader, configuredUuid) { // Renamed from parse_vless_client_header
    log('debug', 'Starting to parse CoreService header from client stream'); // Text changed
    try {
        const coreServiceDetails = await read_coreservice_protocol_header(clientStreamReader, configuredUuid); // Function name and var name changed
        log('debug', 'CoreService header parsed successfully'); // Text changed
        return coreServiceDetails; // Var name changed
    } catch (err) {
        log('error', `CoreService header parse error: ${err.message}`); // Text changed
        throw new Error(`Read CoreService header error: ${err.message}`); // Text changed
    }
}

function timed_connect(hostname, port, timeoutMs) {
    return new Promise((resolve, reject) => {
        const socket = net.createConnection({ host: hostname, port: port });
        const timer = setTimeout(() => {
            socket.destroy();
            reject(new Error(`Connection timeout to ${hostname}:${port} after ${timeoutMs}ms`));
        }, timeoutMs);
        socket.on('connect', () => {
            clearTimeout(timer);
            resolve(socket);
        });
        socket.on('error', (err) => {
            clearTimeout(timer);
            reject(err);
        });
    });
}

async function connect_to_remote_host(hostname, port) {
    const connectionTimeout = 8000;
    try {
        const remoteSocket = await timed_connect(hostname, port, connectionTimeout);
        remoteSocket.setNoDelay(SETTINGS.TCP_NODELAY);
        remoteSocket.setKeepAlive(SETTINGS.TCP_KEEPALIVE, 1000); 
        remoteSocket.bufferSize = parseInt(SETTINGS.BUFFER_SIZE, 10) * 1024;
        log('info', `Connected to remote ${hostname}:${port}`);
        return remoteSocket;
    } catch (err) {
        log('error', `Remote connection failed: ${err.message}`);
        throw err;
    }
}

function isSignificantRelayError(err) {
    return !err.message.includes('aborted') && 
           !err.message.includes('socket hang up') &&
           !err.message.includes('Stream ended prematurely') && 
           !err.message.includes('Writable stream closed prematurely'); 
}

function create_pipe_pump() {
    async function pump(sourceStream, destinationStream, firstPacketIfAny) {
        const chunkSize = parseInt(SETTINGS.CHUNK_SIZE, 10);
        
        if (firstPacketIfAny && firstPacketIfAny.length > 0) {
            if (destinationStream.write) { 
                destinationStream.cork();
                destinationStream.write(firstPacketIfAny);
                process.nextTick(() => destinationStream.uncork());
            } else { 
                const writer = destinationStream.writable.getWriter();
                try {
                    await writer.write(firstPacketIfAny);
                } finally {
                    writer.releaseLock();
                }
            }
        }
        
        try {
            if (sourceStream.pipe) { 
                sourceStream.pause(); 
                sourceStream.pipe(destinationStream, { end: true, highWaterMark: chunkSize });
                sourceStream.resume();
            } else { 
                await sourceStream.readable.pipeTo(destinationStream.writable, {
                    preventClose: false, preventAbort: false, preventCancel: false,
                    signal: AbortSignal.timeout(SETTINGS.SESSION_TIMEOUT)
                });
            }
        } catch (err) {
            if (isSignificantRelayError(err)) {
                log('error', 'Relay pump error:', err.message);
            }
            throw err; 
        }
    }
    return pump;
}

function convert_socket_to_web_streams(socket) {
    let readController;
    let writeController;

    socket.on('error', (err) => {
        log('error', 'Socket error in WebStream conversion:', err.message);
        readController?.error(err);
        writeController?.error(err); 
    });

    return {
        readable: new ReadableStream({
            start(controller) {
                readController = controller;
                socket.on('data', (chunk) => {
                    try { controller.enqueue(chunk); } 
                    catch (e) { log('error', 'Read controller enqueue error:', e.message); }
                });
                socket.on('end', () => {
                    try { controller.close(); } 
                    catch (e) { log('error', 'Read controller close error:', e.message); }
                });
            },
            cancel() { socket.destroy(); }
        }),
        writable: new WritableStream({
            start(controller) { writeController = controller; },
            write(chunk) {
                return new Promise((resolve, reject) => {
                    if (socket.destroyed) {
                        return reject(new Error('Socket is destroyed, cannot write'));
                    }
                    socket.write(chunk, (err) => {
                        if (err) reject(err); else resolve();
                    });
                });
            },
            close() { if (!socket.destroyed) socket.end(); },
            abort(err) { socket.destroy(err); }
        })
    };
}

// Parameter coreServiceDetails was vlessDetails
function setup_relay(clientWebStream, remoteSocket, coreServiceDetails) {
    const pump = create_pipe_pump();
    let isClosing = false;
    
    const remoteWebStream = convert_socket_to_web_streams(remoteSocket);
    
    function cleanupRelayResources() {
        if (!isClosing) {
            isClosing = true;
            log('debug', 'Cleaning up relay resources.');
            try {
                remoteSocket.destroy();
            } catch (err) {
                if (isSignificantRelayError(err)) {
                    log('error', `Relay cleanup error: ${err.message}`);
                }
            }
        }
    }

    const uploaderPromise = pump(clientWebStream, remoteWebStream, coreServiceDetails.data) // Var name changed
        .catch(err => {
            if (isSignificantRelayError(err)) log('error', `Uplink error: ${err.message}`);
        })
        .finally(() => {
            if (typeof clientWebStream.reading_done === 'function') {
                clientWebStream.reading_done();
            }
        });

    const downloaderPromise = pump(remoteWebStream, clientWebStream, coreServiceDetails.responseBytes) // Var name changed
        .catch(err => {
            if (isSignificantRelayError(err)) log('error', `Downlink error: ${err.message}`);
        });

    Promise.allSettled([uploaderPromise, downloaderPromise])
        .finally(cleanupRelayResources);
}


const sessions = new Map();

class Session {
    constructor(id) {
        this.id = id;
        this.nextExpectedSeq = 0;
        this.isDownstreamStarted = false;
        this.lastActivityTime = Date.now();
        this.coreServiceDetails = null; // Renamed from vlessDetails
        this.remoteSocket = null;
        this.isInitialized = false;
        this.coreServiceResponseHeader = null; // Renamed from vlessResponseHeader
        this.isCoreServiceHeaderSentToClient = false; // Renamed from isVlessHeaderSentToClient
        this.pendingClientDataBuffers = new Map();
        this.isCleanedUp = false;
        this.httpDownstreamResponse = null; 
        log('debug', `Session created: ${id}`);
    }

    // Renamed from initializeVlessConnection
    async initializeCoreServiceConnection(firstClientPacket) {
        if (this.isInitialized) return true;
        
        try {
            log('debug', `Initializing CoreService for session ${this.id} from first packet`); // Text changed
            const clientReadableStream = new ReadableStream({
                start(controller) {
                    controller.enqueue(firstClientPacket);
                    controller.close(); 
                }
            });
            
            const clientWritableStream = new WritableStream(); 
            const tempClientWebStream = { readable: clientReadableStream, writable: clientWritableStream };

            // Func name and var name changed
            this.coreServiceDetails = await parse_coreservice_client_header(tempClientWebStream.readable.getReader(), SETTINGS.UUID);
            // Var name changed, text changed
            log('info', `CoreService parsed for ${this.id}: ${this.coreServiceDetails.hostname}:${this.coreServiceDetails.port}`);
            
            this.remoteSocket = await connect_to_remote_host(this.coreServiceDetails.hostname, this.coreServiceDetails.port); // Var name changed
            log('info', `Remote connected for ${this.id}`);
            
            this.coreServiceResponseHeader = Buffer.from(this.coreServiceDetails.responseBytes); // Var name changed (both)
            this.isInitialized = true;

            if (this.httpDownstreamResponse) {
                this._initiateDownstreamDataFlow();
            }
            return true;
        } catch (err) {
            log('error', `Session ${this.id} CoreService initialization failed: ${err.message}`); // Text changed
            this.cleanup(); 
            return false;
        }
    }

    async handleClientDataPacket(sequence, dataBuffer) {
        this.lastActivityTime = Date.now();
        this.pendingClientDataBuffers.set(sequence, dataBuffer);
        log('debug', `Session ${this.id}: buffered packet seq=${sequence}, size=${dataBuffer.length}`);
            
        let processedCount = 0;
        while (this.pendingClientDataBuffers.has(this.nextExpectedSeq)) {
            const nextData = this.pendingClientDataBuffers.get(this.nextExpectedSeq);
            this.pendingClientDataBuffers.delete(this.nextExpectedSeq);
                
            if (!this.isInitialized && this.nextExpectedSeq === 0) {
                // Func name changed
                if (!await this.initializeCoreServiceConnection(nextData)) {
                    // Text changed
                    throw new Error('Failed to initialize CoreService connection during packet processing');
                }
                 await this._writeToRemoteSocket(this.coreServiceDetails.data); // Var name changed

            } else if (this.isInitialized) {
                await this._writeToRemoteSocket(nextData);
            } else {
                log('warn', `Session ${this.id}: Received out-of-order packet seq=${this.nextExpectedSeq} before initialization completed.`);
            }
            this.nextExpectedSeq++;
            processedCount++;
            log('debug', `Session ${this.id}: processed packet seq=${this.nextExpectedSeq-1}`);
        }

        if (this.pendingClientDataBuffers.size > SETTINGS.MAX_BUFFERED_POSTS) {
            log('error', `Session ${this.id}: Too many buffered packets (${this.pendingClientDataBuffers.size}). Cleaning up.`);
            this.cleanup();
            throw new Error('Too many buffered packets');
        }
        return processedCount > 0;
    }
    
    _initiateDownstreamDataFlow() {
        // Var name changed
        if (!this.httpDownstreamResponse || !this.coreServiceResponseHeader || !this.remoteSocket || !this.isInitialized) {
             log('debug', `Session ${this.id}: Downstream conditions not met. Res: ${!!this.httpDownstreamResponse}, Header: ${!!this.coreServiceResponseHeader}, Remote: ${!!this.remoteSocket}, Initialized: ${this.isInitialized}`);
            return;
        }
        
        try {
            // Var name changed (both)
            if (!this.isCoreServiceHeaderSentToClient) {
                log('debug', `Session ${this.id}: Sending CoreService response header to client: ${this.coreServiceResponseHeader.length} bytes`); // Text and var name changed
                this.httpDownstreamResponse.write(this.coreServiceResponseHeader); // Var name changed
                this.isCoreServiceHeaderSentToClient = true; // Var name changed
            }
            
            this.remoteSocket.pipe(this.httpDownstreamResponse);

            this.remoteSocket.on('end', () => {
                log('debug', `Session ${this.id}: Remote socket ended. Ending client response.`);
                if (!this.httpDownstreamResponse.writableEnded) {
                    this.httpDownstreamResponse.end();
                }
            });
            
            this.remoteSocket.on('error', (err) => {
                log('error', `Session ${this.id}: Remote socket error: ${err.message}. Ending client response.`);
                if (isSignificantRelayError(err)) {
                    // Log significant errors
                }
                if (!this.httpDownstreamResponse.writableEnded) {
                    this.httpDownstreamResponse.end();
                }
                this.cleanup(); 
            });

            this.isDownstreamStarted = true;
            log('info', `Session ${this.id}: Downstream data flow initiated.`);

        } catch (err) {
            log('error', `Session ${this.id}: Error starting downstream: ${err.message}`);
            this.cleanup();
        }
    }


    startHttpClientDownstream(res, httpHeaders) {
        this.lastActivityTime = Date.now();
        if (res.headersSent) {
            log.warn(`Session ${this.id}: Headers already sent for HTTP downstream.`);
        } else {
            res.writeHead(HTTP_STATUS_OK, httpHeaders);
        }

        this.httpDownstreamResponse = res;
        
        // Var name changed
        if (this.isInitialized && this.coreServiceResponseHeader && this.remoteSocket) {
            this._initiateDownstreamDataFlow();
        } else {
            // Text changed
            log('debug', `Session ${this.id}: Waiting for CoreService initialization to start downstream.`);
        }
        
        res.on('close', () => {
            log('info', `Session ${this.id}: Client HTTP connection closed.`);
            this.cleanup(); 
        });
        return true;
    }

    async _writeToRemoteSocket(data) {
        if (!this.remoteSocket || this.remoteSocket.destroyed) {
            throw new Error(`Session ${this.id}: Remote socket not available or destroyed.`);
        }
        return new Promise((resolve, reject) => {
            this.remoteSocket.write(data, (err) => {
                if (err) {
                    log('error', `Session ${this.id}: Failed to write to remote: ${err.message}`);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    cleanup() {
        if (this.isCleanedUp) return;
        this.isCleanedUp = true;
        log('debug', `Cleaning up session ${this.id}`);
        if (this.remoteSocket) {
            this.remoteSocket.destroy();
            this.remoteSocket = null;
        }
        if (this.httpDownstreamResponse && !this.httpDownstreamResponse.writableEnded) {
            this.httpDownstreamResponse.end();
            this.httpDownstreamResponse = null;
        }
        this.pendingClientDataBuffers.clear();
        this.isInitialized = false;
        this.isCoreServiceHeaderSentToClient = false; // Var name changed
        sessions.delete(this.id); 
        log('info', `Session ${this.id} cleaned up and removed.`);
    }
} 

const metaInfo = execSync(
    'curl -s https://speed.cloudflare.com/meta | awk -F\\" \'{print $26"-"$18}\' | sed -e \'s/ /_/g\'',
    { encoding: 'utf-8' }
);
const ISP = metaInfo.trim();
let IP = DOMAIN;
if (!DOMAIN) {
    try {
        IP = execSync('curl -s --max-time 2 ipv4.ip.sb', { encoding: 'utf-8' }).trim();
    } catch (err) {
        try {
            IP = `[${execSync('curl -s --max-time 1 ipv6.ip.sb', { encoding: 'utf-8' }).trim()}]`;
        } catch (ipv6Err) {
            log('error', 'Failed to get public IP address:', ipv6Err.message);
            IP = 'localhost'; 
        }
    }
}
log('info', `Using IP: ${IP} for CoreService configuration.`); // Text changed

function generatePadding(min, max) {
    const length = min + Math.floor(Math.random() * (max - min + 1));
    return Buffer.alloc(length, 'X').toString('base64'); 
}

function handleRootRequest(req, res) {
    res.writeHead(HTTP_STATUS_OK, { 'Content-Type': 'text/plain' });
    res.end('Hello, World\nService is operational.');
}

function handleSubscriptionRequest(req, res) {
    // IMPORTANT: The "vless://" scheme name here MUST NOT be changed to preserve functionality.
    const coreServiceURL = `vless://${SETTINGS.UUID}@${IP}:443?encryption=none&security=tls&sni=${IP}&fp=chrome&allowInsecure=1&type=xhttp&host=${IP}&path=${SETTINGS.XPATH}&mode=packet-up#${NAME}-${ISP}`; // Var name changed
    const base64Content = Buffer.from(coreServiceURL).toString('base64'); // Var name changed
    res.writeHead(HTTP_STATUS_OK, { 'Content-Type': 'text/plain' });
    res.end(base64Content + '\n');
}

function handleNotFoundResponse(req, res) {
    res.writeHead(HTTP_STATUS_NOT_FOUND);
    res.end();
}

function handleCoreServiceGetRequest(req, res, sessionId, baseHeaders) { // Renamed from handleVlessGetRequest
    const httpHeaders = {
        ...baseHeaders,
        'Content-Type': 'application/octet-stream',
        'Transfer-Encoding': 'chunked',
    };

    let session = sessions.get(sessionId);
    if (!session) {
        session = new Session(sessionId);
        sessions.set(sessionId, session);
        log('info', `New session ${sessionId} created for GET request.`);
    } else {
        log('info', `Using existing session ${sessionId} for GET request.`);
    }
    
    session.lastActivityTime = Date.now(); 

    if (!session.startHttpClientDownstream(res, httpHeaders)) {
        log('error', `Failed to start downstream for session: ${sessionId}`);
        if (!res.headersSent) res.writeHead(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        if (!res.writableEnded) res.end();
        session.cleanup(); 
    }
}

async function handleCoreServicePostRequest(req, res, sessionId, sequenceNum, baseHeaders) { // Renamed from handleVlessPostRequest
    let session = sessions.get(sessionId);
    if (!session) {
        session = new Session(sessionId);
        sessions.set(sessionId, session);
        log('info', `New session ${sessionId} created for POST seq ${sequenceNum}.`);
        
        setTimeout(() => {
            const currentSession = sessions.get(sessionId);
            if (currentSession && !currentSession.isDownstreamStarted && !currentSession.isCleanedUp) {
                log('warn', `Session ${sessionId} timed out (no downstream GET). Cleaning up.`);
                currentSession.cleanup();
            }
        }, SETTINGS.SESSION_TIMEOUT);
    } else {
         log('debug', `Using existing session ${sessionId} for POST seq ${sequenceNum}.`);
    }

    session.lastActivityTime = Date.now(); 

    let requestDataChunks = [];
    let totalReceivedBytes = 0;
    let postHeadersSent = false; 

    req.on('data', chunk => {
        totalReceivedBytes += chunk.length;
        if (totalReceivedBytes > SETTINGS.MAX_POST_SIZE) {
            if (!postHeadersSent) {
                res.writeHead(HTTP_STATUS_PAYLOAD_TOO_LARGE, baseHeaders);
                res.end();
                postHeadersSent = true;
            }
            req.destroy(new Error('Payload too large')); 
            return;
        }
        requestDataChunks.push(chunk);
    });

    req.on('end', async () => {
        if (postHeadersSent || req.destroyed) return; 
        
        try {
            const completeDataBuffer = Buffer.concat(requestDataChunks);
            log('info', `Session ${sessionId}: Processing POST packet seq=${sequenceNum}, size=${completeDataBuffer.length}`);
            
            await session.handleClientDataPacket(sequenceNum, completeDataBuffer);
            
            if (!postHeadersSent) {
                res.writeHead(HTTP_STATUS_OK, baseHeaders);
                postHeadersSent = true;
            }
            res.end();
            
        } catch (err) {
            log('error', `Session ${sessionId}: Failed to process POST (seq ${sequenceNum}): ${err.message}`);
            session.cleanup(); 
            
            if (!postHeadersSent) {
                res.writeHead(HTTP_STATUS_INTERNAL_SERVER_ERROR, baseHeaders);
            }
            res.end();
        }
    });

    req.on('error', (err) => {
        log('error', `Session ${sessionId}: POST request stream error: ${err.message}`);
        session.cleanup();
        if (!postHeadersSent && !res.writableEnded) {
            res.writeHead(HTTP_STATUS_INTERNAL_SERVER_ERROR, baseHeaders);
            res.end();
        }
    });
}


const server = http.createServer((req, res) => {
    const commonHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST',
        'Cache-Control': 'no-store',
        'X-Accel-Buffering': 'no', 
        'X-Padding': generatePadding(100, 1000),
    };

    log('debug', `Incoming request: ${req.method} ${req.url}`);

    if (req.url === '/') {
        return handleRootRequest(req, res);
    } 
    
    if (req.url === `/${SUB_PATH}`) {
        return handleSubscriptionRequest(req, res);
    }

    const pathMatch = req.url.match(CORESERVICE_PATH_REGEX); // Regex var name changed
    if (!pathMatch) {
        return handleNotFoundResponse(req, res);
    }
    
    const [, sessionId, seqStr] = pathMatch;
    const sequenceNum = seqStr ? parseInt(seqStr, 10) : null;

    if (req.method === 'GET' && sequenceNum === null) {
        // Func name changed
        return handleCoreServiceGetRequest(req, res, sessionId, commonHeaders);
    }
    
    if (req.method === 'POST' && sequenceNum !== null) {
        // Func name changed
        return handleCoreServicePostRequest(req, res, sessionId, sequenceNum, commonHeaders);
    }

    return handleNotFoundResponse(req, res); 
});

server.on('clientError', (err, socket) => {
    log('error', `HTTP ClientError: ${err.message}`);
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
});

server.on('secureConnection', (socket) => { 
    log('debug', `New secure connection using: ${socket.alpnProtocol || 'http/1.1'}`);
});

server.keepAliveTimeout = 620 * 1000; 
server.headersTimeout = 625 * 1000;   

server.on('error', (err) => {
    log('error', `Server error: ${err.message}`);
});

setInterval(() => {
    const now = Date.now();
    sessions.forEach(session => {
        if (now - session.lastActivityTime > SETTINGS.SESSION_TIMEOUT * 2) { 
            log('warn', `Session ${session.id} is stale (no activity). Cleaning up.`);
            session.cleanup();
        }
    });
}, SETTINGS.SESSION_TIMEOUT);


server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    // Text changed
    log('info', `CoreService proxy server started on port ${PORT}. UUID: ${SETTINGS.UUID}. Path: ${SETTINGS.XPATH}`);
});
