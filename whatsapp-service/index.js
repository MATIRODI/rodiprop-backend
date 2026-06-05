const { Client, LocalAuth } = require('whatsapp-web.js');
const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// Detecta la ruta de Chromium: prioriza variable de entorno,
// luego el Chromium bundleado con puppeteer, luego rutas de sistema.
function getChromiumPath() {
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
        return process.env.PUPPETEER_EXECUTABLE_PATH;
    }
    // Intentar usar el Chromium descargado por puppeteer
    try {
        const puppeteer = require('puppeteer');
        const ep = puppeteer.executablePath();
        if (ep && fs.existsSync(ep)) {
            console.log('Chromium (puppeteer bundled): ' + ep);
            return ep;
        }
    } catch(e) {}
    // Rutas de sistema comunes
    const sysPaths = [
        '/usr/bin/chromium', '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable',
        '/usr/local/bin/chromium',
    ];
    for (const p of sysPaths) {
        if (fs.existsSync(p)) {
            console.log('Chromium (sistema): ' + p);
            return p;
        }
    }
    // Último recurso: dejar que puppeteer lo encuentre
    try {
        const { execSync } = require('child_process');
        const found = execSync('which chromium 2>/dev/null || which chromium-browser 2>/dev/null || echo ""').toString().trim();
        if (found) { console.log('Chromium (which): ' + found); return found; }
    } catch(e) {}
    console.warn('Chromium no encontrado — el cliente puede fallar al iniciar');
    return '/usr/bin/chromium';
}

// Eliminar lock files de Chromium antes de iniciar para evitar
// el error "profile appears to be in use" al reiniciar el contenedor
function cleanChromiumLocks() {
    const searchDirs = ['./.wwebjs_auth', './.wwebjs_cache'];
    const lockFiles = ['SingletonLock', 'SingletonCookie', 'SingletonSocket'];
    searchDirs.forEach(dir => {
        if (!fs.existsSync(dir)) return;
        const walk = (d) => {
            try {
                fs.readdirSync(d).forEach(entry => {
                    const full = path.join(d, entry);
                    try {
                        if (fs.statSync(full).isDirectory()) walk(full);
                        else if (lockFiles.includes(entry)) { fs.unlinkSync(full); console.log('Lock eliminado: ' + full); }
                    } catch(e) {}
                });
            } catch(e) {}
        };
        walk(dir);
    });
}

cleanChromiumLocks();

const chromiumPath = getChromiumPath();
console.log('Iniciando con Chromium: ' + chromiumPath);

const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-extensions',
            '--disable-software-rasterizer',
            '--single-process',
        ],
        headless: true,
        executablePath: chromiumPath,
    }
});

let qrCode = null;
let isReady = false;

client.on('qr', (qr) => {
    qrCode = qr;
    isReady = false;
    console.log('QR generado - visitá /qr para escanear');
    // También loguear QR en terminal para verlo en Railway logs
    try { require('qrcode-terminal').generate(qr, { small: true }); } catch(e) {}
});

client.on('ready', () => {
    isReady = true;
    qrCode = null;
    console.log('WhatsApp conectado y listo!');
});

client.on('disconnected', (reason) => {
    isReady = false;
    console.log('WhatsApp desconectado: ' + reason);
    // Reintentar conexión después de 10 segundos
    setTimeout(() => {
        console.log('Reintentando inicializar...');
        client.initialize().catch(e => console.error('Error al reinicializar: ' + e.message));
    }, 10000);
});

client.on('auth_failure', (msg) => {
    console.error('Error de autenticación: ' + msg);
    qrCode = null;
    isReady = false;
});

// Manejo de errores no capturados para evitar crash del proceso
process.on('unhandledRejection', (reason) => {
    console.error('unhandledRejection: ' + reason);
});
process.on('uncaughtException', (err) => {
    console.error('uncaughtException: ' + err.message);
    // No hacer process.exit() — mantener el servidor HTTP vivo
});

app.get('/qr', (req, res) => {
    if (isReady) return res.json({ status: 'conectado' });
    if (!qrCode) return res.json({ status: 'esperando_qr', mensaje: 'El QR se genera ~30s después de iniciar' });
    res.send(`
        <!DOCTYPE html><html><body style="background:#0a0d14;color:#f4efe8;text-align:center;font-family:sans-serif;padding:2rem">
        <h2>Escaneá este QR con WhatsApp</h2>
        <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(qrCode)}" style="border-radius:12px"/>
        <p style="color:#888;margin-top:1rem">Actualizá la página cada 30 segundos hasta conectar</p>
        <script>setTimeout(()=>location.reload(),30000)</script>
        </body></html>
    `);
});

app.get('/status', (req, res) => {
    res.json({ ready: isReady, qr_disponible: !!qrCode });
});

app.get('/health', (req, res) => {
    res.json({ status: 'ok', ready: isReady });
});

app.post('/send', async (req, res) => {
    const { numero, mensaje } = req.body;
    if (!isReady) return res.status(503).json({ error: 'WhatsApp no conectado. Escaneá el QR en /qr' });
    if (!numero || !mensaje) return res.status(400).json({ error: 'Faltan numero o mensaje' });
    try {
        const num = numero.replace(/\D/g, '');
        const chatId = num.startsWith('54') ? num + '@c.us' : '54' + num + '@c.us';
        await client.sendMessage(chatId, mensaje);
        res.json({ status: 'enviado', numero: chatId });
    } catch (e) {
        console.error('Error al enviar: ' + e.message);
        res.status(500).json({ error: e.message });
    }
});

client.initialize().catch(e => {
    console.error('Error al inicializar cliente: ' + e.message);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('WhatsApp service en puerto ' + PORT));
