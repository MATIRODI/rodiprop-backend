const { Client, LocalAuth } = require('whatsapp-web.js');
const express = require('express');
const fs = require('fs');
const path = require('path');
const app = express();
app.use(express.json());

// Eliminar lock files de Chromium antes de iniciar para evitar el error
// "profile appears to be in use" cuando el contenedor crashea y se reinicia
function cleanChromiumLocks() {
    const searchDirs = ['./.wwebjs_auth', './.wwebjs_cache'];
    const lockFiles = ['SingletonLock', 'SingletonCookie', 'SingletonSocket'];
    searchDirs.forEach(dir => {
        if (!fs.existsSync(dir)) return;
        try {
            const walk = (d) => {
                fs.readdirSync(d).forEach(entry => {
                    const full = path.join(d, entry);
                    try {
                        if (fs.statSync(full).isDirectory()) {
                            walk(full);
                        } else if (lockFiles.includes(entry)) {
                            fs.unlinkSync(full);
                            console.log('Lock eliminado: ' + full);
                        }
                    } catch(e) {}
                });
            };
            walk(dir);
        } catch(e) {
            console.log('Lock cleanup error: ' + e.message);
        }
    });
}

cleanChromiumLocks();

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
            '--single-process'
        ],
        headless: true,
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium'
    }
});

let qrCode = null;
let isReady = false;

client.on('qr', (qr) => {
    qrCode = qr;
    isReady = false;
    console.log('QR generado - visitá /qr para verlo');
});

client.on('ready', () => {
    isReady = true;
    qrCode = null;
    console.log('WhatsApp conectado!');
});

client.on('disconnected', () => {
    isReady = false;
    console.log('WhatsApp desconectado');
});

app.get('/qr', (req, res) => {
    if (isReady) return res.json({ status: 'conectado' });
    if (!qrCode) return res.json({ status: 'esperando_qr' });
    res.send(`
        <html><body style="text-align:center;font-family:sans-serif">
        <h2>Escaneá este QR con WhatsApp</h2>
        <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(qrCode)}"/>
        <p>Actualizá la página cada 30 segundos</p>
        </body></html>
    `);
});

app.get('/status', (req, res) => {
    res.json({ ready: isReady });
});

app.post('/send', async (req, res) => {
    const { numero, mensaje } = req.body;
    if (!isReady) return res.status(503).json({ error: 'WhatsApp no conectado' });
    try {
        const num = numero.startsWith('+')
            ? numero.replace('+', '') + '@c.us'
            : '549' + numero + '@c.us';
        await client.sendMessage(num, mensaje);
        res.json({ status: 'enviado' });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

client.initialize();
app.listen(3000, () => console.log('WhatsApp service en puerto 3000'));
