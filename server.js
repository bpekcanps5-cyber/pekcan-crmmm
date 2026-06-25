// server.js — WhatsApp + Sunucu + WebSocket + MEDYA (foto/ses/belge) + grup açıklaması
// Çalıştır: node server.js  → http://localhost:3000 panel, terminalde QR

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  downloadMediaMessage,
} = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const QRImage = require('qrcode'); // panelde QR resmi gostermek icin
const express = require('express');
const path = require('path');
const fs = require('fs');
const http = require('http');
const https = require('https');
const { WebSocketServer } = require('ws');
const db = require('./db'); // Supabase (PostgreSQL) veri katmani

// ============================================================
// LOG GURULTU FILTRESI
// Baileys/libsignal bazen "Bad MAC", "Failed to decrypt", "Session error",
// "Closing session/open session" gibi SIFRELEME hatalarini dogrudan console'a basar.
// Bunlar ZARARSIZ gurultu (sunucu cokmez) ama ekrani doldurur + cok log yazmak yuk.
// Bu satirlari gizleyip sadece ANLAMLI loglari gosteririz.
// (Onemli: kendi loglarimizi engellemez — sadece bilinen WhatsApp gurultusunu susturur.)
const _origLog = console.log.bind(console);
const _origErr = console.error.bind(console);
const _gurultuKaliplari = [
  'Bad MAC', 'Failed to decrypt', 'Session error', 'decryptWhisperMessage',
  'Closing session', 'Closing open session', 'MessageCounterError', 'No session found',
  'SessionEntry', 'libsignal', 'verifyMAC', 'queue_job', 'session_cipher',
  'No matching sessions', 'Key used already', 'prekey', 'senderKeyDistribution',
  'incoming prekey bundle', 'chainKey', 'ephemeralKeyPair', 'currentRatchet',
  'remoteIdentityKey', 'registrationId', 'rootKey', '_chains',
];
function _gurultuMu(args) {
  try {
    // Tum argumanlari (string + nesne) tek metne cevirip kaliplari ara.
    // Boylece "Closing open session" + koca SessionEntry nesnesi dokumunu de yakalariz.
    let s = '';
    for (const a of args) {
      if (typeof a === 'string') s += ' ' + a;
      else if (a && typeof a === 'object') {
        // nesnenin anahtarlarina ve message alanina bak (tum JSON'u stringify etmek pahali olabilir)
        if (a.message) s += ' ' + a.message;
        try { s += ' ' + Object.keys(a).join(' '); } catch (_) {}
      }
    }
    return _gurultuKaliplari.some(k => s.includes(k));
  } catch (e) { return false; }
}
console.log = (...args) => { if (!_gurultuMu(args)) _origLog(...args); };
console.error = (...args) => { if (!_gurultuMu(args)) _origErr(...args); };

const PORT = 3000;
// Mesaj saklama suresi: bundan eski mesajlar panele dusmez, DB'ye yazilmaz ve periyodik silinir.
const MESAJ_SAKLAMA_GUN = 30;
const MESAJ_SAKLAMA_MS = MESAJ_SAKLAMA_GUN * 24 * 60 * 60 * 1000;
const MEDIA_DIR = path.join(__dirname, 'public', 'media');
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });
const AUTH_BASE = path.join(__dirname, 'auth'); // her hat: auth/<lineId>/
if (!fs.existsSync(AUTH_BASE)) fs.mkdirSync(AUTH_BASE, { recursive: true });

// ============================================================
// COK HATLI (MULTI-LINE) YAPI
// Her WhatsApp hatti ayri bir "line" objesi. En fazla MAX_LINES hat.
// ============================================================
const MAX_LINES = 5;
const lines = new Map(); // lineId -> line objesi

// Bir hat objesi olustur (henuz baglanmamis)
function createLine(lineId, label, ownerUser) {
  return {
    id: lineId,            // benzersiz hat kimligi (orn. "line1")
    label: label || lineId, // gorunen ad (orn. "Ofis Ana Hat")
    owner: ownerUser || null, // bu hatti ekleyen/sahibi kullanici adi
    sock: null,            // Baileys soketi
    connected: false,      // bagli mi
    myNumber: null,        // bu hattin numarasi
    myLID: null,           // bu hattin LID'i
    myName: '',            // bu hattin WhatsApp adi
    lastQR: null,          // bu hat icin son QR resmi
    manualLogout: false,   // panelden cikis yapildi mi
    chats: new Map(),      // bu hattin sohbetleri (jid -> chat)
    authDir: path.join(AUTH_BASE, lineId), // bu hattin oturum klasoru
    starting: false,       // baglanma islemi suruyor mu (cift baslatmayi onler)
  };
}

// Bir hattin durumunu panele yayinla
function lineStatus(line) {
  return {
    id: line.id, label: line.label, connected: line.connected,
    myNumber: line.myNumber, myName: line.myName,
    hasQR: !line.connected && !!line.lastQR,
  };
}

// ---- Geriye donuk uyumluluk koprusu ----
// Eski kod "waSock", "chats", "myNumber" gibi tek-hat degiskenlerini kullaniyordu.
// Bunlari, su an islem yapilan "aktif hatta" yonlendiriyoruz ki eski fonksiyonlar calismaya devam etsin.
// Cogu fonksiyon bir "line" parametresi alacak sekilde guncellenecek; gecis surecinde bu kopru is gorur.
let activeLine = null; // o an islem yapilan hat (mesaj islerken set edilir)

// Not: Asagidaki global'ler artik "aktif hat"tan turetilir (gecis kolayligi icin).
const chats = new Map();   // GECICI: artik her hattin kendi chats'i var; bu bos kalacak / kaldirilacak
let waSock = null;         // GECICI kopru
let waConnected = false;   // GECICI kopru
let _sonWaAktivite = 0;    // WhatsApp'tan en son ne zaman veri/olay geldi (canlilik kontrolu icin)
let myNumber = null;
let myLID = null;
let lastQR = null;
let manualLogout = false;


// ---- Web sunucusu + WebSocket ----
const app = express();
// nginx/Cloudflare ARKASINDA calisirken kullanicinin GERCEK IP'sini al
// (yoksa hep nginx'in IP'si gelir). IP kisitlamasi icin sart.
app.set('trust proxy', true);

// Medya indirme: ?name= varsa o isimle indir (belgeler gercek adiyla insin)
app.get('/media/:file', (req, res, next) => {
  const wanted = req.query.name;
  const filePath = path.join(MEDIA_DIR, req.params.file);
  if (!fs.existsSync(filePath)) return next();
  if (wanted) {
    // gercek isimle indir
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(wanted)}"`);
  }
  return res.sendFile(filePath);
});

app.use(express.static(path.join(__dirname, 'public')));

// ---- GIRIS SISTEMI (login + kullanici yonetimi) ----
// Basit oturum: giris yapan kullaniciya bir token verilir, panel bunu saklar.
const sessions = new Map(); // token -> { username, displayName, role, ts }
function makeToken() { return Math.random().toString(36).slice(2) + Date.now().toString(36); }

// ---- IP KISITLAMA ----
// IP IZIN SISTEMI — IKI AYRI LISTE (kapsam bazli):
//  ofisIpler   : OFIS linkinden girenler icin izinli IP'ler (genelde 1 sabit ofis IP'si)
//  disariIpler : DISARI linkinden girenler icin izinli IP'ler (subeler, izinli evler — 5-10 adet)
// Yonetici (admin) HER ZAMAN her IP'den, her linkten girer (muaf).
// IP_KISITLAMA_KAPALI=1 olursa (acil durum) tum kisitlama kapanir -> herkes girer.
let ofisIpler = new Set();
let disariIpler = new Set();
const ipKisitlamaKapali = () => process.env.IP_KISITLAMA_KAPALI === '1';
// OFIS domain(ler)i: .env'de OFIS_DOMAIN=ofis.site.com (virgulle birden fazla olabilir).
// Istek bu domain(ler)den geldiyse "ofis linki", degilse "disari linki" sayilir.
const _ofisDomainler = () => (process.env.OFIS_DOMAIN || '')
  .split(',').map(d => d.trim().toLowerCase()).filter(Boolean);
// istekten gercek IP'yi al (nginx arkasinda x-forwarded-for'dan gelir; trust proxy acik)
function gercekIp(req) {
  let ip = (req.ip || '').trim();
  // IPv6-mapped IPv4 (::ffff:1.2.3.4) -> 1.2.3.4
  if (ip.startsWith('::ffff:')) ip = ip.slice(7);
  return ip;
}
// istek HANGI linkten geldi? Host basligina bakar -> 'ofis' veya 'disari'
function istekKapsami(req) {
  const host = (req.headers.host || '').toLowerCase().split(':')[0]; // port'u at
  const ofisler = _ofisDomainler();
  if (ofisler.length && ofisler.includes(host)) return 'ofis';
  return 'disari';
}
// bu IP, bu KAPSAM (ofis/disari) icin izinli mi?
function ipIzinliMi(ip, kapsam = 'disari') {
  if (ipKisitlamaKapali()) return true;       // acil durum: kisitlama kapali
  const liste = (kapsam === 'ofis') ? ofisIpler : disariIpler;
  if (liste.size === 0) return true;           // bu liste bossa o link icin kisitlama yok (kilitlenmeyi onler)
  return liste.has(ip);
}
// izinli IP'leri DB'den bellege yukle (kapsamlara ayir)
async function izinliIpleriYukle() {
  if (!db.isReady()) return;
  try {
    const liste = await db.loadAllowedIps();
    ofisIpler = new Set(liste.filter(x => x.kapsam === 'ofis').map(x => x.ip));
    disariIpler = new Set(liste.filter(x => x.kapsam !== 'ofis').map(x => x.ip));
    console.log(`🔒 IP listeleri yuklendi: ofis=${ofisIpler.size}, disari=${disariIpler.size}`);
  } catch (e) { console.error('izinli IP yukleme hatasi:', e.message); }
}

// Giris yap
app.post('/api/login', express.json(), async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.json({ ok: false, error: 'Kullanıcı adı ve şifre gerekli' });
  if (!db.isReady()) return res.json({ ok: false, error: 'Veritabanı bağlı değil, giriş yapılamıyor' });
  const user = await db.checkLogin(username.trim(), password);
  if (!user) return res.json({ ok: false, error: 'Kullanıcı adı veya şifre hatalı' });
  // IP KISITLAMA: normal kullanici sadece izinli IP'den girebilir.
  // Yonetici (admin) her IP'den/her linkten girer ki kilitlenmesin + IP'leri yonetebilsin.
  if (user.role !== 'admin') {
    const ip = gercekIp(req);
    const kapsam = istekKapsami(req); // hangi linkten geldi: ofis / disari
    if (!ipIzinliMi(ip, kapsam)) {
      console.log(`⛔ IP engellendi: ${username} | IP: ${ip} | link: ${kapsam} (izinli degil)`);
      const linkAdi = kapsam === 'ofis' ? 'ofis' : 'dışarı';
      return res.json({ ok: false, error: `Bu konumdan (IP) ${linkAdi} girişine izin yok. Yöneticinize başvurun.`, ipBlocked: true, ip });
    }
  }
  const token = makeToken();
  // Kullanicinin HANGI hatta bagli oldugunu bul (ofis kullanicilari 'ofis', pazarlamacilar kendi hatti)
  const hatBilgi = await db.getUserLine(user.username);
  const lineId = hatBilgi.line_id || 'ofis';
  const lineTip = hatBilgi.tip || 'ofis';
  sessions.set(token, { username: user.username, displayName: user.display_name, role: user.role, ts: Date.now(), lineId, lineTip });
  // oturumu Supabase'e de yaz (sunucu restart olunca kaybolmasin)
  db.saveSession(token, user.username, user.display_name, user.role).catch(() => {});
  // PAZARLAMACI girisi: kendi hatti henuz baglanmamissa BASLAT (QR uretsin / kayitli oturumla baglansin).
  // Ofis hatti zaten acilista baslatiliyor, ona dokunma.
  if (lineTip === 'pazarlama' && lineId !== 'ofis') {
    const mevcutHat = lines.get(lineId);
    if (!mevcutHat || (!mevcutHat.connected && !mevcutHat.starting)) {
      console.log(`📱 Pazarlamaci girisi: '${lineId}' hatti baslatiliyor (${user.username})...`);
      startWA(lineId).catch(e => console.error(`Hat baslatilamadi (${lineId}):`, e.message));
    }
  }
  res.json({ ok: true, token, displayName: user.display_name, role: user.role, username: user.username, lineId, lineTip });
});

// Token gecerli mi (panel acilinca kontrol)
app.post('/api/whoami', express.json(), async (req, res) => {
  const { token } = req.body || {};
  if (!token) return res.json({ ok: false });
  let s = sessions.get(token);
  // Bellekte yoksa (sunucu yeniden baslamis olabilir) Supabase'den yukle ve bellege geri koy.
  if (!s && db.isReady()) {
    const rows = await db.loadSessions();
    for (const r of rows) sessions.set(r.token, { username: r.username, displayName: r.display_name, role: r.role, ts: Date.now() });
    s = sessions.get(token);
    // session DB'den geldiyse hat bilgisi eksik olabilir — kullanicinin hattini cek + session'a ekle
    if (s && !s.lineId) {
      const hb = await db.getUserLine(s.username);
      s.lineId = hb.line_id || 'ofis';
      s.lineTip = hb.tip || 'ofis';
    }
  }
  if (!s) return res.json({ ok: false });
  const lineId = s.lineId || 'ofis';
  const lineTip = s.lineTip || 'ofis';
  // PAZARLAMACI ise ve hatti bagli degilse, hattini baslat (sayfa yenilemede de QR gelsin)
  if (lineTip === 'pazarlama' && lineId !== 'ofis') {
    const mevcut = lines.get(lineId);
    if (!mevcut || (!mevcut.connected && !mevcut.starting)) {
      startWA(lineId).catch(() => {});
    }
  }
  res.json({ ok: true, displayName: s.displayName, role: s.role, username: s.username, lineId, lineTip });
});

// Cikis (token sil)
app.post('/api/applogout', express.json(), (req, res) => {
  const { token } = req.body || {};
  if (token) { sessions.delete(token); db.deleteSession(token).catch(() => {}); }
  res.json({ ok: true });
});

// Yardimci: istek yoneticiden mi geliyor?
function isAdmin(token) {
  const s = token && sessions.get(token);
  return s && s.role === 'admin';
}

// ---- IZINLI IP YONETIMI (sadece yonetici) ----
// Iki ayri liste: ofis (ofis linki icin) ve disari (disari linki icin).
app.post('/api/ips', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const liste = await db.loadAllowedIps();
  res.json({
    ok: true,
    ofisIps: liste.filter(x => x.kapsam === 'ofis'),
    disariIps: liste.filter(x => x.kapsam !== 'ofis'),
    benimIp: gercekIp(req),                 // yoneticinin su anki IP'si (tek tikla eklemek icin)
    benimKapsam: istekKapsami(req),         // yonetici su an hangi linkten girmis
    kisitlamaKapali: ipKisitlamaKapali(),   // acil durum bayragi acik mi
  });
});
// IP ekle (kapsam: ofis | disari)
app.post('/api/ips/add', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  let ip = (req.body?.ip || '').trim();
  const aciklama = (req.body?.aciklama || '').trim();
  const kapsam = req.body?.kapsam === 'ofis' ? 'ofis' : 'disari';
  if (!ip) return res.json({ ok: false, error: 'IP boş olamaz' });
  if (ip.startsWith('::ffff:')) ip = ip.slice(7);
  await db.addAllowedIp(ip, aciklama, kapsam);
  await izinliIpleriYukle(); // bellegi tazele
  res.json({ ok: true });
});
// IP cikar
app.post('/api/ips/remove', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const ip = (req.body?.ip || '').trim();
  if (!ip) return res.json({ ok: false, error: 'IP boş' });
  await db.removeAllowedIp(ip);
  await izinliIpleriYukle();
  res.json({ ok: true });
});

// Kullanici listesi (sadece yonetici)
app.post('/api/users', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const users = await db.listUsers();
  res.json({ ok: true, users });
});

// Aktif (su an panelde acik) kullanicilar (sadece yonetici)
// WebSocket'i acik olan her kullanici "aktif". Tum kullanici listesiyle birlestirip
// kim aktif (yesil) kim degil (kirmizi) doneriz.
app.post('/api/users/active', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  // o an bagli WS'lerden aktif username'leri topla
  const aktifSet = new Set();
  wss.clients.forEach((c) => { if (c.readyState === 1 && c._username) aktifSet.add(c._username); });
  const users = await db.listUsers();
  const liste = users.map(u => ({
    username: u.username,
    displayName: u.display_name || u.username,
    role: u.role,
    active: aktifSet.has(u.username),
  }));
  res.json({ ok: true, users: liste });
});

// Yeni kullanici ekle (sadece yonetici)
app.post('/api/users/add', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const { username, password, displayName, role, tip } = req.body || {};
  if (!username || !password) return res.json({ ok: false, error: 'Kullanıcı adı ve şifre gerekli' });
  const uname = username.trim();
  const r = await db.addUser(uname, password, displayName, role === 'admin' ? 'admin' : 'agent');
  if (r.ok) {
    // KULLANICI TIPI: 'pazarlama' ise kendi ayri hattini olustur, 'ofis' ise ortak hatta bagla.
    const kullaniciTipi = (tip === 'pazarlama') ? 'pazarlama' : 'ofis';
    if (kullaniciTipi === 'pazarlama') {
      const lineId = 'pzr_' + uname; // her pazarlamaciya ozel hat (orn. pzr_fatma)
      await db.saveLine(lineId, (displayName || uname) + ' (Pazarlama)', 'pazarlama', uname);
      await db.setUserLine(uname, lineId, 'pazarlama');
    } else {
      await db.setUserLine(uname, 'ofis', 'ofis'); // ofis kullanicisi ortak hatta
    }
  }
  res.json(r);
});

// Kullanici sil (sadece yonetici)
app.post('/api/users/delete', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const r = await db.deleteUser(req.body?.id);
  res.json(r);
});

// MEVCUT kullanicinin HAT TIPINI degistir (ofis <-> pazarlama). Sadece yonetici.
// Kullanim: eski/yanlis eslenmis kullaniciyi pazarlamaya cevirmek icin
// (orn. Volkan 'ofis'e dusmus -> pazarlamaya al, kendi hatti olsun).
app.post('/api/users/setline', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const username = (req.body?.username || '').trim();
  const tip = req.body?.tip === 'pazarlama' ? 'pazarlama' : 'ofis';
  if (!username) return res.json({ ok: false, error: 'Kullanıcı adı gerekli' });
  try {
    if (tip === 'pazarlama') {
      const lineId = 'pzr_' + username; // her pazarlamaciya ozel hat
      await db.saveLine(lineId, username + ' (Pazarlama)', 'pazarlama', username);
      await db.setUserLine(username, lineId, 'pazarlama');
      console.log(`🔧 Kullanici '${username}' PAZARLAMA yapildi -> hat: ${lineId}`);
      return res.json({ ok: true, username, lineId, tip: 'pazarlama', message: `${username} artık pazarlama (hat: ${lineId}). Yeniden giriş yapmalı.` });
    } else {
      await db.setUserLine(username, 'ofis', 'ofis');
      console.log(`🔧 Kullanici '${username}' OFIS yapildi (ortak hat)`);
      return res.json({ ok: true, username, lineId: 'ofis', tip: 'ofis', message: `${username} artık ofis (ortak hat). Yeniden giriş yapmalı.` });
    }
  } catch (e) {
    console.error('setline hatasi:', e.message);
    return res.json({ ok: false, error: e.message });
  }
});

// ============================================================
// SATIŞ TAKİBİ API (kontrol sekmesi)
// ============================================================
// Yardimci: token'dan {session, lineId, isAdmin} cikar
function satisYetki(token) {
  const s = token && sessions.get(token);
  if (!s) return null;
  return { s, lineId: s.lineId || 'ofis', isAdmin: s.role === 'admin', username: s.username, displayName: s.displayName };
}
// Tarih araligi yardimcisi: 'bugun' | 'hafta' | 'tum' -> {bas, bit} (epoch ms) veya null
function tarihAraligi(kapsam) {
  const now = new Date();
  if (kapsam === 'bugun') {
    const bas = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0).getTime();
    const bit = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999).getTime();
    return { bas, bit };
  }
  if (kapsam === 'hafta') {
    const bit = Date.now();
    const bas = bit - 7 * 24 * 60 * 60 * 1000; // son 7 gun
    return { bas, bit };
  }
  return null; // tum
}

// Satışları getir. Pazarlamaci KENDI hattini, yonetici TUMUNU (veya secili hat) gorur.
app.post('/api/satislar', express.json(), async (req, res) => {
  const y = satisYetki(req.body?.token);
  if (!y) return res.json({ ok: false, error: 'Oturum yok' });
  const kapsam = req.body?.kapsam || 'bugun'; // bugun | hafta | tum | ozel
  // ÖZEL tarih araligi: panel baslangic/bitis (YYYY-MM-DD) gonderebilir
  let ar = tarihAraligi(kapsam);
  if (kapsam === 'ozel' && req.body?.bas && req.body?.bit) {
    const basMs = new Date(req.body.bas + 'T00:00:00').getTime();
    const bitMs = new Date(req.body.bit + 'T23:59:59.999').getTime();
    if (!isNaN(basMs) && !isNaN(bitMs)) ar = { bas: basMs, bit: bitMs };
  }
  const saticiFiltre = (req.body?.satici || '').trim().toLowerCase(); // opsiyonel: belirli satici
  try {
    let satislar;
    if (y.isAdmin) {
      const istenenHat = req.body?.lineId; // opsiyonel hat filtresi
      if (istenenHat) {
        satislar = await db.loadSatislar(istenenHat, ar?.bas ?? null, ar?.bit ?? null);
      } else {
        satislar = ar ? await db.loadTumSatislar(ar.bas, ar.bit) : await db.loadTumSatislar();
      }
    } else {
      // pazarlamaci: SADECE kendi hatti (baskasini goremez)
      satislar = await db.loadSatislar(y.lineId, ar?.bas ?? null, ar?.bit ?? null);
    }
    // satici listesini cikar (panel "kisi sec" icin) — FILTRELEMEDEN once
    const saticiSet = {};
    satislar.forEach(s => { const ad = (s.satici || '').trim(); if (ad) saticiSet[ad] = (saticiSet[ad] || 0) + 1; });
    const saticilar = Object.keys(saticiSet).sort();
    // satici filtresi uygula (istendiyse)
    if (saticiFiltre) {
      satislar = satislar.filter(s => (s.satici || '').toLowerCase().includes(saticiFiltre));
    }
    res.json({ ok: true, satislar, kapsam, isAdmin: y.isAdmin, lineId: y.lineId, saticilar });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Satis adedini DUZENLE. Pazarlamaci KENDI hattindaki satisi duzenleyebilir.
// Duzenleyince YONETICIYE bildirim gider (canli + kayit).
app.post('/api/satislar/duzenle', express.json(), async (req, res) => {
  const y = satisYetki(req.body?.token);
  if (!y) return res.json({ ok: false, error: 'Oturum yok' });
  const id = req.body?.id;
  const yeniAdet = parseInt(req.body?.adet, 10);
  if (!id || isNaN(yeniAdet) || yeniAdet < 1 || yeniAdet > 9999) return res.json({ ok: false, error: 'Geçersiz adet' });
  // BRANŞ (opsiyonel): verildiyse gecerli 9 brans icinden olmali
  let yeniUrun = null;
  if (req.body?.urun) {
    const istenenBrans = String(req.body.urun).toLowerCase().trim();
    // GECERLI_BRANSLAR map'inden normalize et (yesilkart -> yeşilkart vs.)
    yeniUrun = GECERLI_BRANSLAR[istenenBrans] || (BRANS_LISTESI.includes(istenenBrans) ? istenenBrans : null);
    if (!yeniUrun) return res.json({ ok: false, error: 'Geçersiz branş. Sadece tanımlı branşlar seçilebilir.' });
  }
  try {
    // YETKI: pazarlamaci sadece KENDI hattindaki satisi duzenleyebilir
    if (!y.isAdmin) {
      const kontrol = await db.loadSatislar(y.lineId, null, null);
      const benimMi = kontrol.find(x => x.id === id);
      if (!benimMi) return res.json({ ok: false, error: 'Bu satışı düzenleme yetkiniz yok.' });
    }
    const r = await db.updateSatisAdet(id, yeniAdet, y.displayName || y.username, yeniUrun);
    if (!r.ok) return res.json({ ok: false, error: r.error || 'Düzenlenemedi' });
    // YONETICIYE BILDIRIM: ne degisti (adet ve/veya brans)
    let degisim = [];
    if (r.eskiAdet !== yeniAdet) degisim.push(`adet ${r.eskiAdet} → ${yeniAdet}`);
    if (yeniUrun && r.eskiUrun !== yeniUrun) degisim.push(`branş ${r.eskiUrun} → ${yeniUrun}`);
    const degisimMetni = degisim.length ? degisim.join(', ') : 'güncelleme yapıldı';
    broadcastHat('ofis', {
      type: 'satisDuzenlemeBildirim',
      mesaj: `${y.displayName || y.username} bir satışı düzenledi: ${degisimMetni}`,
      satisId: id, eskiAdet: r.eskiAdet, yeniAdet,
    });
    console.log(`✏️  SATIŞ DÜZENLENDİ: ${id} | ${degisimMetni} | ${y.displayName || y.username}`);
    res.json({ ok: true, satis: r.row });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Satis ONAYLA / onay kaldir (SADECE yonetici)
app.post('/api/satislar/onayla', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Sadece yönetici onaylayabilir' });
  const r = await db.setSatisOnay(req.body?.id, req.body?.onayli !== false);
  res.json(r.ok ? { ok: true, satis: r.row } : { ok: false, error: r.error });
});

// Satis SIL (SADECE yonetici — yanlis/mukerrer kayit)
app.post('/api/satislar/sil', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Sadece yönetici silebilir' });
  const r = await db.deleteSatis(req.body?.id);
  res.json(r);
});

// PANELDEN DIREKT SATIŞ EKLE (pazarlamaci composer'daki butonla).
// Gruba MESAJ GITMEZ — sadece kontrole kaydedilir. Satici = ekleyen kullanici.
app.post('/api/satislar/ekle', express.json(), async (req, res) => {
  const y = satisYetki(req.body?.token);
  if (!y) return res.json({ ok: false, error: 'Oturum yok' });
  // BRANŞ doğrula (sadece gecerli 9 brans)
  const istenenBrans = String(req.body?.urun || '').toLowerCase().trim();
  const urun = GECERLI_BRANSLAR[istenenBrans] || (BRANS_LISTESI.includes(istenenBrans) ? istenenBrans : null);
  if (!urun) return res.json({ ok: false, error: 'Geçersiz branş' });
  const adet = parseInt(req.body?.adet, 10);
  if (isNaN(adet) || adet < 1 || adet > 9999) return res.json({ ok: false, error: 'Geçersiz adet' });
  const chatJid = (req.body?.chatJid || '').trim();
  if (!chatJid) return res.json({ ok: false, error: 'Grup seçili değil' });
  // grup adini bul (o hattin sohbetlerinden)
  const C = hatChats(y.lineId);
  const chat = C.get(chatJid);
  const chatName = chat?.name || (req.body?.chatName || '').trim() || chatJid.split('@')[0];
  // benzersiz id: panelden eklenenler icin zaman + rastgele (mesaj id yok)
  const satisId = 'satis_' + y.lineId + '_panel_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const kayit = {
    id: satisId,
    chatJid: chatJid,
    chatName: chatName,
    urun: urun,
    adet: adet,
    satici: y.displayName || y.username,
    saticiJid: '', // panelden eklendi, jid yok
    mesajId: '',
    hamMesaj: '(panelden eklendi)',
    ts: Date.now(),
  };
  try {
    const r = await db.saveSatis(kayit, y.lineId);
    if (r.ok && r.yeni) {
      console.log(`💰 SATIŞ (panelden) [${y.lineId}]: ${urun} x${adet} | ${y.displayName || y.username} | ${chatName.slice(0, 25)}`);
      // canli haber ver (kontrol sekmesi aciksa guncellensin) + yoneticiye bildir
      broadcastHat(y.lineId, { type: 'yeniSatis', satis: { ...kayit, line_id: y.lineId, onayli: false } });
      if (y.lineId !== 'ofis') {
        broadcastHat('ofis', { type: 'satisBildirim', mesaj: `Yeni satış: ${urun} x${adet} (${y.displayName || y.username})`, lineId: y.lineId });
      }
      return res.json({ ok: true, satis: kayit });
    }
    return res.json({ ok: false, error: 'Kaydedilemedi' });
  } catch (e) {
    return res.json({ ok: false, error: e.message });
  }
});

// GUNU KAPAT (SADECE yonetici). Secili hattin (veya tum hatlarin) bugununu kilitler.
app.post('/api/satislar/gunu-kapat', express.json(), async (req, res) => {
  const y = satisYetki(req.body?.token);
  if (!y || !y.isAdmin) return res.json({ ok: false, error: 'Sadece yönetici günü kapatabilir' });
  const tarih = req.body?.tarih || new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const hedefHat = req.body?.lineId; // belirli hat, yoksa TUM hatlar
  try {
    let sonuc = [];
    if (hedefHat) {
      const r = await db.gunuKapat(hedefHat, tarih, y.displayName || y.username);
      sonuc.push({ lineId: hedefHat, ...r });
    } else {
      // tum hatlar: once hatlari bul
      const hatlar = await db.loadLines();
      const tumHatIdler = ['ofis', ...(hatlar || []).map(h => h.line_id).filter(l => l && l !== 'ofis')];
      for (const lid of [...new Set(tumHatIdler)]) {
        const r = await db.gunuKapat(lid, tarih, y.displayName || y.username);
        sonuc.push({ lineId: lid, ...r });
      }
    }
    const toplam = sonuc.reduce((a, s) => a + (s.toplam || 0), 0);
    console.log(`🔒 GÜN KAPATILDI: ${tarih} | toplam ${toplam} satış | ${y.displayName || y.username}`);
    res.json({ ok: true, tarih, sonuc, toplam });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Rol degistir - yonetici yap/geri al (sadece yonetici)
app.post('/api/users/role', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok' });
  const yeniRol = req.body?.role === 'admin' ? 'admin' : 'agent';
  const r = await db.setUserRole(req.body?.id, yeniRol);
  // bu kullanicinin acik oturumlarinin rolunu de guncelle (bellek + DB)
  // (id -> username: kullanici listesinden bul)
  try {
    const users = await db.listUsers();
    const u = users.find(x => String(x.id) === String(req.body?.id));
    if (u) {
      for (const [tok, s] of sessions) { if (s.username === u.username) s.role = yeniRol; }
      db.updateSessionRole(u.username, yeniRol).catch(() => {});
    }
  } catch (e) {}
  res.json(r);
});

// ---- HIZLI YANITLAR (quick replies) — ortak sablonlar ----
// Supabase settings tablosunda 'quick_replies' anahtarinda bir dizi olarak tutulur:
//   [{ id, title, text }]
// Herkes OKUR; sadece yonetici EKLER/SILER/DUZENLER.
const QR_KEY = 'quick_replies';

// Listeyi getir (giris yapan herkes)
app.post('/api/quickreplies', express.json(), async (req, res) => {
  const s = req.body?.token && sessions.get(req.body.token);
  if (!s) return res.json({ ok: false, error: 'Giris gerekli' });
  const list = await db.getSetting(QR_KEY, []);
  res.json({ ok: true, items: Array.isArray(list) ? list : [] });
});

// Ekle (sadece yonetici)
app.post('/api/quickreplies/add', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok (sadece yonetici)' });
  const title = (req.body?.title || '').trim();
  const text = (req.body?.text || '').trim();
  if (!title || !text) return res.json({ ok: false, error: 'Baslik ve metin gerekli' });
  const list = await db.getSetting(QR_KEY, []);
  const arr = Array.isArray(list) ? list : [];
  const id = 'qr_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
  arr.push({ id, title, text });
  await db.saveSetting(QR_KEY, arr);
  res.json({ ok: true, items: arr });
});

// Sil (sadece yonetici)
app.post('/api/quickreplies/delete', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok (sadece yonetici)' });
  const id = req.body?.id;
  const list = await db.getSetting(QR_KEY, []);
  const arr = (Array.isArray(list) ? list : []).filter(x => x.id !== id);
  await db.saveSetting(QR_KEY, arr);
  res.json({ ok: true, items: arr });
});

// Guncelle (sadece yonetici)
app.post('/api/quickreplies/update', express.json(), async (req, res) => {
  if (!isAdmin(req.body?.token)) return res.json({ ok: false, error: 'Yetki yok (sadece yonetici)' });
  const { id, title, text } = req.body || {};
  if (!id || !(title || '').trim() || !(text || '').trim()) return res.json({ ok: false, error: 'Eksik bilgi' });
  const list = await db.getSetting(QR_KEY, []);
  const arr = (Array.isArray(list) ? list : []).map(x => x.id === id ? { id, title: title.trim(), text: text.trim() } : x);
  await db.saveSetting(QR_KEY, arr);
  res.json({ ok: true, items: arr });
});


// Panelden dosya yukleme (foto/pdf/belge) -> WhatsApp'a gonder
app.post('/upload', express.raw({ type: '*/*', limit: '64mb' }), async (req, res) => {
  try {
    // HAT KIMLIGI: panel token'iyla hangi hatta ait oldugunu belirle (IZOLASYON).
    // Token yoksa/cozulemezse 'ofis' varsayilir (geriye uyumlu).
    const s = req.query.token && sessions.get(req.query.token);
    const upLineId = (s && s.lineId) ? s.lineId : 'ofis';
    const upLine = lines.get(upLineId); // hem ofis hem pazarlama icin o hattin objesi
    // KRITIK: ofis dahil KENDI line.sock'u kullan (global waSock pazarlama baglaninca eziliyor)
    const upSock = upLine ? upLine.sock : (upLineId === 'ofis' ? waSock : null);
    const upConnected = upLine ? !!upLine.connected : (upLineId === 'ofis' ? waConnected : false);
    if (!upSock || !upConnected) return res.status(503).json({ error: 'WhatsApp bağlı değil' });
    const jid = req.query.jid;
    // Dosya adini GUVENLI coz: Turkce karakter/bosluk iceren adlarda decodeURIComponent
    // hata firlatabilir -> o zaman ham halini kullan (fileName ASLA bos kalmasin,
    // yoksa WhatsApp dosyayi taniyamaz ve karsi taraf ACAMAZ).
    let fileName;
    try { fileName = decodeURIComponent(req.query.name || ''); }
    catch (e) { fileName = req.query.name || ''; }
    fileName = (fileName || '').trim();
    let mime = req.query.mime || 'application/octet-stream';
    const agent = (() => { try { return decodeURIComponent(req.query.agent || 'Ben'); } catch (e) { return 'Ben'; } })();
    if (!jid || !req.body?.length) return res.status(400).json({ error: 'Eksik veri' });

    // MIME DUZELTME: tarayici bazen mime'i bos/yanlis gonderir (ozellikle PDF).
    // Dosya uzantisindan dogru mime'i belirle ki karsi tarafta dosya ACILSIN.
    let uzanti = (fileName.includes('.') ? fileName.split('.').pop() : '').toLowerCase();
    const mimeTablo = {
      pdf: 'application/pdf',
      doc: 'application/msword',
      docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      xls: 'application/vnd.ms-excel',
      xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      ppt: 'application/vnd.ms-powerpoint',
      pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
      txt: 'text/plain', csv: 'text/csv', zip: 'application/zip', rar: 'application/x-rar-compressed',
      jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp',
      mp4: 'video/mp4', mov: 'video/quicktime', avi: 'video/x-msvideo',
      mp3: 'audio/mpeg', ogg: 'audio/ogg', wav: 'audio/wav', m4a: 'audio/mp4',
    };
    // mime->uzanti ters tablo (fileName uzantisizsa mime'dan uzanti bulmak icin)
    const mimedenUzanti = {
      'application/pdf': 'pdf', 'application/msword': 'doc',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
      'application/vnd.ms-excel': 'xls',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
      'text/plain': 'txt', 'text/csv': 'csv', 'application/zip': 'zip',
      'image/jpeg': 'jpg', 'image/png': 'png', 'video/mp4': 'mp4', 'audio/mpeg': 'mp3',
    };
    // mime bos/genel ise VEYA uzanti biliniyorsa, uzantidan gelen dogru mime'i kullan
    if (mimeTablo[uzanti] && (mime === 'application/octet-stream' || !mime || mime === 'application/pdf' || !mime.includes('/'))) {
      mime = mimeTablo[uzanti];
    }
    // FILENAME KESINLESTIRME: bos veya uzantisizsa duzgun bir ad ver.
    // (WhatsApp belge adi olmadan dosyayi taniyamaz, karsi taraf ACAMAZ.)
    if (!uzanti && mimedenUzanti[mime]) uzanti = mimedenUzanti[mime]; // mime'dan uzanti bul
    if (!fileName) {
      // ad hic yok: mime'dan uzantili varsayilan ad
      fileName = 'belge' + (uzanti ? '.' + uzanti : '');
    } else if (!fileName.includes('.') && uzanti) {
      // ad var ama uzanti yok: uzanti ekle
      fileName = fileName + '.' + uzanti;
    }

    // dosyayi diske kaydet (panelde gostermek icin)
    const ext = fileName.includes('.') ? fileName.split('.').pop() : 'bin';
    const savedName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}.${ext}`;
    // req.body Buffer olmali (express.raw); degilse Buffer'a cevir (bozuk dosya gitmesin)
    const dosyaBuf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
    const boyutMB = (dosyaBuf.length / 1048576).toFixed(2);
    console.log(`📎 Dosya yukleniyor: ${fileName} (${boyutMB} MB, ${mime})`);
    fs.writeFileSync(path.join(MEDIA_DIR, savedName), dosyaBuf);
    const webPath = '/media/' + savedName;

    // tipe gore WhatsApp'a gonder
    let kind = 'document';
    let waMsg;
    if (mime.startsWith('image/')) {
      kind = 'image';
      waMsg = { image: dosyaBuf, caption: req.query.caption ? decodeURIComponent(req.query.caption) : undefined };
    } else if (mime.startsWith('video/')) {
      kind = 'video';
      waMsg = { video: dosyaBuf, caption: req.query.caption ? decodeURIComponent(req.query.caption) : undefined };
    } else if (mime.startsWith('audio/')) {
      kind = 'audio';
      waMsg = { audio: dosyaBuf, mimetype: mime };
    } else {
      kind = 'document';
      // Belge: fileName + dogru mimetype SART (yoksa karsi tarafta acilmaz).
      waMsg = { document: dosyaBuf, fileName, mimetype: mime, caption: req.query.caption ? decodeURIComponent(req.query.caption) : undefined };
    }
    // BUYUK dosyalar icin yeterli sure tani (WhatsApp'a yukleme zaman alir).
    // Eskiden timeout yoktu -> buyuk dosya askida kalip BOZUK gidebiliyordu.
    // 90sn icinde yuklenmezse hata don (kullanici tekrar denesin).
    let sent;
    try {
      const gonderP = upSock.sendMessage(jid, waMsg);
      const timeoutP = new Promise((_, rej) => setTimeout(() => rej(new Error('dosya yukleme zaman asimi (cok buyuk olabilir)')), 90000));
      sent = await Promise.race([gonderP, timeoutP]);
    } catch (gonderHata) {
      console.error(`⚠️  Dosya gonderilemedi (${fileName}, ${boyutMB} MB):`, gonderHata.message);
      return res.status(502).json({ error: `Dosya gönderilemedi (${boyutMB} MB). Çok büyük olabilir veya bağlantı sorunu. Tekrar deneyin.` });
    }
    if (!sent || !sent.key) {
      // gonderim onaylanmadi -> panele hata don (kullanici gittigini sanmasin)
      return res.status(502).json({ error: 'WhatsApp dosyayı kabul etmedi, tekrar deneyin.' });
    }
    console.log(`✅ Dosya gonderildi: ${fileName} (${boyutMB} MB)`);

    addMessage(jid, {
      id: sent.key.id, key: sent.key,
      raw: sent, // kendi gonderdigimiz medyayi sonradan yanitlayabilmek icin
      fromMe: true, kind,
      text: kind === 'document' ? fileName : (req.query.caption ? decodeURIComponent(req.query.caption) : ''),
      caption: req.query.caption ? decodeURIComponent(req.query.caption) : '',
      fileName: kind === 'document' ? fileName : undefined,
      mime: mime,
      mediaUrl: webPath, sender: agent, time: nowTime(),
      durum: 2, // gonderildi (tek tik)
    }, {}, upLineId);
    res.json({ ok: true });
  } catch (e) {
    console.error('Yukleme hatasi:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GRUP FOTOGRAFINI DEGISTIR (sadece grup yoneticisi yapabilir).
// Panelden secilen fotograf -> WhatsApp grubuna profil resmi olarak yuklenir.
// Hat-izole: hangi panel istediyse (token) o hattin soketiyle yuklenir.
app.post('/upload-group-photo', express.raw({ type: '*/*', limit: '16mb' }), async (req, res) => {
  try {
    // HAT KIMLIGI (izolasyon): token'dan hangi hat oldugunu bul
    const s = req.query.token && sessions.get(req.query.token);
    const gpLineId = (s && s.lineId) ? s.lineId : 'ofis';
    const gpLine = lines.get(gpLineId);
    const gpSock = gpLine ? gpLine.sock : (gpLineId === 'ofis' ? waSock : null);
    const gpConnected = gpLine ? !!gpLine.connected : (gpLineId === 'ofis' ? waConnected : false);
    if (!gpSock || !gpConnected) return res.status(503).json({ error: 'WhatsApp bağlı değil' });

    const jid = req.query.jid;
    if (!jid || !jid.endsWith('@g.us')) return res.status(400).json({ error: 'Geçerli bir grup seçilmedi.' });
    if (!req.body?.length) return res.status(400).json({ error: 'Fotoğraf alınamadı.' });

    const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
    // boyut kontrolu (cok buyuk resim WhatsApp'ta sorun olabilir)
    const boyutMB = buf.length / 1048576;
    if (boyutMB > 12) return res.status(400).json({ error: 'Fotoğraf çok büyük (en fazla 12 MB).' });

    // WhatsApp'a grup profil resmini yukle (zaman asimiyla — asili kalmasin)
    const gpTimeout = new Promise((_, rej) => setTimeout(() => rej(new Error('foto yukleme zaman asimi')), 45000));
    try {
      await Promise.race([gpSock.updateProfilePicture(jid, buf), gpTimeout]);
    } catch (e) {
      console.error('⚠️  Grup fotosu degistirilemedi:', e.message);
      return res.status(502).json({ error: 'Fotoğraf değiştirilemedi. Yönetici olman gerekebilir veya bağlantı sorunu olabilir. Tekrar deneyin.' });
    }
    console.log(`🖼️  Grup fotosu degistirildi: ${jid.split('@')[0]} (hat: ${gpLineId})`);

    // Yeni fotoyu taze cek + panele yansit (avatar onbellegini atlayarak)
    // Kisa bir gecikme: WhatsApp'in yeni resmi islemesi icin
    setTimeout(async () => {
      try {
        const yeniUrl = await getAvatar(jid, true); // taze=true: onbellegi atla, yeniden cek
        const C2 = hatChats(gpLineId);
        const chat = C2.get(jid);
        if (chat) {
          chat.avatar = yeniUrl || chat.avatar;
          broadcastHat(gpLineId, { type: 'message', jid, chat: stripRaw(chat) });
          if (db.isReady()) db.saveChat(chat, gpLineId).catch(() => {});
        }
      } catch (e) {}
    }, 2000);

    res.json({ ok: true });
  } catch (e) {
    console.error('Grup foto yukleme hatasi:', e.message);
    res.status(500).json({ error: e.message });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function broadcast(obj) {
  const data = JSON.stringify(obj);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(data); });
}

// SADECE belirli bir hatta bagli panellere gonder (IZOLASYON).
// ofis hatti -> ofis kullanicilarinin panellerine. pazarlama -> sadece o pazarlamaciya.
// Bir ws'in hatti ws._lineId'de tutulur (baglanirken token'dan belirlenir).
function broadcastHat(lineId, obj) {
  const hedef = lineId || 'ofis';
  const data = JSON.stringify(obj);
  wss.clients.forEach((c) => {
    if (c.readyState !== 1) return;
    // ws'in hatti belirlenmemisse (eski/kimliksiz baglanti) ofis say (geriye uyumlu).
    const wsLine = c._lineId || 'ofis';
    if (wsLine === hedef) c.send(data);
  });
}

wss.on('connection', (ws) => {
  ws._lineId = 'ofis'; // varsayilan; panel 'merhaba' mesajiyla kendi hattini bildirecek
  // Ilk status'u GONDERME — panel 'merhaba' der demez, KENDI hattinin dogru
  // durumunu (bagli/QR) gonderecegiz. Boylece pazarlamaci ofisin durumunu gormez.
  // (Asagidaki 'merhaba' handler'i dogru status + chats gonderir.)
  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);

      // PANEL KIMLIGI: panel baglaninca token'iyla "merhaba" der, biz hattini buluruz.
      // Boylece bu ws'e SADECE kendi hattinin mesajlari gider (izolasyon).
      if (msg.type === 'merhaba') {
        let s = msg.token && sessions.get(msg.token);
        // RESTART SONRASI: bellek bos olabilir. Token varsa DB'den oturumu geri yukle,
        // hat bilgisi eksikse kullanicinin hattini cek. Yoksa pazarlamaci RESTART'ta
        // 'ofis'e dusup ofisin numarasina/sohbetlerine baglaniyordu (izolasyon kirilirdi!).
        if (!s && msg.token && db.isReady()) {
          try {
            const rows = await db.loadSessions();
            for (const r of rows) {
              if (!sessions.has(r.token)) sessions.set(r.token, { username: r.username, displayName: r.display_name, role: r.role, ts: Date.now() });
            }
            s = sessions.get(msg.token);
          } catch (e) {}
        }
        // session var ama hat bilgisi yok (DB'den geldiyse lineId tasinmaz) -> kullanicinin hattini cek
        if (s && !s.lineId && db.isReady()) {
          try {
            const hb = await db.getUserLine(s.username);
            s.lineId = hb.line_id || 'ofis';
            s.lineTip = hb.tip || 'ofis';
          } catch (e) {}
        }
        const lineId = (s && s.lineId) ? s.lineId : 'ofis';
        ws._lineId = lineId;
        ws._username = s ? s.username : null;
        ws._role = s ? s.role : null; // wipeAll gibi yonetici-only islemler icin
        console.log(`   🔗 merhaba: token ${msg.token ? 'var' : 'YOK'} | kullanici=${ws._username||'-'} | hat=${lineId}`);
        // PAZARLAMACI ise ve hatti henuz baglanmadiysa baslat (restart sonrasi QR/baglanti gelsin)
        if (lineId !== 'ofis') {
          const mevcut = lines.get(lineId);
          if (!mevcut || (!mevcut.connected && !mevcut.starting)) {
            startWA(lineId).catch(() => {});
          }
        }
        // Bu hattin GERCEK durumunu gonder (bagli mi, QR'i var mi) — panel dogru ekrani gostersin.
        const line = lines.get(lineId);
        const bagli = line ? line.connected : (lineId === 'ofis' ? waConnected : false);
        const qrImg = line ? line.lastQR : (lineId === 'ofis' ? lastQR : null);
        const myJid = line && line.myNumber ? line.myNumber + '@s.whatsapp.net' : null;
        const myName = line ? line.myName : '';
        ws.send(JSON.stringify({ type: 'status', connected: bagli, myJid, myName, qr: !bagli && !!qrImg, qrImage: (!bagli ? qrImg : null) }));
        // bu hattin GUNCEL sohbetlerini gonder (ofis ise global, pazarlama ise kendi hatti)
        const C = hatChats(lineId);
        ws.send(JSON.stringify({ type: 'chats', chats: Array.from(C.values()).map(stripRaw) }));
        return;
      }

      // QR DURUMU ISTEGI: panel acilista (ve QR gelene kadar) bunu cagirir.
      // Boylece QR sunucuda hazirsa ANINDA panele gider (broadcast'i kacirmis olsa bile).
      if (msg.type === 'getQR') {
        // Bu panelin KENDI hattinin durumunu/QR'ini dondur (ofisinkini degil).
        const wsLine = ws._lineId || 'ofis';
        const line = lines.get(wsLine);
        const bagli = line ? line.connected : (wsLine === 'ofis' ? waConnected : false);
        const qrImg = line ? line.lastQR : (wsLine === 'ofis' ? lastQR : null);
        if (!bagli && qrImg) {
          ws.send(JSON.stringify({ type: 'status', connected: false, qr: true, qrImage: qrImg }));
        } else if (bagli) {
          ws.send(JSON.stringify({ type: 'status', connected: true }));
        }
        return;
      }

      // 0) WS KIMLIK: panel giris yapinca "ben buyum" der; ws'i o kullaniciya bagla.
      //    Ic mesajlari dogru kisiye canli iletmek icin gerekli.
      if (msg.type === 'auth') {
        const s = msg.token && sessions.get(msg.token);
        if (s) {
          ws._username = s.username;
          ws._displayName = s.displayName;
          // baglanir baglanmaz toplam okunmamis ic mesaj sayisini gonder (sekme rozeti)
          if (db.isReady()) {
            const n = await db.internalUnreadCount(s.username);
            ws.send(JSON.stringify({ type: 'internalUnread', count: n }));
          }
          // COKLU OTURUM TESPITI: ayni kullanici BASKA yer(ler)de de acik mi?
          // WhatsApp tek hat oldugundan, ayni hesabin cok yerde acik olmasi sifreleme
          // oturumunu bozabilir. Kullaniciyi uyaralim.
          let ayniKullaniciSayisi = 0;
          wss.clients.forEach((c) => {
            if (c.readyState === 1 && c._username === s.username) ayniKullaniciSayisi++;
          });
          if (ayniKullaniciSayisi > 1) {
            // bu kullanicinin TUM acik panellerine bildir
            wss.clients.forEach((c) => {
              if (c.readyState === 1 && c._username === s.username) {
                c.send(JSON.stringify({ type: 'coklisession', adet: ayniKullaniciSayisi }));
              }
            });
          }
        }
        return;
      }

      // ---- IC MESAJLAR (ekip uyeleri arasi, WhatsApp'tan bagimsiz) ----
      // Konusma listesi: kiminle yazismis, son mesaj, okunmamis
      if (msg.type === 'internalList') {
        if (!ws._username || !db.isReady()) { ws.send(JSON.stringify({ type: 'internalListResult', items: [] })); return; }
        const rows = await db.listInternalConversations(ws._username);
        // kullanici listesini de ekle (yeni konusma baslatmak icin tum ekip)
        const users = await db.listUsers();
        ws.send(JSON.stringify({
          type: 'internalListResult',
          items: rows,
          users: users.map(u => ({ username: u.username, displayName: u.display_name, role: u.role })),
          me: ws._username,
        }));
        return;
      }

      // Bir konusmayi ac (iki kullanici arasi gecmis)
      if (msg.type === 'internalLoad') {
        if (!ws._username || !db.isReady()) { ws.send(JSON.stringify({ type: 'internalConversation', other: msg.other, messages: [] })); return; }
        const rows = await db.loadInternalConversation(ws._username, msg.other, 300);
        // acilinca okundu isaretle
        await db.markInternalRead(ws._username, msg.other);
        const n = await db.internalUnreadCount(ws._username);
        ws.send(JSON.stringify({ type: 'internalConversation', other: msg.other, messages: rows }));
        ws.send(JSON.stringify({ type: 'internalUnread', count: n }));
        return;
      }

      // Ic mesaj gonder
      if (msg.type === 'internalSend') {
        if (!ws._username || !db.isReady()) { ws.send(JSON.stringify({ type: 'opError', error: 'İç mesaj gönderilemedi.' })); return; }
        const to = (msg.to || '').trim();
        const text = (msg.text || '').trim();
        if (!to || !text) return;
        const mid = 'im_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
        const r = await db.saveInternalMessage({ id: mid, from: ws._username, to, text, ts: Date.now() });
        if (!r.ok) { ws.send(JSON.stringify({ type: 'opError', error: 'İç mesaj kaydedilemedi.' })); return; }
        const payload = { id: mid, from: ws._username, fromName: ws._displayName || ws._username, to, text, ts: r.row?.ts || Date.now() };
        // Gonderene geri yolla (kendi ekraninda gorsun)
        ws.send(JSON.stringify({ type: 'internalMessage', msg: payload }));
        // ALICIYA canli ilet: o kullanicinin acik WS'lerini bul
        let aliciCevrimici = false;
        wss.clients.forEach((c) => {
          if (c.readyState === 1 && c._username === to) {
            c.send(JSON.stringify({ type: 'internalMessage', msg: payload }));
            aliciCevrimici = true;
          }
        });
        // alicinin yeni okunmamis sayisini guncelle (acik tum sekmelerine)
        if (aliciCevrimici && db.isReady()) {
          const n = await db.internalUnreadCount(to);
          wss.clients.forEach((c) => { if (c.readyState === 1 && c._username === to) c.send(JSON.stringify({ type: 'internalUnread', count: n })); });
        }
        return;
      }

      // Bir konusmayi okundu isaretle
      if (msg.type === 'internalRead') {
        if (!ws._username || !db.isReady()) return;
        await db.markInternalRead(ws._username, msg.other);
        const n = await db.internalUnreadCount(ws._username);
        ws.send(JSON.stringify({ type: 'internalUnread', count: n }));
        return;
      }

      // ============================================================
      // HAT-DUYARLI KATMAN (IZOLASYON): Bu noktadan sonraki TUM WhatsApp
      // islemleri (gonder/sil/yanit/forward/reaksiyon vb.) bu ws'in KENDI
      // hattini kullanir. Ofis -> global waSock/waConnected/chats.
      // Pazarlama -> kendi hattinin sock/connected/chats.
      //   _LID      : bu panelin hat kimligi ('ofis' veya 'pzr_xxx')
      //   C         : bu hattin sohbet Map'i (hatChats)
      //   SOCK      : bu hattin Baileys soketi (gonderim icin)
      //   CONNECTED : bu hat WhatsApp'a bagli mi
      // ASLA global 'waSock'/'chats'/'broadcast' kullanma; izolasyon kirilir.
      // ============================================================
      const _LID = ws._lineId || 'ofis';
      const _line = lines.get(_LID); // hem ofis hem pazarlama icin o hattin objesi
      const C = hatChats(_LID);
      // KRITIK: ofis dahil HER hat KENDI line.sock'unu kullanir. Global 'waSock' KULLANMA —
      // cunku pazarlama hatti baglaninca 'waSock = sock' ile global eziliyordu ve ofisin
      // mesaji yanlis (son baglanan) hattin soketinden gidiyordu. line.sock her zaman dogru hat.
      const SOCK = _line ? _line.sock : (_LID === 'ofis' ? waSock : null);
      const CONNECTED = _line ? !!_line.connected : (_LID === 'ofis' ? waConnected : false);

      // 1) Metin / yanit gonderme
      if (msg.type === 'send' && SOCK && CONNECTED) {
        let replyTo = null;
        let quotedOpt = undefined;
        if (msg.replyId) {
          const chat = C.get(msg.jid);
          const orig = chat?.messages.find(x => x.id === msg.replyId);
          if (orig) {
            replyTo = { sender: orig.fromMe ? 'Siz' : orig.sender, text: replyPreview(orig) };
            if (orig.raw && orig.raw.key) {
              // EN IYI: tam ham mesaj varsa onu kullan
              quotedOpt = { quoted: orig.raw };
              console.log(`   ↩️  yanit alintisi hazir (tam raw)`);
            } else if (orig.key) {
              // RAW YOK ama KEY var (DB'den yuklenen mesaj): key + icerikten quoted insa et.
              // Baileys quoted icin { key, message } bekler. Metni conversation olarak veriyoruz.
              const quotedMsg = insaQuotedMesaj(orig);
              if (quotedMsg) {
                quotedOpt = { quoted: quotedMsg };
                console.log(`   ↩️  yanit alintisi key'den insa edildi`);
              } else {
                console.log(`   ⚠️  yanit: key var ama quoted insa edilemedi -> alintisiz`);
              }
            } else {
              console.log(`   ⚠️  yanit: orig bulundu ama raw VE key yok -> alintisiz`);
            }
          } else {
            console.log(`   ⚠️  yanit: orijinal mesaj bellekte bulunamadi (id: ${msg.replyId}) -> alintisiz`);
          }
        }
        const content = { text: msg.text };
        const mentionJids = (msg.text.match(/@(\d{10,15})/g) || []).map(t => t.slice(1) + '@s.whatsapp.net');
        if (mentionJids.length) content.mentions = mentionJids;
        // GONDERIMI try-catch ile SAR: basarisiz olursa panele "gonderilemedi" bildir.
        try {
          let sent;
          const timeoutP = () => new Promise((_, rej) => setTimeout(() => rej(new Error('gonderim zaman asimi')), 30000));
          // ALINTILI (yanit) gondermeyi dene; alinti BOZUKSA (eski/eksik raw) hata verir.
          // O durumda mesaji ALINTISIZ gonder ki YANIT METNI yine de gitsin (kaybolmasin).
          if (quotedOpt) {
            try {
              sent = await Promise.race([SOCK.sendMessage(msg.jid, content, quotedOpt), timeoutP()]);
            } catch (alintiHatasi) {
              console.error('   ↳ alintili gonderim basarisiz, alintisiz deneniyor:', alintiHatasi.message);
              sent = await Promise.race([SOCK.sendMessage(msg.jid, content), timeoutP()]);
              replyTo = null; // alinti gitmedi, panelde de alinti gosterme
            }
          } else {
            sent = await Promise.race([SOCK.sendMessage(msg.jid, content), timeoutP()]);
          }
          if (!sent || !sent.key) throw new Error('WhatsApp gonderimi onaylamadi');
          addMessage(msg.jid, {
            id: sent.key.id, key: sent.key,
            raw: sent, // GONDERIM SONUCU: kendi mesajimizi sonradan YANITLAYINCA alinti icin gerekli
            fromMe: true, kind: 'text', text: msg.text,
            sender: msg.agent || 'Ben', time: nowTime(), replyTo,
            durum: 2, // gonderildi (tek tik) — WhatsApp onayladi
            // EKIP ETIKETLERI: panelden gelen, etiketlenen ekip uyesi kullanici adlari.
            // Bu kullanicilar panele girince "Bahsedilmeler"de gorur.
            teamMentions: Array.isArray(msg.teamMentions) ? msg.teamMentions : undefined,
          }, _LID);
        } catch (e) {
          console.error('⚠️  MESAJ GONDERILEMEDI:', e.message, '| grup:', (msg.jid || '').split('@')[0]);
          // MESAJI yine de ekrana ekle ama HATA durumuyla (durum:-1) -> WhatsApp gibi
          // kirmizi unlem cikar, kullanici gormedigini sanmaz, silip yeniden gonderir.
          const hataId = 'fail_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
          addMessage(msg.jid, {
            id: hataId, fromMe: true, kind: 'text', text: msg.text,
            sender: msg.agent || 'Ben', time: nowTime(), replyTo,
            durum: -1, // GONDERILEMEDI (kirmizi unlem)
            gonderilemedi: true,
          }, _LID);
          // panele ayrica bildir (toast + metni geri koymak istersen)
          ws.send(JSON.stringify({ type: 'sendError', jid: msg.jid, text: msg.text, error: 'Mesaj gönderilemedi! Kırmızı ünlemli mesajı silip tekrar deneyin.' }));
        }
      }
      else if (msg.type === 'send') {
        // WhatsApp BAGLI DEGILKEN gonderme denemesi -> mesaji hata durumuyla goster + bildir
        const hataId = 'fail_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
        addMessage(msg.jid, {
          id: hataId, fromMe: true, kind: 'text', text: msg.text,
          sender: msg.agent || 'Ben', time: nowTime(),
          durum: -1, gonderilemedi: true,
        }, _LID);
        ws.send(JSON.stringify({ type: 'sendError', jid: msg.jid, text: msg.text, error: 'WhatsApp bağlı değil — mesaj gönderilemedi.' }));
      }

      // 1b) Gonderilen mesaji DUZENLE (yaklasik 15 dk icinde)
      else if (msg.type === 'edit' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        const orig = chat?.messages.find(x => x.id === msg.id);
        if (orig?.key) {
          try {
            const editTimeout = new Promise((_, rej) => setTimeout(() => rej(new Error('duzenleme zaman asimi')), 15000));
            await Promise.race([SOCK.sendMessage(msg.jid, { text: msg.text, edit: orig.key }), editTimeout]);
            // WhatsApp kabul etti -> bellekte ve DB'de guncelle (yenileyince kaybolmasin)
            orig.text = msg.text;
            orig.edited = true;
            broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
            if (db.isReady()) db.saveMessage(msg.jid, orig, _LID).catch(() => {});
            ws.send(JSON.stringify({ type: 'opOk', message: 'Mesaj düzenlendi.' }));
          } catch (e) {
            // DURUST: duzenleme gitmediyse panelde de degistirme (yoksa kullanici degisti sanir)
            console.error('⚠️  DUZENLEME BASARISIZ:', e.message);
            ws.send(JSON.stringify({ type: 'opError', error: 'Düzenlenemedi (WhatsApp 15 dakikadan eski mesajların düzenlenmesine izin vermez veya bağlantı sorunu).' }));
          }
        } else {
          ws.send(JSON.stringify({ type: 'opError', error: 'Düzenlenecek mesaj bulunamadı.' }));
        }
      }

      // 1c) Gonderilen mesaji SIL (herkes icin, ~48 saat icinde)
      else if (msg.type === 'delete' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        const orig = chat?.messages.find(x => x.id === msg.id);
        if (orig?.key) {
          // Normal durum: WhatsApp'tan sil (key var)
          try {
            // SILME KEY'INI TEMIZLE: DB'den yuklenen/eksik key'lerde silme basarisiz olabiliyordu.
            // WhatsApp'in bekledigi temiz formata getir (reaksiyon mantigiyla ayni).
            let silKey = orig.key;
            if (silKey && (!silKey.remoteJid || silKey.id !== msg.id)) {
              silKey = {
                remoteJid: msg.jid,
                id: silKey.id || msg.id,
                fromMe: silKey.fromMe !== undefined ? !!silKey.fromMe : true,
                ...(silKey.participant ? { participant: silKey.participant } : {})
              };
            }
            // ZAMAN ASIMI ekle: silme isteği asili kalmasin (15sn)
            const silTimeout = new Promise((_, rej) => setTimeout(() => rej(new Error('silme zaman asimi')), 15000));
            await Promise.race([SOCK.sendMessage(msg.jid, { delete: silKey }), silTimeout]);
            orig.deleted = true;
            orig.text = '';
            orig.kind = 'text';
            orig.mediaUrl = null;
            orig.silenKisi = msg.agent || ''; // KIM sildi (panelde "X sildi" gostermek icin)
            broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
            if (db.isReady()) db.saveMessage(msg.jid, orig, _LID).catch(() => {});
            ws.send(JSON.stringify({ type: 'opOk', message: 'Mesaj silindi.' }));
          } catch (e) {
            // SILME BASARISIZ: panele DURUST bildir — "silindi" gibi gosterme (yoksa kullanici
            // sildim sanir ama WhatsApp'ta durur). Mesaj oldugu gibi kalsin.
            console.error('⚠️  SILME BASARISIZ:', e.message, '| id:', (msg.id||'').slice(0,12));
            ws.send(JSON.stringify({ type: 'opError', error: 'Mesaj silinemedi! (WhatsApp 2 dakikadan eski mesajları herkesten silmeye izin vermeyebilir veya bağlantı sorunu olabilir.) Tekrar deneyin.' }));
          }
        } else if (orig && chat) {
          // key YOK = mesaj WhatsApp'a hic gitmemis (hayalet/gonderilememis mesaj).
          // Onu bellekten TAMAMEN kaldir ki kullanici kurtulsun (WhatsApp'ta zaten yok).
          chat.messages = chat.messages.filter(x => x.id !== msg.id);
          // DB'den de sil (yoksa yenileyince geri gelir)
          if (db.isReady() && db.deleteMessage) db.deleteMessage(msg.jid, msg.id, _LID).catch(() => {});
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
          ws.send(JSON.stringify({ type: 'opOk', message: 'Gönderilememiş mesaj kaldırıldı.' }));
        } else {
          // mesaj bulunamadi
          ws.send(JSON.stringify({ type: 'opError', error: 'Silinecek mesaj bulunamadı (eski mesaj olabilir).' }));
        }
      }

      // 2) Yeni sohbet baslatma: numara dogrula + (varsa) ilk mesaji gonder
      else if (msg.type === 'newChat' && SOCK && CONNECTED) {
        // numarayi temizle ve Turkiye formatina normallestir
        let num = (msg.number || '').replace(/\D/g, '');
        // farkli girisleri duzelt:
        if (num.startsWith('0090')) num = num.slice(2);        // 0090... -> 90...
        else if (num.startsWith('0')) num = '90' + num.slice(1); // 05XX -> 905XX
        else if (num.length === 10 && num.startsWith('5')) num = '90' + num; // 5XX... -> 905XX (90 yazmadan)
        // zaten 90 ile basliyorsa dokunma
        if (num.length < 10) {
          ws.send(JSON.stringify({ type: 'newChatResult', ok: false, error: 'Numarayı kontrol et.' }));
          return;
        }
        const jid = num + '@s.whatsapp.net';
        // numara WhatsApp'ta var mi?
        try {
          const [res] = await SOCK.onWhatsApp(jid);
          if (!res?.exists) {
            ws.send(JSON.stringify({ type: 'newChatResult', ok: false, error: 'Bu numara WhatsApp kullanmıyor.' }));
            return;
          }
        } catch (e) {
          ws.send(JSON.stringify({ type: 'newChatResult', ok: false, error: 'Numara doğrulanamadı.' }));
          return;
        }
        // ilk mesaj varsa gonder
        if (msg.text) {
          try {
            const ncTimeout = new Promise((_, rej) => setTimeout(() => rej(new Error('zaman asimi')), 30000));
            const ncSent = await Promise.race([SOCK.sendMessage(jid, { text: msg.text }), ncTimeout]);
            if (!ncSent || !ncSent.key) throw new Error('gonderim onaylanmadi');
            addMessage(jid, {
              id: ncSent.key.id, key: ncSent.key, raw: ncSent,
              fromMe: true, kind: 'text', text: msg.text,
              sender: msg.agent || 'Ben', time: nowTime(), durum: 2,
            }, { name: msg.name || num }, _LID);
          } catch (e) {
            console.error('⚠️  Yeni sohbet ilk mesaj gonderilemedi:', e.message);
            ws.send(JSON.stringify({ type: 'newChatResult', ok: false, error: 'Sohbet açıldı ama ilk mesaj gönderilemedi. Sohbetten tekrar deneyin.' }));
            return;
          }
        } else {
          // bos sohbet olustur
          if (!C.has(jid)) {
            C.set(jid, {
              jid, name: msg.name || num, isGroup: false, description: '',
              messages: [], unread: 0, lastTime: nowTime(), lastTs: Date.now(),
            });
          }
          broadcastHat(_LID, { type: 'message', jid, chat: stripRaw(C.get(jid)) });
        }
        ws.send(JSON.stringify({ type: 'newChatResult', ok: true, jid }));
      }

      // 4) OZELDEN YANITLA: gruptaki bir mesaji, atan kisinin DM'ine alintilayarak yanitla
      else if (msg.type === 'replyPrivate' && SOCK && CONNECTED) {
        // msg.groupJid: grup, msg.msgId: gruptaki orijinal mesaj, msg.text: yanit
        const groupChat = C.get(msg.groupJid);
        const orig = groupChat?.messages.find(x => x.id === msg.msgId);
        const targetJid = orig?.senderJid;
        if (!targetJid) {
          ws.send(JSON.stringify({ type: 'opError', error: 'Kişi numarası bulunamadı.' }));
          return;
        }
        try {
          // alintiyla birlikte DM'e gonder
          if (orig.raw) {
            await SOCK.sendMessage(targetJid, { text: msg.text }, { quoted: orig.raw });
          } else {
            await SOCK.sendMessage(targetJid, { text: msg.text });
          }
          // DM sohbetine ekle (alinti onizlemesiyle)
          addMessage(targetJid, {
            fromMe: true, kind: 'text', text: msg.text,
            sender: msg.agent || 'Ben', time: nowTime(),
            replyTo: { sender: orig.sender, text: replyPreview(orig) },
          }, { name: orig.senderPush || targetJid.split('@')[0] }, _LID);
          ws.send(JSON.stringify({ type: 'openChat', jid: targetJid }));
        } catch (e) {
          ws.send(JSON.stringify({ type: 'opError', error: 'Özelden yanıt gönderilemedi.' }));
        }
      }

      // 5) Tek bir sohbeti okundu yap (+ bahsedilme isaretini KALICI temizle)
      // Not: WhatsApp baglantisindan BAGIMSIZ calisir — isaret kaldirma her zaman olmali.
      else if (msg.type === 'markRead') {
        const chat = C.get(msg.jid);
        if (chat) {
          chat.unread = 0;
          chat.hasMention = false; // ÖNEMLI: bahsedilme isareti de kalksin, yoksa geri gelir
          // WhatsApp'a okundu bilgisi gonder (baglantI varsa; yoksa sorun degil, isaret zaten kalkti)
          if (SOCK && CONNECTED) {
            try {
              const keys = chat.messages.filter(m => !m.fromMe && m.key).slice(-20).map(m => m.key);
              if (keys.length) await SOCK.readMessages(keys);
            } catch (e) {}
          }
          // DB'ye de yaz ki sunucu restart olsa bile isaret geri gelmesin
          if (db.isReady()) db.saveChat(chat, _LID).catch(() => {});
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
        }
      }

      // 6) TUMUNU okundu yap (+ tum bahsedilme isaretlerini temizle)
      else if (msg.type === 'markAllRead' && SOCK && CONNECTED) {
        for (const chat of C.values()) {
          if (chat.unread > 0 || chat.hasMention) {
            chat.unread = 0;
            chat.hasMention = false;
            try {
              const keys = chat.messages.filter(m => !m.fromMe && m.key).slice(-20).map(m => m.key);
              if (keys.length) await SOCK.readMessages(keys);
            } catch (e) {}
            if (db.isReady()) db.saveChat(chat, _LID).catch(() => {});
            broadcastHat(_LID, { type: 'message', jid: chat.jid, chat: stripRaw(chat) });
          }
        }
      }

      // 7) Mesaji ILET (forward) - baska sohbet(ler)e
      else if (msg.type === 'forward' && SOCK && CONNECTED) {
        // msg.fromJid: kaynak sohbet, msg.msgId: iletilecek mesaj, msg.targets: hedef jid listesi
        const srcChat = C.get(msg.fromJid);
        const orig = srcChat?.messages.find(x => x.id === msg.msgId);
        if (!orig) {
          ws.send(JSON.stringify({ type: 'opError', error: 'İletilecek mesaj bulunamadı.' }));
          return;
        }
        const targets = Array.isArray(msg.targets) ? msg.targets : [];
        let okCount = 0;
        let basarisizlar = []; // iletilémeyen hedefler (kullaniciya bildirilecek)
        // mime->uzanti (fileName uzantisizsa tamamlamak icin)
        const _mimedenUzanti = {
          'application/pdf': 'pdf', 'application/msword': 'doc',
          'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
          'application/vnd.ms-excel': 'xls',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
          'text/plain': 'txt', 'text/csv': 'csv', 'image/jpeg': 'jpg', 'image/png': 'png',
          'video/mp4': 'mp4', 'audio/mpeg': 'mp3',
        };
        // iletim icin zaman asimi (medya buyukse asili kalmasin) — send ile ayni mantik
        const _iletTimeout = () => new Promise((_, rej) => setTimeout(() => rej(new Error('iletme zaman asimi')), 60000));
        for (const tjid of targets) {
          try {
            let sent;
            // MEDYA/BELGE ise: raw ile forward GUVENILMEZ (ozellikle PDF -> adsiz/bozuk gider).
            // Onun yerine diskten okuyup YENIDEN gonder (dogru fileName + mime ile).
            const medyaMi = ['image', 'video', 'audio', 'document', 'sticker'].includes(orig.kind) && orig.mediaUrl;
            if (medyaMi) {
              const fp = path.join(__dirname, 'public', orig.mediaUrl.replace(/^\/media\//, 'media/'));
              if (fs.existsSync(fp)) {
                const buf = fs.readFileSync(fp);
                const cap = orig.caption || (orig.kind !== 'document' ? orig.text : '') || '';
                if (orig.kind === 'image') sent = await Promise.race([SOCK.sendMessage(tjid, { image: buf, caption: cap || undefined }), _iletTimeout()]);
                else if (orig.kind === 'video') sent = await Promise.race([SOCK.sendMessage(tjid, { video: buf, caption: cap || undefined }), _iletTimeout()]);
                else if (orig.kind === 'audio') sent = await Promise.race([SOCK.sendMessage(tjid, { audio: buf, mimetype: orig.mime || 'audio/mp4' }), _iletTimeout()]);
                else if (orig.kind === 'sticker') sent = await Promise.race([SOCK.sendMessage(tjid, { sticker: buf }), _iletTimeout()]);
                else {
                  // BELGE: fileName + mime SART. Eksikse dosya yolundan/mime'dan tamamla.
                  let fn = orig.fileName || orig.text || '';
                  const mm = orig.mime || 'application/octet-stream';
                  if (!fn || !fn.includes('.')) {
                    const uz = _mimedenUzanti[mm] || (orig.mediaUrl.split('.').pop()) || 'pdf';
                    fn = (fn || 'belge') + '.' + uz;
                  }
                  sent = await Promise.race([SOCK.sendMessage(tjid, { document: buf, fileName: fn, mimetype: mm }), _iletTimeout()]);
                }
              } else if (orig.raw) {
                // dosya diskte yok ama raw varsa son care: raw ile ilet
                sent = await Promise.race([SOCK.sendMessage(tjid, { forward: orig.raw }), _iletTimeout()]);
              } else {
                sent = await Promise.race([SOCK.sendMessage(tjid, { text: orig.text || '(iletilen mesaj)' }), _iletTimeout()]);
              }
            } else if (orig.raw) {
              // METIN mesaji: raw ile forward (etiket/bicim korunur)
              sent = await Promise.race([SOCK.sendMessage(tjid, { forward: orig.raw }), _iletTimeout()]);
            } else {
              // sadece metin, raw yok
              sent = await Promise.race([SOCK.sendMessage(tjid, { text: orig.text || '' }), _iletTimeout()]);
            }
            // GONDERIM ONAYI: sent.key yoksa WhatsApp kabul etmemis -> basarisiz say
            if (!sent || !sent.key) throw new Error('WhatsApp iletimi onaylamadi');
            // panelde de gosterelim (belgede metin yerine dosya adi gosterilsin)
            addMessage(tjid, {
              id: sent.key.id, key: sent.key,
              fromMe: true, kind: orig.kind,
              text: orig.kind === 'document' ? (orig.fileName || orig.text || '') : orig.text,
              caption: orig.caption || '',
              fileName: orig.fileName || undefined,
              mime: orig.mime || undefined,
              mediaUrl: orig.mediaUrl || null,
              sender: msg.agent || 'Ben', time: nowTime(), forwarded: true,
              durum: 2,
            }, _LID);
            okCount++;
          } catch (e) {
            console.error(`Iletme hatasi (${(tjid||'').split('@')[0]}):`, e.message);
            basarisizlar.push((tjid || '').split('@')[0]);
          }
        }
        // iletme bitti. Hangi sohbete iletildiyse panel onu acsin (gittigini gorsun).
        // Tek hedefse onu, birden fazlaysa ILK hedefi ac.
        const acilacakJid = targets.length ? targets[0] : null;
        // basarisiz hedef varsa kullaniciya bildir (sessizce kaybolmasin)
        if (basarisizlar.length && okCount > 0) {
          ws.send(JSON.stringify({ type: 'opError', error: `${okCount} sohbete iletildi, ${basarisizlar.length} sohbete iletilemedi. Tekrar deneyin.` }));
        } else if (basarisizlar.length && okCount === 0) {
          ws.send(JSON.stringify({ type: 'opError', error: 'İletilemedi! Bağlantı sorunu olabilir, tekrar deneyin.' }));
        }
        ws.send(JSON.stringify({ type: 'forwardResult', ok: okCount > 0, count: okCount, acilacakJid }));
      }

      // 8) Mesaja REAKSIYON ver (emoji tepki) - bos string reaksiyonu kaldirir
      else if (msg.type === 'react' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        const orig = chat?.messages.find(x => x.id === msg.id);
        // key'i bul: once orig.key, yoksa orig.raw.key (gecmisten gelen mesajlar)
        let reactKey = orig?.key || orig?.raw?.key || null;
        // key'i WhatsApp'in bekledigi temiz formata getir
        if (reactKey) {
          reactKey = {
            remoteJid: msg.jid,
            id: reactKey.id || orig.id,
            fromMe: !!reactKey.fromMe,
            ...(reactKey.participant ? { participant: reactKey.participant } : {})
          };
        }
        if (reactKey && reactKey.id) {
          try {
            await SOCK.sendMessage(msg.jid, { react: { text: msg.emoji || '', key: reactKey } });
            // panelde de gosterelim (kendi reaksiyonumuz)
            if (msg.emoji) orig.myReaction = msg.emoji;
            else delete orig.myReaction;
            broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
            console.log(`👍 reaksiyon gonderildi: ${msg.emoji || '(kaldirildi)'} -> ${msg.jid.split('@')[0]}`);
          } catch (e) {
            console.log(`   ⚠️  REAKSIYON HATASI: ${e.message}`);
            const rl = (e.message || '').includes('rate-overlimit') || (e.message || '').includes('429');
            ws.send(JSON.stringify({ type: 'opError', error: rl ? 'WhatsApp şu an yoğun (hız sınırı), birazdan tekrar dene.' : 'Reaksiyon gönderilemedi.' }));
          }
        } else {
          console.log(`   ⚠️  reaksiyon: mesajin key'i bulunamadi (id=${msg.id})`);
          ws.send(JSON.stringify({ type: 'opError', error: 'Bu mesaja tepki verilemedi (eski mesaj olabilir).' }));
        }
      }

      // 9) Grup uye avatarlarini cek (bilgi paneli acildiginda)
      else if (msg.type === 'getMemberAvatars' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (chat?.members?.length) {
          // en fazla 40 uye icin avatar cek, hepsini cekince bir kerede yayinla
          const targets = chat.members.filter(mb => mb.avatar === undefined).slice(0, 40);
          for (const mb of targets) {
            const url = await getAvatar(mb.jid);
            mb.avatar = url; // null da olabilir (pp yok)
            // ismi numara/Bilinmeyen ise, rehber/pushName'den guncellemeyi dene
            const daha = savedContacts.get(mb.jid) || contactNames.get(mb.jid);
            if (daha && (mb.name === mb.number || mb.name === 'Bilinmeyen kişi' || !mb.name)) {
              mb.name = daha;
            }
          }
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
        }
      }

      // Tek bir sohbetin (grup/kisi) kendi avatarini cek (baslik icin).
      // ARTIK her zaman GUNCEL cekiyoruz: WhatsApp'ta logo degismisse panelde de degissin.
      // (Eskiden sadece avatar YOKSA cekiyordu -> degisen logolar guncellenmiyordu.)
      else if (msg.type === 'getChatAvatar' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (chat) {
          try {
            const url = await getAvatar(msg.jid, true); // true = ZORLA taze cek (onbellegi atla)
            // url null olabilir (pp kaldirilmis). Degisiklik varsa guncelle.
            if (url !== chat.avatar) {
              chat.avatar = url; // yeni logo (veya kaldirildiysa null)
              broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
              console.log(`🖼️  avatar guncellendi [${msg.jid.split('@')[0]}]: ${url ? 'yeni logo' : 'kaldirilmis'}`);
            }
          } catch (e) {}
        }
      }

      // Sadece grup UYELERINI cek (mesajlara dokunmadan) — grup bilgisi paneli icin.
      else if (msg.type === 'getGroupMembers' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (chat && chat.isGroup) {
          try {
            const meta = await Promise.race([
              SOCK.groupMetadata(msg.jid),
              new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000)),
            ]);
            if (meta && meta.participants) {
              chat.memberCount = meta.participants.length;
              chat.members = meta.participants.map(p => {
                const r = resolvePhone(p.id, p.phoneNumber || null);
                const nm = savedContacts.get(r.jid) || contactNames.get(r.jid) || contactNames.get(p.id) || (r.isLid ? 'Bilinmeyen kişi' : r.number);
                const av = avatarCache.has(r.jid) ? avatarCache.get(r.jid) : (avatarCache.has(p.id) ? avatarCache.get(p.id) : undefined);
                return { jid: r.jid, number: r.number, name: nm, admin: !!p.admin, isLid: !!r.isLid, avatar: av };
              });
              if (meta.subject && meta.subject.trim()) chat.name = meta.subject.trim();
              if (db.isReady()) db.saveChat(chat, _LID).catch(() => {});
              // SADECE bu sohbetin guncel halini gonder (mesajlar stripRaw'da korunur cunku
              // panel artik az mesajla ezmiyor). Diger sohbetleri etkilemez.
              broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
              console.log(`👥 grup uyeleri cekildi: ${chat.name} (${chat.memberCount} uye)`);
            }
          } catch (e) {
            console.log(`   ⚠️  uye cekme hatasi: ${e.message}`);
          }
        }
      }

      // Tek bir grubun gercek adini cek (adi sayiysa tiklayinca duzelsin)
      else if (msg.type === 'refreshGroupName' && SOCK && CONNECTED) {
        let chat = C.get(msg.jid);
        const grupMu = msg.jid && msg.jid.endsWith('@g.us'); // jid'den grup oldugunu anla
        console.log(`🔍 grup adi yenileme istegi: ${msg.jid} (chat var mi: ${!!chat}, isGroup: ${chat?.isGroup}, jid grup mu: ${grupMu})`);
        if (grupMu) {
          // chat yoksa bile metadata cekmeyi dene (0 uyeli/eksik yuklenmis gruplar icin)
          try {
            console.log(`   ⏳ WhatsApp'tan metadata cekiliyor...`);
            // DOGRUDAN groupMetadata cagir (getGroupMeta onbellek/kuyruk kullanir; burada taze istiyoruz)
            let meta = null;
            try {
              meta = await Promise.race([
                SOCK.groupMetadata(msg.jid),
                new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000)),
              ]);
            } catch (e2) {
              console.log(`   ⚠️  groupMetadata hata: ${e2.message}`);
            }
            if (meta && meta.subject && meta.subject.trim()) {
              if (!chat) {
                // chat yoktu, yeni olustur
                chat = { jid: msg.jid, isGroup: true, name: meta.subject.trim(), messages: [], lastTs: 0 };
                C.set(msg.jid, chat);
              }
              chat.isGroup = true;
              chat.name = meta.subject.trim();
              chat.description = (meta.desc && meta.desc.trim()) ? meta.desc.trim() : '';
              // UYE LISTESINI de doldur (adlari + numaralari ile) — grup bilgisinde gozuksun
              if (meta.participants) {
                chat.memberCount = meta.participants.length;
                chat.members = meta.participants.map(p => {
                  const r = resolvePhone(p.id, p.phoneNumber || null);
                  const nm = savedContacts.get(r.jid) || contactNames.get(r.jid) || contactNames.get(p.id) || (r.isLid ? 'Bilinmeyen kişi' : r.number);
                  const av = avatarCache.has(r.jid) ? avatarCache.get(r.jid) : (avatarCache.has(p.id) ? avatarCache.get(p.id) : undefined);
                  return { jid: r.jid, number: r.number, name: nm, admin: !!p.admin, isLid: !!r.isLid, avatar: av };
                });
              }
              grupAdlari.set(msg.jid, meta.subject.trim());
              if (db.isReady()) db.saveChat(chat, _LID).catch(() => {});
              broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
              console.log(`   ✅ grup adi bulundu: "${chat.name}" (${chat.memberCount || 0} uye)`);
            } else {
              console.log(`   ❌ metadata bos geldi (subject yok). Grup gizli/erisilemez olabilir.`);
              // bellekte ad varsa onu kullan
              if (chat && grupAdlari.has(msg.jid)) {
                chat.name = grupAdlari.get(msg.jid);
                broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
                console.log(`   ↳ bellekteki ad kullanildi: ${chat.name}`);
              }
              ws.send(JSON.stringify({ type: 'opError', error: 'Grup adı çekilemedi. WhatsApp bu grubun bilgisini vermedi (meşgul veya erişim yok). Birazdan tekrar deneyin ya da "Değiştir" ile elle yazın.' }));
            }
          } catch (e) {
            console.log(`   ⚠️  grup adi yenileme hatasi: ${e.message}`);
            ws.send(JSON.stringify({ type: 'opError', error: 'Grup adı çekilirken hata oluştu: ' + e.message }));
          }
        } else {
          console.log(`   ⚠️  bu bir grup jid'i degil: ${msg.jid}`);
        }
      }

      // Grup ADINI degistir (sadece yonetici yapabilir)
      else if (msg.type === 'setGroupName' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (!chat || !chat.isGroup) { ws.send(JSON.stringify({ type: 'opError', error: 'Bu bir grup değil.' })); return; }
        const yeni = (msg.name || '').trim();
        if (!yeni) { ws.send(JSON.stringify({ type: 'opError', error: 'İsim boş olamaz.' })); return; }
        try {
          await SOCK.groupUpdateSubject(msg.jid, yeni);
          chat.name = yeni;
          if (grupAdlari) grupAdlari.set(msg.jid, yeni); // bellekteki ad onbellegini de guncelle
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
          if (db.isReady()) db.saveChat(chat, _LID).catch(() => {}); // kalici olsun
          ws.send(JSON.stringify({ type: 'opOk', message: 'Grup adı güncellendi.' }));
        } catch (e) {
          ws.send(JSON.stringify({ type: 'opError', error: 'Grup adı değiştirilemedi. Yönetici olman gerekebilir.' }));
        }
      }

      // Gruba KISI EKLE (sadece yonetici yapabilir). numbers: ["905xx", ...] veya tek numara
      // ==== ETIKETLER (labels) ====
      // Etiket listesini + grup-etiket baglantilarini panele gonder
      else if (msg.type === 'getLabels') {
        const cl = {};
        for (const [cjid, ids] of chatLabels.entries()) cl[cjid] = ids;
        ws.send(JSON.stringify({ type: 'labelsList', labels, chatLabels: cl }));
      }
      // Yeni etiket olustur (veya guncelle)
      else if (msg.type === 'saveLabel') {
        const id = msg.id || ('lbl_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6));
        const name = (msg.name || '').trim().slice(0, 40);
        const color = msg.color || '#25d366';
        if (!name) { ws.send(JSON.stringify({ type: 'opError', error: 'Etiket adı boş olamaz.' })); return; }
        const mevcut = labels.find(l => l.id === id);
        if (mevcut) { mevcut.name = name; mevcut.color = color; }
        else labels.push({ id, name, color });
        db.addLabel(id, name, color).catch(() => {});
        broadcastHat('ofis', { type: 'labelsUpdate', labels, chatLabels: Object.fromEntries(chatLabels) });
        ws.send(JSON.stringify({ type: 'opOk', message: 'Etiket kaydedildi.' }));
      }
      // Etiketi sil
      else if (msg.type === 'deleteLabel') {
        const id = msg.id;
        labels = labels.filter(l => l.id !== id);
        // tum gruplardan bu etiketi cikar
        for (const [cjid, ids] of chatLabels.entries()) {
          const yeni = ids.filter(x => x !== id);
          if (yeni.length) chatLabels.set(cjid, yeni); else chatLabels.delete(cjid);
        }
        db.deleteLabel(id).catch(() => {});
        broadcastHat('ofis', { type: 'labelsUpdate', labels, chatLabels: Object.fromEntries(chatLabels) });
        ws.send(JSON.stringify({ type: 'opOk', message: 'Etiket silindi.' }));
      }
      // Bir gruba etiket ekle/cikar (toggle)
      else if (msg.type === 'toggleChatLabel') {
        const cjid = msg.jid;
        const labelId = msg.labelId;
        if (!cjid || !labelId) return;
        const mevcut = chatLabels.get(cjid) || [];
        let yeni;
        if (mevcut.includes(labelId)) {
          yeni = mevcut.filter(x => x !== labelId);
          db.removeChatLabel(cjid, labelId).catch(() => {});
        } else {
          yeni = [...mevcut, labelId];
          db.addChatLabel(cjid, labelId).catch(() => {});
        }
        if (yeni.length) chatLabels.set(cjid, yeni); else chatLabels.delete(cjid);
        broadcastHat('ofis', { type: 'chatLabelUpdate', jid: cjid, labelIds: yeni });
        ws.send(JSON.stringify({ type: 'opOk', message: 'Etiket güncellendi.' }));
      }

      // Ekip uyelerini (giris yapan kullanicilar) panele gonder — @ ile etiketleme icin
      else if (msg.type === 'getTeam') {
        try {
          const users = await db.listUsers();
          const liste = (users || []).map(u => ({ username: u.username, displayName: u.display_name || u.username }));
          ws.send(JSON.stringify({ type: 'teamList', team: liste }));
        } catch (e) {
          ws.send(JSON.stringify({ type: 'teamList', team: [] }));
        }
      }

      // Gruba kullanici ATA (birden fazla olabilir)
      else if (msg.type === 'assignUsers') {
        const cjid = msg.jid;
        const usernames = Array.isArray(msg.usernames) ? msg.usernames : [];
        if (!cjid || !chats.has(cjid)) { ws.send(JSON.stringify({ type: 'opError', error: 'Grup bulunamadı.' })); return; }
        const mevcut = chatAssignments.get(cjid) || [];
        const yeni = [...new Set([...mevcut, ...usernames])]; // tekrarsiz birlestir
        chatAssignments.set(cjid, yeni);
        // Supabase'e yaz
        for (const u of usernames) { if (!mevcut.includes(u)) db.addAssignment(cjid, u).catch(() => {}); }
        // tum panellere bildir (atama degisti)
        broadcastHat('ofis', { type: 'assignmentUpdate', jid: cjid, users: yeni });
        ws.send(JSON.stringify({ type: 'opOk', message: 'Atama güncellendi.' }));
      }

      // Gruptan kullaniciyi CIKAR (herkes herkesi cikarabilir)
      else if (msg.type === 'unassignUser') {
        const cjid = msg.jid;
        const username = msg.username;
        if (!cjid) return;
        const mevcut = chatAssignments.get(cjid) || [];
        const yeni = mevcut.filter(u => u !== username);
        if (yeni.length) chatAssignments.set(cjid, yeni);
        else chatAssignments.delete(cjid);
        db.removeAssignment(cjid, username).catch(() => {});
        broadcastHat('ofis', { type: 'assignmentUpdate', jid: cjid, users: yeni });
        ws.send(JSON.stringify({ type: 'opOk', message: 'Çıkarıldı.' }));
      }

      else if (msg.type === 'getContacts') {
        // Panele kayitli kisileri (isim + numara) gonder — gruba isimle ekleme icin.
        // Hem manuel/ofis kisileri (savedContacts) hem kisi sohbetleri toplanir.
        const harita = new Map(); // numara -> isim (tekrarsiz)
        // 1) savedContacts (ofis ekibi + manuel kayitlar) — sadece numarali olanlar
        for (const [jid, isim] of savedContacts.entries()) {
          if (jid.endsWith('@s.whatsapp.net')) {
            const num = jid.split('@')[0];
            if (num && isim) harita.set(num, isim);
          }
        }
        // 2) kisi sohbetleri (gruplar haric) — adi olanlar
        for (const c of C.values()) {
          if (!c.isGroup && c.jid.endsWith('@s.whatsapp.net')) {
            const num = c.jid.split('@')[0];
            const isim = c.customName || c.name;
            // isim numaranin kendisi degilse (yani gercek bir isimse) ekle
            if (num && isim && isim !== num && !harita.has(num)) harita.set(num, isim);
          }
        }
        const liste = Array.from(harita.entries()).map(([number, name]) => ({ number, name }))
          .sort((a, b) => a.name.localeCompare(b.name, 'tr'));
        ws.send(JSON.stringify({ type: 'contactsList', contacts: liste }));
      }

      // Gruba KISI EKLE (sadece yonetici yapabilir). numbers: ["905xx", ...] veya tek numara
      else if (msg.type === 'addGroupMember' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (!chat || !chat.isGroup) { ws.send(JSON.stringify({ type: 'opError', error: 'Bu bir grup değil.' })); return; }
        // numarayi/numaralari temizle ve Turkiye formatina cevir
        const ham = Array.isArray(msg.numbers) ? msg.numbers : [msg.number];
        const jidler = [];
        for (let n of ham) {
          if (!n) continue;
          n = String(n).replace(/\D/g, '');           // sadece rakam
          if (n.startsWith('0090')) n = n.slice(2);    // 0090... -> 90...
          else if (n.startsWith('0')) n = '90' + n.slice(1); // 05XX -> 905XX
          else if (!n.startsWith('90') && n.length === 10) n = '90' + n; // 5XX... -> 905XX
          if (n.length >= 12) jidler.push(n + '@s.whatsapp.net');
        }
        if (!jidler.length) { ws.send(JSON.stringify({ type: 'opError', error: 'Geçerli numara yok.' })); return; }
        try {
          const sonuc = await SOCK.groupParticipantsUpdate(msg.jid, jidler, 'add');
          // sonuc: her numara icin durum doner. Basari/hata ayikla.
          let eklenen = 0; let hatali = [];
          for (const r of (sonuc || [])) {
            // status '200' = eklendi; digerleri sorun (davet gerekebilir, numara yok vs.)
            if (r.status === '200') eklenen++;
            else hatali.push((r.jid || '').split('@')[0]);
          }
          // grup uye listesini tazele (arka planda)
          getGroupMeta(msg.jid, 0).then((meta) => {
            if (meta && meta.participants) {
              const c = C.get(msg.jid);
              if (c) { c.memberCount = meta.participants.length; broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(c) }); }
            }
          }).catch(() => {});
          if (eklenen > 0 && !hatali.length) {
            ws.send(JSON.stringify({ type: 'opOk', message: eklenen + ' kişi gruba eklendi.' }));
          } else if (eklenen > 0) {
            ws.send(JSON.stringify({ type: 'opOk', message: eklenen + ' eklendi. Bazıları eklenemedi (gizlilik/davet gerekebilir).' }));
          } else {
            ws.send(JSON.stringify({ type: 'opError', error: 'Eklenemedi. Kişinin gizlilik ayarı davet gerektiriyor olabilir veya numara WhatsApp\'ta yok.' }));
          }
        } catch (e) {
          console.error('Gruba ekleme hatasi:', e.message);
          ws.send(JSON.stringify({ type: 'opError', error: 'Eklenemedi. Yönetici olman gerekebilir.' }));
        }
      }

      // Grup ACIKLAMASINI degistir (sadece yonetici yapabilir)
      else if (msg.type === 'setGroupDesc' && SOCK && CONNECTED) {
        const chat = C.get(msg.jid);
        if (!chat || !chat.isGroup) { ws.send(JSON.stringify({ type: 'opError', error: 'Bu bir grup değil.' })); return; }
        const yeni = (msg.desc || '').trim();
        try {
          await SOCK.groupUpdateDescription(msg.jid, yeni);
          chat.description = yeni;
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
          if (db.isReady()) db.saveChat(chat, _LID).catch(() => {}); // kalici olsun
          ws.send(JSON.stringify({ type: 'opOk', message: 'Grup açıklaması güncellendi.' }));
        } catch (e) {
          ws.send(JSON.stringify({ type: 'opError', error: 'Açıklama değiştirilemedi. Yönetici olman gerekebilir.' }));
        }
      }
      // Sohbet acilinca o kisinin/grubun "yaziyor" durumuna abone ol
      // Panelden kayitli isim degistir (kalici - Supabase'e yazilir)
      else if (msg.type === 'setCustomName') {
        const chat = C.get(msg.jid);
        const isim = (msg.name || '').trim();
        if (chat && isim) {
          chat.customName = isim;
          chat.name = isim;
          // kisi ise rehber ismi olarak da kaydet (her yerde gorunsun)
          if (!chat.isGroup) {
            savedContacts.set(msg.jid, isim);
            contactNames.set(msg.jid, isim);
            if (db.isReady()) db.saveContact(msg.jid, isim, true).catch(() => {});
          }
          if (db.isReady()) db.saveChat(chat, _LID).catch(() => {});
          broadcastHat(_LID, { type: 'message', jid: msg.jid, chat: stripRaw(chat) });
          console.log(`✏️  isim kaydedildi (kalici): ${isim}`);
        }
      }

      // Sohbet acilinca mesajlarini Supabase'den yukle (DB'den gelen sohbetler icin)
      else if (msg.type === 'loadMessages') {
        const chat = C.get(msg.jid);
        if (!chat) { /* sohbet yok */ }
        else {
          // Grup adi hala ID (sayi) ise, bellekteki grupAdlari'ndan gercek adi al (aninda duzelir).
          if (chat.isGroup && /^\d+$/.test(chat.name || '') && grupAdlari.has(msg.jid)) {
            chat.name = grupAdlari.get(msg.jid);
          }
          // 1) ANINDA: bellekte ne varsa hemen gonder — kullanici beklemesin (yavaslik/bos acilis biter).
          if (chat.messages && chat.messages.length) {
            ws.send(JSON.stringify({ type: 'message', jid: msg.jid, chat: stripRaw(chat) }));
          }
          // 2) ARKA PLANDA: DB'den son mesajlari cek, EKSIK olanlari ekle, sonra tekrar gonder.
          //    Boylece acilis hizli olur, eksik mesaj varsa hemen ardindan tamamlanir.
          if (db.isReady()) {
            db.loadMessages(msg.jid, 80, _LID).then((rows) => {
              console.log(`📨 loadMessages [${(chat.name||msg.jid).slice(0,30)}]: bellekte ${chat.messages?.length||0}, DB'den ${rows.length} mesaj`);
              const dbMsgs = rows.map(r => ({
                id: r.id, fromMe: r.from_me, kind: r.kind, text: r.text || '',
                mediaUrl: r.media_url || null, thumb: r.thumb || null,
                sender: r.sender || '', senderJid: r.sender_jid || '', senderPush: r.sender_push || '',
                replyTo: r.reply_to || null, contact: r.contact_data || null, contacts: r.contacts_data || null,
                reaction: r.reaction || null, myReaction: r.my_reaction || null,
                forwarded: r.forwarded || false, mentionsMe: r.mentions_me || false,
                edited: r.edited || false, deleted: r.deleted || false,
                time: r.time || '', ts: Number(r.ts) || 0, key: r.key_data || null, mentions: r.mentions || null, caption: r.caption || '',
              }));
              // bellek + DB birlestir (id'ye gore tekilastir)
              const birlesik = new Map();
              for (const x of (chat.messages || [])) birlesik.set(x.id, x);
              let eklendi = false;
              for (const x of dbMsgs) if (!birlesik.has(x.id)) { birlesik.set(x.id, x); eklendi = true; }
              // Sadece DB'den GERCEKTEN yeni mesaj eklendiyse VEYA bellek bostiysa tekrar gonder
              // (bos yere ikinci kez gondermeyelim — panel gereksiz render etmesin).
              if (eklendi || !chat.messages.length) {
                chat.messages = Array.from(birlesik.values()).sort((a, b) => (a.ts || 0) - (b.ts || 0));
                const last = chat.messages[chat.messages.length - 1];
                if (last) { chat.lastTs = last.ts || chat.lastTs; chat.lastTime = last.time || chat.lastTime; }
                ws.send(JSON.stringify({ type: 'message', jid: msg.jid, chat: stripRaw(chat) }));
              }
            }).catch(() => {});
          } else if (!chat.messages.length) {
            // DB kapali + bellek bos: yine de bos chat gonder ki "yukleniyor" kalkmasin
            ws.send(JSON.stringify({ type: 'message', jid: msg.jid, chat: stripRaw(chat) }));
          }
        }
      }

      else if (msg.type === 'subscribePresence' && SOCK && CONNECTED) {
        try { await SOCK.presenceSubscribe(msg.jid); } catch (e) {}
      }

      // PERIYODIK TAZELEME: panel acik sohbet icin DB'den son mesajlari cekip
      // bellekte EKSIK olanlari tamamlar. WhatsApp'a DEGIL, kendi veritabanina sorar
      // (sifir rate-limit / ban riski). Canli kacan mesaj DB'ye dustuyse boylece panele gelir.
      else if (msg.type === 'refreshChat') {
        const chat = C.get(msg.jid);
        if (chat && db.isReady()) {
          const rows = await db.loadMessages(msg.jid, 80, _LID);
          if (rows && rows.length > 0) {
            // bellekteki mevcut id'ler
            const mevcutIds = new Set((chat.messages || []).map(x => x.id));
            let eklenen = 0;
            for (const r of rows) {
              if (mevcutIds.has(r.id)) continue; // zaten var
              // DB satirini bellek mesaj formatina cevir (loadMessages ile ayni esleme)
              chat.messages.push({
                id: r.id, fromMe: r.from_me, kind: r.kind, text: r.text || '',
                mediaUrl: r.media_url || null, thumb: r.thumb || null,
                sender: r.sender || '', senderJid: r.sender_jid || '', senderPush: r.sender_push || '',
                replyTo: r.reply_to || null, contact: r.contact_data || null, contacts: r.contacts_data || null,
                reaction: r.reaction || null, myReaction: r.my_reaction || null,
                forwarded: r.forwarded || false, mentionsMe: r.mentions_me || false,
                edited: r.edited || false, deleted: r.deleted || false,
                time: r.time || '', ts: Number(r.ts) || 0, key: r.key_data || null,
                mentions: r.mentions || null, caption: r.caption || '',
              });
              eklenen++;
            }
            if (eklenen > 0) {
              // zamana gore sirala (kacan mesaj araya dogru yere girsin)
              chat.messages.sort((a, b) => (a.ts || 0) - (b.ts || 0));
              const last = chat.messages[chat.messages.length - 1];
              if (last) { chat.lastTs = last.ts || chat.lastTs; chat.lastTime = last.time || chat.lastTime; }
              console.log(`🔄 tazeleme: ${eklenen} eksik mesaj DB'den eklendi (${(chat.name||msg.jid).substring(0,25)})`);
              ws.send(JSON.stringify({ type: 'message', jid: msg.jid, chat: stripRaw(chat) }));
            }
          }
        }
      }

      // Panelden CIKIS YAP (WhatsApp baglantisini kes, oturumu sil, yeni QR uret)
      else if (msg.type === 'logout') {
        console.log(`🚪 Panelden cikis istendi... [hat: ${_LID}]`);
        if (_LID === 'ofis') {
          // OFIS: mevcut davranis (global ofis hatti)
          manualLogout = true;
          try { if (waSock) await waSock.logout(); } catch (e) {}
          try { if (waSock) waSock.end(); } catch (e) {}
          // oturum klasorunu sil (ofis auth)
          try {
            fs.rmSync(path.join(__dirname, 'auth', 'ofis'), { recursive: true, force: true });
          } catch (e) { console.error('auth/ofis silinemedi:', e.message); }
          // NOT: chats/contactNames/savedContacts'i SILMIYORUZ! Bunlar DB'de kalici ve
          // ayni numaraya yeniden baglaninca lazim. Sadece BAGLANTI durumunu sifirla.
          // (Tamamen temiz baslangic isteyen 'wipeAll' kullanir.)
          avatarCache.clear(); groupMetaCache.clear();
          myNumber = null; myLID = null; lastQR = null; waConnected = false;
          broadcastHat('ofis', { type: 'status', connected: false, loggedOut: true });
          console.log('   ↳ ofis cikis tamam (sohbetler korundu). Yeni QR icin baglaniliyor...');
          // yeni baglanti baslat (yeni QR uretecek)
          manualLogout = false;
          setTimeout(() => startWA('ofis'), 1500);
        } else {
          // PAZARLAMA: SADECE bu hatti kapat. Ofise/digerlerine dokunma.
          const line = lines.get(_LID);
          if (line) {
            line.manualLogout = true;
            try { if (line.sock) await line.sock.logout(); } catch (e) {}
            try { if (line.sock) line.sock.end(); } catch (e) {}
            // bu hattin auth klasorunu sil
            try { fs.rmSync(line.authDir, { recursive: true, force: true }); }
            catch (e) { console.error(`${_LID} auth silinemedi:`, e.message); }
            // baglanti durumunu sifirla (kendi sohbetleri DB'de korunur)
            line.connected = false; line.myNumber = null; line.myLID = null;
            line.lastQR = null; line.manualLogout = false;
          }
          broadcastHat(_LID, { type: 'status', connected: false, loggedOut: true });
          console.log(`   ↳ ${_LID} cikis tamam (kendi sohbetleri korundu). Yeni QR icin baglaniliyor...`);
          // bu hat icin yeni baglanti baslat (yeni QR)
          setTimeout(() => startWA(_LID), 1500);
        }
      }

      // Panelden YENI QR iste (baglanmadan once QR gelmezse)
      else if (msg.type === 'requestQR') {
        if (_LID === 'ofis') {
          if (!waConnected) {
            if (lastQR) { ws.send(JSON.stringify({ type: 'status', connected: false, qr: true, qrImage: lastQR })); }
            else { manualLogout = false; startWA('ofis'); }
          }
        } else {
          const line = lines.get(_LID);
          if (!line || !line.connected) {
            if (line && line.lastQR) { ws.send(JSON.stringify({ type: 'status', connected: false, qr: true, qrImage: line.lastQR })); }
            else { startWA(_LID); }
          }
        }
      }

      // TUM verileri sil (bellek + Supabase). Cikistan ayri, kasitli temizlik.
      else if (msg.type === 'wipeAll') {
        // GUVENLIK: "Tum verileri sil" SADECE yonetici yapabilir. Panelde buton kaldirildi
        // ama yine de sunucu tarafinda da kilitliyoruz (ws uzerinden kotuye kullanim olmasin).
        if (ws._role !== 'admin') {
          console.log(`⛔ wipeAll reddedildi (yonetici degil): ${ws._username || '?'} [hat: ${_LID}]`);
          ws.send(JSON.stringify({ type: 'opError', error: 'Bu işlem için yetkiniz yok.' }));
          return;
        }
        console.log(`🗑️  TUM veriler siliniyor (panel istegi) [hat: ${_LID}]...`);
        if (_LID === 'ofis') {
          // OFIS: global temizlik (mevcut davranis)
          chats.clear(); contactNames.clear(); savedContacts.clear();
          avatarCache.clear(); groupMetaCache.clear(); lidToPn.clear();
          // Supabase bagliysa oradaki tablolari da temizle
          if (typeof db !== 'undefined' && db && db.wipeAll) {
            try { await db.wipeAll(); console.log('   ↳ Supabase verileri silindi'); }
            catch (e) { console.error('   ⚠️  Supabase silme hatasi:', e.message); }
          }
          broadcastHat('ofis', { type: 'chats', chats: [] });
          broadcastHat('ofis', { type: 'opOk', message: 'Tüm veriler silindi.' });
        } else {
          // PAZARLAMA: SADECE bu hattin verisini sil. Ofise/digerlerine ASLA dokunma.
          const C2 = hatChats(_LID);
          if (C2 && C2.clear) C2.clear();
          // Supabase'de SADECE bu hattin satirlarini sil
          if (db.isReady() && db.deleteLineData) {
            try { await db.deleteLineData(_LID); console.log(`   ↳ Supabase'de ${_LID} verileri silindi`); }
            catch (e) { console.error('   ⚠️  Supabase hat silme hatasi:', e.message); }
          }
          broadcastHat(_LID, { type: 'chats', chats: [] });
          broadcastHat(_LID, { type: 'opOk', message: 'Bu hesabın tüm verileri silindi.' });
        }
        console.log('   ↳ tamam.');
      }

      // SADECE GRUPLARI sil (kayitli kisileri KORU). Temiz baslangic + eski avatar 404'lerini temizler.
      else if (msg.type === 'wipeGroups') {
        if (ws._role !== 'admin') {
          console.log(`⛔ wipeGroups reddedildi (yonetici degil): ${ws._username || '?'} [hat: ${_LID}]`);
          ws.send(JSON.stringify({ type: 'opError', error: 'Bu işlem için yetkiniz yok.' }));
          return;
        }
        console.log(`🗑️  Sadece GRUPLAR siliniyor (kisiler korunuyor) [hat: ${_LID}]...`);
        const C2 = hatChats(_LID);
        // Bellekte: sadece grup olan sohbetleri sil, bire-bir kisileri birak
        if (C2 && C2.forEach) {
          const silinecek = [];
          C2.forEach((chat, jid) => { if (chat && chat.isGroup) silinecek.push(jid); });
          silinecek.forEach(jid => C2.delete(jid));
          console.log(`   ↳ bellekten ${silinecek.length} grup silindi`);
        }
        // Avatar onbellegini temizle ki eski (404 veren) avatar adresleri gitsin
        avatarCache.clear(); groupMetaCache.clear();
        // Supabase'de sadece gruplari sil (bu hatta ait)
        if (db.isReady() && db.wipeGroups) {
          try {
            await db.wipeGroups(_LID === 'ofis' ? null : _LID);
            console.log('   ↳ Supabase grup verileri silindi (kisiler korundu)');
          } catch (e) { console.error('   ⚠️  Supabase grup silme hatasi:', e.message); }
        }
        // Panele guncel listeyi gonder (sadece kalan kisiler)
        const kalan = [];
        if (C2 && C2.forEach) C2.forEach(c => kalan.push(stripRaw(c)));
        broadcastHat(_LID, { type: 'chats', chats: kalan });
        broadcastHat(_LID, { type: 'opOk', message: 'Gruplar silindi. Kayıtlı kişiler korundu. Yeni mesaj geldikçe gruplar temiz şekilde geri gelecek.' });
        console.log('   ↳ tamam.');
      }
    } catch (e) { console.error('Panel mesaji islenemedi:', e.message); }
  });
});

function nowTime() {
  const d = new Date();
  return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
}

// ============================================================
// SATIŞ KOMUTU AYRIŞTIRMA: "/trafik2", "/kasko 1", "/dask" gibi
// ============================================================
// GEÇERLİ BRANŞLAR (sadece bunlar satis sayilir; baska /xxx yazilirsa null doner).
// Türkçe karakterler normalize edilir: yesilkart/yeşilkart, oss/öss, isyeri/işyeri ikisi de gecerli.
const GECERLI_BRANSLAR = {
  'trafik': 'trafik',
  'kasko': 'kasko',
  'dask': 'dask',
  'tss': 'tss',
  'yesilkart': 'yeşilkart', 'yeşilkart': 'yeşilkart', 'yesil': 'yeşilkart',
  'konut': 'konut',
  'isyeri': 'işyeri', 'işyeri': 'işyeri',
  'oss': 'öss', 'öss': 'öss',
  'imm': 'imm',
};
// Panelde/raporda gosterilecek sira (pazarlamaci bilgi kutusu + dashboard icin)
const BRANS_LISTESI = ['trafik', 'kasko', 'dask', 'tss', 'yeşilkart', 'konut', 'işyeri', 'öss', 'imm'];

// / ile baslar, sonra urun adi (harf), sonra (opsiyonel) adet (sayi, yoksa 1).
// Eslesmezse VEYA gecerli bir brans degilse null doner (normal mesaj sayilir).
function satisAyristir(text) {
  if (!text || typeof text !== 'string') return null;
  const t = text.trim();
  if (!t.startsWith('/')) return null;
  const m = t.match(/^\/([a-zA-ZçğıöşüÇĞİÖŞÜ]+)\s*(\d+)?$/);
  if (!m) return null;
  const ham = m[1].toLowerCase();
  // KATI KONTROL: sadece gecerli branslar (listede yoksa satis degil)
  const urun = GECERLI_BRANSLAR[ham];
  if (!urun) return null;
  const adet = m[2] ? parseInt(m[2], 10) : 1;
  if (adet < 1 || adet > 9999) return null; // mantiksiz adet
  return { urun, adet };
}

// Bir satis komutunu DB'ye kaydet (hat-izole). Panele de canli haber verir.
// m: ham WhatsApp mesaji, parsed: {urun, adet}, lineId: hat, chat: sohbet objesi
// Son islenen satislar (mukerrer koruma): "lineId|mesajId|grup" -> zaman.
// notify+append ayni mesaji iki kez getirebiliyor; ayni mesaj id'si kisa surede
// tekrar gelirse ATLA (cift kayit olmasin). Periyodik temizlenir.
const _islenenSatislar = new Map();
// Basit string hash (deterministik id uretmek icin — ayni icerik = ayni hash)
function _basitHash(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) { h = ((h << 5) - h + s.charCodeAt(i)) | 0; }
  return Math.abs(h).toString(36);
}
function _satisMukerrerMi(anahtar) {
  const simdi = Date.now();
  // 30 saniyeden eski kayitlari temizle (bellek sismesin)
  if (_islenenSatislar.size > 500) {
    for (const [k, t] of _islenenSatislar) { if (simdi - t > 30000) _islenenSatislar.delete(k); }
  }
  if (_islenenSatislar.has(anahtar)) {
    const oncekiZaman = _islenenSatislar.get(anahtar);
    if (simdi - oncekiZaman < 30000) return true; // 30sn icinde ayni mesaj -> mukerrer
  }
  _islenenSatislar.set(anahtar, simdi);
  return false;
}

async function satisKaydet(m, parsed, lineId, chat, saticiAdi, saticiJid) {
  if (!db.isReady()) return;
  const mesajId = m.key?.id || '';
  const grupJid = chat?.jid || m.key?.remoteJid || '';
  const ts = m.messageTimestamp ? Number(m.messageTimestamp) * 1000 : Date.now();

  // ---- ÇİFT KAYIT KORUMASI (iki katmanli, mesajId'ye GUVENMEZ) ----
  // notify+append ayni mesaji iki kez getirebiliyor; bazen m.key.id bos veya farkli gelebiliyor.
  // O yuzden ICERIK parmak izi kullaniyoruz: ayni hat+grup+satici+urun+adet kisa surede
  // tekrar gelirse AYNI satistir, atla. (Ayni kisi ayni saniyede ayni seyi iki kez yazamaz.)
  const icerikAnahtar = [lineId, grupJid, (saticiJid || saticiAdi || ''), parsed.urun, parsed.adet].join('|');
  // ham mesaj zamanini saniyeye yuvarla -> ayni mesajin notify+append'i ayni saniyeye duser
  const saniye = Math.floor(ts / 1000);
  const mukerrerAnahtar = icerikAnahtar + '|' + saniye;
  if (_satisMukerrerMi(mukerrerAnahtar)) {
    console.log(`   ⏭️  satis atlandi (mukerrer/yansima): ${parsed.urun} x${parsed.adet} | ${(saticiAdi||'?')} | ${lineId}`);
    return;
  }

  // benzersiz satis id: ICERIK + saniye'den turet (mesajId'ye guvenme — bos/degisken olabilir).
  // Boylece ayni mesajin ikinci gelisinde AYNI id uretilir -> DB ON CONFLICT de yakalar (cift emniyet).
  let satisId;
  if (mesajId) {
    satisId = 'satis_' + lineId + '_' + mesajId;
  } else {
    // mesajId yoksa icerikten deterministik id (rastgele DEGIL — yoksa cift olurdu)
    satisId = 'satis_' + lineId + '_c_' + _basitHash(icerikAnahtar + '_' + saniye);
  }
  const kayit = {
    id: satisId,
    chatJid: grupJid,
    chatName: chat?.name || '',
    urun: parsed.urun,
    adet: parsed.adet,
    satici: saticiAdi || '',
    saticiJid: saticiJid || '',
    mesajId: mesajId,
    hamMesaj: (m.message?.conversation || m.message?.extendedTextMessage?.text || '').trim().slice(0, 100),
    ts: ts,
  };
  try {
    const r = await db.saveSatis(kayit, lineId);
    if (r.ok && r.yeni) {
      console.log(`💰 SATIŞ [${lineId}]: ${parsed.urun} x${parsed.adet} | satici: ${saticiAdi || '?'} | grup: ${(kayit.chatName || kayit.chatJid).slice(0, 25)}`);
      // panele canli haber ver (kontrol sekmesi aciksa aninda guncellensin)
      broadcastHat(lineId, { type: 'yeniSatis', satis: { ...kayit, line_id: lineId, onayli: false } });
      // YONETICIYE de haber ver (ofis panellerine)
      if (lineId !== 'ofis') {
        broadcastHat('ofis', { type: 'satisBildirim', mesaj: `Yeni satış: ${parsed.urun} x${parsed.adet} (${saticiAdi || lineId})`, lineId });
      }
    } else if (r.ok && !r.yeni) {
      console.log(`   ⏭️  satis atlandi (DB'de zaten var): ${mesajId.slice(0, 12)}`);
    }
  } catch (e) { console.error('satisKaydet hatasi:', e.message); }
}

// Adi sayi kalan gruplar icin artan araliklarla tekrar dener (her grup icin tek seferlik kilit)
// ---- Grup metadata istek KUYRUGU (rate-overlimit'i onler) ----
// WhatsApp'i bogmamak icin: ayni anda tek istek + istekler arasi bekleme + rate limit gelince geri cekilme
let metaQueue = [];
let metaBusy = false;
let rateLimitUntil = 0; // bu zamana kadar istek atma (rate limit yedikten sonra)
const META_GAP = 1200;  // istekler arasi bekleme (ms) - nazik ol

function metaQueuePush(jid, resolve) {
  metaQueue.push({ jid, resolve });
  metaQueueRun();
}
async function metaQueueRun() {
  if (metaBusy) return;
  metaBusy = true;
  while (metaQueue.length) {
    // rate limit yediyse, suresi gecene kadar bekle
    const now = Date.now();
    if (now < rateLimitUntil) {
      await new Promise(r => setTimeout(r, rateLimitUntil - now));
    }
    const { jid, resolve } = metaQueue.shift();
    let result = null;
    try {
      result = await waSock.groupMetadata(jid);
      if (result?.subject && result.subject.trim()) groupMetaCache.set(jid, { meta: result, ts: Date.now() });
    } catch (e) {
      if ((e.message || '').includes('rate-overlimit') || (e.message || '').includes('429')) {
        // WhatsApp "yavasla" dedi: 60 sn boyunca hic istek atma
        rateLimitUntil = Date.now() + 60000;
        console.log('   ⏸️  WhatsApp hiz siniri (rate-overlimit) — 60 sn istekleri durduruyorum');
        // bu istegi tekrar kuyruga koy (sonra denensin)
        metaQueue.unshift({ jid, resolve });
        continue;
      }
      // baska hata: bos don
    }
    resolve(result);
    await new Promise(r => setTimeout(r, META_GAP)); // nazik bekleme
  }
  metaBusy = false;
}

const retryingGroups = new Set();
function retryGroupName(jid) {
  if (retryingGroups.has(jid)) return; // zaten deneniyor
  retryingGroups.add(jid);
  // Daha cok deneme + genis araliklar (nazik ama israrci). Grup ID'de kalmasin.
  const gecikmeler = [2000, 6000, 15000, 35000, 70000, 120000];
  let i = 0;
  const dene = async () => {
    const ch = chats.get(jid);
    if (!ch) { retryingGroups.delete(jid); return; } // sohbet artik yok
    // adi zaten duzelmisse dur
    if (ch.name && !/^\d+$/.test(ch.name)) { retryingGroups.delete(jid); return; }
    // 1) ONCE bellekteki grupAdlari'na bak (fetchAllGroups doldurmus olabilir) — bedava, hizli
    if (grupAdlari.has(jid)) {
      ch.name = grupAdlari.get(jid);
      broadcastHat('ofis', { type: 'message', jid, chat: stripRaw(ch) });
      console.log(`🔤 grup adi bellekten geldi: ${ch.name}`);
      retryingGroups.delete(jid);
      return;
    }
    // 2) rate limit yoksa WhatsApp'tan taze cek
    if (rateLimitUntil < Date.now()) {
      try {
        const meta = await getGroupMeta(jid, 0);
        if (meta?.subject && meta.subject.trim()) {
          ch.name = meta.subject.trim();
          if (meta.participants) ch.memberCount = meta.participants.length;
          // ACIKLAMA: desc varsa onu, YOKSA bos string ata. (Eskiden "if(meta.desc)" idi;
          // aciklamasiz grupta eski/baska grubun aciklamasi kaliyordu — bug buydu.)
          ch.description = (meta.desc && meta.desc.trim()) ? meta.desc.trim() : '';
          if (meta.subject) grupAdlari.set(jid, meta.subject.trim()); // bellege de yaz
          broadcastHat('ofis', { type: 'message', jid, chat: stripRaw(ch) });
          console.log(`🔤 grup adi geldi (deneme ${i + 1}): ${ch.name}`);
          retryingGroups.delete(jid);
          return;
        }
      } catch (e) {}
    }
    i++;
    if (i < gecikmeler.length) {
      setTimeout(dene, gecikmeler[i]);
    } else {
      retryingGroups.delete(jid);
      // son care: periyodik tazeleme yine deneyecek (fetchAllGroups)
    }
  };
  setTimeout(dene, gecikmeler[0]);
}

// Grup metadata'sini onbellekten al (yoksa KUYRUK uzerinden cek). Rate-overlimit'i onler.
async function getGroupMeta(jid, maxYas = 5 * 60 * 1000) {
  const cached = groupMetaCache.get(jid);
  // sadece GERCEK adi olan onbellegi kullan (sayi/bos onbellek tekrar denensin)
  if (cached && cached.meta?.subject && cached.meta.subject.trim() && (Date.now() - cached.ts) < maxYas) {
    return cached.meta;
  }
  // kuyruga koy, sonucu bekle
  return new Promise((resolve) => metaQueuePush(jid, resolve));
}

// ============================================================
// KACAN MESAJ AKTIF CEKME KUYRUGU
// chats.update sinyali gelince ilgili sohbetin son mesajini WhatsApp'tan cekmeyi dener.
// 7500 grupta sistemi/WhatsApp'i bogmamak icin: KUYRUK + yavas isleme + tekrar engelleme.
// Onemli: Bu "best effort" (elinden geleni yapar) — cekemese bile sohbet zaten chats.update
// ile en uste cikmis ve okunmamis isaretlenmis olur, yani kullanici kacirmaz.
// ============================================================
const _mesajCekKuyruk = [];
const _mesajCekBekleyen = new Set(); // ayni sohbeti kuyruga 2 kez ekleme
let _mesajCekCalisiyor = false;

function mesajCekKuyruguEkle(jid) {
  if (!jid || _mesajCekBekleyen.has(jid)) return;
  _mesajCekBekleyen.add(jid);
  _mesajCekKuyruk.push(jid);
  if (!_mesajCekCalisiyor) mesajCekKuyruguIsle();
}

async function mesajCekKuyruguIsle() {
  if (_mesajCekCalisiyor) return;
  _mesajCekCalisiyor = true;
  while (_mesajCekKuyruk.length > 0) {
    const jid = _mesajCekKuyruk.shift();
    _mesajCekBekleyen.delete(jid);
    try {
      await mesajiAktifCek(jid);
    } catch (e) { /* sessizce gec — sohbet zaten en uste cikti */ }
    // WhatsApp'i yormamak icin her cekme arasi kisa bekleme (rate-limit korumasi)
    await new Promise(r => setTimeout(r, 600));
  }
  _mesajCekCalisiyor = false;
}

// Bir sohbetin son mesajlarini WhatsApp'tan cekmeyi dene.
// Baileys surumune gore fetchMessageHistory imzasi degisebilir; guvenli sekilde deniyoruz.
async function mesajiAktifCek(jid) {
  if (!waSock || !waConnected) return;
  const chat = chats.get(jid);
  if (!chat) return;
  // Bizde bu sohbetin EN SON mesaj key'i varsa, ondan sonrasini iste.
  // Yoksa cekme yapilamaz (Baileys baslangic noktasi ister) — sorun degil, sohbet zaten isaretli.
  const sonMesaj = chat.messages && chat.messages.length ? chat.messages[chat.messages.length - 1] : null;
  if (!sonMesaj || !sonMesaj.key) return;
  try {
    if (typeof waSock.fetchMessageHistory === 'function') {
      // (adet, baslangicKey, baslangicTs) — son mesajdan itibaren birkac mesaj iste
      await waSock.fetchMessageHistory(5, sonMesaj.key, sonMesaj.ts ? Math.floor(sonMesaj.ts / 1000) : undefined);
      // Gelen mesajlar normal messaging-history.set / messages.upsert akisindan dusecek,
      // oradan addMessage + DB + broadcast zaten calisir.
    }
  } catch (e) { /* desteklenmiyorsa veya hata olursa sessizce gec */ }
}

// Gruplari SIRALI ve NAZIK Supabase'e yazar (DB'yi bogmamak icin).
// Ayni anda binlerce sorgu yerine, kucuk partiler halinde aralarinda bekleyerek yazar.
let _siraliKaydetCalisiyor = false;
let _siraliKuyruk = [];
async function siraliKaydet(chatlar) {
  // kuyruga ekle (tekrarlari jid'e gore ele)
  const mevcutJidler = new Set(_siraliKuyruk.map(c => c.jid));
  for (const c of chatlar) { if (!mevcutJidler.has(c.jid)) _siraliKuyruk.push(c); }
  if (_siraliKaydetCalisiyor) return; // zaten calisiyor, kuyruga eklendi yeter
  _siraliKaydetCalisiyor = true;
  const PARTI = 20;       // her seferinde 20 grup yaz
  const BEKLE = 400;      // partiler arasi 400ms nefes (DB rahatlasin)
  try {
    while (_siraliKuyruk.length) {
      const parti = _siraliKuyruk.splice(0, PARTI);
      // partiyi sirayla yaz (paralel degil — havuzu doldurmasin)
      for (const chat of parti) {
        try { await db.saveChat(chat); } catch (e) {}
      }
      if (_siraliKuyruk.length) await new Promise(r => setTimeout(r, BEKLE));
    }
  } finally {
    _siraliKaydetCalisiyor = false;
  }
}

async function fetchAllGroups() {
  // Bu fonksiyon SADECE ofis hatti icindir. Global waSock yerine ofis hattinin
  // kendi soketini kullan — yoksa iki hat acikken Volkan'in soketiyle calisip karisir.
  const ofisLine = lines.get('ofis');
  const ofisSock = ofisLine ? ofisLine.sock : waSock;
  if (!ofisSock || !ofisLine || !ofisLine.connected) return;
  try {
    const groups = await ofisSock.groupFetchAllParticipating(); // { jid: metadata }
    let guncellenen = 0;
    for (const [jid, meta] of Object.entries(groups || {})) {
      if (!jid.endsWith('@g.us')) continue;
      const gercekAd = meta.subject && meta.subject.trim() ? meta.subject.trim() : null;
      const uyeSayisi = meta.participants?.length || 0;
      // Grup ADINI her zaman bellege al (isim cozumlemesi + mesaj gelince hemen dogru ad icin).
      // Bu, grubu LISTEYE eklemez — sadece adini hatirlar.
      if (gercekAd) grupAdlari.set(jid, gercekAd);
      // Grup ZATEN listedeyse (yani mesaji varsa) adini/uye sayisini guncelle.
      // Listede DEGILSE EKLEME — kullanici "bos/olu gruplar gorunmesin, mesaj geldikce eklensin" dedi.
      if (chats.has(jid)) {
        const chat = chats.get(jid);
        if (gercekAd) chat.name = gercekAd;
        if (uyeSayisi) chat.memberCount = uyeSayisi;
        if (meta.desc) chat.description = meta.desc;
        guncellenen++;
      }
    }
    broadcastHat('ofis', { type: 'chats', chats: Array.from(chats.values()).map(stripRaw) });
    console.log(`👥 Grup adlari alindi: ${grupAdlari.size} grup adi bellekte, ${guncellenen} aktif grup guncellendi`);
    // ID'de (sayi) kalmis listedeki gruplari grupAdlari'ndan duzelt
    let duzeltilen = 0;
    for (const ch of chats.values()) {
      if (ch.isGroup && /^\d+$/.test(ch.name || '') && grupAdlari.has(ch.jid)) {
        ch.name = grupAdlari.get(ch.jid);
        duzeltilen++;
      }
    }
    // TEK toplu broadcast (eskiden her grup icin ayri broadcast vardi -> panel yavasliyordu)
    if (duzeltilen) {
      console.log(`   🔤 ${duzeltilen} grubun adi ID'den gercek ada duzeltildi`);
      broadcastHat('ofis', { type: 'chats', chats: Array.from(chats.values()).map(stripRaw) });
    }
    broadcastHat('ofis', { type: 'syncStatus', done: true, chatCount: chats.size });
    // SADECE listedeki (mesaji olan) gruplari DB'ye yaz — tum 7547'yi degil (hiz icin).
    if (db.isReady()) {
      const yazilacaklar = Array.from(chats.values()).filter(c => c.isGroup);
      if (yazilacaklar.length) siraliKaydet(yazilacaklar);
    }
  } catch (e) {
    console.error('Gruplar cekilemedi:', e.message);
  }
}

// Bir mesajin kisa onizlemesi (yanit alintisinda gosterilir)
function replyPreview(m) {
  if (m.kind === 'image') return '📷 Fotograf';
  if (m.kind === 'audio') return '🎤 Sesli mesaj';
  if (m.kind === 'video') return '🎬 Video';
  if (m.kind === 'document') return '📄 ' + (m.text || 'Belge');
  return m.text || '';
}

// RAW olmadan alinti (quoted) nesnesi insa et.
// DB'den yuklenen mesajlarda tam ham veri (raw) yok ama key var.
// Baileys'in alinti icin bekledigi minimal yapi: { key, message }.
// Mesajin tipine gore uygun message govdesi olusturuyoruz.
function insaQuotedMesaj(orig) {
  if (!orig || !orig.key) return null;
  // key icinde en az id olmali
  if (!orig.key.id) return null;
  let message;
  const metin = orig.text || orig.caption || '';
  if (orig.kind === 'image') {
    message = { imageMessage: { caption: orig.caption || '' } };
  } else if (orig.kind === 'video') {
    message = { videoMessage: { caption: orig.caption || '' } };
  } else if (orig.kind === 'audio') {
    message = { audioMessage: {} };
  } else if (orig.kind === 'document') {
    message = { documentMessage: { fileName: orig.fileName || orig.text || 'belge', caption: orig.caption || '' } };
  } else {
    // metin mesaji (en yaygin)
    message = { conversation: metin || ' ' };
  }
  return { key: orig.key, message };
}

// Sohbete SISTEM mesaji ekle (grup adi/aciklamasi degisti gibi bilgi satirlari).
// WhatsApp tarzi: ortada kucuk gri bilgi yazisi. DB'ye de kaydedilir (kalici).
function sistemMesajiEkle(jid, metin) {
  const chat = chats.get(jid);
  if (!chat) return;
  const now = Date.now();
  const m = {
    id: 'sys_' + now + '_' + Math.random().toString(36).slice(2, 7),
    kind: 'system',
    text: metin,
    fromMe: false,
    sender: '',
    time: nowTime(),
    ts: now,
  };
  chat.messages.push(m);
  chat.lastTs = now;
  chat.lastTime = m.time;
  if (db.isReady()) db.saveMessage(jid, m).catch(() => {});
}

// Bir hattin chats Map'ini dondur. lineId verilmezse veya 'ofis' ise GLOBAL chats
// (eski sistem — geriye uyumlu). Pazarlama hatlari icin o hattin kendi chats'i.
function hatChats(lineId) {
  if (!lineId || lineId === 'ofis') return chats; // ofis = mevcut global (degismedi)
  const line = lines.get(lineId);
  return line ? line.chats : chats; // hat yoksa guvenli sekilde global'e dus
}

function addMessage(jid, message, meta = {}, lineId = 'ofis') {
  const now = Date.now();
  message.ts = now; // gercek zaman damgasi (siralama icin)
  const C = hatChats(lineId); // bu hattin sohbetleri (ofis ise global chats)
  // AYNI mesaj iki kez eklenmesin (gonderdigimiz mesaji WhatsApp geri yansitir -> cift kayit)
  if (message.id && C.has(jid)) {
    const varolan = C.get(jid).messages.find(x => x.id === message.id);
    if (varolan) {
      // zaten var: sadece eksik bilgileri guncelle (orn. key, mediaUrl), tekrar EKLEME
      let degisti = false;
      if (message.key && !varolan.key) { varolan.key = message.key; degisti = true; }
      if (message.mediaUrl && !varolan.mediaUrl) { varolan.mediaUrl = message.mediaUrl; degisti = true; }
      if (message.thumb && !varolan.thumb) { varolan.thumb = message.thumb; degisti = true; }
      // COZULME: var olan mesaj sifresi cozulemeyen placeholder ise ve simdi gercek
      // icerik (text/medya) geldiyse, onu GUNCELLE (ekrandaki "cozulemedi" yazisi kalksin).
      if (varolan.kind === 'undecryptable' && message.kind && message.kind !== 'undecryptable' && message.kind !== 'skip') {
        varolan.kind = message.kind;
        if (message.text) varolan.text = message.text;
        if (message.contact) varolan.contact = message.contact;
        if (message.contacts) varolan.contacts = message.contacts;
        degisti = true;
        console.log(`🔓 cozulemeyen mesaj upsert ile cozuldu: ${String(message.id).substring(0,12)} -> ${message.kind}`);
      }
      // Eksik bilgi sonradan geldiyse (orn. medya arka planda indi): panele + DB'ye yansit
      if (degisti) {
        broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(C.get(jid)) });
        if (db.isReady()) db.saveMessage(jid, varolan, lineId).catch(() => {});
      }
      return; // cift eklemeyi onle
    }
  }
  if (!C.has(jid)) {
    // Yeni grup ilk mesajla ekleniyor: adini once meta'dan, yoksa grupAdlari belleginden
    // (fetchAllGroups doldurdu), o da yoksa gecici olarak ID'den al (sonra duzelir).
    const grupAdi = jid.endsWith('@g.us') ? (meta.name || grupAdlari.get(jid) || jid.split('@')[0]) : (meta.name || jid.split('@')[0]);
    C.set(jid, {
      jid,
      name: grupAdi,
      isGroup: jid.endsWith('@g.us'),
      description: meta.description || '',
      avatar: meta.avatar || null,
      memberCount: meta.memberCount || 0,
      members: meta.members || [],
      messages: [],
      unread: 0,
      lastTime: message.time,
      lastTs: now,
    });
  }
  const chat = C.get(jid);
  if (meta.name) chat.name = meta.name;
  if (meta.description !== undefined) chat.description = meta.description;
  if (meta.avatar !== undefined && meta.avatar !== null) chat.avatar = meta.avatar;
  if (meta.memberCount) chat.memberCount = meta.memberCount;
  if (meta.members) chat.members = meta.members;
  chat.messages.push(message);
  // BELLEK OPTIMIZASYONU (40 kullanici): her sohbette bellekte en fazla 200 mesaj tut.
  // Daha eskiler bellekten dusurulur (DB'de KALIR — sohbet acilinca oradan yuklenir).
  // Boylece sunucu hafizasi 7500 sohbet x sinirsiz mesaj ile sismez.
  if (chat.messages.length > 200) {
    chat.messages = chat.messages.slice(-200);
  }
  chat.lastTime = message.time;
  chat.lastTs = now;
  if (!message.fromMe) chat.unread++;
  // beni etiketleyen okunmamis mesaj geldiyse isaretle
  if (meta.mentionsMe) chat.hasMention = true;
  broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
  // Supabase'e kaydet (arka planda, mesaji bekletmez)
  // AMA gonderilemeyen mesaji (durum:-1) DB'ye YAZMA — gitmedi, kalici olmamali.
  // (Kullanici silince veya yenileyince kaybolsun; DB'de "hayalet hata mesaji" kalmasin.)
  if (db.isReady() && message.durum !== -1 && !message.gonderilemedi) {
    db.saveChat(chat, lineId).catch((e) => { if (!global._saveChatHataLog) { global._saveChatHataLog = true; console.log('⚠️  saveChat HATASI (ilk): ' + e.message); } });
    db.saveMessage(jid, message, lineId).catch((e) => { if (!global._saveMsgHataLog) { global._saveMsgHataLog = true; console.log('⚠️  saveMessage HATASI (ilk): ' + e.message); } });
  }
}

// raw + key (buyuk/hassas alanlar) panele gonderilmez — sadece sunucuda tutulur
// Ayrica panele en fazla son 120 mesaj gonderilir (performans: 500 mesaji her seferinde yollamak kasiyor)
function stripRaw(chat) {
  // 40 kullanici icin trafik optimizasyonu: her sohbet guncellemesinde son 60 mesaj gonderilir
  const recent = chat.messages.length > 60 ? chat.messages.slice(-60) : chat.messages;
  return {
    ...chat,
    // ACIKLAMA her zaman TANIMLI gitsin: undefined ise panel "eskisini koru" deyip
    // baska grubun aciklamasini gosteriyordu. Grup ise mevcut deger veya '', grup degilse ''.
    description: (chat.isGroup ? (chat.description || '') : ''),
    messages: recent.map(({ raw, key, ...rest }) => rest),
    atananlar: chatAssignments.get(chat.jid) || [], // bu gruba atanan ekip uyeleri
    etiketler: chatLabels.get(chat.jid) || [],      // bu gruba bagli etiket id'leri
  };
}

// Mesajin tipini ve metnini coz
function describeMessage(m) {
  let msg = m.message || {};

  // senderKeyDistributionMessage cogu zaman ASIL mesajin yaninda gelir (grup sifrelemesi).
  // Onu yok sayip kalan gercek icerige bakalim.
  if (msg.senderKeyDistributionMessage) {
    const rest = { ...msg };
    delete rest.senderKeyDistributionMessage;
    delete rest.messageContextInfo;
    // baska bir icerik kaldiysa onu kullan, yoksa bu sadece teknik mesajdir -> atla
    if (Object.keys(rest).length === 0) return { kind: 'skip' };
    msg = rest;
  }

  // Ic ice sarmalanmis mesajlari ac (kaybolan mesaj, tek-seferlik, cihaz mesaji vs.)
  let guard = 0;
  while (guard++ < 5) {
    if (msg.ephemeralMessage?.message) { msg = msg.ephemeralMessage.message; continue; }
    if (msg.viewOnceMessage?.message) { msg = msg.viewOnceMessage.message; continue; }
    if (msg.viewOnceMessageV2?.message) { msg = msg.viewOnceMessageV2.message; continue; }
    if (msg.viewOnceMessageV2Extension?.message) { msg = msg.viewOnceMessageV2Extension.message; continue; }
    if (msg.deviceSentMessage?.message) { msg = msg.deviceSentMessage.message; continue; }
    if (msg.documentWithCaptionMessage?.message) { msg = msg.documentWithCaptionMessage.message; continue; }
    if (msg.editedMessage?.message) { msg = msg.editedMessage.message; continue; }
    if (msg.associatedChildMessage?.message) { msg = msg.associatedChildMessage.message; continue; }
    if (msg.botInvokeMessage?.message) { msg = msg.botInvokeMessage.message; continue; }
    // secretEncryptedMessage: WhatsApp'in yeni nesil sarmalayicisi (etkinlik/ozel mesaj).
    // Gercek icerik bazen icinde bir 'message' alaninda gelir; varsa onu ac.
    if (msg.secretEncryptedMessage?.message) { msg = msg.secretEncryptedMessage.message; continue; }
    // bazi surumlerde gercek icerik 'targetMessage' altinda olabilir
    if (msg.secretEncryptedMessage?.targetMessage?.message) { msg = msg.secretEncryptedMessage.targetMessage.message; continue; }
    // sarmalama sonrasi tekrar senderKey cikarsa onu da temizle
    if (msg.senderKeyDistributionMessage && Object.keys(msg).filter(k => k !== 'senderKeyDistributionMessage' && k !== 'messageContextInfo').length > 0) {
      const r = { ...msg }; delete r.senderKeyDistributionMessage; delete r.messageContextInfo; msg = r; continue;
    }
    break;
  }

  if (msg.conversation) return { kind: 'text', text: msg.conversation };
  if (msg.extendedTextMessage?.text) return { kind: 'text', text: msg.extendedTextMessage.text };
  if (msg.imageMessage) return { kind: 'image', text: msg.imageMessage.caption || '' };
  if (msg.videoMessage) return { kind: 'video', text: msg.videoMessage.caption || '' };
  if (msg.audioMessage) return { kind: 'audio', text: '' };
  // BELGE: dosya adi + (varsa) ACIKLAMA metni (caption). WhatsApp'ta belgeye yazilan not
  // documentMessage.caption'da VEYA documentWithCaptionMessage sarmalayicisinda gelir.
  if (msg.documentWithCaptionMessage?.message?.documentMessage) {
    const dm = msg.documentWithCaptionMessage.message.documentMessage;
    return { kind: 'document', text: dm.fileName || 'Belge', caption: dm.caption || '', _fileName: dm.fileName || '', _mime: dm.mimetype || '' };
  }
  if (msg.documentMessage) {
    return { kind: 'document', text: msg.documentMessage.fileName || 'Belge', caption: msg.documentMessage.caption || '', _fileName: msg.documentMessage.fileName || '', _mime: msg.documentMessage.mimetype || '' };
  }
  if (msg.stickerMessage) return { kind: 'sticker', text: '' };
  // Yaygin diger tipler
  if (msg.locationMessage) {
    const lat = msg.locationMessage.degreesLatitude, lng = msg.locationMessage.degreesLongitude;
    return { kind: 'text', text: '📍 Konum: ' + lat + ', ' + lng };
  }
  if (msg.contactMessage) {
    const name = msg.contactMessage.displayName || '';
    const vcard = msg.contactMessage.vcard || '';
    const phoneMatch = vcard.match(/waid=(\d+)/) || vcard.match(/TEL[^:]*:([+\d\s()-]+)/i);
    let phone = '';
    if (phoneMatch) phone = phoneMatch[1].replace(/[^\d]/g, '');
    return { kind: 'contact', text: name, _contact: { name, phone } };
  }
  if (msg.contactsArrayMessage) {
    const arr = msg.contactsArrayMessage.contacts || [];
    const list = arr.map(ct => {
      const vcard = ct.vcard || '';
      const pm = vcard.match(/waid=(\d+)/) || vcard.match(/TEL[^:]*:([+\d\s()-]+)/i);
      return { name: ct.displayName || '', phone: pm ? pm[1].replace(/[^\d]/g, '') : '' };
    });
    return { kind: 'contacts', text: arr.length + ' kisi', _contacts: list };
  }
  if (msg.pollCreationMessage || msg.pollCreationMessageV3) {
    const p = msg.pollCreationMessage || msg.pollCreationMessageV3;
    return { kind: 'text', text: '📊 Anket: ' + (p.name || '') };
  }
  // Albüm (toplu fotograf/video gonderimi) - bir sarmalayici.
  // Icindeki foto/videolar ayri mesajlar olarak ZATEN gelir, o yuzden albumun kendisini atla.
  if (msg.albumMessage) {
    return { kind: 'skip' };
  }
  // Canli konum paylasimi
  if (msg.liveLocationMessage) {
    return { kind: 'text', text: '📍 Canlı konum paylaşıldı' };
  }
  // Reaksiyon (bir mesaja emoji ile tepki)
  if (msg.reactionMessage) {
    const emoji = msg.reactionMessage.text || '';
    return { kind: 'reaction', text: emoji, _reactKey: msg.reactionMessage.key };
  }
  // Taninmayan tip
  const keys = Object.keys(msg).filter(k => k !== 'messageContextInfo');
  const realType = keys[0] || 'bos-mesaj';
  // Gercek icerik tasimayan teknik/sistem mesajlari - kullaniciya gosterme
  const skipTypes = [
    'protocolMessage', 'senderKeyDistributionMessage', 'associatedChildMessage',
    'messageContextInfo', 'reactionMessage', 'pollUpdateMessage', 'keepInChatMessage',
    'deviceSentMessage', 'botInvokeMessage', 'encReactionMessage', 'pinInChatMessage',
    'pollResultSnapshotMessage', 'eventCoverImage', 'statusMentionMessage',
    // secretEncryptedMessage: yukaridaki dongude acilamadiysa (gercekten sifreli/ic
    // icerik okunamadi) — kafa karistiran "sifresi cozulemedi" yerine SESSIZCE atla.
    // Bu tip mesajin gercek hali cogunlukla ayri bir mesaj olarak zaten gelir.
    'secretEncryptedMessage',
  ];
  if (keys.length === 0 || skipTypes.includes(realType)) {
    return { kind: 'skip' };
  }
  console.log('⚠️  Desteklenmeyen mesaj. Bulunan alanlar:', JSON.stringify(keys));
  // Sifreleme/anahtar sorunu olan mesajlar icin kullanici dostu aciklama
  return { kind: 'undecryptable', text: 'Bu mesajın şifresi çözülemedi. Gönderenin mesajı tekrar göndermesini isteyebilirsin.' };
}

// Sessiz logger (Baileys'in beklediği formatta) — console logger stream hatasi firlatabiliyor
const silentLogger = {
  level: 'silent',
  child: () => silentLogger,
  trace: () => {}, debug: () => {}, info: () => {},
  warn: () => {}, error: () => {}, fatal: () => {},
};

// Profil fotosu onbellegi (her seferinde cekmemek icin)
const avatarCache = new Map(); // jid -> url | null
// Kisi isimleri onbellegi (uye listesi + etiketleme icin)
const contactNames = new Map(); // jid -> isim (pushName veya rehber)
const savedContacts = new Map(); // jid -> SADECE telefon rehberine kayitli isim
const groupMetaCache = new Map(); // grup jid -> { meta, ts } (tekrar tekrar cekmeyi onler)
// LID -> gercek numara (PN) esleme onbellegi
const lidToPn = new Map(); // '...@lid' -> '...@s.whatsapp.net'
// Grup adlari (jid -> ad). fetchAllGroups ile doldurulur; grubu listeye EKLEMEDEN
// adini hatirlamak icin. Mesaj gelince yeni grup eklenirken dogru ad hemen kullanilir.
const grupAdlari = new Map();
// Gruba ATAMA: chat_jid -> [username, ...] (hangi ekip uyeleri bu grupla ilgileniyor)
// Supabase'den acilista yuklenir, degisince oraya yazilir.
const chatAssignments = new Map();
// ETIKETLER: labels = [{id,name,color}], chatLabels = chat_jid -> [labelId,...]
let labels = [];
const chatLabels = new Map();

// Grup adi/aciklamasi degisiminde sistem mesaji EKLENSIN mi?
// Sunucu yeni acildiginda WhatsApp GECMIS tum grup degisikliklerini birden gonderir
// (senkron). Bunlari "yeni degisti" sanip panele doldurmamak icin: acilistan sonra
// kisa bir sure (isinma) sistem mesaji EKLEME. Sure dolunca gercek CANLI degisiklikler eklenir.
let grupDegisimCanli = false;
function grupDegisimCanliyiAc() {
  // her baglanti acilisindan 35sn sonra canli moda gec (senkron bitmis olur)
  grupDegisimCanli = false;
  setTimeout(() => { grupDegisimCanli = true; console.log('✅ grup degisim bildirimleri artik CANLI (gecmis senkron bitti)'); }, 35000);
}

// Kisi sohbeti jid'ini tek standart forma getir (ayni kisi = tek sohbet)
// lineId: hangi hattin numarasini "kendi numaram" sayacagiz. Ofis -> global myNumber,
//         pazarlama -> o hattin kendi numarasi (line.myNumber). Bu KRITIK: yoksa ofis
//         Volkan'a yazinca, Volkan'in hatti gelen mesaji "kendine mesaj" sanip fromMe gibi gosterir.
function normalizeChatJid(jid, m, lineId = 'ofis') {
  if (!jid) return jid;
  // bu hattin kendi numarasi (kendine mesaj tespiti icin)
  const benimNumaram = lineId === 'ofis' ? myNumber : (lines.get(lineId)?.myNumber || null);
  // Kendine mesaj: senin numaranin her varyasyonu tek jid olsun
  if (benimNumaram) {
    const num = jid.split('@')[0];
    if (num === benimNumaram) return benimNumaram + '@s.whatsapp.net';
  }
  // LID ise gercek numaraya cevirmeyi dene (birden cok yoldan)
  if (jid.endsWith('@lid')) {
    const alt = m?.key?.remoteJidAlt || m?.key?.participantAlt || m?.key?.remoteJidPn || null;
    const r = resolvePhone(jid, alt);
    if (!r.isLid && r.jid && r.jid.endsWith('@s.whatsapp.net')) return r.jid; // cozulduyse normal numara
    // onbellekte eslesme var mi? (resolvePhone disinda son bir kontrol)
    if (lidToPn.has(jid)) {
      const pn = lidToPn.get(jid);
      if (pn && pn.endsWith('@s.whatsapp.net')) return pn;
    }
    return jid; // cozulemezse LID kalsin
  }
  // @s.whatsapp.net disindaki kucuk varyasyonlari standarda cek
  if (jid.endsWith('@s.whatsapp.net')) return jid;
  if (jid.endsWith('@c.us')) return jid.split('@')[0] + '@s.whatsapp.net';
  return jid;
}

// Iki sohbeti birlestir: LID'li sohbet, numara cozulunce numara sohbetine tasinir.
// Boylece ayni kisi 2-3 kez gorunmez, tek sohbette toplanir.
function sohbetleriBirlestir(lidJid, numaraJid) {
  if (lidJid === numaraJid) return;
  const lidChat = chats.get(lidJid);
  const numChat = chats.get(numaraJid);
  if (!lidChat) return; // birlestirilecek LID sohbeti yok
  if (!numChat) {
    // numara sohbeti yoksa: LID sohbetini numaraya tasi (jid degistir)
    lidChat.jid = numaraJid;
    chats.set(numaraJid, lidChat);
    chats.delete(lidJid);
    broadcastHat('ofis', { type: 'chatMerged', oldJid: lidJid, newJid: numaraJid });
    broadcastHat('ofis', { type: 'message', jid: numaraJid, chat: stripRaw(lidChat) });
    return;
  }
  // her iki sohbet de varsa: mesajlari birlestir (id'ye gore tekrarsiz)
  const mevcutIdler = new Set(numChat.messages.map(x => x.id));
  for (const msg of lidChat.messages) {
    if (!mevcutIdler.has(msg.id)) numChat.messages.push(msg);
  }
  // zamana gore sirala
  numChat.messages.sort((a, b) => (a.ts || 0) - (b.ts || 0));
  // son 200 ile sinirla
  if (numChat.messages.length > 200) numChat.messages = numChat.messages.slice(-200);
  // okunmamis + son zaman guncelle
  numChat.unread = (numChat.unread || 0) + (lidChat.unread || 0);
  if ((lidChat.lastTs || 0) > (numChat.lastTs || 0)) { numChat.lastTs = lidChat.lastTs; numChat.lastTime = lidChat.lastTime; }
  chats.delete(lidJid);
  broadcastHat('ofis', { type: 'chatMerged', oldJid: lidJid, newJid: numaraJid });
  broadcastHat('ofis', { type: 'message', jid: numaraJid, chat: stripRaw(numChat) });
  console.log(`🔗 sohbet birlestirildi: ${lidJid.split('@')[0]} -> ${numaraJid.split('@')[0]}`);
}

// Bir JID'den gercek telefon numarasini cikarmaya calis.
// LID (gizli kimlik) ise once esleme deposuna, sonra onbellege bakar.
function resolvePhone(jidRaw, altJid) {
  if (!jidRaw) return { jid: jidRaw, number: '' };
  // Zaten normal numaraysa direkt dondur
  if (jidRaw.endsWith('@s.whatsapp.net')) {
    return { jid: jidRaw, number: jidRaw.split('@')[0] };
  }
  // LID ise: alternatif alandan gelen gercek numara var mi?
  if (jidRaw.endsWith('@lid')) {
    // 1) mesajla birlikte gelen alternatif (PN) alani
    if (altJid && altJid.endsWith('@s.whatsapp.net')) {
      lidToPn.set(jidRaw, altJid);
      return { jid: altJid, number: altJid.split('@')[0] };
    }
    // 2) daha once eslestirdiysek onbellekten
    if (lidToPn.has(jidRaw)) {
      const pn = lidToPn.get(jidRaw);
      return { jid: pn, number: pn.split('@')[0] };
    }
    // 3) Baileys'in dahili LID->PN deposu
    try {
      const store = waSock?.signalRepository?.lidMapping;
      if (store?.getPNForLID) {
        const pn = store.getPNForLID(jidRaw);
        if (pn && pn.endsWith('@s.whatsapp.net')) {
          lidToPn.set(jidRaw, pn);
          return { jid: pn, number: pn.split('@')[0] };
        }
      }
    } catch (e) {}
    // cozulemedi: LID numarasini goster (gercek numara WhatsApp tarafindan gizli)
    return { jid: jidRaw, number: jidRaw.split('@')[0], isLid: true };
  }
  return { jid: jidRaw, number: jidRaw.split('@')[0] };
}

// Mesaj metnindeki @numara etiketlerini, biliniyorsa isimle degistir.
// mentionedJid: etiketlenen kisilerin jid listesi (contextInfo'dan)
// uyeler: o grubun uye listesi (varsa) — LID'i uye listesinden cozmek icin
function prettifyMentions(text, mentionedJids, uyeler) {
  if (!text || !mentionedJids || !mentionedJids.length) return { text, mentions: [] };
  let out = text;
  const mentions = []; // panele gidecek: [{ display, jid, number }]
  for (const mj of mentionedJids) {
    const num = (mj || '').split('@')[0];
    if (!num) continue;
    // etiketlenen BEN miyim? (numaram veya LID'im)
    const benMiyim = (myNumber && num === myNumber) || (myLID && num === myLID);
    // LID ise once numaraya cevirmeyi dene. ONCE resolvePhone (Baileys dahili depo dahil),
    // sonra lidToPn. Boylece daha fazla LID cozulur, "@kişi" azalir.
    let cozumNum = num;
    let cozumJid = mj;
    if (mj.endsWith('@lid')) {
      const r = resolvePhone(mj, null);
      if (r && !r.isLid && r.jid && r.jid.endsWith('@s.whatsapp.net')) {
        cozumJid = r.jid;
        cozumNum = r.number || num;
      } else if (lidToPn.has(mj)) {
        const pn = lidToPn.get(mj);
        cozumJid = pn;
        cozumNum = (pn || '').split('@')[0] || num;
      }
    }
    // Grup UYE listesinden isim/numara bulmayi dene (etiketlenen kisi uyeyse)
    let uyeAdi = '';
    let uyeNum = '';
    if (uyeler && uyeler.length) {
      // hem LID hem cozulmus numara ile uye ara
      const aday = uyeler.find(u => {
        const ujid = u.jid || '';
        const unum = (ujid.split('@')[0]) || (u.number || '');
        return ujid === mj || ujid === cozumJid || unum === num || unum === cozumNum;
      });
      if (aday) {
        uyeAdi = aday.name && !/^\d+$/.test(aday.name) ? aday.name : '';
        uyeNum = (aday.jid ? aday.jid.split('@')[0] : aday.number) || '';
      }
    }
    const name = benMiyim ? 'Ben'
               : (savedContacts.get(mj)
                  || savedContacts.get(cozumJid)
                  || savedContacts.get(cozumNum + '@s.whatsapp.net')
                  || contactNames.get(mj)
                  || contactNames.get(cozumJid)
                  || contactNames.get(cozumNum + '@s.whatsapp.net')
                  || uyeAdi
                  || '');
    // GORUNUM onceligi:
    //  1) Kayitli isim varsa -> "@isim"
    //  2) Isim yok ama NUMARA biliniyorsa (normal/cozulmus LID/uye numarasi) -> "@numara"
    //  3) Hicbiri yok -> "@kişi"
    let display;
    if (name) {
      display = '@' + name;
    } else if (!mj.endsWith('@lid')) {
      display = '@' + num;                       // normal numara
    } else if (cozumNum !== num) {
      display = '@' + cozumNum;                   // LID cozuldu (resolvePhone/lidToPn)
    } else if (uyeNum) {
      display = '@' + uyeNum;                     // uye listesinden numara
    } else {
      display = '@kişi';                          // gercekten cozulemedi
    }
    out = out.split('@' + num).join(display);

    // Tiklayinca acilacak sohbet jid'ini belirle:
    //  - Normal numara ise dogrudan kullan.
    //  - LID cozulduyse (resolvePhone/lidToPn) veya uye listesinden numara bulunduysa onu kullan.
    let tiklanabilirJid = null;
    let tiklanabilirNum = null;
    if (!mj.endsWith('@lid')) {
      tiklanabilirJid = num + '@s.whatsapp.net';
      tiklanabilirNum = num;
    } else if (cozumNum !== num) {
      tiklanabilirJid = cozumNum + '@s.whatsapp.net';
      tiklanabilirNum = cozumNum;
    } else if (uyeNum) {
      tiklanabilirJid = uyeNum + '@s.whatsapp.net';
      tiklanabilirNum = uyeNum;
    }
    mentions.push({
      display,                       // "@Pekcan Sigorta Emre"
      jid: tiklanabilirJid,          // numarasi biliniyorsa dolu, LID+bilinmiyorsa null
      number: tiklanabilirNum,
      benMiyim,
    });
  }
  return { text: out, mentions };
}
// Bir URL'den resmi indirip diske kaydet, web yolunu dondur
function downloadToFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);
    https.get(url, (res) => {
      if (res.statusCode !== 200) { file.close(); fs.unlink(filePath, () => {}); return reject(new Error('HTTP ' + res.statusCode)); }
      res.pipe(file);
      file.on('finish', () => file.close(() => resolve(true)));
    }).on('error', (e) => { file.close(); fs.unlink(filePath, () => {}); reject(e); });
  });
}
async function getAvatar(jid, taze = false) {
  // taze=false: onbellekten ver (hizli). taze=true: WhatsApp'tan YENIDEN cek (logo degistiyse yakala).
  if (!taze && avatarCache.has(jid)) return avatarCache.get(jid);
  let result = null;
  try {
    // 8 sn zaman asimi: bazi (LID) jid'lerde profilePictureUrl sonsuza kadar bekleyebilir
    const urlPromise = waSock.profilePictureUrl(jid, 'image');
    const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('avatar zaman asimi')), 8000));
    const url = await Promise.race([urlPromise, timeout]);
    if (url) {
      const safe = jid.split('@')[0].replace(/[^\d]/g, '');
      // taze cekimde dosya adina zaman damgasi ekle ki tarayici ESKI logoyu cache'ten gostermesin
      const fname = taze ? `pp_${safe}_${Date.now()}.jpg` : `pp_${safe}.jpg`;
      try {
        await downloadToFile(url, path.join(MEDIA_DIR, fname));
        result = '/media/' + fname;
      } catch (e) {
        result = url;
      }
    }
  } catch (e) { result = null; }
  avatarCache.set(jid, result);
  return result;
}

// Medyayi indir, public/media'ya kaydet, web yolunu dondur.
// 30 sn icinde inmezse veya hata olursa null doner — sunucu ASLA cokmemeli.
async function saveMedia(m, kind, sock = waSock) {
  const extMap = { image: 'jpg', video: 'mp4', audio: 'ogg', document: '', sticker: 'webp' };
  try {
    const downloadPromise = downloadMediaMessage(
      m, 'buffer', {},
      { logger: silentLogger, reuploadRequest: (sock || waSock).updateMediaMessage }
    );
    // zaman asimi: 30 sn icinde inmezse vazgec
    const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('indirme zaman asimi')), 30000));
    const buffer = await Promise.race([downloadPromise, timeout]);

    let ext = extMap[kind] || 'bin';
    if (kind === 'document') {
      // belge documentMessage VEYA documentWithCaptionMessage icinde olabilir
      const docM = m.message?.documentMessage
                || m.message?.documentWithCaptionMessage?.message?.documentMessage
                || m.message?.ephemeralMessage?.message?.documentMessage
                || m.message?.viewOnceMessage?.message?.documentMessage;
      const fn = docM?.fileName || '';
      ext = fn.includes('.') ? fn.split('.').pop() : 'bin';
    }
    const fileName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}.${ext}`;
    fs.writeFileSync(path.join(MEDIA_DIR, fileName), buffer);
    return '/media/' + fileName;
  } catch (e) {
    console.error('Medya indirilemedi:', e.message);
    return null;
  }
}

// ---- WhatsApp baglantisi ----
let _waStarting = false;
let _reconnectGecikme = 3000; // gecici kopmada yeniden baglanma beklemesi (backoff ile artar)
// startWA(lineId): bir HATTI baslatir. Varsayilan 'ofis' (geriye uyumlu).
// Her hat kendi auth klasorunu (auth/<lineId>) ve kendi line objesini kullanir.
async function startWA(lineId = 'ofis') {
  // bu hat icin line objesini al/olustur
  let line = lines.get(lineId);
  if (!line) { line = createLine(lineId, lineId === 'ofis' ? 'Ofis Ana Hat' : lineId); lines.set(lineId, line); }
  if (line.starting) return; // bu hat zaten baglaniyor, cift baslatma
  line.starting = true;
  activeLine = line; // su an islem yapilan hat (kopru icin)
  _waStarting = true; // (eski global bayrak — geriye uyumluluk)

  // her hattin KENDI auth klasoru: auth/<lineId>
  const { state, saveCreds } = await useMultiFileAuthState(line.authDir);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version, auth: state,
    logger: silentLogger,         // ÖNEMLI: Baileys'in JSON log selini sustur (terminal okunabilir kalsin)
    printQRInTerminal: false,
    browser: ['Anka CRM', 'Chrome', '1.0.0'],
    syncFullHistory: false,       // tüm gecmisi cekme — 7500+ sohbette sunucuyu bogup CANLI mesajlari engelliyordu.
                                  // false = baglaninca sadece yakin gecmis gelir, canli akisa hemen gecer.
    markOnlineOnConnect: false,   // panel "cevrimici" gorunmesin — cevrimici iken WhatsApp bazi gelen
                                  // mesaj bildirimlerini farkli/eksik iletebiliyor. false daha guvenilir akis verir.
    // ↓↓↓ BAGLANTI KARARLILIGI (surekli kopma + "Precondition Required" + sendRetryRequest hatasi icin) ↓↓↓
    // KRITIK: getMessage — WhatsApp bir mesaji cozemeyip "tekrar gonder" (retry) isterse,
    // Baileys o mesaji bizden ister. Bu fonksiyon yoksa retry basarisiz olup BAGLANTI DUSUYOR.
    // Bellekteki mesaj deposundan ilgili mesaji dondururuz -> retry basarili -> baglanti kopmaz.
    getMessage: async (key) => {
      try {
        const jid = key.remoteJid;
        const C = hatChats(lineId);
        const chat = C && C.get ? C.get(jid) : null;
        if (chat && chat.messages) {
          const m = chat.messages.find(x => x && x.id === key.id);
          if (m && m._raw) return m._raw.message || undefined;
        }
      } catch (e) {}
      return undefined; // bulunamazsa undefined (Baileys bos mesajla devam eder, kopmaz)
    },
    retryRequestDelayMs: 350,       // retry istekleri arasi bekleme (cok hizli retry WhatsApp'i kizdirir)
    maxMsgRetryCount: 3,            // bir mesaj icin en fazla 3 retry (sonsuz retry dongüsünü onler)
    connectTimeoutMs: 60000,        // baglanti kurma zaman asimi (yavas agda kopmasin)
    keepAliveIntervalMs: 15000,     // 15 sn'de bir "hayatta miyim" sinyali -> olu baglanti erken yakalanir
    emitOwnEvents: false,           // kendi gonderdigimiz mesajlari geri event olarak alma (gereksiz yuk)
  });
  line.sock = sock;   // hattin kendi soketi (HER hat icin dogru — bunu kullan)
  // KRITIK: global 'waSock' koprusu SADECE ofis hatti icin guncellensin.
  // Eskiden her hat (pazarlama dahil) burada waSock'u eziyordu -> ofis paneli mesaj
  // atinca son baglanan pazarlama hattinin soketinden gidiyordu (Volkan'in numarasindan!).
  // Artik waSock hep ofis soketi; pazarlama hatlari line.sock ile izole calisir.
  if (lineId === 'ofis') waSock = sock;

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      console.log('\n📱 QR kodu telefonundan okut (Ayarlar > Bagli cihazlar > Cihaz bagla):\n');
      qrcode.generate(qr, { small: true });
      // QR'i resim (data URL) olarak panele de gonder — HEMEN, beklemeden.
      // SADECE bu hattin kullanicilarina gider (pazarlamacinin QR'i ofise gitmez).
      QRImage.toDataURL(qr, { width: 280, margin: 1 }, (err, url) => {
        if (!err && url) {
          line.lastQR = url;
          if (lineId === 'ofis') lastQR = url; // ofis icin global de guncel kalsin (eski kod)
          broadcastHat(lineId, { type: 'status', connected: false, qr: true, qrImage: url });
          console.log(`   ✅ QR panele gonderildi (hat: ${lineId}).`);
        } else {
          broadcastHat(lineId, { type: 'status', connected: false, qr: true });
        }
      });
    }
    if (connection === 'open') {
      line.connected = true;       // hattin kendi durumu
      line.starting = false;       // hat baglandi
      line.lastQR = null;
      const myJid = sock.user?.id ? sock.user.id.split(':')[0].split('@')[0] + '@s.whatsapp.net' : null;
      const myName = sock.user?.name || sock.user?.verifiedName || 'Ben';
      const buNumara = sock.user?.id ? sock.user.id.split(':')[0].split('@')[0] : null;
      const buLID = sock.user?.lid ? sock.user.lid.split(':')[0].split('@')[0] : null;
      line.myNumber = buNumara; line.myLID = buLID; line.myName = myName;
      // Global'ler (myNumber, waConnected, lastQR) SADECE ofis hatti icin guncellensin —
      // pazarlama hatti baglaninca ofisin global durumunu EZMESIN.
      if (lineId === 'ofis') {
        waConnected = true;
        _waStarting = false;
        _sonWaAktivite = Date.now();
        lastQR = null;
        myNumber = buNumara; myLID = buLID;
      }
      _reconnectGecikme = 3000;
      console.log(`\n✅ WhatsApp baglandi (hat: ${lineId})! Panel: http://localhost:${PORT}\n`);
      console.log(`   👤 numaram: ${buNumara}${buLID ? ' | LID: ' + buLID : ''}`);
      broadcastHat(lineId, { type: 'status', connected: true, myJid, myName });
      if (lineId === 'ofis') {
        // ---- OFIS HATTI (mevcut davranis, degismedi) ----
        // GUVENCE: bellek bossa DB'den sohbetleri geri yukle.
        if (chats.size === 0 && db.isReady()) {
          console.log('   📂 Bellek bos — DB\'den sohbetler yeniden yukleniyor...');
          try {
            await loadFromDB();
            broadcastHat('ofis', { type: 'chats', chats: Array.from(chats.values()).map(stripRaw) });
            console.log(`   ✅ ${chats.size} sohbet DB'den geri yuklendi.`);
          } catch (e) { console.log('   ⚠️  DB yukleme hatasi: ' + e.message); }
        }
        // Katildigim TUM gruplari cek (ofis ortak hatti — tum gruplari gorur)
        setTimeout(() => fetchAllGroups(), 8000);
        setTimeout(() => fetchAllGroups(), 30000);
        setTimeout(() => fetchAllGroups(), 75000);
      } else {
        // ---- PAZARLAMA HATTI ----
        // Kullanicinin istegi: ESKI gruplari toplu CEKME. Sadece QR sonrasi GELEN mesajlar
        // bu hatta kaydedilir. Onceki girislerde kendi hattina kaydedilenleri DB'den yukle.
        if (db.isReady()) {
          try {
            const veri = await db.loadAll(lineId); // SADECE bu hattin sohbetleri
            line.chats.clear();
            for (const row of veri.chats) {
              line.chats.set(row.jid, {
                jid: row.jid, name: row.name || row.jid.split('@')[0],
                isGroup: row.is_group, description: row.description || '',
                avatar: row.avatar || null, memberCount: row.member_count || 0,
                members: row.members || [], messages: [],
                unread: row.unread || 0, lastTime: row.last_time || '', lastTs: Number(row.last_ts) || 0,
                pinned: row.pinned, archived: row.archived, hasMention: row.has_mention,
              });
            }
            broadcastHat(lineId, { type: 'chats', chats: Array.from(line.chats.values()).map(stripRaw) });
            console.log(`   ✅ Pazarlama hatti '${lineId}': ${line.chats.size} kendi sohbeti yuklendi (eski gruplar cekilmedi).`);
          } catch (e) { console.log(`   ⚠️  Pazarlama hatti yukleme hatasi (${lineId}): ` + e.message); }
        }
        // Pazarlama icin fetchAllGroups YOK (eski gruplar karismasin, sadece canli akis).
      }
      // PERIYODIK tazeleme: her 10 dakikada bir OFIS grup adlarini yenile.
      // Boylece sonradan ID'de kalan/yeni gruplarin adlari otomatik duzelir.
      // (fetchAllGroups zaten kendi icinde sadece ofis icin calisir.)
      if (lineId === 'ofis' && !global._grupTazelemeTimer) {
        global._grupTazelemeTimer = setInterval(() => {
          const ol = lines.get('ofis');
          if (ol && ol.connected) fetchAllGroups();
        }, 10 * 60 * 1000); // 10 dakika
      }
      // CANLILIK KONTROLU: WhatsApp "yari-acik" kalabilir (baglanti acik gorunur ama
      // veri akmaz). Duzenli kontrol: uzun suredir hic aktivite yoksa, baglantiyi
      // canli tutmak icin hafif bir istek at; yanit gelmezse "olu" say ve panele bildir.
      if (!global._canlilikTimer) {
        global._canlilikTimer = setInterval(async () => {
          if (!waConnected || !waSock) return;
          const gecenSure = Date.now() - _sonWaAktivite;
          // 75 saniyedir hic veri gelmedi -> baglanti GERCEKTEN canli mi diye sunucuya soralim.
          if (gecenSure > 75 * 1000) {
            // GERCEK TEST: WhatsApp sunucusundan YANIT bekleyen bir sorgu yap.
            // sendPresenceUpdate yetmiyor (yanit beklemez, olu baglantida bile "gecer").
            // onWhatsApp/query sunucudan cevap bekler — cevap gelmezse baglanti OLUDUR.
            let canli = false;
            try {
              const test = await Promise.race([
                // kendi numaramizi sorgula — sunucudan donus bekler
                (myNumber ? waSock.onWhatsApp(myNumber) : waSock.query({
                  tag: 'iq',
                  attrs: { to: '@s.whatsapp.net', type: 'get', xmlns: 'w:p' },
                })),
                new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000)),
              ]);
              // donus geldiyse (herhangi bir sonuc) baglanti canli
              if (test !== undefined) canli = true;
            } catch (e) {
              canli = false;
            }
            if (canli) {
              _sonWaAktivite = Date.now(); // baglanti gercekten canli
            } else {
              // YANIT GELMEDI -> baglanti OLU. Panele bildir + yeniden baglan.
              console.log('⚠️  WhatsApp sunucusu yanit vermiyor (olu/yari-acik baglanti). Yeniden baglaniliyor...');
              if (lineId === 'ofis') waConnected = false;
              line.connected = false;
              broadcastHat(lineId, { type: 'status', connected: false, oluBaglanti: true });
              try { waSock.end(new Error('canlilik kontrolu basarisiz')); } catch (_) {}
              try { waSock.ws?.close?.(); } catch (_) {}
              if (!line.manualLogout) setTimeout(() => startWA(lineId), 3000); // bu HATTI yeniden baslat
            }
          }
        }, 25 * 1000); // her 25 saniyede kontrol
      }
    }
    if (connection === 'close') {
      if (lineId === 'ofis') waConnected = false;
      line.connected = false;
      line.starting = false;
      _waStarting = false; // koptu, yeniden baslatilabilir
      const code = lastDisconnect?.error?.output?.statusCode;
      broadcastHat(lineId, { type: 'status', connected: false });
      if (code === DisconnectReason.loggedOut) {
        // OTURUM GECERSIZ (telefondan cikis, baska cihaz cakismasi, 401/440).
        // Bozuk auth ile tekrar denemek ayni hataya dusurur (sonsuz dongü) — bu yuzden
        // auth klasorunu OTOMATIK temizle ki temiz bir QR uretebilelim.
        // Boylece elle "auth klasorunu sil" yapmaya gerek kalmaz.
        console.log('⚠️  Oturum gecersiz oldu (telefondan cikis/cakisma olabilir). Auth temizleniyor, yeni QR uretilecek...');
        try {
          fs.rmSync(line.authDir, { recursive: true, force: true }); // bu HATTIN auth'u
          console.log('   🗑️  auth klasoru temizlendi.');
        } catch (e) { console.error('   auth temizlenemedi:', e.message); }
        if (lineId === 'ofis') { myNumber = null; myLID = null; lastQR = null; }
        line.myNumber = null; line.myLID = null; line.lastQR = null;
        _reconnectGecikme = 3000;
        // panele bildir: baglanti gitti, yeni QR geliyor
        broadcastHat(lineId, { type: 'status', connected: false, loggedOut: true });
        if (!line.manualLogout) setTimeout(() => startWA(lineId), 2000); // bu HATTI yeniden baslat
      } else {
        // Gecici kopma: ust uste kopmalarda WhatsApp'i kizdirmamak icin artan bekleme
        // (3sn -> 6 -> 12 -> 24 ... en fazla 60sn). Basarili baglantida 3sn'ye doner.
        const bekle = _reconnectGecikme;
        _reconnectGecikme = Math.min(_reconnectGecikme * 2, 60000);
        console.log(`Baglanti koptu, ${Math.round(bekle / 1000)} sn sonra yeniden baglaniyorum...`);
        setTimeout(() => startWA(lineId), bekle); // bu HATTI yeniden baslat
      }
    }
  });

  sock.ev.on('creds.update', saveCreds);

  // Telefon rehberindeki kayitli isimleri al (Busra Dogan gibi)
  function kaydetKisiler(contacts) {
    if (!Array.isArray(contacts)) return;
    let n = 0;
    for (const ct of contacts) {
      const jid = ct.id;
      if (!jid) continue;
      const num = jid.split('@')[0];
      // 1) rehbere kaydedilmis gercek isim (en oncelikli)
      const rehberIsmi = ct.name || ct.verifiedName;
      if (rehberIsmi && rehberIsmi.trim()) {
        savedContacts.set(jid, rehberIsmi.trim());
        if (num) savedContacts.set(num + '@s.whatsapp.net', rehberIsmi.trim());
        contactNames.set(jid, rehberIsmi.trim());
        if (num) contactNames.set(num + '@s.whatsapp.net', rehberIsmi.trim());
        n++;
      }
      // 2) rehber yoksa, en azindan notify (kisinin kendi koydugu isim) yedek olsun
      else if (ct.notify && ct.notify.trim()) {
        if (!contactNames.has(jid)) { contactNames.set(jid, ct.notify.trim()); if (num) contactNames.set(num + '@s.whatsapp.net', ct.notify.trim()); }
      }
    }
    if (n) console.log(`📇 ${n} kayitli rehber ismi alindi (toplam rehber: ${savedContacts.size})`);
  }
  sock.ev.on('contacts.set', ({ contacts }) => { console.log(`📇 contacts.set tetiklendi: ${contacts?.length||0} kisi`); kaydetKisiler(contacts); });
  sock.ev.on('contacts.upsert', (contacts) => { console.log(`📇 contacts.upsert tetiklendi: ${contacts?.length||0} kisi`); kaydetKisiler(contacts); });
  sock.ev.on('contacts.update', (contacts) => { kaydetKisiler(contacts); });


  // Baglaninca WhatsApp son sohbetleri ve mesajlari gonderir - bunlari panele yukle
  sock.ev.on('messaging-history.set', async ({ chats: histChats, messages: histMessages, isLatest }) => {
    // PAZARLAMA hatlari icin gecmis YUKLEME — kullanici istegi: sadece QR sonrasi gelen
    // mesajlar gorunsun, eski toplu gecmis cekilmesin. Sadece ofis hatti gecmis yukler.
    if (lineId !== 'ofis') return;
    try {
      // 1) Sohbet listesini doldur (isim, son zaman)
      if (Array.isArray(histChats)) {
        for (const hc of histChats) {
          let jid = hc.id;
          if (!jid || jid === 'status@broadcast' || jid.endsWith('@newsletter')) continue;
          const isG = jid.endsWith('@g.us');
          if (!isG) jid = normalizeChatJid(jid, { key: { remoteJid: jid } });
          if (!chats.has(jid)) {
            chats.set(jid, {
              jid,
              name: hc.name || hc.subject || jid.split('@')[0],
              isGroup: isG,
              description: '',
              avatar: null,
              memberCount: 0,
              members: [],
              messages: [],
              unread: hc.unreadCount || 0,
              lastTime: '',
              lastTs: hc.conversationTimestamp ? Number(hc.conversationTimestamp) * 1000 : 0,
            });
          }
        }
      }
      // 2) Gelen gecmis mesajlari ilgili sohbetlere ekle (en son birkaci)
      if (Array.isArray(histMessages)) {
        for (const m of histMessages) {
          try {
            let jid = m.key?.remoteJid;
            if (!jid || jid === 'status@broadcast' || jid.endsWith('@newsletter')) continue;
            // YAS FILTRESI: 30 gunden eski gecmis mesajlari hic isleme (panel + DB temiz kalsin)
            const mTs = m.messageTimestamp ? Number(m.messageTimestamp) * 1000 : Date.now();
            if (mTs < Date.now() - MESAJ_SAKLAMA_MS) continue;
            const info = describeMessage(m);
            if (info.kind === 'skip' || info.kind === 'reaction') continue;
            const isG = jid.endsWith('@g.us');
            if (!isG) jid = normalizeChatJid(jid, m);
            const fromMe = !!m.key.fromMe;
            // sohbet yoksa olustur
            if (!chats.has(jid)) {
              chats.set(jid, {
                jid, name: m.pushName || jid.split('@')[0], isGroup: isG,
                description: '', avatar: null, memberCount: 0, members: [],
                messages: [], unread: 0, lastTime: '', lastTs: 0,
              });
            }
            const chat = chats.get(jid);
            // ayni mesaj zaten varsa atla
            if (chat.messages.some(x => x.id === m.key.id)) continue;
            const ts = m.messageTimestamp ? Number(m.messageTimestamp) * 1000 : Date.now();
            const histMsg = {
              id: m.key.id, raw: m, key: m.key, fromMe,
              kind: info.kind, text: info.text, mediaUrl: null,
              contact: info._contact || null, contacts: info._contacts || null,
              sender: fromMe ? 'Ben' : (m.pushName || ''),
              senderJid: m.key.participant || (fromMe ? '' : jid),
              time: new Date(ts).toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit' }),
              ts,
            };
            chat.messages.push(histMsg);
            // ÖNEMLI: gecmis mesajlari da Supabase'e yaz. Kopma sirasinda kacan mesajlar
            // WhatsApp'tan cogunlukla bu event ile geri gelir; DB'ye yazilmazsa sunucu
            // restart olunca kaybolur. (chat_jid,id) PRIMARY KEY oldugu icin tekrar yazim guvenli.
            if (db.isReady()) db.saveMessage(jid, histMsg).catch(() => {});
          } catch (e) {}
        }
        // her sohbetin mesajlarini zamana gore sirala + son zamani guncelle
        for (const chat of chats.values()) {
          chat.messages.sort((a, b) => (a.ts || 0) - (b.ts || 0));
          const last = chat.messages[chat.messages.length - 1];
          if (last) { chat.lastTs = last.ts || chat.lastTs; chat.lastTime = last.time || chat.lastTime; }
          // sohbetin son durumunu (son zaman) DB'ye yaz ki acilis sirasi dogru olsun
          if (db.isReady()) db.saveChat(chat).catch(() => {});
        }
      }
      // panele guncel listeyi gonder (ofis gecmisi -> sadece ofis panellerine)
      broadcastHat('ofis', { type: 'chats', chats: Array.from(chats.values()).map(stripRaw) });
      console.log(`📚 Gecmis yuklendi: ${histChats?.length || 0} sohbet, ${histMessages?.length || 0} mesaj`);
    } catch (e) { console.error('Gecmis yukleme hatasi:', e.message); }
  });

  // Karsi taraf bir mesaji silince / duzenleyince yakala
  sock.ev.on('messages.update', (updates) => {
    _sonWaAktivite = Date.now(); // WhatsApp aktivitesi
    const CC = hatChats(lineId); // bu hattin sohbetleri
    for (const u of updates) {
      const jid = u.key?.remoteJid;
      const id = u.key?.id;
      if (!jid || !id) continue;
      const chat = CC.get(jid);
      if (!chat) continue;
      const m = chat.messages.find(x => x.id === id);
      if (!m) continue;
      const upd = u.update || {};
      // --- MESAJ DURUMU (TIK) — DURUST MOD ---
      // SORUN: Baileys, sifreleme oturumu bozukken bile status=3 (iletildi/cift tik)
      // gonderebiliyor. Bu YANILTICI — mesaj aslinda gitmemisken cift tik gosteriyordu.
      // COZUM: status'tan gelen bilgiyle SADECE tek tik'e (gonderildi=2) kadar cikariyoruz.
      // Cift tik (iletildi=3) ve mavi (okundu=4) ARTIK SADECE 'message-receipt.update'ten
      // gelir — o gercek teslimat makbuzudur, guvenilirdir. Boylece cift tik yaniltmaz.
      if (m.fromMe && typeof upd.status !== 'undefined' && upd.status !== null) {
        let yeniDurum = Number(upd.status);
        if (yeniDurum > 2) yeniDurum = 2; // status en fazla "gonderildi" (tek tik) saysin
        const eski = m.durum || 0;
        if (yeniDurum > eski) {
          m.durum = yeniDurum;
          broadcastHat(lineId, { type: 'msgStatus', jid, id, durum: yeniDurum });
        }
      }
      // silindi mi? (protokol mesaji REVOKE)
      if (upd.messageStubType === 1 || upd.message === null) {
        m.deleted = true; m.text = ''; m.kind = 'text'; m.mediaUrl = null;
        broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
        // DB'ye de yaz: silme kalici olsun (yenileyince geri gelmesin)
        if (db.isReady()) db.saveMessage(jid, m, lineId).catch(() => {});
      }
      // duzenlendi mi?
      else if (upd.message?.editedMessage || upd.message?.protocolMessage?.editedMessage) {
        const em = upd.message.editedMessage?.message || upd.message.protocolMessage?.editedMessage;
        const newText = em?.conversation || em?.extendedTextMessage?.text;
        if (newText) {
          m.text = newText;
          m.edited = true;
          broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
          // DB'ye de yaz: yoksa sayfa yenilenince / baska kullanicida ESKI metin gorunur.
          if (db.isReady()) db.saveMessage(jid, m, lineId).catch(() => {});
          console.log(`✏️  mesaj duzenlendi: ${id.substring(0,12)} -> "${newText.substring(0,30)}"`);
        }
      }
      // ŞIFRESI COZULEMEYEN mesajin COZULMUS hali sonradan geldi mi?
      // Baileys bazen once cozulememis placeholder ("undecryptable") gonderir,
      // saniyeler sonra gercek icerigi messages.update ile yollar. Bunu yakalayip guncelliyoruz,
      // yoksa ekranda "sifresi cozulemedi" yazisi kalir ama mesaj aslinda gelmistir.
      else if (m.kind === 'undecryptable' && upd.message) {
        try {
          const yeni = describeMessage({ key: u.key, message: upd.message });
          if (yeni && yeni.kind !== 'undecryptable' && yeni.kind !== 'skip') {
            m.kind = yeni.kind;
            m.text = yeni.text || '';
            if (yeni._contact) m.contact = yeni._contact;
            if (yeni._contacts) m.contacts = yeni._contacts;
            // medya ise arka planda indir (mesaji bekletmeden), inince guncellenir
            if (['image','video','audio','document','sticker'].includes(yeni.kind)) {
              const mm = { key: u.key, message: upd.message, messageTimestamp: m.ts ? m.ts/1000 : undefined };
              saveMedia(mm, yeni.kind, sock).then((url) => {
                if (url) { m.mediaUrl = url; broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
                  if (db.isReady()) db.saveMessage(jid, m, lineId).catch(()=>{}); }
              }).catch(()=>{});
            }
            console.log(`🔓 cozulemeyen mesaj sonradan cozuldu: ${id.substring(0,12)} -> ${yeni.kind}`);
            broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
            if (db.isReady()) db.saveMessage(jid, m, lineId).catch(()=>{});
          }
        } catch (e) { console.error('   ⚠️  cozulme guncelleme hatasi:', e.message); }
      }
    }
  });

  // MESAJ ALINDI BILGISI (receipt): iletildi/okundu durumunu daha guvenilir verir (ozellikle grup).
  sock.ev.on('message-receipt.update', (updates) => {
    const CC = hatChats(lineId); // bu hattin sohbetleri
    for (const u of updates) {
      const jid = u.key?.remoteJid;
      const id = u.key?.id;
      if (!jid || !id) continue;
      const chat = CC.get(jid);
      if (!chat) continue;
      const m = chat.messages.find(x => x.id === id);
      if (!m || !m.fromMe) continue;
      // receipt tipi: 'delivery'=iletildi(3), 'read'/'played'=okundu(4)
      const r = u.receipt || {};
      let yeniDurum = 0;
      if (r.readTimestamp || r.playedTimestamp) yeniDurum = 4;
      else if (r.receiptTimestamp) yeniDurum = 3;
      const eski = m.durum || 0;
      if (yeniDurum > eski) {
        m.durum = yeniDurum;
        broadcastHat(lineId, { type: 'msgStatus', jid, id, durum: yeniDurum });
      }
    }
  });

  // Grup bilgisi degisince (isim, aciklama vs) yakala ve guncelle
  // ============================================================
  // chats.update: WhatsApp "bu sohbette hareket var" sinyali gonderir.
  // Mesaj messages.upsert'e dusmese bile bu event gelir. Bunu yakalayip:
  //  1) sohbeti listede EN USTE cikar (lastTs guncelle) + okunmamis isaretle
  //  2) son mesaji WhatsApp'tan AKTIF cek (sadece bu sohbet — 7500 grup degil) -> kuyruga koy
  // Boylece "guncel mesaj en uste cikmiyor" sorunu cozulur.
  // ============================================================
  sock.ev.on('chats.update', async (updates) => {
    try {
      const CC = hatChats(lineId); // bu hattin sohbetleri
      for (const u of updates) {
        const jid = u.id;
        if (!jid || jid === 'status@broadcast' || jid.endsWith('@newsletter')) continue;
        const chat = CC.get(jid);
        if (!chat) continue; // bilmedigimiz sohbet (yeni grup) -> groups.upsert/fetchAllGroups halleder
        // conversationTimestamp = son aktivite zamani (saniye cozunurlukte gelir).
        const ts = u.conversationTimestamp ? Number(u.conversationTimestamp) * 1000 : 0;
        let degisti = false;

        // --- 1) SIRALAMA SENKRONU (sahte hareket YARATMADAN) ---
        // WhatsApp bu sohbetin gercek son-aktivite zamanini (conversationTimestamp) gonderir.
        // Bunu lastTs'e yansitiriz ki LISTE SIRASI WhatsApp'la ayni olsun.
        // ÖNEMLI: Bu sadece SIRALAMA icindir — sohbeti "yeni mesaj geldi" diye zip latmaz,
        // okunmamis isareti koymaz. Yani sahte hareket olmaz ama sira dogru olur.
        // Sadece anlamli bir fark varsa (>3sn) guncelle ki gereksiz broadcast olmasin.
        if (ts && Math.abs(ts - (chat.lastTs || 0)) > 3000) {
          chat.lastTs = ts;
          chat.lastTime = new Date(ts).toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit' });
          degisti = true;
        }

        // --- 2) GERCEK YENI MESAJ (okunmamis artisi) -> okunmamis isareti ---
        // unreadCount GERCEKTEN arttiysa WhatsApp tarafinda yeni okunmamis var demektir.
        if (typeof u.unreadCount === 'number' && u.unreadCount > 0 && u.unreadCount !== chat.unread) {
          chat.unread = u.unreadCount;
          degisti = true;
        }
        const gercekYeniMesaj = (typeof u.unreadCount === 'number' && u.unreadCount > 0 && u.unreadCount !== (chat._oncekiUnread || 0));
        if (degisti) {
          broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
          // AKTIF mesaj cekmeyi SADECE gercek yeni mesaj (okunmamis artisi) varsa dene.
          // Sadece siralama senkronu icin mesaj cekmeye gerek yok (gereksiz yuk olur).
          if (gercekYeniMesaj) mesajCekKuyruguEkle(jid);
        }
        chat._oncekiUnread = chat.unread || 0;
      }
    } catch (e) { console.error('⚠️  chats.update hatasi:', e.message); }
  });

  sock.ev.on('groups.update', async (updates) => {
    try {
      const CC = hatChats(lineId); // bu hattin sohbetleri
      for (const u of updates) {
        const jid = u.id;
        if (!jid || !jid.endsWith('@g.us')) continue;
        // grup listemizde yoksa olustur (yeni katildigimiz grup olabilir)
        if (!CC.has(jid)) {
          CC.set(jid, {
            jid, name: jid.split('@')[0], isGroup: true,
            description: '', avatar: null, memberCount: 0, members: [],
            messages: [], unread: 0, lastTime: '', lastTs: 0,
          });
        }
        const chat = CC.get(jid);
        let degisti = false;
        const eskiAd = chat.name;
        const eskiAciklama = chat.description || '';
        let yeniAd = null, yeniAciklama = null;
        if (u.subject && u.subject.trim()) { yeniAd = u.subject.trim(); }
        if (u.desc !== undefined) { yeniAciklama = u.desc || ''; }
        // subject olayda gelmediyse, guncel adi/aciklamayi metadata'dan cek
        if (!u.subject) {
          try {
            const meta = await getGroupMeta(jid, 0); // degisiklik oldu, taze cek
            if (meta?.subject && meta.subject.trim()) yeniAd = meta.subject.trim();
            if (meta?.desc !== undefined) yeniAciklama = meta.desc || '';
            if (meta?.participants) chat.memberCount = meta.participants.length;
          } catch (e) {}
        }
        // --- AD degisti mi? (sessizce guncelle, bilgi satiri EKLEME) ---
        // Not: Grup adi/aciklamasi degisince sohbete sistem mesaji EKLENMIYOR
        // (kullanici istemedi). Sadece grubun adi/aciklamasi guncel tutulur.
        if (yeniAd && yeniAd !== eskiAd) {
          chat.name = yeniAd;
          degisti = true;
        }
        // --- ACIKLAMA degisti mi? (sessizce guncelle) ---
        if (yeniAciklama !== null && yeniAciklama !== eskiAciklama) {
          chat.description = yeniAciklama;
          degisti = true;
        }
        if (degisti) {
          broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
          console.log(`✏️  grup guncellendi: ${chat.name}`);
        }
      }
    } catch (e) {}
  });

  // Gruba uye eklenince/cikinca uye sayisini guncelle
  sock.ev.on('group-participants.update', async ({ id, participants, action }) => {
    try {
      const CC = hatChats(lineId); // bu hattin sohbetleri
      if (!id || !CC.has(id)) return;
      const chat = CC.get(id);
      // guncel uye sayisini cek (taze)
      try {
        const meta = await getGroupMeta(id, 0);
        if (meta) {
          if (meta.subject && meta.subject.trim()) chat.name = meta.subject.trim();
          chat.memberCount = meta.participants?.length || chat.memberCount;
        }
      } catch (e) {}
      broadcastHat(lineId, { type: 'message', jid: id, chat: stripRaw(chat) });
    } catch (e) {}
  });

  // Karsi taraf yaziyor mu? (presence.update)
  sock.ev.on('presence.update', ({ id, presences }) => {
    try {
      if (!id || !presences) return;
      // presences: { participantJid: { lastKnownPresence: 'composing'|'available'|... } }
      let typing = false;
      let whoJid = null;
      for (const [pjid, info] of Object.entries(presences)) {
        const st = info?.lastKnownPresence;
        if (st === 'composing' || st === 'recording') { typing = true; whoJid = pjid; break; }
      }
      // yazan kisinin adini bul
      let who = '';
      if (whoJid) {
        const r = resolvePhone(whoJid, null);
        who = contactNames.get(r.jid) || contactNames.get(whoJid) || '';
      }
      broadcastHat(lineId, { type: 'typing', jid: id, typing, who });
      if(typing) console.log(`⌨️  yaziyor: ${id.split('@')[0]}${who?' ('+who+')':''}`);
    } catch (e) {}
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    _sonWaAktivite = Date.now(); // WhatsApp'tan veri geldi -> baglanti canli
    // 'notify' yeni mesaj, 'append' senkronizasyon/ilk mesaj - ikisini de al
    if (type !== 'notify' && type !== 'append') return;
    // HAT KISAYOLLARI: bu event hangi hatta ait? (closure'daki lineId/line).
    //  CC      : bu hattin sohbet Map'i (ofis -> global chats, pazarlama -> line.chats)
    //  myNum/myLid : bu hattin kendi numarasi/LID'i (bahsedilme tespiti dogru hatta olsun)
    const CC = hatChats(lineId);
    const myNum = lineId === 'ofis' ? myNumber : line.myNumber;
    const myLidV = lineId === 'ofis' ? myLID : line.myLID;
    for (const m of messages) {
     try {
      let jid = m.key.remoteJid;
      // sohbet olmayan jid'leri atla (durum guncellemeleri, broadcast)
      if (!jid || jid === 'status@broadcast' || jid.endsWith('@newsletter')) continue;
      const isGroup = jid.endsWith('@g.us');
      let fromMe = !!m.key.fromMe; // baska cihazdan gonderdigin mesajlar da gelir

      // ════════════════════════════════════════════════════════════════
      // ORTAK GRUP YANSIMASI DUZELTMESI (kritik bug):
      // Ofis ve pazarlama hatti AYNI WhatsApp grubunda uye olabilir. Ofis gruba
      // yazinca, WhatsApp ayni mesaji pazarlama hattina da [append]/fromMe=true
      // olarak yansitiyor (cok-cihaz senkronu gibi). Sonuc: Volkan'in panelinde
      // ofisin mesaji "Volkan gondermis" gibi gorunuyordu.
      // COZUM: grup mesaji fromMe=true ise, GERCEKTEN bu hattin numarasindan mi
      // gonderilmis dogrula. Gonderenin (participant) numarasi bu hattin numarasi
      // DEGILSE, bu baska hattin yansimasidir -> fromMe=false (gelen mesaj say).
      if (isGroup && fromMe) {
        const benimNum = lineId === 'ofis' ? myNumber : (line ? line.myNumber : null);
        const benimLid = lineId === 'ofis' ? myLID : (line ? line.myLID : null);
        // gonderenin numarasini cikar (participant veya alternatif alanlar)
        const gonderenHam = m.key.participant || m.key.participantPn || m.participant || '';
        const gonderenNum = gonderenHam ? gonderenHam.split('@')[0].split(':')[0] : '';
        if (gonderenNum && benimNum && gonderenNum !== benimNum && gonderenNum !== benimLid) {
          // participant ÇÖZÜLDÜ ve benim numaram DEĞİL -> kesinlikle baska hattin yansimasi
          console.log(`   ⚠️  ORTAK GRUP YANSIMASI: fromMe=true ama gonderen ${gonderenNum} ≠ benim ${benimNum} -> gelen mesaj sayiliyor`);
          fromMe = false;
        } else if (!gonderenNum && type === 'append') {
          // participant BOŞ + [append] tipi: WhatsApp cok-cihaz/ortak grup yansimasi.
          // Gercekten kendi gonderdigin mesajlar 'notify' veya panel uzerinden gelir
          // (panelden gonderince zaten addMessage ile fromMe=true ekleniyor, id eslesince
          // mukerrer onlenir). Bu yuzden append+participant yok olan fromMe'yi gelen say.
          console.log(`   ⚠️  ORTAK GRUP YANSIMASI ([append], participant yok): fromMe iptal -> gelen mesaj`);
          fromMe = false;
        }
      }

      // YAS FILTRESI: 30 gunden eski mesajlari isleme — ne panele dusur ne DB'ye yaz.
      // Acilista Baileys eski gecmis cekse bile bunlar elenir (panel hizli/temiz kalir).
      const msgTs = m.messageTimestamp ? Number(m.messageTimestamp) * 1000 : Date.now();
      if (msgTs < Date.now() - MESAJ_SAKLAMA_MS) continue;

      // KISI sohbetlerinde jid'i normallestir (ayni kisi farkli formatlarda gelince tek sohbet olsun)
      if (!isGroup) {
        const ham = jid; // gelen orijinal jid (LID olabilir)
        jid = normalizeChatJid(jid, m, lineId); // lineId KRITIK: bu hattin kendi numarasiyla "kendine mesaj" tespiti
        // Eger orijinal LID idi ve numaraya cozuldu, eski LID sohbetini numara sohbetiyle BIRLESTIR.
        // Boylece ayni kisi 2-3 ayri sohbet olarak kalmaz.
        // NOT: sohbetleriBirlestir/normalizeChatJid global chats+myNumber kullanir (ofis-merkezli).
        //      Pazarlama hatlarinda bu birlestirmeyi ATLIYORUZ (en kotu durumda ayni kisi 2 sohbet
        //      gorunur — kozmetik; veri sizintisi DEGIL). Ofiste eskisi gibi calisir.
        if (lineId === 'ofis' && ham !== jid && ham.endsWith('@lid') && chats.has(ham)) {
          sohbetleriBirlestir(ham, jid);
        }
      }

      const info = describeMessage(m);
      // TESHIS: gelen her mesaji logla
      console.log(`📩 [${type}] ${isGroup?'grup':'kisi'} ${jid.split('@')[0]} | tip=${info.kind} | fromMe=${fromMe}`);

      // Icerik tasimayan protokol/sistem mesajlarini atla
      if (info.kind === 'skip') { console.log('   ↳ atlandi (sistem mesaji)'); continue; }

      // Reaksiyon ise: ayri mesaj ekleme, ilgili mesaja ekle
      if (info.kind === 'reaction') {
        const chat = CC.get(jid);
        const targetId = info._reactKey?.id;
        if (chat && targetId) {
          const target = chat.messages.find(x => x.id === targetId);
          if (target) {
            if (info.text) target.reaction = info.text; // emoji
            else delete target.reaction;                // bos = reaksiyon kaldirildi
            broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(chat) });
          }
        }
        continue; // reaksiyon islendi, sonraki mesaja gec
      }
      let senderName = '';
      let senderJid = '';      // grupta mesaji atan kisinin numarasi (ozelden yanit icin)
      let senderPush = '';     // o kisinin gercek adi
      let senderOfis = false;  // bu kisi ofis ekibi/kayitli mi (panelde rozet icin)
      let description = '';
      let memberCount = 0;
      let members = null;
      let chatName = isGroup ? jid.split('@')[0] : (m.pushName || jid.split('@')[0]); // varsayilan
      // grup zaten listede ve duzgun adi varsa onu kullan (subject bos gelirse sayiya donmesin)
      if (isGroup && CC.has(jid) && CC.get(jid).name && !/^\d+$/.test(CC.get(jid).name)) {
        chatName = CC.get(jid).name;
      }
      if (isGroup) {
        // MESAJI METADATA ICIN BEKLETME! (Eskiden 'await getGroupMeta' mesaji 5-10sn
        //  geciktiriyordu — onbellekte yoksa WhatsApp sorgusunu bekliyordu.)
        // Adi ZATEN bildigimiz kaynaklardan al (aninda): mevcut chat > grupAdlari > onbellek.
        const mevcut = CC.get(jid);
        if (mevcut && mevcut.name && mevcut.name !== jid.split('@')[0]) {
          chatName = mevcut.name;
        } else if (grupAdlari.has(jid)) {
          chatName = grupAdlari.get(jid);
        } else {
          const c = groupMetaCache.get(jid);
          if (c && c.meta?.subject && c.meta.subject.trim()) chatName = c.meta.subject.trim();
        }
        // Uye listesi / tam metadata ARKA PLANDA cekilsin (mesaji bekletmeden).
        getGroupMeta(jid).then((meta) => {
          if (!meta) return;
          const c = CC.get(jid);
          if (!c) return;
          let degisti = false;
          if (meta.subject && meta.subject.trim() && c.name !== meta.subject.trim()) { c.name = meta.subject.trim(); degisti = true; }
          if (meta.desc !== undefined && c.description !== meta.desc) { c.description = meta.desc || ''; degisti = true; }
          if (meta.participants) {
            c.memberCount = meta.participants.length;
            c.members = meta.participants.map(p => {
              const r = resolvePhone(p.id, p.phoneNumber || null);
              const nm = savedContacts.get(r.jid) || contactNames.get(r.jid) || contactNames.get(p.id) || (r.isLid ? 'Bilinmeyen kişi' : r.number);
              const av = avatarCache.has(r.jid) ? avatarCache.get(r.jid) : (avatarCache.has(p.id) ? avatarCache.get(p.id) : undefined);
              return { jid: r.jid, number: r.number, name: nm, admin: !!p.admin, isLid: !!r.isLid, avatar: av };
            });
            degisti = true;
          }
          if (degisti) { broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(c) }); if (db.isReady()) db.saveChat(c, lineId).catch(() => {}); }
        }).catch(() => {});
        // Hala sayi adindaysa: arka planda artan araliklarla TEKRAR TEKRAR dene
        if (/^\d+$/.test(chatName)) {
          retryGroupName(jid);
        }
        // gonderenin gercek numarasini cozmeye calis (LID ise alternatif alanlardan)
        const altSender = m.key.participantPn || m.key.participantAlt || m.participantPn || null;
        const resolved = resolvePhone(m.key.participant || '', altSender);
        senderJid = resolved.jid;
        // gonderen adi ONCELIK: KAYITLI isim (savedContacts — kullanicinin kalici kayitlari)
        //  > pushName > onbellek > (LID degilse numara) > "Bilinmeyen"
        // Kayitli isim EN ONCE gelir ki ofis ekibi her grupta SABIT isimle gorunsun
        // (WhatsApp'in pushName'i degil, kullanicinin verdigi isim kullanilsin).
        const rNum = resolved.number ? (resolved.number + '@s.whatsapp.net') : '';
        const kayitliIsim = savedContacts.get(resolved.jid)
          || savedContacts.get(rNum)
          || savedContacts.get(m.key.participant || '');
        // KAYITLI isim varsa bu kisi OFIS EKIBI/elle kaydedilmis demektir -> isaretle.
        // Boylece panel, ofis ekibini gruptaki normal kisilerden ayirt edip rozet koyar.
        senderOfis = !!kayitliIsim;
        senderPush = kayitliIsim
          || m.pushName
          || contactNames.get(resolved.jid)
          || contactNames.get(m.key.participant || '')
          || (resolved.isLid ? 'Bilinmeyen kişi' : resolved.number);
        senderName = senderPush;
        // eslemeyi onbellege al (uye listesinde de kullanmak icin)
        if (m.key.participant && resolved.jid !== m.key.participant) {
          lidToPn.set(m.key.participant, resolved.jid);
        }
        // gonderenin adini onbellege al — HER FORMATTA (LID, cozulmus jid, numara@s.whatsapp.net).
        // Boylece bu kisi SONRADAN ETIKETLENINCE (farkli formatta gelse bile) adi bulunur.
        // (Sorun: Yusuf gruba yazinca "Yusuf" gorunuyordu ama etiketlenince "@kişi" cikiyordu —
        //  cunku etiket farkli kimlikle geliyordu. Artik numara bazinda da kayitli.)
        if (m.pushName) {
          contactNames.set(senderJid, m.pushName);
          if (m.key.participant) contactNames.set(m.key.participant, m.pushName);
          if (rNum) contactNames.set(rNum, m.pushName);
          if (resolved.number) contactNames.set(resolved.number + '@s.whatsapp.net', m.pushName);
        }
      } else {
        // kisi sohbeti: KAYITLI isim (rehber/kalici) > pushName > numara
        const kNum = (jid.split('@')[0]) + '@s.whatsapp.net';
        const rehber = savedContacts.get(jid) || savedContacts.get(kNum);
        senderName = rehber || m.pushName || contactNames.get(jid) || chatName;
        chatName = rehber || m.pushName || contactNames.get(jid) || chatName;
        if (m.pushName && !rehber) contactNames.set(jid, m.pushName);
      }

      // Profil fotosu (sohbet/grup avatari) — ağ beklemesi olmasin diye mesaji
      // bekletmeden, addMessage'tan SONRA arka planda cekiyoruz (asagida).
      let avatarUrl = CC.get(jid)?.avatar || null;

      // NOT: Medya (foto/video/ses/belge) indirme de ağ islemidir ve yogunlukta
      // mesajlari bekletir. Mesaji once metin/kayit olarak DUSURUP medyayi arka
      // planda indiriyoruz; indi mi addMessage tekrar cagrilip mediaUrl guncellenir.
      const hasMedia = ['image', 'video', 'audio', 'document', 'sticker'].includes(info.kind);

      // Onizleme (jpegThumbnail): FOTOGRAF + belge + video icin.
      // Mesajla birlikte gelen kucuk onizlemeyi ANINDA gosteririz; tam cozunurluk arkada iner.
      // (WhatsApp boyle yapar — foto hemen gorunur, beklemezsin.)
      let thumbUrl = null;
      try {
        const docMsg = m.message?.documentMessage || m.message?.documentWithCaptionMessage?.message?.documentMessage;
        const thumb = m.message?.imageMessage?.jpegThumbnail   // <-- FOTOGRAF onizlemesi (yeni)
                   || docMsg?.jpegThumbnail
                   || m.message?.videoMessage?.jpegThumbnail;
        if (thumb && thumb.length) {
          const tname = `thumb_${Date.now()}_${Math.random().toString(36).slice(2,6)}.jpg`;
          const buf = Buffer.isBuffer(thumb) ? thumb : Buffer.from(thumb, 'base64');
          fs.writeFileSync(path.join(MEDIA_DIR, tname), buf);
          thumbUrl = '/media/' + tname;
        }
      } catch (e) {}

      // Eger gelen mesaj bir baska mesaja yanitsa, onun onizlemesini cek
      let incomingReply = null;
      const ctx = m.message?.extendedTextMessage?.contextInfo
                || m.message?.imageMessage?.contextInfo
                || m.message?.videoMessage?.contextInfo;
      if (ctx?.quotedMessage) {
        const q = ctx.quotedMessage;
        let qText = q.conversation || q.extendedTextMessage?.text
          || (q.imageMessage ? '📷 Fotoğraf' : '')
          || (q.audioMessage ? '🎤 Sesli mesaj' : '')
          || (q.documentMessage ? '📄 ' + (q.documentMessage.fileName || 'Belge') : '')
          || '';
        // alintilanan kisinin ismini bul (LID/numara yerine)
        const qpRaw = ctx.participant || '';
        let qSender = contactNames.get(qpRaw) || '';
        if (!qSender) {
          const r = resolvePhone(qpRaw, null);
          // LID ise ismi yoksa "biri" de, gercek numara ise numarayi goster
          qSender = r.isLid ? 'biri' : (contactNames.get(r.jid) || r.number);
        }
        incomingReply = {
          id: ctx.stanzaId || null,  // alintilanan mesajin id'si — panelde tiklayinca ona gitmek icin
          sender: qSender,
          text: qText,
        };
      }

      // Mesaj iletilmis mi? (forward)
      const anyCtx = m.message?.extendedTextMessage?.contextInfo
                  || m.message?.imageMessage?.contextInfo
                  || m.message?.videoMessage?.contextInfo
                  || m.message?.documentMessage?.contextInfo
                  || m.message?.audioMessage?.contextInfo;
      const isForwarded = !!(anyCtx?.isForwarded || (anyCtx?.forwardingScore > 0));

      // Metindeki @etiketleri isimle degistir (LID/garip numara gizlensin)
      // + panele gidecek mention eslemesini (isim->numara) hazirla.
      let msgMentions = [];
      if (info.text && anyCtx?.mentionedJid?.length) {
        // grup uye listesini (varsa) ver ki etiketlenen kisi uyeyse adi/numarasi bulunabilsin
        const chatUyeleri = (isGroup && CC.get(jid)?.members) || null;
        const pm = prettifyMentions(info.text, anyCtx.mentionedJid, chatUyeleri);
        info.text = pm.text;
        msgMentions = pm.mentions;
      }

      // Beni etiketlemis mi? (mentionedJid icinde benim numaram VEYA LID'im var mi)
      let mentionsMe = false;
      let bahsedilmeKime = undefined; // ortak hat etiketlenince: bu bahsedilme kimlere ait
      if (!fromMe && anyCtx?.mentionedJid?.length) {
        mentionsMe = anyCtx.mentionedJid.some(mj => {
          const num = (mj || '').split('@')[0];
          return (myNum && num === myNum) || (myLidV && num === myLidV);
        });
        if (mentionsMe) {
          console.log(`   🔔 BAHSEDILME: ${chatName || jid.split('@')[0]}`);
          // Ortak hat etiketlendi. Bu grup birine ETIKETLENMIS mi?
          const atananlar = chatAssignments.get(jid) || [];
          if (atananlar.length) {
            // gruba etiketlenenler varsa: bahsedilme SADECE onlara ait
            bahsedilmeKime = atananlar;
          } else {
            // grup kimseye etiketlenmemis: yoneticilere ait (panel role==='admin' kontrol eder)
            bahsedilmeKime = '__admins__';
          }
        }
      }

      addMessage(jid, {
        id: m.key.id,
        raw: m,
        key: m.key,
        fromMe: fromMe,
        kind: info.kind,
        text: info.text,
        caption: info.caption || '', // belge/dosya aciklamasi (varsa)
        fileName: info._fileName || undefined, // belge adi (iletme icin saklanir)
        mime: info._mime || undefined,         // belge tipi (iletme icin saklanir)
        mediaUrl: null, // medya arka planda inecek; indince addMessage tekrar guncelleyecek
        thumb: thumbUrl,
        contact: info._contact || null,
        contacts: info._contacts || null,
        sender: fromMe ? 'Ben' : senderName,
        senderJid,
        senderPush,
        senderOfis, // bu kisi ofis ekibi/kayitli mi (panelde rozet icin)
        time: nowTime(),
        replyTo: incomingReply,
        forwarded: isForwarded,
        mentionsMe,
        bahsedilmeKime, // ortak hat etiketlenince: bu bahsedilme kimlere ait (panel suzer)
        mentions: msgMentions,
      }, { name: chatName, description, avatar: avatarUrl, memberCount, members, mentionsMe }, lineId);

      // --- SATIŞ KOMUTU KONTROLÜ: "/trafik2" gibi mesajlar satis olarak kaydedilir ---
      // Sadece GRUP mesajlarinda + metin mesajlarinda kontrol et (DM'de satis sayma).
      // Satici = mesaji yazan kisi (grup uyesi pazarlamaci). Musteri de atabilir ama
      // o zaman satici musteri gorunur — kullanici "mesaji kim yazdiysa o satici" dedi.
      if (isGroup && info.kind === 'text') {
        const satis = satisAyristir(info.text);
        if (satis) {
          // satici: fromMe ise hat sahibi (ben), degilse mesaji yazan kisi
          const saticiAdi = fromMe ? (line?.myName || 'Ben') : (senderName || senderPush || '');
          const saticiJid2 = fromMe ? (myNum ? myNum + '@s.whatsapp.net' : '') : (senderJid || '');
          const chatObj = CC.get(jid);
          satisKaydet(m, satis, lineId, chatObj, saticiAdi, saticiJid2).catch(() => {});
        }
      }

      // --- ARKA PLAN: medya + avatar indir (mesaji bekletmeden) ---
      // Medya indip diske yazilinca addMessage'i ayni id ile tekrar cagiririz;
      // addMessage var olan mesajin mediaUrl'unu doldurup panele + DB'ye yansitir.
      if (hasMedia) {
        // RETRY'LI medya indirme: ilk denemede inmezse (ag/zaman asimi) birkac kez
        // tekrar dene. Eskiden tek deneme vardi -> inmeyen GORSEL/medya KALICI eksik
        // kaliyordu (kullanici "eksik gorsel" sikayeti). Artik 4 deneme + artan bekleme.
        const medyaIndirRetry = async (deneme = 1) => {
          try {
            const url = await saveMedia(m, info.kind, sock);
            if (url) {
              addMessage(jid, { id: m.key.id, mediaUrl: url, fromMe }, {}, lineId);
              return; // basarili
            }
          } catch (e) { /* asagida tekrar denenecek */ }
          // basarisiz: en fazla 4 deneme, her seferinde biraz daha bekle (4s, 8s, 16s)
          if (deneme < 4) {
            const bekle = 4000 * Math.pow(2, deneme - 1); // 4s, 8s, 16s
            console.log(`   ⏳ medya inmedi (deneme ${deneme}/4), ${bekle/1000}sn sonra tekrar: ${String(m.key.id).slice(0,10)}`);
            setTimeout(() => medyaIndirRetry(deneme + 1), bekle);
          } else {
            console.error(`   ❌ medya 4 denemede inmedi, vazgecildi: ${String(m.key.id).slice(0,10)} (${info.kind})`);
          }
        };
        medyaIndirRetry(1);
      }
      // Avatar daha onceden yoksa arka planda cek (sohbet basligi/listesi icin)
      if (!avatarUrl) {
        getAvatar(jid).then((url) => {
          const c = CC.get(jid);
          if (url && c && !c.avatar) {
            c.avatar = url;
            broadcastHat(lineId, { type: 'message', jid, chat: stripRaw(c) });
            if (db.isReady()) db.saveChat(c, lineId).catch(() => {});
          }
        }).catch(() => {});
      }

      const label = info.kind === 'text' ? info.text : `[${info.kind}]${info.text ? ' ' + info.text : ''}`;
      console.log(isGroup ? `👥 [${chatName}] ${senderName}: ${label}` : `💬 ${chatName}: ${label}`);
     } catch (err) {
       console.error('⚠️  Mesaj islenirken hata (atlandi):', err.message);
     }
    }
  });
}

server.listen(PORT, async () => {
  console.log(`🌐 Panel hazir: http://localhost:${PORT}`);
  // 1) Supabase'i baslat ve test et
  db.init();
  const dbOk = await db.test();
  // DB koparsa otomatik yeniden baglanmayi dene (15sn'de bir, sessizce)
  db.startKeepAlive(15);
  // Eski mesaj temizligi: 30 gunden eski mesajlari gunde bir Supabase'den sil
  db.startCleanup();
  if (dbOk) {
    // Ilk yoneticiyi olustur (Burak Pekcan) - .env'den okur, yoksa varsayilan
    const adminUser = process.env.ADMIN_USER || 'burak';
    const adminPass = process.env.ADMIN_PASS || 'pekcan';
    await db.ensureAdmin(adminUser, adminPass, 'Burak Pekcan');
    // 2) Kayitli veriyi bellege yukle (WhatsApp'tan once - hizli acilis + kalicilik)
    await loadFromDB();
    await izinliIpleriYukle(); // izinli IP listesini bellege al
  } else {
    console.log('   ⚠️  Supabase kapali — veriler sadece bellekte tutulacak (eskisi gibi).');
  }
  console.log('   (WhatsApp baglantisi baslatiliyor...)\n');
  // Otomatik baglan: kayitli oturum varsa ona, yoksa yeni QR uretir.
  // (Demo/kullanim kolayligi: panel acilinca WhatsApp da hazirlanir)
  const credsPath = path.join(__dirname, 'auth', 'creds.json');
  if (fs.existsSync(credsPath)) {
    console.log('   🔁 Kayitli oturum bulundu, otomatik baglaniliyor...');
  } else {
    console.log('   📱 Oturum yok — QR uretiliyor, panelden okutun.');
  }
  startWA();
});

// Supabase'den tum veriyi bellege yukle (acilista)
async function loadFromDB() {
  try {
    const data = await db.loadAll();
    let n = 0;
    for (const row of data.chats) {
      chats.set(row.jid, {
        jid: row.jid,
        name: row.custom_name || row.name || row.jid.split('@')[0],
        isGroup: row.is_group,
        description: row.description || '',
        avatar: row.avatar || null,
        memberCount: row.member_count || 0,
        members: row.members || [],
        messages: [], // mesajlar sohbet acilinca yuklenecek (performans)
        unread: row.unread || 0,
        lastTime: row.last_time || '',
        lastTs: Number(row.last_ts) || 0,
        pinned: row.pinned || false,
        archived: row.archived || false,
        hasMention: row.has_mention || false,
        customName: row.custom_name || null,
        _fromDB: true, // bu sohbet DB'den geldi (mesajlari henuz yuklenmedi)
      });
      n++;
    }
    // kayitli isimler
    let manuelSayisi = 0;
    for (const c of data.contacts) {
      if (c.is_manual) { savedContacts.set(c.jid, c.name); manuelSayisi++; }
      contactNames.set(c.jid, c.name);
    }
    if (manuelSayisi) console.log(`📇 ${manuelSayisi} kalici isim yuklendi (ofis ekibi vs.)`);
    // OTURUMLAR: kayitli token'lari bellege yukle (restart sonrasi kimse atilmasin)
    try {
      const oturumlar = await db.loadSessions();
      for (const r of oturumlar) sessions.set(r.token, { username: r.username, displayName: r.display_name, role: r.role, ts: Date.now() });
      if (oturumlar.length) console.log(`🔑 ${oturumlar.length} oturum yuklendi (kullanicilar atilmadi)`);
    } catch (e) {}
    // ATAMALAR: hangi grup kime atanmis (Supabase'den yukle)
    try {
      const atamalar = await db.loadAssignments();
      let sayac = 0;
      for (const [cjid, users] of Object.entries(atamalar)) {
        chatAssignments.set(cjid, users);
        sayac += users.length;
      }
      if (sayac) console.log(`👤 ${Object.keys(atamalar).length} grup atamasi yuklendi (${sayac} atama)`);
    } catch (e) {}
    // ETIKETLER: etiket tanimlari + grup-etiket baglantilari (Supabase'den yukle)
    try {
      labels = await db.loadLabels();
      const cl = await db.loadChatLabels();
      for (const [cjid, ids] of Object.entries(cl)) chatLabels.set(cjid, ids);
      if (labels.length) console.log(`🏷️  ${labels.length} etiket yuklendi`);
    } catch (e) {}
    console.log(`📂 Supabase'den yuklendi: ${n} sohbet, ${data.contacts.length} kayitli isim`);
  } catch (e) {
    console.error('⚠️  DB yukleme hatasi:', e.message);
  }
}

// ---- GUVENLIK AGI: hicbir yakalanmayan hata sunucuyu kapatmasin ----
// Medya indirme, ag kopmasi gibi beklenmedik hatalarda sunucu cokmek yerine
// hatayi loglar ve calismaya devam eder.
process.on('uncaughtException', (err) => {
  console.error('⚠️  Yakalanmayan hata (sunucu calismaya devam ediyor):', err.message);
});
process.on('unhandledRejection', (reason) => {
  console.error('⚠️  Islenmeyen reddetme (sunucu calismaya devam ediyor):', reason?.message || reason);
});
