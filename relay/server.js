// server.js
//
// Proceso "puente" entre Binance y el navegador. Corre local (npm start) o
// desplegado (Render, ver README) y hace TRES cosas:
//
//   1) Se conecta a Binance por websocket (streams @depth y @aggTrade) y
//      mantiene el order book en memoria con orderbook.js, siguiendo el
//      metodo oficial de sincronizacion (snapshot REST + eventos delta).
//      Si detecta un gap en la secuencia (evento perdido), se re-sincroniza
//      solo pidiendo un snapshot nuevo -- antes esto no se detectaba y el
//      book, quedaba desincronizado silenciosamente.
//
//   2) Levanta un servidor HTTP que sirve `frontend/` como sitio estatico
//      Y ademas un websocket (mismo puerto) al que se conecta el frontend.
//      Antes eran dos piezas separadas (abrir index.html a mano); ahora
//      un solo proceso sirve todo, que es lo que necesita Render/Railway/etc.
//
//   3) Cada INTERVALO_ENVIO_MS manda al frontend un snapshot agregado del
//      book (paredes bid/ask por separado) + los trades que fueron llegando
//      + el delta de volumen agresor (compra vs venta) de esa ventana.
//
//   4) Cada GEX_INTERVALO_MS (por default 60s) pide a Deribit el open
//      interest y la IV de las opciones de BTC, y calcula un estimador de
//      Gamma Exposure (GEX) neto -- ver gamma.js para el detalle y las
//      limitaciones de esto. Es independiente del book de Binance, así que
//      si Deribit falla o tarda, el resto de la app sigue funcionando.
//
//   5) Mantiene un precio "ancla" para la ventana de precios visible, que
//      NO se mueve en cada tick -- solo se re-centra cuando el precio en
//      vivo se acerca al borde de la ventana (a menos de MARGEN_RECENTRADO
//      del limite). Asi el precio oscila DENTRO de un rango fijo, como en
//      un grafico de trading normal, en vez de que la ventana persiga al
//      precio y este quede siempre pegado al medio de la pantalla. El
//      frontend puede tambien pedir un pan MANUAL (flechas arriba/abajo),
//      que pausa este auto-recentrado hasta que el usuario pide volver a
//      seguir el precio en vivo -- ver modoManual mas abajo.
//
// Config por variables de entorno (todas opcionales, ver .env.example):
//   SYMBOL, BUCKET_SIZE, RANGO_BUCKETS, INTERVALO_ENVIO_MS, PORT, MODO_DEMO,
//   GEX_INTERVALO_MS, MARGEN_RECENTRADO_USD, PASO_PAN_USD

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket, { WebSocketServer } from "ws";
import { OrderBook } from "./orderbook.js";
import { obtenerGammaExposure } from "./gamma.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIR_FRONTEND = path.join(__dirname, "..", "frontend");

const SYMBOL = (process.env.SYMBOL || "btcusdt").toLowerCase();
const BUCKET_SIZE = Number(process.env.BUCKET_SIZE || 5); // USD por pared
const RANGO_BUCKETS = Number(process.env.RANGO_BUCKETS || 220); // paredes por lado ($1100 c/lado con BUCKET_SIZE=5 = $2200 total)
const INTERVALO_ENVIO_MS = Number(process.env.INTERVALO_ENVIO_MS || 250);
const PUERTO = Number(process.env.PORT || 8081);
const MODO_DEMO = process.env.MODO_DEMO === "1"; // datos sinteticos, sin Binance (para probar sin red)
const GEX_INTERVALO_MS = Number(process.env.GEX_INTERVALO_MS || 60_000);
// Cuando el precio en vivo llega a estar a menos de esto (en USD) del borde
// de la ventana visible, se re-centra. Con BUCKET_SIZE*RANGO_BUCKETS=1100
// de medio-rango y 700 de margen, el precio puede moverse 400 USD desde el
// centro antes de que la ventana empiece a re-centrarse.
const MARGEN_RECENTRADO_USD = Number(process.env.MARGEN_RECENTRADO_USD || 700);
// Cuanto se mueve el ancla (en USD) por cada click de las flechas de pan
// manual del frontend (ver mas abajo, mensajes {tipo:"pan"} del cliente).
const PASO_PAN_USD = Number(process.env.PASO_PAN_USD || 150);

const book = new OrderBook();
let bufferEventos = [];
let esperandoSnapshot = false;
let tradesRecientes = [];
let deltaAgresorVentana = 0; // + compra agresiva, - venta agresiva (USD notional)
let wsBinance = null;
let ultimoEstadoBinance = "desconectado";

// Precio ancla de la ventana visible (ver punto 5 arriba). null hasta el
// primer tick con datos, ahi arranca centrado en el mid de ese momento.
let anclaCentro = null;

// Modo manual: se activa cuando el usuario usa las flechas de pan del
// frontend (mensaje {tipo:"pan"}) y pausa el auto-recentrado de abajo, para
// que no "pelee" contra el pan manual y lo cancele en el tick siguiente. Se
// desactiva con {tipo:"seguir"} (click en el badge de estado del frontend
// cuando esta en manual), que ademas re-centra de una en el mid actual.
let modoManual = false;

/**
 * Actualiza (si hace falta) el ancla de la ventana visible. Se re-centra
 * en el mid EN VIVO solo cuando este se acerca a menos de
 * MARGEN_RECENTRADO_USD del borde de la ventana -- el resto del tiempo el
 * ancla se queda quieta y el precio se mueve libremente dentro del rango.
 * Si el usuario esta paneando a mano (modoManual), no se toca el ancla aca
 * -- la mueven directamente los mensajes "pan"/"seguir" de mas abajo.
 */
function actualizarAncla(mid) {
  if (anclaCentro === null) {
    anclaCentro = mid;
    return;
  }
  if (modoManual) return;
  const medioRango = BUCKET_SIZE * RANGO_BUCKETS;
  const bordeSuperior = anclaCentro + medioRango - MARGEN_RECENTRADO_USD;
  const bordeInferior = anclaCentro - medioRango + MARGEN_RECENTRADO_USD;
  if (mid >= bordeSuperior || mid <= bordeInferior) {
    anclaCentro = mid;
  }
}

// -----------------------------------------------------
// 1) Conexion a Binance (con resync automatico ante gaps)
// -----------------------------------------------------
async function pedirSnapshotYAplicar() {
  esperandoSnapshot = true;
  book.reset();
  bufferEventos = [];
  try {
    const resp = await fetch(
      `https://api.binance.com/api/v3/depth?symbol=${SYMBOL.toUpperCase()}&limit=1000`
    );
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const snapshot = await resp.json();
    book.aplicarSnapshotRest(snapshot);

    // aplicar lo bufferizado mientras esperabamos el snapshot, en orden
    for (const evento of bufferEventos) {
      const resultado = book.aplicarEventoDepth(evento);
      if (resultado === "gap") {
        console.warn("[binance] gap durante replay del buffer, reintentando snapshot...");
        esperandoSnapshot = false;
        return pedirSnapshotYAplicar();
      }
    }
    bufferEventos = [];
    esperandoSnapshot = false;
    console.log("[binance] snapshot aplicado, lastUpdateId:", book.lastUpdateId);
  } catch (err) {
    console.error("[binance] error pidiendo snapshot REST:", err.message, "-- reintento en 2s");
    esperandoSnapshot = false;
    setTimeout(pedirSnapshotYAplicar, 2000);
  }
}

function conectarBinance() {
  const streams = `${SYMBOL}@depth@100ms/${SYMBOL}@aggTrade`;
  wsBinance = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);

  wsBinance.on("open", () => {
    console.log("[binance] conectado, streams:", streams);
    ultimoEstadoBinance = "conectado";
    pedirSnapshotYAplicar();
  });

  wsBinance.on("message", (raw) => {
    const msg = JSON.parse(raw.toString());
    const stream = msg.stream;
    const data = msg.data;

    if (stream.endsWith("@depth@100ms")) {
      if (esperandoSnapshot || !book.ready) {
        bufferEventos.push(data);
        return;
      }
      const resultado = book.aplicarEventoDepth(data);
      if (resultado === "gap") {
        console.warn("[binance] gap detectado en la secuencia, re-sincronizando...");
        pedirSnapshotYAplicar();
      }
    } else if (stream.endsWith("@aggTrade")) {
      const precio = parseFloat(data.p);
      const qty = parseFloat(data.q);
      tradesRecientes.push({
        precio,
        qty,
        esVentaAgresiva: data.m, // true = taker vendio (venta agresiva)
        tiempo: data.T,
      });
      deltaAgresorVentana += data.m ? -(precio * qty) : precio * qty;
    }
  });

  wsBinance.on("close", () => {
    ultimoEstadoBinance = "desconectado";
    console.log("[binance] desconectado, reintentando en 3s...");
    setTimeout(conectarBinance, 3000);
  });

  wsBinance.on("error", (err) => {
    console.error("[binance] error de conexion:", err.message);
  });
}

// -----------------------------------------------------
// Modo demo: genera un book + trades sinteticos con random walk,
// para poder probar el frontend sin salir a internet / sin depender
// de que Binance este accesible.
// -----------------------------------------------------
function arrancarModoDemo() {
  console.log("[demo] MODO_DEMO=1 -- generando datos sinteticos, no se usa Binance");
  let mid = 65000;
  const bidsDemo = new Map();
  const asksDemo = new Map();

  function regenerarNiveles() {
    bidsDemo.clear();
    asksDemo.clear();
    for (let i = 1; i <= 400; i++) {
      const p = Math.round((mid - i * 1.25) * 100) / 100;
      bidsDemo.set(p, Math.random() * 2 + (i % 20 === 0 ? 8 : 0));
      const a = Math.round((mid + i * 1.25) * 100) / 100;
      asksDemo.set(a, Math.random() * 2 + (i % 17 === 0 ? 8 : 0));
    }
  }
  regenerarNiveles();

  book.ready = true;
  book.bids = bidsDemo;
  book.asks = asksDemo;

  setInterval(() => {
    mid += (Math.random() - 0.5) * 8;
    regenerarNiveles();
    book.bids = bidsDemo;
    book.asks = asksDemo;

    if (Math.random() < 0.7) {
      const esVenta = Math.random() < 0.5;
      const precio = mid + (Math.random() - 0.5) * 4;
      const qty = Math.random() * 0.5 + 0.01;
      tradesRecientes.push({ precio, qty, esVentaAgresiva: esVenta, tiempo: Date.now() });
      deltaAgresorVentana += esVenta ? -(precio * qty) : precio * qty;
    }
  }, 150);

  ultimoEstadoBinance = "demo";
}

if (MODO_DEMO) arrancarModoDemo();
else conectarBinance();

// -----------------------------------------------------
// Gamma Exposure (Deribit, opciones de BTC) -- ver gamma.js
// -----------------------------------------------------
let ultimoGEX = null;

async function actualizarGEX() {
  try {
    ultimoGEX = await obtenerGammaExposure();
    console.log(
      `[gex] actualizado: netGEX=${(ultimoGEX.netGEX / 1e6).toFixed(1)}M USD, regimen=${ultimoGEX.regimen}, instrumentos=${ultimoGEX.instrumentosUsados}`
    );
  } catch (err) {
    console.error("[gex] error consultando Deribit:", err.message);
    // no tocamos ultimoGEX -- mejor mostrar el ultimo valor valido (aunque
    // este un poco viejo) que borrarlo por un fallo transitorio de red
  }
}

if (!MODO_DEMO) {
  actualizarGEX();
  setInterval(actualizarGEX, GEX_INTERVALO_MS);
}

// -----------------------------------------------------
// 2) Servidor HTTP (sirve frontend/ + upgrade a websocket)
// -----------------------------------------------------
const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

const httpServer = http.createServer((req, res) => {
  if (req.url === "/salud") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, binance: ultimoEstadoBinance, bookListo: book.ready }));
    return;
  }

  let urlPath = req.url === "/" ? "/index.html" : req.url;
  urlPath = urlPath.split("?")[0];
  const rutaArchivo = path.normalize(path.join(DIR_FRONTEND, urlPath));

  // evitar path traversal fuera de frontend/
  if (!rutaArchivo.startsWith(DIR_FRONTEND)) {
    res.writeHead(403);
    res.end("prohibido");
    return;
  }

  fs.readFile(rutaArchivo, (err, contenido) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("no encontrado");
      return;
    }
    const ext = path.extname(rutaArchivo);
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(contenido);
  });
});

const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (cliente) => {
  console.log("[local] frontend conectado");
  // le mandamos la config para que no tenga que hardcodearla
  cliente.send(JSON.stringify({
    tipo: "config",
    symbol: SYMBOL.toUpperCase(),
    bucketSize: BUCKET_SIZE,
    intervaloMs: INTERVALO_ENVIO_MS,
  }));

  // Mensajes entrantes: pan manual (flechas del frontend) y "volver a
  // seguir en vivo" (click en el badge de estado cuando esta en manual).
  // Cualquier cliente conectado puede mandarlos -- no hay multi-usuario
  // real todavia, es un tablero personal, asi que no hace falta distinguir
  // quien pidio que.
  cliente.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // mensaje invalido, se ignora
    }
    if (msg.tipo === "pan" && anclaCentro !== null) {
      modoManual = true;
      anclaCentro += (msg.direccion >= 0 ? 1 : -1) * PASO_PAN_USD;
    } else if (msg.tipo === "seguir") {
      modoManual = false;
      const midActual = book.midPrice();
      if (midActual !== null) anclaCentro = midActual;
    }
  });
});

setInterval(() => {
  if (!book.ready) return;

  const midActual = book.midPrice();
  if (midActual === null) return;
  actualizarAncla(midActual);

  const { mid, mejorBid, mejorAsk, centro, niveles } = book.agregarPorBuckets(
    BUCKET_SIZE,
    RANGO_BUCKETS,
    anclaCentro
  );
  if (mid === null) return;

  const payload = JSON.stringify({
    tipo: "tick",
    tiempo: Date.now(),
    mid,
    centro,
    mejorBid,
    mejorAsk,
    niveles,
    trades: tradesRecientes,
    deltaAgresor: deltaAgresorVentana,
    binance: ultimoEstadoBinance,
    gex: ultimoGEX,
    modoManual,
  });

  tradesRecientes = [];
  deltaAgresorVentana = 0;

  for (const cliente of wss.clients) {
    if (cliente.readyState === WebSocket.OPEN) cliente.send(payload);
  }
}, INTERVALO_ENVIO_MS);

httpServer.listen(PUERTO, () => {
  console.log(`[local] sirviendo frontend + websocket en http://localhost:${PUERTO}`);
});
