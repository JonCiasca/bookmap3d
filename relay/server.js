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
//   6) Spot vs Futuros (USD-M): UN SOLO mercado conectado a la vez (ver
//      MERCADOS y cambiarMercado()). El frontend pide el cambio con un
//      mensaje {tipo:"mercado", valor:"spot"|"futuros"}; el server corta la
//      conexion vieja y arranca un book nuevo desde cero contra el otro
//      endpoint de Binance.
//
//   7) Detecta posibles ordenes iceberg (un nivel que se consume por un
//      trade real y vuelve a aparecer con tamaño similar varias veces) y
//      "imanes" (niveles cuyo tamaño se sostuvo grande en el tiempo, no solo
//      en el tick actual) -- ver ICEBERG_* / IMAN_* mas abajo. Las dos son
//      heuristicas de mejor esfuerzo, mismo espiritu que el estimador de GEX.
//
// Config por variables de entorno (todas opcionales, ver .env.example):
//   SYMBOL, BUCKET_SIZE, RANGO_BUCKETS, INTERVALO_ENVIO_MS, PORT, MODO_DEMO,
//   GEX_INTERVALO_MS, MARGEN_RECENTRADO_USD, PASO_PAN_USD, MERCADO_INICIAL,
//   ICEBERG_ACTIVADO, ICEBERG_DISTANCIA_USD, ICEBERG_CAIDA_MINIMA,
//   ICEBERG_REFILL_MINIMO, ICEBERG_VENTANA_REFILL_MS, ICEBERG_REFILLS_PARA_FLAG,
//   ICEBERG_EXPIRA_MS, ICEBERG_QTY_MINIMA, IMAN_ACTIVADO, IMAN_ALPHA,
//   IMAN_FACTOR, IMAN_MAX_ENVIADOS, IMAN_QTY_MINIMA

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
const RANGO_BUCKETS = Number(process.env.RANGO_BUCKETS || 500); // paredes por lado ($2500 c/lado con BUCKET_SIZE=5 = $5000 total)
const INTERVALO_ENVIO_MS = Number(process.env.INTERVALO_ENVIO_MS || 250);
const PUERTO = Number(process.env.PORT || 8081);
const MODO_DEMO = process.env.MODO_DEMO === "1"; // datos sinteticos, sin Binance (para probar sin red)
const GEX_INTERVALO_MS = Number(process.env.GEX_INTERVALO_MS || 60_000);

// -----------------------------------------------------
// Spot vs Futuros (USD-M): UN SOLO mercado conectado a la vez -- correr los
// dos en simultaneo duplicaria conexion a Binance + book en memoria + costo
// de agregado por tick para nada (nadie mira los dos a la vez). En cambio,
// el frontend manda un mensaje {tipo:"mercado", valor:"spot"|"futuros"} y el
// server corta la conexion actual y arranca una nueva contra el otro
// endpoint, con su propio book desde cero (ver cambiarMercado()).
// -----------------------------------------------------
const MERCADOS = {
  spot: {
    restSnapshot: (symbol) => `https://api.binance.com/api/v3/depth?symbol=${symbol.toUpperCase()}&limit=1000`,
    wsUrl: (symbol) => `wss://stream.binance.com:9443/stream?streams=${symbol}@depth@100ms/${symbol}@aggTrade`,
    esFuturos: false,
  },
  futuros: {
    restSnapshot: (symbol) => `https://fapi.binance.com/fapi/v1/depth?symbol=${symbol.toUpperCase()}&limit=1000`,
    wsUrl: (symbol) => `wss://fstream.binance.com/stream?streams=${symbol}@depth@100ms/${symbol}@aggTrade`,
    esFuturos: true,
  },
};
let mercadoActual = (process.env.MERCADO_INICIAL || "spot").toLowerCase() === "futuros" ? "futuros" : "spot";

// -----------------------------------------------------
// Deteccion de posibles ordenes iceberg: un nivel que, tras ser consumido
// por un trade real (no una simple cancelacion), vuelve a aparecer con un
// tamaño similar varias veces seguidas -- la firma clasica de un iceberg
// (solo se muestra una fraccion del tamaño real, y se va "recargando" a
// medida que se ejecuta). Heuristica de mejor esfuerzo, igual espiritu que
// el estimador de GEX de gamma.js: util como señal, no es certeza.
// -----------------------------------------------------
const ICEBERG_ACTIVADO = process.env.ICEBERG_ACTIVADO !== "0";
const ICEBERG_DISTANCIA_USD = Number(process.env.ICEBERG_DISTANCIA_USD || 250); // solo trackear niveles cerca del touch
const ICEBERG_CAIDA_MINIMA = Number(process.env.ICEBERG_CAIDA_MINIMA || 0.6); // qty tiene que caer por debajo de este % para contar como "consumido"
const ICEBERG_REFILL_MINIMO = Number(process.env.ICEBERG_REFILL_MINIMO || 0.75); // y volver a al menos este % para contar como "refill"
const ICEBERG_VENTANA_REFILL_MS = Number(process.env.ICEBERG_VENTANA_REFILL_MS || 4000);
const ICEBERG_REFILLS_PARA_FLAG = Number(process.env.ICEBERG_REFILLS_PARA_FLAG || 3);
const ICEBERG_EXPIRA_MS = Number(process.env.ICEBERG_EXPIRA_MS || 45000);
const ICEBERG_QTY_MINIMA = Number(process.env.ICEBERG_QTY_MINIMA || 0.05); // ignora niveles chicos (ruido)
const VENTANA_HISTORIAL_TRADES_MS = 3000; // para confirmar que la caida fue por un trade, no una cancelacion

// -----------------------------------------------------
// "Imanes": niveles cuyo tamaño se sostuvo grande de forma consistente en
// el tiempo (EMA lenta), no solo en el tick actual -- candidatos a zona de
// posible absorcion/soporte-resistencia. Igual que iceberg, es una
// heuristica basada en lo que se ve en el book, no una certeza.
// -----------------------------------------------------
const IMAN_ACTIVADO = process.env.IMAN_ACTIVADO !== "0";
const IMAN_ALPHA = Number(process.env.IMAN_ALPHA || 0.01); // que tan rapido se adapta la EMA (bajo = tiene que sostenerse mucho tiempo)
const IMAN_FACTOR = Number(process.env.IMAN_FACTOR || 4); // "iman" si su EMA supera FACTOR veces el promedio de EMAs del momento
const IMAN_MAX_ENVIADOS = Number(process.env.IMAN_MAX_ENVIADOS || 10);
const IMAN_QTY_MINIMA = Number(process.env.IMAN_QTY_MINIMA || 0.3); // ignora destaques chicos en horas de poco volumen
// Cuando el precio en vivo llega a estar a menos de esto (en USD) del borde
// de la ventana visible, se re-centra. Con BUCKET_SIZE*RANGO_BUCKETS=2500
// de medio-rango y 700 de margen, el precio puede moverse 1800 USD desde el
// centro antes de que la ventana empiece a re-centrarse.
const MARGEN_RECENTRADO_USD = Number(process.env.MARGEN_RECENTRADO_USD || 700);
// Cuanto se mueve el ancla (en USD) por cada click de las flechas de pan
// manual del frontend (ver mas abajo, mensajes {tipo:"pan"} del cliente).
const PASO_PAN_USD = Number(process.env.PASO_PAN_USD || 150);

let book = new OrderBook({ esFuturos: MERCADOS[mercadoActual].esFuturos });
let bufferEventos = [];
let esperandoSnapshot = false;
let tradesRecientes = [];
let deltaAgresorVentana = 0; // + compra agresiva, - venta agresiva (USD notional)
let wsBinance = null;
let ultimoEstadoBinance = "desconectado";
let generacionConexion = 0; // se incrementa en cada cambiarMercado() para invalidar callbacks de la conexion vieja

// Estado del detector de iceberg -- ver ICEBERG_* arriba.
const trackerIceberg = new Map(); // precio -> { esBid, refills, ultimoRefillTs, consumidoDesde, qtyPreConsumo }
let historialTradesIceberg = []; // { precio, tiempo } recientes, para confirmar que una caida de qty fue por un trade

// Estado del detector de imanes -- ver IMAN_* arriba.
const emaPorNivel = new Map(); // precio -> EMA de (bid+ask) en ese bucket

// -----------------------------------------------------
// Salud de la sincronizacion: si el book esta re-sincronizando todo el
// tiempo (gap tras gap), en la practica pasa la mayor parte del tiempo
// recien reseteado y vacio -- desde el frontend eso se ve como "no pasa
// nada" sin ningun aviso de que hay un problema real de conexion. Contamos
// los resyncs en una ventana corta y, si se pasa de rosca, lo mandamos en
// el tick para que el badge de estado lo muestre en vez de quedar en
// silencio.
// -----------------------------------------------------
let resyncsRecientes = []; // timestamps de gaps detectados
const UMBRAL_RESYNCS_INESTABLE = 5;
const VENTANA_RESYNCS_MS = 10_000;

function registrarResync() {
  const ahora = Date.now();
  resyncsRecientes.push(ahora);
  resyncsRecientes = resyncsRecientes.filter((t) => ahora - t < VENTANA_RESYNCS_MS);
}

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
    const resp = await fetch(MERCADOS[mercadoActual].restSnapshot(SYMBOL));
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

// -----------------------------------------------------
// Deteccion de iceberg: se llama por cada nivel que toca un evento de
// depth, ANTES de que book.aplicarEventoDepth lo pise, asi conocemos el qty
// previo real. Solo trackeamos niveles cerca del touch (ICEBERG_DISTANCIA_
// USD) para que el costo sea chico -- lejos del touch un iceberg no es
// accionable igual.
// -----------------------------------------------------
function huboTradeReciente(precio, ahora) {
  while (historialTradesIceberg.length && ahora - historialTradesIceberg[0].tiempo > VENTANA_HISTORIAL_TRADES_MS) {
    historialTradesIceberg.shift();
  }
  return historialTradesIceberg.some((t) => t.precio === precio);
}

function trackearIceberg(precio, qtyNueva, esBid, midActual, qtyPrevia) {
  if (midActual == null || Math.abs(precio - midActual) > ICEBERG_DISTANCIA_USD) {
    trackerIceberg.delete(precio); // fuera de la zona de interes, no vale la pena seguir sosteniendolo
    return;
  }
  const ahora = Date.now();
  let t = trackerIceberg.get(precio);

  if (!t) {
    // Recien ahora vemos una caida fuerte y confirmada por un trade real --
    // arranca el ciclo de espera de un refill. Si no hay caida, no hace
    // falta trackear el nivel todavia (evita crear entradas para cada nivel
    // del book que nunca va a mostrar el patron).
    if (qtyNueva < qtyPrevia * ICEBERG_CAIDA_MINIMA && qtyPrevia >= ICEBERG_QTY_MINIMA && huboTradeReciente(precio, ahora)) {
      trackerIceberg.set(precio, { esBid, refills: 0, ultimoRefillTs: 0, consumidoDesde: ahora, qtyPreConsumo: qtyPrevia });
    }
    return;
  }

  if (t.consumidoDesde === null) {
    if (qtyNueva < qtyPrevia * ICEBERG_CAIDA_MINIMA && qtyPrevia >= ICEBERG_QTY_MINIMA && huboTradeReciente(precio, ahora)) {
      t.consumidoDesde = ahora;
      t.qtyPreConsumo = qtyPrevia;
    }
  } else if (ahora - t.consumidoDesde > ICEBERG_VENTANA_REFILL_MS) {
    t.consumidoDesde = null; // se paso el tiempo de espera sin refill, se cierra el ciclo sin contar
  } else if (qtyNueva >= t.qtyPreConsumo * ICEBERG_REFILL_MINIMO) {
    t.refills += 1;
    t.ultimoRefillTs = ahora;
    t.consumidoDesde = null;
  }
}

function capturarPreviosYTrackear(evento, midActual) {
  for (const [precioStr, qtyStr] of evento.b) {
    const p = parseFloat(precioStr);
    const previa = book.bids.get(p) || 0;
    trackearIceberg(p, parseFloat(qtyStr), true, midActual, previa);
  }
  for (const [precioStr, qtyStr] of evento.a) {
    const p = parseFloat(precioStr);
    const previa = book.asks.get(p) || 0;
    trackearIceberg(p, parseFloat(qtyStr), false, midActual, previa);
  }
}

function conectarBinance() {
  const miGeneracion = ++generacionConexion;
  const cfg = MERCADOS[mercadoActual];
  wsBinance = new WebSocket(cfg.wsUrl(SYMBOL));

  wsBinance.on("open", () => {
    if (miGeneracion !== generacionConexion) return;
    console.log(`[binance] conectado (${mercadoActual}), streams: ${SYMBOL}@depth@100ms/${SYMBOL}@aggTrade`);
    ultimoEstadoBinance = "conectado";
    pedirSnapshotYAplicar();
  });

  wsBinance.on("message", (raw) => {
    if (miGeneracion !== generacionConexion) return;
    const msg = JSON.parse(raw.toString());
    const stream = msg.stream;
    const data = msg.data;

    if (stream.endsWith("@depth@100ms") || stream.endsWith("@depth")) {
      if (esperandoSnapshot || !book.ready) {
        bufferEventos.push(data);
        return;
      }
      // Capturamos previos y trackeamos iceberg ANTES de aplicar el evento
      // (aplicarEventoDepth pisa book.bids/book.asks con los valores
      // nuevos) -- si el evento resulta ser un "gap", igual no pasa nada
      // grave, el tracker en el peor caso registra un falso consumo que se
      // va a limpiar solo por ICEBERG_VENTANA_REFILL_MS.
      if (ICEBERG_ACTIVADO) capturarPreviosYTrackear(data, book.midPrice());
      const resultado = book.aplicarEventoDepth(data);
      if (resultado === "gap") {
        registrarResync();
        console.warn(`[binance] gap detectado en la secuencia (${resyncsRecientes.length} en los ultimos ${VENTANA_RESYNCS_MS / 1000}s), re-sincronizando...`);
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
      if (ICEBERG_ACTIVADO) {
        historialTradesIceberg.push({ precio, tiempo: data.T });
        if (historialTradesIceberg.length > 500) historialTradesIceberg.splice(0, historialTradesIceberg.length - 500);
      }
    }
  });

  wsBinance.on("close", () => {
    if (miGeneracion !== generacionConexion) return; // esta conexion ya fue reemplazada a proposito (cambio de mercado)
    ultimoEstadoBinance = "desconectado";
    console.log("[binance] desconectado, reintentando en 3s...");
    setTimeout(() => {
      if (miGeneracion === generacionConexion) conectarBinance();
    }, 3000);
  });

  wsBinance.on("error", (err) => {
    if (miGeneracion !== generacionConexion) return;
    console.error("[binance] error de conexion:", err.message);
  });
}

// -----------------------------------------------------
// Cambio de mercado (spot <-> futuros) a pedido del frontend (ver mensaje
// {tipo:"mercado"} mas abajo). Corta la conexion vieja, arranca un book
// nuevo desde cero (spot y futuros son libros completamente distintos, no
// tiene sentido mezclar niveles de uno con el otro) y reconecta. En modo
// demo no hay conexion real que cambiar -- solo actualizamos la etiqueta.
// -----------------------------------------------------
function cambiarMercado(nuevo) {
  if (nuevo !== "spot" && nuevo !== "futuros") return;
  if (nuevo === mercadoActual) return;
  console.log(`[mercado] cambiando de ${mercadoActual} a ${nuevo}...`);

  if (MODO_DEMO) {
    mercadoActual = nuevo;
    return;
  }

  if (wsBinance) {
    wsBinance.removeAllListeners();
    wsBinance.close();
    wsBinance = null;
  }
  generacionConexion++; // invalida cualquier callback pendiente de la conexion vieja (open/message/close/error)
  mercadoActual = nuevo;
  book = new OrderBook({ esFuturos: MERCADOS[nuevo].esFuturos });
  bufferEventos = [];
  esperandoSnapshot = false;
  tradesRecientes = [];
  deltaAgresorVentana = 0;
  anclaCentro = null;
  modoManual = false;
  trackerIceberg.clear();
  emaPorNivel.clear();
  historialTradesIceberg = [];
  resyncsRecientes = [];
  ultimoEstadoBinance = "desconectado";
  conectarBinance();
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
    mercado: mercadoActual,
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
    } else if (msg.tipo === "mercado") {
      cambiarMercado(msg.valor);
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

  const ahora = Date.now();

  // Poda de iceberg: entradas que dejaron de refrescarse hace rato (ni un
  // consumo nuevo ni un refill) ya no aportan nada -- se olvidan.
  if (ICEBERG_ACTIVADO) {
    for (const [precio, t] of trackerIceberg) {
      const ref = t.ultimoRefillTs || t.consumidoDesde || 0;
      if (ref && ahora - ref > ICEBERG_EXPIRA_MS) trackerIceberg.delete(precio);
    }
  }
  const icebergs = ICEBERG_ACTIVADO
    ? Array.from(trackerIceberg.entries())
        .filter(([, t]) => t.refills >= ICEBERG_REFILLS_PARA_FLAG)
        .map(([precio, t]) => ({ precio, esBid: t.esBid, refills: t.refills }))
    : [];

  // Imanes: EMA lenta por nivel, reusando el mismo agregado por buckets que
  // ya se calculo arriba para no iterar el book de nuevo.
  let imanes = [];
  if (IMAN_ACTIVADO) {
    let sumaEma = 0;
    for (const nivel of niveles) {
      const qty = nivel.bid + nivel.ask;
      const prev = emaPorNivel.get(nivel.precio) ?? qty;
      const ema = prev * (1 - IMAN_ALPHA) + qty * IMAN_ALPHA;
      emaPorNivel.set(nivel.precio, ema);
      sumaEma += ema;
    }
    // poda perezosa: si el mapa crecio mucho mas alla de la ventana visible
    // actual (el ancla se movio con el tiempo), se descartan los niveles
    // que quedaron lejos -- se recalculan solos si el precio vuelve a pasar.
    if (emaPorNivel.size > niveles.length * 3) {
      const precioMin = niveles[0].precio - BUCKET_SIZE * RANGO_BUCKETS;
      const precioMax = niveles[niveles.length - 1].precio + BUCKET_SIZE * RANGO_BUCKETS;
      for (const p of emaPorNivel.keys()) {
        if (p < precioMin || p > precioMax) emaPorNivel.delete(p);
      }
    }
    const promedio = niveles.length > 0 ? sumaEma / niveles.length : 0;
    const umbral = promedio * IMAN_FACTOR;
    if (umbral > 0) {
      const candidatos = [];
      for (const nivel of niveles) {
        const ema = emaPorNivel.get(nivel.precio) || 0;
        if (ema >= umbral && ema >= IMAN_QTY_MINIMA) {
          candidatos.push({ precio: nivel.precio, esBid: nivel.bid >= nivel.ask, intensidad: ema / umbral });
        }
      }
      candidatos.sort((a, b) => b.intensidad - a.intensidad);
      imanes = candidatos.slice(0, IMAN_MAX_ENVIADOS);
    }
  }

  const payload = JSON.stringify({
    tipo: "tick",
    tiempo: ahora,
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
    mercado: mercadoActual,
    icebergs,
    imanes,
    syncInestable: resyncsRecientes.length >= UMBRAL_RESYNCS_INESTABLE,
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
