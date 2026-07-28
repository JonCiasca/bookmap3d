// orderbook.js
//
// Reconstruye el order book local aplicando el metodo OFICIAL de Binance spot:
//   1) Abrir el websocket de depth y bufferizar los eventos que lleguen.
//   2) Pedir UNA vez el snapshot REST (/api/v3/depth?limit=1000).
//   3) Descartar del buffer los eventos con u <= lastUpdateId del snapshot.
//   4) El PRIMER evento aplicado debe cumplir  U <= lastUpdateId+1 <= u.
//   5) De ahi en mas, cada evento nuevo debe ser CONTINUO: su U tiene que
//      ser exactamente lastUpdateId+1. Si hay un salto (gap), el book quedo
//      desincronizado y hay que volver a pedir snapshot (resync).
//
// El paso 5 es la diferencia clave con la version anterior: antes un evento
// perdido pasaba silenciosamente y el book quedaba "mentiroso" para siempre
// (niveles fantasma, paredes que ya no existen). Ahora se detecta y se
// resincroniza solo.
//
// bids/asks se guardan como Map(precio -> cantidad). Cantidad 0 = borrar.

export class OrderBook {
  constructor() {
    this.bids = new Map(); // precio -> qty
    this.asks = new Map();
    this.lastUpdateId = 0;
    this.ready = false;
    this.primerEventoAplicado = false;
    this.desincronizado = false; // true => hace falta resync (nuevo snapshot)
  }

  reset() {
    this.bids.clear();
    this.asks.clear();
    this.lastUpdateId = 0;
    this.ready = false;
    this.primerEventoAplicado = false;
    this.desincronizado = false;
  }

  aplicarSnapshotRest(snapshot) {
    this.bids.clear();
    this.asks.clear();
    for (const [precio, qty] of snapshot.bids) {
      const q = parseFloat(qty);
      if (q > 0) this.bids.set(parseFloat(precio), q);
    }
    for (const [precio, qty] of snapshot.asks) {
      const q = parseFloat(qty);
      if (q > 0) this.asks.set(parseFloat(precio), q);
    }
    this.lastUpdateId = snapshot.lastUpdateId;
    this.ready = true;
    this.primerEventoAplicado = false;
    this.desincronizado = false;
  }

  /**
   * Aplica un evento crudo del stream <symbol>@depth (formato spot).
   * Devuelve:
   *   "aplicado"   - se aplico bien
   *   "viejo"      - evento anterior al snapshot, se descarta (normal al inicio)
   *   "gap"        - se perdio al menos un evento => book invalido => resync
   */
  aplicarEventoDepth(evento) {
    if (!this.ready || this.desincronizado) return "gap";

    // Evento enteramente anterior al snapshot: descartar.
    if (evento.u <= this.lastUpdateId) return "viejo";

    if (!this.primerEventoAplicado) {
      // Regla oficial para el primer evento tras el snapshot:
      // U <= lastUpdateId+1 <= u
      if (evento.U > this.lastUpdateId + 1) {
        this.desincronizado = true;
        return "gap";
      }
      this.primerEventoAplicado = true;
    } else {
      // Continuidad estricta: el U de cada evento nuevo debe ser
      // exactamente lastUpdateId + 1. Si no, se perdio un evento.
      if (evento.U !== this.lastUpdateId + 1) {
        this.desincronizado = true;
        return "gap";
      }
    }

    for (const [precio, qty] of evento.b) {
      const p = parseFloat(precio);
      const q = parseFloat(qty);
      if (q === 0) this.bids.delete(p);
      else this.bids.set(p, q);
    }
    for (const [precio, qty] of evento.a) {
      const p = parseFloat(precio);
      const q = parseFloat(qty);
      if (q === 0) this.asks.delete(p);
      else this.asks.set(p, q);
    }
    this.lastUpdateId = evento.u;
    return "aplicado";
  }

  mejorBidAsk() {
    let mejorBid = -Infinity;
    let mejorAsk = Infinity;
    for (const p of this.bids.keys()) if (p > mejorBid) mejorBid = p;
    for (const p of this.asks.keys()) if (p < mejorAsk) mejorAsk = p;
    if (mejorBid === -Infinity || mejorAsk === Infinity) return null;
    return { mejorBid, mejorAsk };
  }

  midPrice() {
    const tope = this.mejorBidAsk();
    return tope ? (tope.mejorBid + tope.mejorAsk) / 2 : null;
  }

  /**
   * Agrega el book en "buckets" de precio alrededor del mid.
   *
   * Mejoras de precision vs la version anterior:
   *  - Math.floor en vez de Math.round: cada bucket cubre exactamente
   *    [precio, precio + bucketSize). Antes, con round, medio bucket de
   *    volumen se asignaba al vecino equivocado.
   *  - bid y ask se acumulan POR SEPARADO. Antes se sumaban juntos y el
   *    lado se adivinaba por posicion vs mid, lo que mezclaba compra y
   *    venta justo alrededor del precio (la zona mas importante).
   *
   * Devuelve { mid, mejorBid, mejorAsk, niveles } donde cada nivel es
   * { precio, bid, ask }.
   */
  agregarPorBuckets(bucketSize, rango) {
    const tope = this.mejorBidAsk();
    if (!tope) return { mid: null, niveles: [] };
    const mid = (tope.mejorBid + tope.mejorAsk) / 2;

    const bucketDe = (precio) => Math.floor(precio / bucketSize) * bucketSize;

    const bidsPorBucket = new Map();
    const asksPorBucket = new Map();
    for (const [precio, qty] of this.bids) {
      const b = bucketDe(precio);
      bidsPorBucket.set(b, (bidsPorBucket.get(b) || 0) + qty);
    }
    for (const [precio, qty] of this.asks) {
      const b = bucketDe(precio);
      asksPorBucket.set(b, (asksPorBucket.get(b) || 0) + qty);
    }

    const centro = bucketDe(mid);
    const niveles = [];
    for (let i = -rango; i <= rango; i++) {
      const precio = centro + i * bucketSize;
      niveles.push({
        precio,
        bid: bidsPorBucket.get(precio) || 0,
        ask: asksPorBucket.get(precio) || 0,
      });
    }

    return { mid, mejorBid: tope.mejorBid, mejorAsk: tope.mejorAsk, niveles };
  }
}
