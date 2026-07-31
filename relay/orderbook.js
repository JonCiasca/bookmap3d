// orderbook.js
//
// Reconstruye el order book local aplicando el metodo OFICIAL de Binance.
// OJO: spot y futuros (USD-M) usan reglas LIGERAMENTE distintas -- no es
// solo el "pu" de continuidad (eso ya estaba separado), tambien difieren el
// descarte de eventos viejos y la condicion del primer evento. Mezclar la
// regla de spot para futuros hace que el book futuros re-sincronice todo el
// tiempo (cada evento normal se mal-interpreta como gap), lo que en la
// practica significa que el book pasa la mayor parte del tiempo recien
// reseteado y vacio -- de ahi que "no se vea nada" en futuros.
//
// SPOT (https://api.binance.com, streams @depth):
//   1) Abrir el websocket de depth y bufferizar los eventos que lleguen.
//   2) Pedir UNA vez el snapshot REST (/api/v3/depth?limit=1000).
//   3) Descartar del buffer los eventos con u <= lastUpdateId del snapshot.
//   4) El PRIMER evento aplicado debe cumplir  U <= lastUpdateId+1 <= u.
//   5) De ahi en mas, cada evento nuevo debe ser CONTINUO: su U tiene que
//      ser exactamente lastUpdateId+1. Si hay un salto (gap), el book quedo
//      desincronizado y hay que volver a pedir snapshot (resync).
//
// FUTUROS USD-M (https://fapi.binance.com, streams @depth) -- ver "How to
// manage a local order book correctly" en developers.binance.com:
//   3) Descartar del buffer los eventos con u < lastUpdateId (sin el "=").
//   4) El PRIMER evento aplicado debe cumplir  U <= lastUpdateId <= u  (SIN
//      el +1 que usa spot).
//   5) De ahi en mas, cada evento nuevo debe tener pu === u del evento
//      anterior aplicado. Si no, resync.
//
// El paso 5 (deteccion de gap) es la diferencia clave con la version
// anterior a todo esto: antes un evento perdido pasaba silenciosamente y el
// book quedaba "mentiroso" para siempre (niveles fantasma, paredes que ya
// no existen). Ahora se detecta y se resincroniza solo.
//
// bids/asks se guardan como Map(precio -> cantidad). Cantidad 0 = borrar.

export class OrderBook {
  /**
   * @param {{esFuturos?: boolean}} opciones - esFuturos cambia la regla de
   * continuidad que usa aplicarEventoDepth: spot valida con "U" (debe ser
   * exactamente lastUpdateId+1), futuros valida con "pu" (previous update
   * id, tiene que matchear el "u" del evento anterior aplicado) -- son
   * streams distintos de Binance con formatos de sync ligeramente
   * distintos, ver docs oficiales de cada uno.
   */
  constructor(opciones = {}) {
    this.esFuturos = !!opciones.esFuturos;
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
   * Aplica un evento crudo del stream <symbol>@depth (spot o futuros, segun
   * this.esFuturos -- ver comentario de la clase con la diferencia exacta
   * de reglas entre los dos).
   * Devuelve:
   *   "aplicado"   - se aplico bien
   *   "viejo"      - evento anterior al snapshot, se descarta (normal al inicio)
   *   "gap"        - se perdio al menos un evento => book invalido => resync
   */
  aplicarEventoDepth(evento) {
    if (!this.ready || this.desincronizado) return "gap";

    // Evento enteramente anterior al snapshot: descartar. Futuros usa "<"
    // (no "<="): un evento con u === lastUpdateId es exactamente el que
    // hace falta para validar el primer evento un poco mas abajo, spot en
    // cambio SI lo descarta (su regla del primer evento usa lastUpdateId+1,
    // no lastUpdateId).
    if (this.esFuturos ? evento.u < this.lastUpdateId : evento.u <= this.lastUpdateId) {
      return "viejo";
    }

    if (!this.primerEventoAplicado) {
      if (this.esFuturos) {
        // Futuros: U <= lastUpdateId <= u (SIN el +1 de spot). El "u >=
        // lastUpdateId" ya quedo garantizado por el descarte de arriba.
        if (evento.U > this.lastUpdateId) {
          this.desincronizado = true;
          return "gap";
        }
      } else if (evento.U > this.lastUpdateId + 1) {
        // Spot: U <= lastUpdateId+1 <= u
        this.desincronizado = true;
        return "gap";
      }
      this.primerEventoAplicado = true;
    } else if (this.esFuturos) {
      // Futuros (USD-M): la continuidad se valida con "pu" (previous update
      // id del evento), que tiene que ser EXACTAMENTE el "u" del evento
      // anterior aplicado -- "U" en futuros no sirve para esto (a
      // diferencia de spot).
      if (evento.pu !== this.lastUpdateId) {
        this.desincronizado = true;
        return "gap";
      }
    } else {
      // Spot: continuidad estricta, el U de cada evento nuevo debe ser
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
   * Agrega el book en "buckets" de precio alrededor de un precio ANCLA.
   *
   * OJO con el parametro `precioAncla`: a proposito NO es siempre el mid
   * en vivo. Si lo fuera, la ventana de precios se re-centraria en CADA
   * tick, y visualmente el precio actual quedaria siempre pegado al medio
   * de la pantalla (el book "se mueve", el precio nunca se mueve) -- eso
   * es lo contrario de un grafico de trading normal, donde el precio
   * oscila DENTRO de un rango fijo y solo se re-centra cuando se acerca
   * al borde. Quien llama a esta funcion (server.js) decide el ancla con
   * esa logica de margen; esta funcion solo dibuja los buckets alrededor
   * de lo que le pasen.
   *
   * Mejoras de precision vs la version anterior:
   *  - Math.floor en vez de Math.round: cada bucket cubre exactamente
   *    [precio, precio + bucketSize). Antes, con round, medio bucket de
   *    volumen se asignaba al vecino equivocado.
   *  - bid y ask se acumulan POR SEPARADO. Antes se sumaban juntos y el
   *    lado se adivinaba por posicion vs mid, lo que mezclaba compra y
   *    venta justo alrededor del precio (la zona mas importante).
   *
   * Devuelve { mid, mejorBid, mejorAsk, centro, niveles } donde cada nivel
   * es { precio, bid, ask }. `centro` es el precio exacto (redondeado a
   * bucket) alrededor del cual se armaron los niveles -- el frontend lo
   * necesita para ubicar el precio actual y los trades en su posicion
   * real dentro de la ventana, en vez de asumir que siempre esta al medio.
   */
  agregarPorBuckets(bucketSize, rango, precioAncla) {
    const tope = this.mejorBidAsk();
    if (!tope) return { mid: null, niveles: [] };
    const mid = (tope.mejorBid + tope.mejorAsk) / 2;
    const ancla = precioAncla ?? mid; // si no pasan ancla, comportamiento viejo (centrado en mid)

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

    const centro = bucketDe(ancla);
    const niveles = [];
    for (let i = -rango; i <= rango; i++) {
      const precio = centro + i * bucketSize;
      niveles.push({
        precio,
        bid: bidsPorBucket.get(precio) || 0,
        ask: asksPorBucket.get(precio) || 0,
      });
    }

    return { mid, mejorBid: tope.mejorBid, mejorAsk: tope.mejorAsk, centro, niveles };
  }
}
