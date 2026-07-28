# Bookmap 3D — BTCUSDT

Motor visual: Three.js (WebGL) puro, sin Streamlit ni Plotly. Datos: websocket
de Binance (push, no polling), así no hay riesgo de rate-limit.

## Estructura

```
bookmap3d/
  relay/            <- proceso Node: se conecta a Binance, mantiene el book,
                        y sirve el frontend + el websocket local
    server.js
    orderbook.js
    package.json
    .env.example
  frontend/         <- lo que ves en el navegador
    index.html
  render.yaml        <- deploy en un click en Render
```

Desde esta versión, **un solo proceso Node hace todo**: sirve `frontend/`
como sitio estático y expone el websocket en el mismo puerto. Ya no hace
falta abrir `index.html` a mano; simplemente entrás a la URL del relay
(local o desplegado) y listo.

## Cómo correrlo local

Necesitás [Node.js](https://nodejs.org) 18 o superior.

```bash
cd bookmap3d/relay
npm install
npm start
```

Abrí `http://localhost:8081` en el navegador. Vas a ver algo como:

```
[binance] conectado, streams: btcusdt@depth@100ms/btcusdt@aggTrade
[binance] snapshot aplicado, lastUpdateId: ...
[local] sirviendo frontend + websocket en http://localhost:8081
```

### Modo demo (sin Binance)

Para probar el visual sin depender de la conexión a Binance:

```bash
npm run demo
```

Genera un book y trades sintéticos con random walk — útil para ajustar
colores, cámara, etc. sin gastar la conexión real.

## Qué vas a ver

- **Paredes**: cada franja horizontal es un instante de tiempo (el más
  nuevo adelante, "ahora"). Cada bloque es un bucket de precio (por
  default $5 de ancho). El **lado bid (compra) y ask (venta) se muestran
  por separado** — ya no se mezclan aunque estén en el mismo nivel de
  precio, que es la zona más importante para ver quién manda.
- **Color**: escala logarítmica con techo adaptativo (sube rápido ante un
  pico, baja lento), igual que Bookmap real — así los colores son
  comparables entre paredes y entre momentos distintos, no "respiran" con
  cada tick. Bid tiende a verde, ask a naranja/rojo; más intensidad =
  más volumen acumulado ahí.
- **Burbujas**: cada trade ejecutado real, tamaño = volumen, celeste =
  compra agresiva, rosa = venta agresiva, conectadas por una línea que
  sigue el recorrido del precio.
- **Delta agresor (10s)**, arriba a la izquierda: volumen neto de compras
  vs. ventas agresivas en la ventana reciente, en USD. Verde = domina la
  compra, rosa = domina la venta.
- **Tooltip**: pasá el mouse sobre una pared para ver precio, y BTC
  acumulados en bid y en ask por separado.

## Qué ajustar primero

Todo esto se configura por variables de entorno (ver `relay/.env.example`)
o directo en Render:

- `SYMBOL` — el par a seguir (default `btcusdt`).
- `BUCKET_SIZE` — ancho en USD de cada pared.
- `RANGO_BUCKETS` — cuántas paredes para cada lado del precio medio.
- `INTERVALO_ENVIO_MS` — cada cuánto se manda un tick nuevo al frontend.

## Precisión: qué cambió

- **Sincronización con re-sync automático**: antes, si se perdía un evento
  del websocket, el book quedaba desincronizado para siempre sin avisar
  (niveles fantasma, paredes que ya no existen). Ahora se valida la
  continuidad estricta de la secuencia de Binance y, ante un salto, se
  vuelve a pedir el snapshot solo.
- **Bid y ask separados por bucket**: antes se sumaban juntos y el lado se
  adivinaba por posición respecto al mid, mezclando compra y venta
  justo en la zona más importante (cerca del precio actual).
- **Bucketing con `floor` en vez de `round`**: cada pared cubre
  exactamente `[precio, precio + bucketSize)`, sin que medio bucket de
  volumen se cuele en el vecino equivocado.
- **Rendimiento**: el historial de paredes ahora usa `InstancedMesh` (una
  sola draw call por fila en vez de un objeto y material por bloque),
  soporta más historial y más buckets sin caerse de FPS.

## Subirlo a la red (Render)

El repo ya incluye `render.yaml`, así que el deploy es casi automático.

### 1. Subilo a GitHub

Si la carpeta `bookmap3d` todavía no es un repo de git:

```bash
cd bookmap3d
git init
git add .
git commit -m "Bookmap 3D"
```

Creá un repo nuevo en GitHub (podés hacerlo desde la web, botón "New
repository", sin README ni .gitignore — ya los tenés) y conectalo:

```bash
git remote add origin https://github.com/TU_USUARIO/bookmap3d.git
git branch -M main
git push -u origin main
```

### 2. Desplegalo en Render

1. Entrá a [render.com](https://render.com) y logueate (podés usar tu
   cuenta de GitHub directamente).
2. **New +** → **Blueprint**.
3. Elegí el repo `bookmap3d` que acabás de subir. Render va a detectar
   el `render.yaml` solo y te va a mostrar el servicio `book-jonflow-mdq`
   listo para crear, en la región **Frankfurt**.
4. Click en **Apply** / **Deploy**. Con el plan free alcanza y sobra.
5. Esperá el build (1-2 minutos). Cuando termine, Render te da una URL
   tipo `https://book-jonflow-mdq.onrender.com` — entrá ahí y listo, ya
   está corriendo para vos (o para cualquiera a quien le pases el link)
   desde el celu o donde sea.

**Por qué la región es Frankfurt y no la que viene por default (Oregon,
EEUU)**: Binance bloquea el acceso a `api.binance.com` desde IPs de
EEUU (es una restricción geográfica de Binance, no un límite de Render).
Si el servicio se crea en Oregon (el default), el relay nunca logra
conectarse a Binance y vas a ver el visualizador vacío, sin paredes ni
precio, aunque el estado diga "en vivo" (eso solo indica que tu navegador
se conectó al relay, no que el relay se conectó a Binance). Frankfurt
evita ese bloqueo. **Importante**: la región no se puede cambiar después
de creado el servicio — si ya creaste uno en Oregon, hay que borrarlo y
crear uno nuevo desde el Blueprint actualizado.

**Nota sobre el plan free de Render**: el servicio se "duerme" después de
~15 minutos sin tráfico, y la primera visita después de eso tarda unos
30-60 segundos en despertar (vas a ver la página en blanco un rato, es
normal — recargá). Si eso te molesta para uso diario, se puede pasar a un
plan pago chico (~7 USD/mes) que no duerme nunca; avisame si querés que
lo dejemos configurado así en vez del free.

### 3. Actualizaciones futuras

Cualquier cambio que hagas y subas a `main` (`git push`) hace que Render
redeploye solo — no hace falta tocar nada del lado de Render de nuevo.

## Para más adelante

- Si en algún momento querés que el frontend viva en otro lado (ej.
  GitHub Pages) separado del relay, el código ya está preparado: el
  frontend detecta automáticamente si lo estás sirviendo desde el mismo
  origen que el relay (usa `location.host`) o si tiene que apuntar a
  `localhost:8081` para desarrollo. Para un origen distinto habría que
  agregar la URL del relay como variable de config.
- Se podría sumar un segundo símbolo (ej. ETHUSDT) corriendo en paralelo,
  pero eso ya es un cambio de alcance — avisame si lo querés y lo
  planificamos aparte.
