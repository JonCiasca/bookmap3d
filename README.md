# Book by JonFlow-MDQ — BTCUSDT

Motor visual: Three.js (WebGL) puro, sin Streamlit ni Plotly. Datos: websocket
de Binance (push, no polling), así no hay riesgo de rate-limit.

## Estructura

```
bookmap3d/
  relay/            <- proceso Node: se conecta a Binance, mantiene el book,
                        y sirve el frontend + el websocket local
    server.js
    orderbook.js
    gamma.js
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
  cada tick. **Switch de paleta** (abajo a la izquierda, arriba del switch
  de tiempo): **Clásico** (verde/rojo saturados, con las paredes más
  grandes resaltadas en amarillo-naranja como en Bookmap real), **Violeta**
  (comprador blanco/gris, vendedor violeta/lila con matiz fijo) y **B/N**
  (blanco y negro puro, comprador claro / vendedor oscuro). Cambiar de
  modo repinta todo al instante, no hace falta esperar a que se regenere
  el historial.
- **Burbujas**: cada trade ejecutado real, tamaño = volumen, celeste =
  compra agresiva, rosa = venta agresiva, conectadas por una línea que
  sigue el recorrido del precio. Flotan apenas por encima de la pared de
  SU propio nivel de precio (no a una altura fija de toda la escena).
- **Marcas de trades persistentes**: además de la burbuja (que se
  desvanece a los pocos segundos), cada trade deja una marca fija —
  amarilla si dominó la compra, naranja si dominó la venta — pegada a la
  pared del historial en la que ocurrió. A diferencia de la burbuja, esta
  marca viaja hacia atrás junto con esa pared y queda como registro
  permanente de dónde hubo ejecuciones, tanto del lado bid como ask.
- **Marcador de precio en vivo**: barra vertical amarilla con el precio
  exacto, la más alta de la escena — muestra dónde está el precio en vivo
  DENTRO de la ventana fija (ver "el precio oscila..." más abajo). Su
  movimiento en X de un tick a otro es justamente lo que hace visible la
  oscilación.
- **Textos de precio en naranja fuerte** (HUD y números del DOM): a
  propósito, para que nunca se confundan con el blanco de las paredes en
  los picos de volumen.
- **Delta agresor (10s)**, arriba a la izquierda: volumen neto de compras
  vs. ventas agresivas en la ventana reciente, en USD. Verde = domina la
  compra, rosa = domina la venta.
- **Tooltip**: pasá el mouse sobre una pared (o sobre el DOM del frente)
  para ver precio, y BTC acumulados en bid y en ask por separado.
- **DOM en vivo**, separado del historial por una línea celeste y con más
  distancia respecto al resto del gráfico que antes: a diferencia de las
  paredes, que quedan fijas en el instante en que se crearon y se alejan
  hacia atrás, esta fila siempre muestra el book actual, notoriamente más
  alta, más ancha y más brillante que el resto (bloque "amplificado" a
  propósito), con los números exactos de bid/ask flotando SIEMPRE por
  encima de cualquier barra vecina (antes podían quedar tapados por una
  pared más alta al lado) — es "la antesala" antes de que ese instante
  pase a formar parte del historial que fluye hacia atrás.
- **Gamma exposure (GEX)**, en el panel de info: estimador de gamma neto
  de las opciones de BTC en Deribit (calculado con open interest público
  + Black-Scholes a partir del IV que reporta Deribit — no es la posición
  real de ninguna mesa, es una heurística estándar basada en OI, la misma
  que usan la mayoría de los trackers públicos de GEX). *Gamma largo*
  (verde) sugiere que el mercado tiende a estabilizarse; *gamma corto*
  (rosa) sugiere que los movimientos tienden a amplificarse. Se actualiza
  cada 60 segundos (configurable, ver `GEX_INTERVALO_MS`) — no depende de
  Binance, así que si Deribit falla momentáneamente el resto sigue andando
  normal y simplemente no se actualiza ese dato.
- **Rango de precio ampliado**: por default ahora se ven $1100 para cada
  lado del precio ancla ($2200 de rango total, más de 2000 pips), antes
  eran ~$300 cada lado. Se ajusta con `RANGO_BUCKETS` (ver más abajo).
- **Switch de rango de tiempo**, abajo a la izquierda (`30s / 2m / 5m`):
  cambia cuánto historial se ve hacia atrás. El historial vive solo en el
  navegador (se arma con los ticks que van llegando, no hay nada guardado
  en el server), así que para los modos más largos el frontend no agrega
  una pared por CADA tick sino una cada N — la cantidad de paredes en
  pantalla se mantiene más o menos constante sea cual sea el modo, lo que
  cambia es cuánto tiempo real representa cada una. El número de cada
  botón es la duración REAL que va a mostrar (se recalcula solo si el
  server usa otro `INTERVALO_ENVIO_MS`, nunca queda desactualizado).
  Cambiar de modo vacía el historial que ya estaba armado y arranca de
  cero con el modo nuevo.
- **El precio oscila dentro de una ventana fija** (en vez de recentrar el
  gráfico en cada tick): la ventana visible de precios queda quieta, y el
  precio en vivo — marcado con una **barra vertical amarilla brillante**
  que cruza el DOM y las paredes, con su valor exacto flotando arriba —
  se mueve libremente adentro de esa ventana, como en un gráfico de
  trading normal. Solo cuando el precio se acerca a menos de
  `MARGEN_RECENTRADO_USD` (default 700) del borde, la ventana entera se
  recentra de golpe en el precio actual y vuelve a quedar fija. Antes el
  gráfico entero se movía en cada tick para mantener el precio siempre en
  el medio, lo que hacía parecer que "el precio nunca se mueve".
- **Podés mover la cámara (pan)** para ir a ver la liquidez en otro nivel
  de precio sin perder el resto de la escena — por ejemplo, mirar qué
  pasa en $63.000 mientras el precio está en $65.000, siempre que ese
  precio esté dentro de la ventana cargada (`RANGO_BUCKETS`). El pan es
  el de OrbitControls de siempre (click derecho + arrastrar en desktop,
  dos dedos en celu); solo se le subió la velocidad porque la escena
  ahora es más ancha.
- **Escena más ancha** (`ANCHO_TOTAL_DESEADO` de 34 a 52) y **colores más
  intensos** (antes se veían un poco pastel/lavados; ahora la saturación
  y el contraste de la escala bid/ask son más altos, se distingue mejor
  la intensidad de cada nivel de un vistazo).
- **Zoom**: además de rueda del mouse (o pellizco en el celu), hay botones
  **+ / −** abajo a la derecha de la pantalla, pensados como alternativa
  cuando el zoom con rueda se siente errático (varía mucho según el mouse
  y el sistema operativo).
- **Más brillo en las paredes lejanas**: se empujó el punto donde empieza
  la niebla (fog) de la escena, así el historial que quedó más atrás en
  el tiempo no se apaga tan rápido.

## Qué ajustar primero

Todo esto se configura por variables de entorno (ver `relay/.env.example`)
o directo en Render:

- `SYMBOL` — el par a seguir (default `btcusdt`).
- `BUCKET_SIZE` — ancho en USD de cada pared.
- `RANGO_BUCKETS` — cuántas paredes para cada lado del precio ancla
  (default 220 → con `BUCKET_SIZE=5` son $1100 para cada lado, $2200
  total). Si querés más rango todavía, subilo — cada bucket extra es más
  trabajo de render (más instancias por fila, x200 filas de historial).
- `MARGEN_RECENTRADO_USD` — qué tan cerca del borde de la ventana (en USD)
  tiene que llegar el precio en vivo para que la ventana se recentre sola
  (default 700). Más chico = la ventana sigue al precio más pegado; más
  grande = el precio puede oscilar más libre antes de que todo se mueva.
  Tiene que ser menor a la mitad del rango total (`RANGO_BUCKETS *
  BUCKET_SIZE`) o la ventana nunca dejaría de recentrarse.
- `INTERVALO_ENVIO_MS` — cada cuánto se manda un tick nuevo al frontend.
- `GEX_INTERVALO_MS` — cada cuánto se recalcula el gamma exposure desde
  Deribit (default 60000 = 60s; no hace falta bajarlo mucho, el open
  interest de opciones no cambia tan rápido).

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

**Ojo con los `envVars` de `render.yaml`**: Render sincroniza automático
las variables NUEVAS que agregás al Blueprint, pero **no siempre actualiza
el VALOR de una variable que ya existía** en un servicio ya creado (por
ejemplo, si cambiás `RANGO_BUCKETS` de 200 a 220 en el archivo, el
servicio en Render puede seguir usando 200 aunque el push haya sido
exitoso). Si después de un deploy no ves reflejado un cambio de una
variable existente, andá al dashboard de Render → tu servicio →
**Environment** y confirmá/editá el valor ahí a mano; eso siempre
funciona y dispara un redeploy solo.

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
