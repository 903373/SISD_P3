// Replicación del registro (tema 5, págs. 35-38): el líder manda
// AppendEntries periódicos (latidos, con las entradas que falten a cada
// seguidor), cuenta cuántos tienen cada entrada y la compromete cuando
// hay mayoría. También recibe las operaciones de los clientes.
// Aquí están los dos lados: el líder que ENVÍA y el seguidor que RECIBE.
package raft

import (
	"time"
)

// Líder: una ronda de latidos y espera 50 ms.
func (nr *NodoRaft) comoLider() {
	nr.enviarLatidos()                  // ronda a todos los seguidores
	<-time.After(50 * time.Millisecond) // espero (como un Sleep)
}

// Manda a cada seguidor un AppendEntries con lo que le falte
// (si no le falta nada, va vacío = latido)
func (nr *NodoRaft) enviarLatidos() {
	for i := range nr.Nodos {
		if i != nr.Yo { // a todos menos a mí,
			go nr.enviarLatidoA(i) // en paralelo: si uno está caído,
		} // los demás no esperan por él
	}
}

// Prepara el AppendEntries para el nodo i según SiguienteIndice[i]:
// le mando todo lo que tengo desde la entrada que le toca.
func (nr *NodoRaft) prepararArgs(i int) *ArgAppendEntries {
	sig := nr.SiguienteIndice[i] // primera entrada que le falta
	return &ArgAppendEntries{
		Mandato:         nr.MandatoActual, // mi mandato
		IdLider:         nr.Yo,            // soy el líder
		IndiceAnterior:  sig - 1,
		MandatoAnterior: nr.Registro[sig-1].Mandato,
		Entradas:        append([]Entrada(nil), nr.Registro[sig:]...),
		IndiceCommit:    nr.IndiceCommit, // hasta dónde he comprometido
	}
}

// Goroutine: envía un AppendEntries a un nodo y procesa su respuesta
// PARÁMETRO nodo: a qué seguidor (posición en nr.Nodos)
func (nr *NodoRaft) enviarLatidoA(nodo int) {
	nr.Mux.Lock()
	args := nr.prepararArgs(nodo) // preparo con el candado cogido
	nr.Mux.Unlock()               // suelto antes de enviar

	var results Results // aquí llegará la respuesta del seguidor
	// RPC: ejecuta AppendEntries en el nodo remoto (máx. 50 ms)
	// args viaja al seguidor (allí se llama "args"); CallTimeout
	// rellena results con lo que el seguidor puso en su "results"
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args,
		&results, 50*time.Millisecond)
	if err == nil { // contestó (si no, ya le llegará el próximo)
		nr.Mux.Lock()
		// le paso: a quién envié, QUÉ le envié (args) y qué contestó
		nr.procesarRespuesta(nodo, args, &results)
		nr.Mux.Unlock()
	}
}

// Actualiza lo que sé del seguidor según su respuesta (pág.57)
//
// PARÁMETROS:
//
//	nodo:    el seguidor que ha contestado
//	args:    el mensaje que YO le mandé (lo creó prepararArgs)
//	results: lo que él contestó en AppendEntries:
//	         results.Mandato = SU mandato, results.Exito = si aceptó
func (nr *NodoRaft) procesarRespuesta(nodo int, args *ArgAppendEntries,
	results *Results) {
	// Si el seguidor tiene un mandato mayor, yo ya no soy el líder
	// válido: me actualizo y bajo a seguidor (pág.48)
	nr.actualizarMandato(results.Mandato)
	// Si no coinciden, la respuesta es de un mandato anterior
	if nr.Estado != Lider || args.Mandato != nr.MandatoActual {
		return // ya no soy líder de ese mandato: respuesta vieja
	}
	if results.Exito { // el seguidor ha aceptado las entradas
		// Ahora tiene hasta la última que le mandé
		// = la anterior que le dije + cuántas nuevas le mandé
		nr.IndiceCoincide[nodo] = args.IndiceAnterior + len(args.Entradas)
		// La próxima vez le mando desde la siguiente
		nr.SiguienteIndice[nodo] = nr.IndiceCoincide[nodo] + 1
		nr.actualizarCommitLider() // ¿ya hay mayoría en alguna?
	} else if nr.SiguienteIndice[nodo] > 1 {
		// Su registro no encajaba en IndiceAnterior: la próxima vez
		// pruebo desde una entrada antes (pág.46; sin fallos no pasa)
		nr.SiguienteIndice[nodo]--
	}
}

// Busca la entrada más alta que ya esté en mayoría y la compromete
// (y con ella todas las anteriores). Solo cuenta entradas de mi
// mandato: regla 2 de la pág.44.
func (nr *NodoRaft) actualizarCommitLider() {
	// Recorro desde la última entrada hacia atrás, sin bajar de lo ya comprometido
	// n: posición que estoy mirando (len-1, len-2, ... hasta commit+1)
	for n := len(nr.Registro) - 1; n > nr.IndiceCommit; n-- {
		if nr.Registro[n].Mandato == nr.MandatoActual &&
			nr.replicadaEnMayoria(n) {
			nr.IndiceCommit = n // comprometida (y todas las anteriores)
			return
		}
	}
}

// ¿Tiene la entrada n una mayoría de nodos (contándome a mí)?
// PARÁMETRO n: la posición del registro que quiere comprometer
// DEVUELVE: true si la tienen más de la mitad de los nodos
func (nr *NodoRaft) replicadaEnMayoria(n int) bool {
	cuantos := 1 // yo la tengo seguro (soy el líder)
	for i := range nr.Nodos {
		// ¿el nodo i tiene al menos hasta la entrada n?
		if i != nr.Yo && nr.IndiceCoincide[i] >= n {
			cuantos++
		}
	}
	return cuantos > len(nr.Nodos)/2 // mayoría: 2 de 3
}

// Un cliente pide añadir una operación (lo llama la RPC
// SometerOperacionRaft). Si soy líder la añado a mi registro y espero
// a que se comprometa por mayoría (pág.36). Devuelve:
//
//	indice:       posición del registro donde ha ido la operación
//	mandato:      mandato en curso
//	comprometida: true si se ha comprometido (false si no soy líder
//	              o dejé de serlo antes de conseguirlo)
//	idLider:      quién es el líder (para que el cliente lo busque)
//	valor:        resultado de la operación (se usará en la P5)
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	nr.Mux.Lock()
	if nr.Estado != Lider { // solo el líder acepta operaciones
		defer nr.Mux.Unlock()
		return -1, nr.MandatoActual, false, nr.IdLider, ""
	}
	indice := len(nr.Registro) // irá detrás de la última que tengo
	mandato := nr.MandatoActual
	// La añado a mi registro (aún NO comprometida). Los latidos se
	// encargan de enviarla a los seguidores
	nr.Registro = append(nr.Registro,
		Entrada{Indice: indice, Mandato: mandato, Operacion: operacion})
	nr.Mux.Unlock() // suelto: los latidos necesitan el candado
	// le paso: la posición de la nueva entrada y mi mandato al crearla
	comprometida := nr.esperarCompromiso(indice, mandato)
	return indice, mandato, comprometida, nr.Yo, ""
}

// Espera (máx. 2 s) a que la entrada "indice" quede comprometida.
// Mira cada 10 ms; deja de esperar si pierdo el liderazgo
//
// PARÁMETROS:
//
//	indice:  posición donde someterOperacion añadió la operación
//	mandato: mi mandato cuando la añadí
func (nr *NodoRaft) esperarCompromiso(indice int, mandato int) bool {
	limite := time.Now().Add(2 * time.Second) // hora límite
	for time.Now().Before(limite) {           // mientras no llegue
		nr.Mux.Lock()
		hecho := nr.IndiceCommit >= indice // ¿ya está comprometida?
		sigoLider := nr.Estado == Lider && nr.MandatoActual == mandato
		nr.Mux.Unlock()
		if hecho || !sigoLider {
			return hecho // terminada (bien o mal)
		}
		time.Sleep(10 * time.Millisecond) // aún no: espero un poco
	}
	return false // pasaron 2 s y no se comprometió
}

// Respuesta de SometerOperacionRaft
type ResultadoRemoto struct {
	ValorADevolver string // resultado de la operación (P5)
	IndiceRegistro int    // en qué entrada se ha colocado
	// Mandato, IdLider y EsLider. EsLider = "comprometida"
	EstadoParcial
}

// RPC: un cliente somete una operación a este nodo
func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	// Relleno la respuesta con los 5 valores de someterOperacion()
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// Argumentos de AppendEntries (figura 2). Sirve para dos cosas:
// latido (Entradas vacío) y replicar entradas del registro
type ArgAppendEntries struct {
	Mandato         int       // mandato del líder
	IdLider         int       // quién es el líder (para redirigir clientes)
	IndiceAnterior  int       // índice de la entrada anterior a las nuevas
	MandatoAnterior int       // mandato de esa entrada anterior
	Entradas        []Entrada // entradas nuevas (vacío = solo latido)
	IndiceCommit    int       // hasta dónde ha comprometido el líder
}

// Respuesta de AppendEntries
type Results struct {
	Mandato int  // mi mandato, para que el líder se actualice
	Exito   bool // ¿he aceptado las entradas?
}

// RPC: el líder me manda un latido o entradas nuevas
// (figura 2, "AppendEntries, Receiver implementation", pasos 1-5)
// Se ejecuta en el seguidor.
//
// PARÁMETROS:
//
//	args:    el mensaje que creó el líder en prepararArgs
//	         (args.Mandato = SU mandato)
//	results: mi respuesta; vuelve al líder como "results" en
//	         enviarLatidoA y de ahí a procesarRespuesta
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	nr.actualizarMandato(args.Mandato) // ¿líder con mandato mayor?
	results.Mandato = nr.MandatoActual // le digo mi mandato
	results.Exito = false              // por defecto, rechazo
	if args.Mandato < nr.MandatoActual {
		return nil // 1. líder antiguo: lo ignoro (pág.48)
	}
	// Es el líder legítimo de este mandato:
	nr.Estado = Seguidor      // si era candidato, he perdido: seguidor
	nr.IdLider = args.IdLider // apunto quién es el líder
	nr.reiniciarTimeout()     // el líder vive: reinicio mi cuenta atrás
	// le paso: dónde dice el líder que van las nuevas y de qué mandato
	if !nr.registroCoincide(args.IndiceAnterior, args.MandatoAnterior) {
		return nil // 2. no tengo la entrada anterior: rechazo
	}
	nr.anadirEntradas(args.IndiceAnterior, args.Entradas) // 3 y 4
	// le paso: hasta dónde ha comprometido el líder, y la posición de
	// la última entrada que me ha mandado (anterior + nº de nuevas)
	nr.avanzarCommit(args.IndiceCommit,
		args.IndiceAnterior+len(args.Entradas)) // 5
	results.Exito = true // todo bien: le digo que las tengo
	return nil
}

// Paso 2: ¿tengo en "indice" una entrada del mandato "mandato"?
// Si sí, mi registro encaja con el del líder hasta ahí (pág.38)
//
// PARÁMETROS:
//
//	indice:  args.IndiceAnterior  (= SiguienteIndice-1 del líder)
//	mandato: args.MandatoAnterior (mandato de esa entrada en el líder)
func (nr *NodoRaft) registroCoincide(indice int, mandato int) bool {
	return indice < len(nr.Registro) && // tengo esa posición...
		nr.Registro[indice].Mandato == mandato // ...y del mismo mandato
}

// Pasos 3 y 4: añade las entradas nuevas detrás de "anterior". Si
// alguna choca con una que ya tengo (mismo índice, distinto mandato),
// borro desde ahí (era basura de un líder antiguo)
//
// PARÁMETROS (vienen del líder):
//
//	anterior: args.IndiceAnterior (las nuevas van detrás de esta)
//	entradas: args.Entradas (las que me faltaban)
func (nr *NodoRaft) anadirEntradas(anterior int, entradas []Entrada) {
	for j, e := range entradas { // j: 0, 1, 2...  e: la entrada
		indice := anterior + 1 + j      // posición donde va esta entrada
		if indice < len(nr.Registro) && // ya tengo algo ahí...
			nr.Registro[indice].Mandato != e.Mandato { // ...distinto
			nr.Registro = nr.Registro[:indice] // 3. borro desde ahí
		}
		if indice >= len(nr.Registro) { // esa posición está libre
			nr.Registro = append(nr.Registro, e) // 4. la añado
		}
	}
}

// Paso 5: comprometo hasta donde dice el líder, pero sin pasar de la
// última entrada que tengo: commitIndex = min(commitLider, ultimoNuevo)
//
// PARÁMETROS:
//
//	commitLider: args.IndiceCommit (hasta dónde comprometió el líder)
//	ultimoNuevo: args.IndiceAnterior + len(args.Entradas) (última
//	             posición que sé que tengo igual que el líder)
func (nr *NodoRaft) avanzarCommit(commitLider int, ultimoNuevo int) {
	if commitLider > nr.IndiceCommit { // el líder va por delante
		nr.IndiceCommit = commitLider
		if ultimoNuevo < commitLider { // pero yo no tengo tantas
			nr.IndiceCommit = ultimoNuevo
		}
	}
}
