// El código está repartido en 3 ficheros del mismo paquete:
//
//	raft.go        núcleo: tipos, creación del nodo, bucle principal
//	               (máquina de estados) y API hacia fuera (estado, parar)
//	eleccion.go    elección de líder: seguidor, candidato y RPC PedirVoto
//	replicacion.go líder: latidos, RPC AppendEntries, compromiso de
//	               entradas y someter operaciones de clientes
package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"raft/internal/comun/rpctimeout"
)

const (
	// Valor -1 para indicar "entero sin inicializar"
	IntNOINICIALIZADO = -1

	// true: cada nodo escribe trazas de depuración en un fichero
	kEnableDebugLogs = true

	// true: las trazas salen por pantalla en vez de a fichero
	kLogToStdout = false

	// Carpeta donde se crea un fichero de trazas por nodo
	kLogOutputDir = "./logs_raft/"
)

// Operación que un cliente pide ejecutar sobre el almacén clave/valor
type TipoOperacion struct {
	Operacion string // "leer" o "escribir"
	Clave     string // clave sobre la que se opera
	Valor     string // valor a escribir ("" si es lectura)
}

// Mensaje que Raft enviará a la máquina de estados por cada entrada
// comprometida
type AplicaOperacion struct {
	Indice    int           // posición de la entrada en el registro
	Operacion TipoOperacion // operación a aplicar
}

// Los 3 estados posibles de un nodo (máquina de estados, tema 5 pág.30)
const (
	Seguidor  = "seguidor"  // obedece al líder y espera latidos
	Candidato = "candidato" // pide votos para ser líder
	Lider     = "lider"     // recibe operaciones y manda latidos
)

// Una entrada del registro de operaciones (tema 5 pág.35)
type Entrada struct {
	Indice    int           // posición en el registro (1, 2, 3...)
	Mandato   int           // mandato del líder que la creó
	Operacion TipoOperacion // la operación del cliente
}

// Estado completo de un nodo (réplica) Raft
type NodoRaft struct {
	Mux sync.Mutex

	// Direcciones "ip:puerto" de todos los nodos, mismo orden en todos
	Nodos   []rpctimeout.HostPort
	Yo      int // mi posición dentro de Nodos
	IdLider int // quién creo que es el líder (-1 si no lo sé)

	// Fichero de trazas propio de este nodo (depuración)
	Logger *log.Logger

	// Estado persistente (figura 2)
	MandatoActual int       // último mandato que ha visto (currentTerm)
	VotadoA       int       // a quién votó en este mandato, -1 si a nadie
	Registro      []Entrada // las entradas del log (pos. 0 = ficticia)

	// Estado volátil en todos
	IndiceCommit   int // última entrada comprometida (commitIndex)
	UltimoAplicado int // última aplicada a la máquina de estados (P5)

	// Estado volátil solo del líder (se crea al ganar la elección)
	SiguienteIndice []int // por cada nodo: próxima entrada a enviarle
	IndiceCoincide  []int // por cada nodo: última que sabe que tiene

	// Implementación (no está en la figura 2)
	Estado           string               // Seguidor, Candidato o Lider
	ReiniciarTimeout chan bool            // seguidor: espera más
	CanalAplicar     chan AplicaOperacion // hacia la máquina de estados
}

// Crea un nuevo nodo Raft y lo pone en marcha.
// QUIÉN LA LLAMA: cmd/srvraft/main.go, al arrancar el proceso
// PARÁMETROS:
//
//	nodos: las direcciones que van detrás del número (os.Args[2:])
//	yo:    el primer número (os.Args[1]): 0, 1 o 2
//	canalAplicarOperacion: canal que crea main (se usará en la P5)
//
// DEVUELVE: el nodo creado; main lo registra para recibir RPCs
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{} // reservo el nodo (todos los campos a cero)
	nr.Nodos = nodos  // guardo las direcciones de todos
	nr.Yo = yo        // y cuál soy yo
	nr.IdLider = -1   // al arrancar no conozco al líder

	nr.Logger = crearLogger(nodos, yo) // trazas de depuración

	nr.MandatoActual = 0
	nr.VotadoA = -1
	// Entrada ficticia en la posición 0: así el índice del slice
	// coincide con el índice Raft (las reales empiezan en 1)
	nr.Registro = []Entrada{{Indice: 0, Mandato: 0}}
	nr.IndiceCommit = 0   // nada comprometido todavía
	nr.UltimoAplicado = 0 // nada aplicado todavía (P5)
	nr.Estado = Seguidor  // todo nodo arranca como seguidor (pág.30)
	// Buffer de 10 para que quien avisa no se quede bloqueado
	nr.ReiniciarTimeout = make(chan bool, 10)
	nr.CanalAplicar = canalAplicarOperacion // lo guardo para la P5

	go nr.gestionarNodo() // arranco la máquina de estados en paralelo
	return nr             // y vuelvo enseguida, como pide el esqueleto
}

// Crea el registro de trazas de depuración del nodo
func crearLogger(nodos []rpctimeout.HostPort, yo int) *log.Logger {
	if !kEnableDebugLogs {
		// Trazas desactivadas: el logger escribe a la "nada"
		return log.New(ioutil.Discard, "", 0)
	}
	// Nombre del nodo a partir de su ip y puerto: "192.168.3.13_29230"
	nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
	logPrefix := fmt.Sprintf("%s", nombreNodo)
	fmt.Println("LogPrefix: ", logPrefix)

	if kLogToStdout {
		// Trazas por pantalla
		return log.New(os.Stdout, nombreNodo+" -->> ",
			log.Lmicroseconds|log.Lshortfile)
	}
	// Trazas a fichero: crea la carpeta logs_raft si no existe
	err := os.MkdirAll(kLogOutputDir, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}
	// Abre (o crea y vacía) logs_raft/<nombreNodo>.txt
	logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
		kLogOutputDir, logPrefix),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		panic(err.Error())
	}
	// Cada línea lleva el prefijo, la hora en µs y fichero:línea
	logger := log.New(logOutputFile,
		logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
	logger.Println("logger initialized")
	return logger
}

// Bucle principal del nodo (se lanza como goroutine en NuevoNodo).
// Implementa la máquina de estados del tema 5, pág.30: en cada vuelta
// mira en qué estado está y ejecuta una "ronda" de ese estado
func (nr *NodoRaft) gestionarNodo() {
	for { // nunca termina: el nodo vive hasta que se para el proceso
		nr.Mux.Lock()
		estado := nr.Estado // copio el estado con el candado cogido...
		nr.Mux.Unlock()     // ...y lo suelto ya, para no bloquear RPCs

		switch estado {
		case Seguidor:
			nr.comoSeguidor() // espera latidos (o pasa a candidato)
		case Candidato:
			nr.comoCandidato() // hace una elección
		case Lider:
			nr.comoLider() // manda una ronda de latidos
		}
		// al volver, el estado puede haber cambiado (lo cambian estas
		// funciones o las RPCs): la siguiente vuelta lo vuelve a mirar
	}
}

// Si veo un mandato más alto que el mío, me he quedado atrasado:
// me actualizo y paso a seguidor (tema 5, pág.48).
// Se llama al recibir cualquier RPC o respuesta.
// PARÁMETRO mandato: el mandato del OTRO nodo, que viene en el mensaje que acabo de recibir.
//
//	PedirVoto(peticion.Mandato)        → del candidato que pide voto
//	AppendEntries(args.Mandato)        → del líder que manda latido
//	pedirVotoA(reply.Mandato)          → de quien contesta a mi voto
//	procesarRespuesta(results.Mandato) → del seguidor que contesta
//
// Se ejecuta con el nr del nodo que RECIBE el mensaje
func (nr *NodoRaft) actualizarMandato(mandato int) {
	if mandato > nr.MandatoActual {
		nr.MandatoActual = mandato // me pongo al día
		nr.VotadoA = -1            // nuevo mandato: aún no he votado
		nr.Estado = Seguidor       // si era líder/candidato, dimito
	}
}

// Reinicia la cuenta atrás del seguidor (pulsa el "botón" que lee
// comoSeguidor) sin bloquearse nunca: si el canal está lleno, ya hay
// avisos pendientes y este sobra
func (nr *NodoRaft) reiniciarTimeout() {
	select {
	case nr.ReiniciarTimeout <- true: // hay hueco: dejo el aviso
	default: // canal lleno: no espero, sigo
	}
}

// Detiene el nodo cuando ya no se necesita (lo llama la RPC ParaNodo).
// Termina el proceso 5 ms después, en otra goroutine, para que antes
// le dé tiempo a la RPC a contestar al que la pidió
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve mi estado (lo llama la RPC ObtenerEstadoNodo; lo usa el
// test para saber quién es el líder):
//
//	yo:      mi posición en Nodos
//	mandato: mi mandato actual
//	esLider: true si creo ser el líder
//	idLider: quién creo que es el líder (-1 si no lo sé)
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock() // suelta el candado al salir
	yo := nr.Yo
	mandato := nr.MandatoActual
	// COMPARA: mi estado con Lider → true solo si soy el líder
	esLider := nr.Estado == Lider // true si mi estado es Lider
	idLider := nr.IdLider

	return yo, mandato, esLider, idLider
}

// Struct vacío: para RPCs que no necesitan argumentos o respuesta
type Vacio struct{}

// RPC: parar este nodo. "defer" hace que para() se llame justo al
// salir, después del return, así la RPC contesta antes de morir
func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

// Parte del estado que se devuelve en varias RPCs
type EstadoParcial struct {
	Mandato int  // mandato actual del nodo
	EsLider bool // ¿se cree líder?
	IdLider int  // quién cree que es el líder
}

// Respuesta de ObtenerEstadoNodo: el id del nodo + su estado.
type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

// RPC: consultar el estado de este nodo (la usa el test)
func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	// Relleno la respuesta con los 4 valores de obtenerEstado()
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider =
		nr.obtenerEstado()
	return nil
}
