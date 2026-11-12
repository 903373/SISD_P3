// Elección de líder (tema 5, págs. 30-34): el seguidor que no recibe
// latidos se hace candidato, pide votos con PedirVoto y, si consigue
// mayoría, pasa a líder. Aquí están los dos lados: el candidato que
// PIDE el voto y el nodo que lo RECIBE y decide si lo da.
package raft

import (
	"math/rand"
	"time"
)

// Seguidor: espera latidos. Si no llega ninguno a tiempo,
// sospecha que el líder ha caído y pasa a candidato (pág.32)
func (nr *NodoRaft) comoSeguidor() {
	select { // espero a lo PRIMERO que ocurra de estas dos cosas:
	case <-nr.ReiniciarTimeout:
		// llegó un aviso (latido del líder o he votado a alguien):
		// no hago nada; gestionarNodo me vuelve a llamar y la cuenta
		// atrás empieza de nuevo
	case <-time.After(timeoutEleccion()):
		// pasó el tiempo sin avisos: el líder ha caído (o no hay)
		nr.Mux.Lock()
		nr.Estado = Candidato // en la próxima vuelta haré una elección
		nr.Mux.Unlock()
	}
}

// Timeout de elección aleatorio entre 150 y 299 ms.
// Aleatorio para que los nodos no expiren a la vez y se repartan
// los votos sin que nadie gane (vivacidad, pág.34; lo pide el guion)
func timeoutEleccion() time.Duration {
	// rand.Intn(150) da un número entre 0 y 149
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// Candidato: hace UNA elección (tema 5, pág.33).
// Si no gana a tiempo, gestionarNodo la vuelve a llamar (nueva elección)
func (nr *NodoRaft) comoCandidato() {
	nr.Mux.Lock()
	nr.MandatoActual++ // 1. nuevo mandato (el anterior no tuvo líder)
	nr.VotadoA = nr.Yo // 2. me voto a mí mismo
	nr.IdLider = -1    //    ya no sé quién es el líder
	// Petición de voto con mi mandato y los datos de mi última entrada
	// (UltimoIndice/UltimoMandato se comprobarán en la P4, pág.41)
	args := &ArgsPeticionVoto{
		Mandato:       nr.MandatoActual,
		IdCandidato:   nr.Yo,
		UltimoIndice:  len(nr.Registro) - 1,
		UltimoMandato: nr.Registro[len(nr.Registro)-1].Mandato,
	}
	nr.Mux.Unlock() // suelto antes de enviar: enviar tarda

	// Canal donde cada goroutine dejará su voto
	votos := make(chan bool, len(nr.Nodos))
	for i := range nr.Nodos {
		if i != nr.Yo { // 3. pido el voto a todos menos a mí,
			go nr.pedirVotoA(i, args, votos) // a la vez (en paralelo)
		}
	}

	nr.esperarVotos(votos, args.Mandato) // 4. cuento los votos
}

// Cuenta votos hasta que pase una de las 3 cosas de la pág.33:
// gano por mayoría, aparece otro líder, o se acaba el tiempo
func (nr *NodoRaft) esperarVotos(votos chan bool, mandato int) {
	conseguidos := 1                         // empiezo con mi voto
	timeout := time.After(timeoutEleccion()) // tiempo máximo de elección
	for {
		select { // espero a lo primero que llegue:
		// v: el true/false que dejó una goroutine pedirVotoA
		case v := <-votos: // llegó la respuesta de un nodo
			if v {
				conseguidos++ // me ha votado
			}
			if conseguidos > len(nr.Nodos)/2 { // mayoría (2 de 3)
				nr.convertirseEnLider(mandato) // a) he ganado
				return
			}
		case <-nr.ReiniciarTimeout:
			// b) llegó un latido de otro líder (o he votado a otro):
			// AppendEntries ya me ha puesto como seguidor
			return
		case <-timeout:
			// c) nadie ganó a tiempo: sigo candidato y gestionarNodo
			// lanzará otra elección con mandato+1
			return
		}
	}
}

// Paso a líder solo si sigo siendo candidato del MISMO mandato
// (mientras contaba votos pudo llegar un mandato mayor y bajarme)
func (nr *NodoRaft) convertirseEnLider(mandato int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock() // se suelta solo al salir de la función
	if nr.Estado != Candidato || nr.MandatoActual != mandato {
		return // esos votos eran de una elección vieja: no hago nada
	}
	nr.Estado = Lider  // he ganado
	nr.IdLider = nr.Yo // el líder soy yo
	// Preparo las "libretas" del líder, una casilla por nodo (pág.57)
	nr.SiguienteIndice = make([]int, len(nr.Nodos))
	nr.IndiceCoincide = make([]int, len(nr.Nodos)) // todo a 0
	for i := range nr.Nodos {
		// Supongo que todos están al día: les tocaría la siguiente a
		// mi última entrada (= len, gracias a la entrada ficticia)
		nr.SiguienteIndice[i] = len(nr.Registro)
	}
}

// Llama a la RPC PedirVoto en el nodo "nodo" (máx. 50 ms).
// Devuelve true si contestó a tiempo (la respuesta queda en reply);
// false si está caído, no se le alcanza por red o tardó demasiado.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply,
		50*time.Millisecond) // 50 ms << duración de una elección

	return err == nil // nil = sin error = contestó
}

// Goroutine (una por nodo, la lanza comoCandidato): pide el voto a un
// nodo y deja el resultado en el canal para que esperarVotos lo cuente
//
// PARÁMETROS (se los pasa comoCandidato):
//
//	nodo: a quién; args: mi petición; voto: canal de votos (el mismo
//	"votos" que lee esperarVotos)
func (nr *NodoRaft) pedirVotoA(nodo int, args *ArgsPeticionVoto,
	voto chan bool) {
	var reply RespuestaPeticionVoto // aquí llega su respuesta
	ok := nr.enviarPeticionVoto(nodo, args, &reply)
	if ok { // contestó
		nr.Mux.Lock()
		// Si va por un mandato mayor que el mío, me he quedado
		// atrasado: me actualizo y dejo de ser candidato (pág.48)
		// reply.Mandato = el mandato del OTRO nodo (lo puso él en
		// PedirVoto con "reply.Mandato = nr.MandatoActual")
		nr.actualizarMandato(reply.Mandato)
		nr.Mux.Unlock()
	}
	// true solo si contestó Y me dio el voto
	voto <- ok && reply.VotoConcedido
}

// Argumentos de PedirVoto (figura 2, RequestVote)
type ArgsPeticionVoto struct {
	Mandato       int // mandato del candidato
	IdCandidato   int // quién pide el voto
	UltimoIndice  int // índice de su última entrada (se usará en P4)
	UltimoMandato int // mandato de su última entrada (se usará en P4)
}

// Respuesta de PedirVoto
type RespuestaPeticionVoto struct {
	Mandato       int  // mi mandato, para que el candidato se actualice
	VotoConcedido bool // ¿le doy el voto?
}

// RPC: un candidato me pide el voto (pág.33-34; figura 2,
// "RequestVote, Receiver implementation")
//
// PARÁMETROS:
//
//	peticion: el "args" que preparó el candidato en comoCandidato
//	          (peticion.Mandato = SU mandato, IdCandidato = quién es)
//	reply:    mi respuesta; viaja de vuelta y le llega al candidato
//	          en el "reply" de enviarPeticionVoto/pedirVotoA
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock() // suelta el candado al salir

	// Si el candidato va por un mandato mayor, me pongo al día
	// (y si era líder o candidato, bajo a seguidor)
	nr.actualizarMandato(peticion.Mandato)
	reply.Mandato = nr.MandatoActual // le digo mi mandato
	reply.VotoConcedido = false      // por defecto, no le voto

	if peticion.Mandato < nr.MandatoActual {
		return nil // 1. candidato atrasado: voto denegado
	}

	// 2. Un solo voto por mandato: solo voto si aún no he votado a
	// nadie, o si ya le voté a él (me lo pide otra vez) (pág.34)
	if nr.VotadoA == -1 || nr.VotadoA == peticion.IdCandidato {
		nr.VotadoA = peticion.IdCandidato // apunto a quién voto
		reply.VotoConcedido = true
		nr.reiniciarTimeout() // he votado: doy tiempo al candidato
	}

	return nil
}
