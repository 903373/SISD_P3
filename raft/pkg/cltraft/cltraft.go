// Cliente sencillo para someter operaciones a un grupo de nodos Raft.
// Busca al líder (siguiendo las pistas de IdLider) y espera a que la
// operación se comprometa (tema 5, pág.49: protocolo de cliente)
package cltraft

import (
	"errors"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"time"
)

// Cliente: conoce todos los nodos y recuerda quién cree que es el líder
type Cliente struct {
	Nodos []rpctimeout.HostPort // "ip:puerto" de todos los nodos
	Lider int                   // a quién preguntar primero
}

// Crea un cliente; al principio no sabe quién es el líder: prueba el 0
func NuevoCliente(nodos []rpctimeout.HostPort) *Cliente {
	return &Cliente{Nodos: nodos, Lider: 0}
}

// Somete una operación y espera a que se comprometa. Si el nodo al que
// pregunta no es el líder, prueba con el que le indiquen (o el
// siguiente). Devuelve la respuesta del líder o error tras 10 intentos
func (c *Cliente) Someter(op raft.TipoOperacion) (raft.ResultadoRemoto,
	error) {
	for intento := 0; intento < 10; intento++ {
		reply, err := c.enviar(op)
		if err == nil && reply.EsLider { // comprometida
			return reply, nil
		}
		c.buscarLider(reply, err) // no era el líder (o no contestó)
	}
	return raft.ResultadoRemoto{}, errors.New("operación no comprometida")
}

// Envía la operación al nodo que creo líder (máx. 3 s de espera,
// más que los 2 s que el líder espera al compromiso)
func (c *Cliente) enviar(op raft.TipoOperacion) (raft.ResultadoRemoto,
	error) {
	var reply raft.ResultadoRemoto
	err := c.Nodos[c.Lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		op, &reply, 3*time.Second)
	return reply, err
}

// Elige a quién preguntar la próxima vez: al líder que me han indicado
// o, si no lo saben (o no contestó), al siguiente nodo de la lista
func (c *Cliente) buscarLider(reply raft.ResultadoRemoto, err error) {
	if err == nil && reply.IdLider >= 0 && reply.IdLider != c.Lider {
		c.Lider = reply.IdLider // me redirigen al líder
		return
	}
	c.Lider = (c.Lider + 1) % len(c.Nodos) // pruebo con el siguiente
	time.Sleep(100 * time.Millisecond)     // doy tiempo a una elección
}
