package db

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ConnectionStore struct {
	mu          sync.RWMutex
	connections map[string]*Connection
}

func NewConnectionStore() *ConnectionStore {
	return &ConnectionStore{
		connections: make(map[string]*Connection),
	}
}

type ConnectionManager struct {
	store              ConnectionStore
	idleExpiration     time.Duration
	absoluteExpiration time.Duration
}

func (c *ConnectionManager) AddConnection(dsn DSN) (string, error) {
	connector, err := InitPool(&dsn)
	s := uuid.New().String()
	if err != nil {
		log.Printf("Unable to create connection pool: %v\n", err)
	} else {
		err = connector.GetPool().Ping()
		log.Printf("Connection %s accepted", s)
	}
	if err != nil {
		log.Printf("Wrong connection pool: %v\n", err.Error())
	}
	if err != nil {
		panic("can't open connection")
	}

	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.connections[s] = &Connection{
		id:             s,
		status:         true,
		connector:      connector,
		createdAt:      time.Now(),
		lastActivityAt: time.Now(),
		version:        GetVersion(connector),
	}
	return s, err
}

func (c *ConnectionStore) getConnection(id string) (*Connection, bool) {
	connection, found := c.connections[id]
	if !found {
		log.Printf("Connection %s not found", id)
	}
	return connection, found
}
func (c *ConnectionStore) removeConnection(id string) {
	connection, found := c.getConnection(id)
	if found {
		connection.close()
		delete(c.connections, id)
	}
}

func (c *ConnectionStore) closeAllConnection() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, connection := range c.connections {
		log.Printf("Closing connection %s", connection.id)
		c.removeConnection(connection.id)
	}
}

func (c *ConnectionManager) GetConnection(id string) (*Connection, bool) {
	return c.store.getConnection(id)
}

func (c *ConnectionManager) RemoveConnection(id string) {
	c.store.mu.Lock()
	c.store.removeConnection(id)
	c.store.mu.Unlock()
}

func (c *ConnectionManager) CloseAllConnection() {
	c.store.closeAllConnection()
}
func NewConnectionManager(
	store ConnectionStore,
	gcInterval,
	idleExpiration,
	absoluteExpiration time.Duration) *ConnectionManager {

	m := &ConnectionManager{
		store:              store,
		idleExpiration:     idleExpiration,
		absoluteExpiration: absoluteExpiration,
	}

	go m.gc(gcInterval)

	return m
}

func (m *ConnectionManager) gc(d time.Duration) {
	ticker := time.NewTicker(d)

	for range ticker.C {
		m.store.gc(m.idleExpiration, m.absoluteExpiration)
	}
}

func (s *ConnectionStore) gc(idleExpiration, absoluteExpiration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Print("GC started")
	for _, connection := range s.connections {
		if time.Since(connection.lastActivityAt) > idleExpiration ||
			time.Since(connection.createdAt) > absoluteExpiration {
			log.Printf("Closing expired connection %s", connection.id)
			s.removeConnection(connection.id)
		}
	}
	log.Print("GC finished")
	return nil
}
