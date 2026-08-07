package db

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func newTestConnection(id string) *Connection {
	pool, _ := sql.Open("postgres", "")
	return &Connection{
		id:        id,
		status:    true,
		connector: &PGConnector{Pool: pool},
		createdAt: time.Now(),
	}
}

func newTestConnectionWithTimes(id string, createdAt, lastActivityAt time.Time) *Connection {
	pool, _ := sql.Open("postgres", "")
	return &Connection{
		id:             id,
		status:         true,
		connector:      &PGConnector{Pool: pool},
		createdAt:      createdAt,
		lastActivityAt: lastActivityAt,
	}
}

func TestConnectionStore_NewStore(t *testing.T) {
	store := NewConnectionStore()
	assert.NotNil(t, store)
	assert.Empty(t, store.connections)
}

func TestConnectionStore_GetConnection_NotFound(t *testing.T) {
	store := NewConnectionStore()
	conn, found := store.getConnection("nonexistent")
	assert.False(t, found)
	assert.Nil(t, conn)
}

func TestConnectionStore_GetConnection_Found(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["test-id"] = newTestConnection("test-id")
	store.mu.Unlock()

	conn, found := store.getConnection("test-id")
	assert.True(t, found)
	assert.NotNil(t, conn)
	assert.Equal(t, "test-id", conn.id)
}

func TestConnectionStore_RemoveConnection(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["test-id"] = newTestConnection("test-id")
	store.mu.Unlock()

	store.removeConnection("test-id")
	_, found := store.getConnection("test-id")
	assert.False(t, found)
}

func TestConnectionStore_RemoveConnection_NotFound(t *testing.T) {
	store := NewConnectionStore()
	store.removeConnection("nonexistent")
}

func TestConnectionStore_CloseAllConnections(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["id1"] = newTestConnection("id1")
	store.connections["id2"] = newTestConnection("id2")
	store.connections["id3"] = newTestConnection("id3")
	store.mu.Unlock()

	store.closeAllConnection()
	assert.Empty(t, store.connections)
}

func TestConnectionStore_GC_NoExpired(t *testing.T) {
	store := NewConnectionStore()
	conn := newTestConnection("fresh")
	conn.lastActivityAt = time.Now()
	store.mu.Lock()
	store.connections["fresh"] = conn
	store.mu.Unlock()

	err := store.gc(10*time.Minute, 30*time.Minute)
	assert.NoError(t, err)
	_, found := store.getConnection("fresh")
	assert.True(t, found)
}

func TestConnectionStore_GC_IdleExpired(t *testing.T) {
	store := NewConnectionStore()
	conn := newTestConnection("stale")
	conn.lastActivityAt = time.Now().Add(-20 * time.Minute)
	store.mu.Lock()
	store.connections["stale"] = conn
	store.mu.Unlock()

	err := store.gc(5*time.Minute, 30*time.Minute)
	assert.NoError(t, err)
	_, found := store.getConnection("stale")
	assert.False(t, found)
}

func TestConnectionStore_GC_AbsoluteExpired(t *testing.T) {
	store := NewConnectionStore()
	conn := newTestConnection("old")
	conn.createdAt = time.Now().Add(-60 * time.Minute)
	conn.lastActivityAt = time.Now()
	store.mu.Lock()
	store.connections["old"] = conn
	store.mu.Unlock()

	err := store.gc(30*time.Minute, 45*time.Minute)
	assert.NoError(t, err)
	_, found := store.getConnection("old")
	assert.False(t, found)
}

func TestConnectionStore_GC_Mixed(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["fresh"] = newTestConnectionWithTimes("fresh", time.Now(), time.Now())
	store.connections["idle"] = newTestConnectionWithTimes("idle", time.Now(), time.Now().Add(-20*time.Minute))
	store.connections["old"] = newTestConnectionWithTimes("old", time.Now().Add(-60*time.Minute), time.Now())
	store.mu.Unlock()

	err := store.gc(10*time.Minute, 30*time.Minute)
	assert.NoError(t, err)

	_, freshFound := store.getConnection("fresh")
	_, idleFound := store.getConnection("idle")
	_, oldFound := store.getConnection("old")

	assert.True(t, freshFound)
	assert.False(t, idleFound)
	assert.False(t, oldFound)
}

func TestConnectionManager_GetConnection(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["test-id"] = newTestConnection("test-id")
	store.mu.Unlock()

	mgr := NewConnectionManager(*store, time.Hour, time.Hour, 2*time.Hour)

	conn, found := mgr.GetConnection("test-id")
	assert.True(t, found)
	assert.Equal(t, "test-id", conn.id)

	_, found = mgr.GetConnection("nonexistent")
	assert.False(t, found)
}

func TestConnectionManager_RemoveConnection(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["test-id"] = newTestConnection("test-id")
	store.mu.Unlock()

	mgr := NewConnectionManager(*store, time.Hour, time.Hour, 2*time.Hour)
	mgr.RemoveConnection("test-id")

	_, found := mgr.GetConnection("test-id")
	assert.False(t, found)
}

func TestConnectionManager_CloseAllConnections(t *testing.T) {
	store := NewConnectionStore()
	store.mu.Lock()
	store.connections["id1"] = newTestConnection("id1")
	store.connections["id2"] = newTestConnection("id2")
	store.mu.Unlock()

	mgr := NewConnectionManager(*store, time.Hour, time.Hour, 2*time.Hour)
	mgr.CloseAllConnection()

	assert.Empty(t, store.connections)
}

func TestConnectionManager_GcRemovesExpiredConnections(t *testing.T) {
	store := NewConnectionStore()
	conn := newTestConnectionWithTimes("stale", time.Now(), time.Now().Add(-20*time.Minute))
	store.mu.Lock()
	store.connections["stale"] = conn
	store.mu.Unlock()

	mgr := NewConnectionManager(*store, 10*time.Millisecond, 5*time.Minute, 30*time.Minute)
	defer mgr.CloseAllConnection()

	time.Sleep(100 * time.Millisecond)

	_, found := mgr.GetConnection("stale")
	assert.False(t, found)
}

func TestConnectionManager_AddConnection_FailsOnPing(t *testing.T) {
	store := NewConnectionStore()
	mgr := NewConnectionManager(*store, time.Hour, time.Hour, 2*time.Hour)
	defer mgr.CloseAllConnection()

	id, err := mgr.AddConnection(DSN{
		DbType:   "postgres",
		Username: "u",
		Password: "p",
		DbHost:   "nonexistent.example.com",
		DbPort:   "5432",
		Database: "testdb",
	})
	assert.Error(t, err)
	assert.Empty(t, id)
}

func TestConnectionManager_AddConnection_FailsUnknownType(t *testing.T) {
	store := NewConnectionStore()
	mgr := NewConnectionManager(*store, time.Hour, time.Hour, 2*time.Hour)
	defer mgr.CloseAllConnection()

	id, err := mgr.AddConnection(DSN{
		DbType: "unknown_type",
	})
	assert.Error(t, err)
	assert.Empty(t, id)
}
