package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"test.org/mdata/db"

	"github.com/alexedwards/scs/v2"
	"github.com/stretchr/testify/assert"
)

func setupTest() http.Handler {
	sessionManager = scs.New()
	connectionManager = db.NewConnectionManager(
		*db.NewConnectionStore(),
		time.Hour, time.Hour, 2*time.Hour,
	)
	return sessionManager.LoadAndSave(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
}

func TestHealthHandler_Get(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	healthHandler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestHealthHandler_MethodNotAllowed(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	w := httptest.NewRecorder()
	healthHandler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestConnectHandler_PostOpen_NoDbHost(t *testing.T) {
	setupTest()
	body := strings.NewReader("dbtype=postgres&username=u&password=p&dbhost=&dbport=5432&database=testdb")
	req := httptest.NewRequest(http.MethodPost, "/connection/open", body)
	req.SetPathValue("operation", "open")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	connectHandler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestConnectHandler_PostClose_NoConnection(t *testing.T) {
	sessionManager = scs.New()
	connectionManager = db.NewConnectionManager(
		*db.NewConnectionStore(),
		time.Hour, time.Hour, 2*time.Hour,
	)
	body := strings.NewReader("")
	req := httptest.NewRequest(http.MethodPost, "/connection/close", body)
	req.SetPathValue("operation", "close")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	sessionManager.LoadAndSave(connectHandler()).ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestConnectHandler_GetStatus_NoConnection(t *testing.T) {
	sessionManager = scs.New()
	connectionManager = db.NewConnectionManager(
		*db.NewConnectionStore(),
		time.Hour, time.Hour, 2*time.Hour,
	)
	req := httptest.NewRequest(http.MethodGet, "/connection/status", nil)
	req.SetPathValue("operation", "status")
	w := httptest.NewRecorder()
	sessionManager.LoadAndSave(connectHandler()).ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestConnectHandler_MethodNotAllowed(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodPut, "/connection/open", nil)
	req.SetPathValue("operation", "open")
	w := httptest.NewRecorder()
	connectHandler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestUniversalHandler_GetTable_MethodNotAllowed(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodPost, "/metadata/public/users", nil)
	w := httptest.NewRecorder()
	universalHandler(metaTable).ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestUniversalHandler_UnknownGoal(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	w := httptest.NewRecorder()
	universalHandler("unknown_goal").ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestUniversalHandler_PostSQL_GetNotAllowed(t *testing.T) {
	setupTest()
	req := httptest.NewRequest(http.MethodGet, "/sql", nil)
	w := httptest.NewRecorder()
	universalHandler(wideSQL).ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestRoutes_Healthz(t *testing.T) {
	setupTest()
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{"healthz GET", http.MethodGet, "/healthz", http.StatusAccepted},
		{"healthz POST", http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			mux := http.NewServeMux()
			mux.Handle("/healthz", healthHandler())
			mux.ServeHTTP(w, req)
			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}
