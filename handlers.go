package eg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi"
)

const (
	S2SEnvKey    = "EG_S2S_AUTH_KEY"
	S2SHeaderKey = "X-EG-S2S-Authorization"
)

type StandardHandler func(ctx context.Context, body io.Reader, method string) (any, error)
type RawHandler func(w http.ResponseWriter, r *http.Request)

type HandleMap map[string]StandardHandler
type RawHandleMap map[string]RawHandler

type Server struct {
	r *chi.Mux
}

func NewServer() *Server {
	return &Server{
		r: chi.NewRouter(),
	}
}

func (s *Server) Handle(apiEoute string, handler StandardHandler, useS2S bool) *Server {
	s.r.HandleFunc(apiEoute, corsMiddleware(http.HandlerFunc(Handler(handler, useS2S))).ServeHTTP)
	return s
}

func (s *Server) HandleAll(handleMap HandleMap) *Server {
	for apiEoute, handler := range handleMap {
		s.Handle(apiEoute, handler, false)
	}
	return s
}

func (s *Server) HandleAllWithS2S(handleMap HandleMap) *Server {
	for apiEoute, handler := range handleMap {
		s.Handle(apiEoute, handler, true)
	}
	return s
}

func (s *Server) HandleRaw(apiEoute string, handler RawHandler) *Server {
	s.r.HandleFunc(apiEoute, corsMiddleware(http.HandlerFunc(handler)).ServeHTTP)
	return s
}

func (s *Server) HandleRawAll(handleMap RawHandleMap) *Server {
	for apiEoute, handler := range handleMap {
		s.HandleRaw(apiEoute, handler)
	}
	return s
}

func (s *Server) Start() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	} else {
		port = "443"
	}

	log.Fatal(http.ListenAndServe("0.0.0.0:"+port, s.r))
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)

			return
		}

		next.ServeHTTP(w, r)
	})
}

func Handler(f StandardHandler, useS2S bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			_ = r.Body.Close()
		}()

		w.Header().Set("Content-Type", "application/json")

		if useS2S {
			s2sKey := os.Getenv(S2SEnvKey)
			s2sHeader := r.Header.Get(S2SHeaderKey)

			if s2sKey != s2sHeader {
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{"error": "s2s auth failed"})

				return
			}
		}

		resp, err := f(r.Context(), r.Body, r.Method)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("%v", err)})

			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}
