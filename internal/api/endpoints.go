package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sean-kim05/hookline/internal/endpoint"
)

// registerEndpointRequest is the JSON body of POST /v1/endpoints.
type registerEndpointRequest struct {
	URL      string `json:"url"`
	Disabled bool   `json:"disabled,omitempty"`
}

// endpointResponse is the public view of an endpoint. The signing secret is
// omitted here; it is returned exactly once, in registerEndpointResponse.
type endpointResponse struct {
	ID        string `json:"id"`
	URL       string `json:"url"`
	Producer  string `json:"producer,omitempty"`
	Disabled  bool   `json:"disabled"`
	CreatedAt string `json:"created_at"`
}

// registerEndpointResponse additionally carries the freshly minted secret. This
// is the only response that ever includes it.
type registerEndpointResponse struct {
	endpointResponse
	Secret string `json:"secret"`
}

func toEndpointResponse(e endpoint.Endpoint) endpointResponse {
	return endpointResponse{
		ID:        e.ID,
		URL:       e.URL,
		Producer:  e.Producer,
		Disabled:  e.Disabled,
		CreatedAt: e.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

func (s *Server) handleRegisterEndpoint(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	var req registerEndpointRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := validateEndpointURL(req.URL); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	producer := producerFromContext(r.Context())
	ep, err := s.registry.Register(r.Context(), endpoint.Endpoint{
		URL:      req.URL,
		Producer: producer,
		Disabled: req.Disabled,
	})
	if err != nil {
		s.log.Error("hookline: register endpoint", "err", err, "producer", producer)
		writeError(w, http.StatusInternalServerError, "could not register endpoint")
		return
	}

	s.log.Info("hookline: endpoint registered", "endpoint", ep.ID, "url", ep.URL, "producer", producer)
	writeJSON(w, http.StatusCreated, registerEndpointResponse{
		endpointResponse: toEndpointResponse(ep),
		Secret:           ep.Secret,
	})
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	producer := producerFromContext(r.Context())
	eps, err := s.registry.List(r.Context(), producer)
	if err != nil {
		s.log.Error("hookline: list endpoints", "err", err, "producer", producer)
		writeError(w, http.StatusInternalServerError, "could not list endpoints")
		return
	}
	out := make([]endpointResponse, 0, len(eps))
	for _, ep := range eps {
		out = append(out, toEndpointResponse(ep))
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": out})
}

func validateEndpointURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errURL("url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errURL("url is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errURL("url must be an http or https URL")
	}
	if u.Host == "" {
		return errURL("url must include a host")
	}
	return nil
}

type validationError struct{ msg string }

func (e validationError) Error() string { return e.msg }

func errURL(msg string) error { return validationError{msg: msg} }
