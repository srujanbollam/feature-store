package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"feature-store/metrics"
	"feature-store/store"
)

type Server struct {
	store   *store.FeatureStore
	router  *gin.Engine
	metrics *metrics.Recorder
}

func NewServer(s *store.FeatureStore) *Server {
	srv := &Server{
		store:   s,
		router:  gin.Default(),
		metrics: metrics.NewRecorder(1000),
	}
	srv.router.Use(srv.latencyMiddleware())
	srv.registerRoutes()
	return srv
}

// latencyMiddleware records how long each request took.
func (s *Server) latencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next() // run the actual handler
		s.metrics.Record(time.Since(start))
	}
}

func (s *Server) registerRoutes() {
	s.router.GET("/features/:key", s.getFeature)
	s.router.PUT("/features/:key", s.setFeature)
	s.router.DELETE("/features/:key", s.deleteFeature)
	s.router.GET("/metrics/latency", s.getLatencyMetrics)
}

func (s *Server) getFeature(c *gin.Context) {
	key := c.Param("key")

	value, err := s.store.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "feature not found", "key": key})
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key, "value": value})
}

type setFeatureRequest struct {
	Value string `json:"value" binding:"required"`
}

func (s *Server) setFeature(c *gin.Context) {
	key := c.Param("key")

	var req setFeatureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "value field is required"})
		return
	}

	if err := s.store.Set(key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not store feature"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key, "value": req.Value})
}

func (s *Server) deleteFeature(c *gin.Context) {
	key := c.Param("key")

	if err := s.store.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete feature"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key, "deleted": true})
}

// getLatencyMetrics exposes current P50/P95/P99/P100 latency stats.
func (s *Server) getLatencyMetrics(c *gin.Context) {
	c.JSON(http.StatusOK, s.metrics.Snapshot())
}

func (s *Server) Start(port string) error {
	return s.router.Run(":" + port)
}