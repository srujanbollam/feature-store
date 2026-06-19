package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"feature-store/cluster"
	"feature-store/metrics"
	"feature-store/ml"
)

type Server struct {
	node     *cluster.Node
	router   *gin.Engine
	metrics  *metrics.Recorder
	pipeline *ml.Pipeline
}

func NewServer(node *cluster.Node, pipeline *ml.Pipeline) *Server {
	srv := &Server{
		node:     node,
		router:   gin.Default(),
		metrics:  metrics.NewRecorder(1000),
		pipeline: pipeline,
	}
	srv.router.Use(srv.latencyMiddleware())
	srv.registerRoutes()
	return srv
}

func (s *Server) latencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		s.metrics.Record(time.Since(start))
	}
}

func (s *Server) registerRoutes() {
	s.router.GET("/features/:key", s.getFeature)
	s.router.PUT("/features/:key", s.setFeature)
	s.router.DELETE("/features/:key", s.deleteFeature)
	s.router.GET("/metrics/latency", s.getLatencyMetrics)
	s.router.POST("/ingest", s.ingestFeatures)

	s.router.POST("/internal/replicate", s.handleReplicate)
	s.router.GET("/health", s.handleHealth)
	s.router.GET("/status", s.handleStatus)
}

func (s *Server) getFeature(c *gin.Context) {
	key := c.Param("key")

	value, err := s.node.Get(key)
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
	if !s.node.IsLeader {
		c.JSON(http.StatusForbidden, gin.H{"error": "writes must go to the leader node"})
		return
	}

	key := c.Param("key")

	var req setFeatureRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "value field is required"})
		return
	}

	if err := s.node.Set(key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not store feature"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key, "value": req.Value})
}

func (s *Server) deleteFeature(c *gin.Context) {
	if !s.node.IsLeader {
		c.JSON(http.StatusForbidden, gin.H{"error": "deletes must go to the leader node"})
		return
	}

	key := c.Param("key")

	if err := s.node.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete feature"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"key": key, "deleted": true})
}

func (s *Server) getLatencyMetrics(c *gin.Context) {
	c.JSON(http.StatusOK, s.metrics.Snapshot())
}

type ingestRequest struct {
	UserID string  `json:"user_id" binding:"required"`
	Age    float64 `json:"age"`
	Income float64 `json:"income"`
	Clicks float64 `json:"clicks"`
}

func (s *Server) ingestFeatures(c *gin.Context) {
	if !s.node.IsLeader {
		c.JSON(http.StatusForbidden, gin.H{"error": "ingestion must go to the leader node"})
		return
	}

	var req ingestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id is required"})
		return
	}

	raw := ml.RawFeatures{
		UserID: req.UserID,
		Age:    req.Age,
		Income: req.Income,
		Clicks: req.Clicks,
	}

	if err := s.pipeline.Ingest(raw); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ingestion failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user_id": req.UserID, "ingested": true})
}

// handleReplicate is called by the leader on follower nodes to apply
// a write that already happened on the leader.
func (s *Server) handleReplicate(c *gin.Context) {
	var cmd cluster.ReplicateCommand
	if err := c.ShouldBindJSON(&cmd); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid replication command"})
		return
	}

	if err := s.node.ApplyReplicated(cmd); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"applied": true})
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "node": s.node.ID})
}

func (s *Server) handleStatus(c *gin.Context) {
	role := "follower"
	if s.node.IsLeader {
		role = "leader"
	}

	c.JSON(http.StatusOK, gin.H{
		"node":    s.node.ID,
		"role":    role,
		"address": s.node.Address,
		"peers":   s.node.Peers,
	})
}

func (s *Server) Start(port string) error {
	return s.router.Run(":" + port)
}