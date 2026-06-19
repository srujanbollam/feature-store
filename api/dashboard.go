package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// handleDashboard serves a static HTML page that polls this node's
// own metrics and status endpoints, plus its peers, to render a
// live view of the cluster.
func (s *Server) handleDashboard(c *gin.Context) {
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, dashboardHTML)
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Feature Store - Cluster Dashboard</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #111318; color: #d6d6d6; padding: 32px; margin: 0; }
  h1 { font-size: 20px; font-weight: 600; margin-bottom: 4px; }
  .subtitle { color: #777; font-size: 13px; margin-bottom: 28px; }
  .section-title { font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin: 28px 0 12px; }

  .nodes { display: flex; gap: 14px; flex-wrap: wrap; }
  .node-card { background: #1a1d24; border: 1px solid #2a2d36; border-radius: 8px; padding: 16px; width: 200px; }
  .node-card.leader { border-color: #3b82f6; }
  .node-name { font-weight: 600; font-size: 14px; }
  .node-role { font-size: 11px; color: #888; margin: 4px 0 10px; }
  .node-status { font-size: 12px; }
  .up { color: #4ade80; }
  .down { color: #f87171; }

  table { border-collapse: collapse; width: 100%; max-width: 640px; }
  th, td { text-align: left; padding: 8px 12px; font-size: 13px; border-bottom: 1px solid #24262e; }
  th { color: #888; font-weight: 500; }
  td.value { font-family: ui-monospace, monospace; }

  .bar-track { background: #24262e; border-radius: 4px; height: 6px; width: 100%; margin-top: 4px; overflow: hidden; }
  .bar-fill { background: #3b82f6; height: 100%; }
</style>
</head>
<body>

<h1>Feature Store</h1>
<p class="subtitle">Cluster status and request latency</p>

<div class="section-title">Nodes</div>
<div class="nodes" id="nodes"></div>

<div class="section-title">Latency percentiles (ms) — leader</div>
<table>
  <tr><th>Percentile</th><th>Value</th><th style="width:200px"></th></tr>
  <tr><td>P50</td><td class="value" id="p50">-</td><td><div class="bar-track"><div class="bar-fill" id="bar50" style="width:0%"></div></div></td></tr>
  <tr><td>P95</td><td class="value" id="p95">-</td><td><div class="bar-track"><div class="bar-fill" id="bar95" style="width:0%"></div></div></td></tr>
  <tr><td>P99</td><td class="value" id="p99">-</td><td><div class="bar-track"><div class="bar-fill" id="bar99" style="width:0%"></div></div></td></tr>
  <tr><td>P100</td><td class="value" id="p100">-</td><td><div class="bar-track"><div class="bar-fill" id="bar100" style="width:0%"></div></div></td></tr>
</table>
<p class="subtitle" id="sample-count" style="margin-top:8px"></p>

<script>
  const NODES = [
    { id: 'node1', port: 8081 },
    { id: 'node2', port: 8082 },
    { id: 'node3', port: 8083 },
  ];

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error('request failed');
    return res.json();
  }

  async function refreshNodes() {
    const container = document.getElementById('nodes');
    container.innerHTML = '';

    for (const node of NODES) {
      const card = document.createElement('div');
      card.className = 'node-card';

      try {
        const status = await fetchJSON('http://localhost:' + node.port + '/status');
        if (status.role === 'leader') card.classList.add('leader');

        card.innerHTML =
          '<div class="node-name">' + status.node + '</div>' +
          '<div class="node-role">' + status.role + ' · :' + node.port + '</div>' +
          '<div class="node-status up">online</div>';

        if (status.role === 'leader') {
          updateLatencyPanel(node.port);
        }
      } catch (e) {
        card.innerHTML =
          '<div class="node-name">' + node.id + '</div>' +
          '<div class="node-role">:' + node.port + '</div>' +
          '<div class="node-status down">unreachable</div>';
      }

      container.appendChild(card);
    }
  }

  async function updateLatencyPanel(port) {
    try {
      const m = await fetchJSON('http://localhost:' + port + '/metrics/latency');
      setMetric('p50', 'bar50', m.p50_ms, m.p100_ms);
      setMetric('p95', 'bar95', m.p95_ms, m.p100_ms);
      setMetric('p99', 'bar99', m.p99_ms, m.p100_ms);
      setMetric('p100', 'bar100', m.p100_ms, m.p100_ms);
      document.getElementById('sample-count').textContent = m.count + ' samples recorded';
    } catch (e) {
      // leader metrics temporarily unavailable; leave previous values
    }
  }

  function setMetric(valueId, barId, value, max) {
    document.getElementById(valueId).textContent = value.toFixed(3);
    const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
    document.getElementById(barId).style.width = pct + '%';
  }

  refreshNodes();
  setInterval(refreshNodes, 3000);
</script>
</body>
</html>`