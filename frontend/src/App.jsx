import React, { useState, useEffect } from 'react';

const API_BASE = "http://localhost:8000/api/v1";

export default function App() {
  const [metrics, setMetrics] = useState({
    total_events: 0,
    total_delivered: 0,
    total_retrying: 0,
    total_dlq: 0,
    success_rate: 100.0,
    avg_latency_ms: 0.0,
    total_subscribers: 0
  });

  const [attempts, setAttempts] = useState([]);
  const [circuitBreakers, setCircuitBreakers] = useState([]);
  const [subscribers, setSubscribers] = useState([]);
  
  // Event Trigger Form
  const [eventType, setEventType] = useState("payment.succeeded");
  const [payloadText, setPayloadText] = useState('{\n  "amount": 9900,\n  "currency": "USD",\n  "customer": "cust_123"\n}');
  const [useIdemKey, setUseIdemKey] = useState(true);
  const [customIdemKey, setCustomIdemKey] = useState("");
  const [triggerStatus, setTriggerStatus] = useState("");

  // Connect SSE for Real-time Updates
  useEffect(() => {
    fetchSubscribers();

    const eventSource = new EventSource(`${API_BASE}/events/stream`);

    eventSource.addEventListener("update", (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.metrics) setMetrics(data.metrics);
        if (data.recent_attempts) setAttempts(data.recent_attempts);
        if (data.circuit_breakers) setCircuitBreakers(data.circuit_breakers);
      } catch (err) {
        console.error("Error parsing SSE update:", err);
      }
    });

    eventSource.onerror = (err) => {
      console.warn("SSE connection error:", err);
    };

    return () => {
      eventSource.close();
    };
  }, []);

  const fetchSubscribers = async () => {
    try {
      const res = await fetch(`${API_BASE}/subscribers`);
      const data = await res.json();
      setSubscribers(data);
    } catch (err) {
      console.error("Failed to fetch subscribers:", err);
    }
  };

  const handleIngestEvent = async (e) => {
    e.preventDefault();
    setTriggerStatus("Ingesting event...");
    try {
      const parsedPayload = JSON.parse(payloadText);
      const idempotencyKey = useIdemKey 
        ? (customIdemKey || `idem-${Date.now()}-${Math.floor(Math.random()*1000)}`)
        : null;

      const res = await fetch(`${API_BASE}/events`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          event_type: eventType,
          payload: parsedPayload,
          idempotency_key: idempotencyKey
        })
      });

      const result = await res.json();
      if (result.is_duplicate) {
        setTriggerStatus(`⚠️ Duplicate Idempotency Key detected! Returned existing event ID: ${result.event_id}`);
      } else {
        setTriggerStatus(`✅ Event accepted! Event ID: ${result.event_id}`);
      }
    } catch (err) {
      setTriggerStatus(`❌ Error: ${err.message}`);
    }
  };

  const toggleSimulateFailure = async (subscriberId, currentFailState) => {
    try {
      await fetch(`${API_BASE}/subscribers/${subscriberId}/simulate-behavior?simulate_failure=${!currentFailState}&simulate_status_code=500`, {
        method: "POST"
      });
      fetchSubscribers();
    } catch (err) {
      console.error("Failed to update simulation behavior:", err);
    }
  };

  const handleReplayDlq = async (eventId) => {
    try {
      const res = await fetch(`${API_BASE}/events/dlq/replay/${eventId}`, { method: "POST" });
      const data = await res.json();
      alert(`Event ${eventId} requeued for redelivery!`);
    } catch (err) {
      alert("Failed to replay DLQ event");
    }
  };

  return (
    <div className="dashboard-container">
      {/* Header */}
      <header className="dashboard-header">
        <div className="header-title">
          <h1>Webhook Delivery System</h1>
          <p>At-Least-Once Delivery Engine • HMAC Signatures • Exponential Backoff • Circuit Breaking</p>
        </div>
        <div className="live-badge">
          <span className="pulse-dot"></span>
          <span>SSE LIVE CONNECTED</span>
        </div>
      </header>

      {/* Metric Cards Grid */}
      <div className="metrics-grid">
        <div className="metric-card">
          <span className="metric-label">Ingested Events</span>
          <span className="metric-value">{metrics.total_events}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">Delivered</span>
          <span className="metric-value" style={{ color: "var(--accent-green)" }}>{metrics.total_delivered}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">Retrying</span>
          <span className="metric-value" style={{ color: "var(--accent-amber)" }}>{metrics.total_retrying}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">Dead-Lettered (DLQ)</span>
          <span className="metric-value" style={{ color: "var(--accent-red)" }}>{metrics.total_dlq}</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">Success Rate</span>
          <span className="metric-value">{metrics.success_rate}%</span>
        </div>
        <div className="metric-card">
          <span className="metric-label">Avg Latency</span>
          <span className="metric-value">{metrics.avg_latency_ms} ms</span>
        </div>
      </div>

      {/* Form & Subscriber Controls Grid */}
      <div className="controls-grid">
        {/* Trigger Event Form */}
        <div className="card">
          <div className="card-title">
            <span>⚡ Fire Webhook Event</span>
          </div>
          <form onSubmit={handleIngestEvent}>
            <div className="form-group">
              <label>Event Type</label>
              <select className="form-control" value={eventType} onChange={(e) => setEventType(e.target.value)}>
                <option value="payment.succeeded">payment.succeeded</option>
                <option value="order.created">order.created</option>
                <option value="user.signup">user.signup</option>
                <option value="invoice.failed">invoice.failed</option>
              </select>
            </div>

            <div className="form-group">
              <label>Payload (JSON)</label>
              <textarea 
                className="form-control" 
                rows="4" 
                value={payloadText} 
                onChange={(e) => setPayloadText(e.target.value)}
              />
            </div>

            <div className="form-group" style={{ flexDirection: "row", alignItems: "center", gap: "10px" }}>
              <input 
                type="checkbox" 
                id="idem-check" 
                checked={useIdemKey} 
                onChange={(e) => setUseIdemKey(e.target.checked)} 
              />
              <label htmlFor="idem-check" style={{ cursor: "pointer" }}>Attach Idempotency Key</label>
            </div>

            {useIdemKey && (
              <div className="form-group">
                <input 
                  type="text" 
                  className="form-control" 
                  placeholder="Optional custom Idempotency Key..." 
                  value={customIdemKey} 
                  onChange={(e) => setCustomIdemKey(e.target.value)} 
                />
              </div>
            )}

            <button type="submit" className="btn" style={{ width: "100%" }}>Dispatch Webhook Event</button>
          </form>

          {triggerStatus && (
            <p style={{ marginTop: "1rem", fontSize: "0.85rem", color: "var(--text-muted)" }}>
              {triggerStatus}
            </p>
          )}
        </div>

        {/* Subscriber & Circuit Breaker Health */}
        <div className="card">
          <div className="card-title">
            <span>🛡️ Active Subscribers & Circuit Breakers</span>
          </div>
          
          {subscribers.length === 0 ? (
            <p style={{ color: "var(--text-muted)", fontSize: "0.9rem" }}>No subscribers registered yet.</p>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
              {subscribers.map((sub) => {
                const cb = circuitBreakers.find(c => c.subscriber_id === sub.id);
                const state = cb ? cb.state : "CLOSED";
                const badgeClass = state === "CLOSED" ? "badge-closed" : (state === "OPEN" ? "badge-open" : "badge-halfopen");

                return (
                  <div key={sub.id} style={{ background: "rgba(0,0,0,0.25)", padding: "1rem", borderRadius: "10px", border: "1px solid var(--border-color)" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <div>
                        <strong style={{ fontSize: "0.95rem" }}>{sub.name}</strong>
                        <br />
                        <span className="code-snippet" style={{ fontSize: "0.75rem" }}>{sub.target_url}</span>
                      </div>
                      <span className={`badge ${badgeClass}`}>CB: {state}</span>
                    </div>

                    <div style={{ marginTop: "0.8rem", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
                        HMAC Secret: <span className="code-snippet">{sub.secret.slice(0, 14)}...</span>
                      </span>

                      <button 
                        className={`btn ${sub.simulate_failure ? 'btn-secondary' : 'btn-danger'}`}
                        style={{ fontSize: "0.75rem", padding: "4px 10px" }}
                        onClick={() => toggleSimulateFailure(sub.id, sub.simulate_failure)}
                      >
                        {sub.simulate_failure ? "Restore Healthy Endpoint" : "Simulate 500 Endpoint Failure"}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Real-time Delivery Audit Feed */}
      <div className="card">
        <div className="card-title">
          <span>📡 Live Webhook Delivery Audit Log</span>
        </div>

        <table className="data-table">
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Event ID</th>
              <th>Subscriber ID</th>
              <th>Attempt #</th>
              <th>Status</th>
              <th>HTTP Code</th>
              <th>Latency</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {attempts.length === 0 ? (
              <tr>
                <td colSpan="8" style={{ textAlign: "center", color: "var(--text-muted)", padding: "2rem" }}>
                  No delivery attempts recorded yet. Dispatch an event above!
                </td>
              </tr>
            ) : (
              attempts.map((att) => {
                const statusBadge = att.status === "SUCCESS" 
                  ? "badge-delivered" 
                  : (att.status === "CIRCUIT_OPEN" ? "badge-open" : "badge-retrying");

                return (
                  <tr key={att.id}>
                    <td>{new Date(att.attempted_at).toLocaleTimeString()}</td>
                    <td><span className="code-snippet">{att.event_id.slice(0, 12)}...</span></td>
                    <td><span className="code-snippet">{att.subscriber_id.slice(0, 8)}...</span></td>
                    <td>Attempt {att.attempt_number}</td>
                    <td><span className={`badge ${statusBadge}`}>{att.status}</span></td>
                    <td>{att.response_status_code ? `HTTP ${att.response_status_code}` : 'N/A'}</td>
                    <td>{att.execution_time_ms} ms</td>
                    <td>
                      {att.status === "FAILED" && (
                        <button 
                          className="btn btn-secondary" 
                          style={{ padding: "2px 8px", fontSize: "0.72rem" }}
                          onClick={() => handleReplayDlq(att.event_id)}
                        >
                          Replay Event
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
