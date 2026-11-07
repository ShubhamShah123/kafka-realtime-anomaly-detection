
import { useEffect, useState } from "react";
import "./App.css";

const API_URL = "http://127.0.0.1:8000";

function App() {
  const [stats, setStats] = useState(null);
  const [sensors, setSensors] = useState([]);
  const [timeline, setTimeline] = useState([]);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadDashboard() {
      try {
        const [statsRes, sensorsRes, timelineRes] =
          await Promise.all([
            fetch(`${API_URL}/stats`),
            fetch(`${API_URL}/stats/sensors`),
            fetch(`${API_URL}/stats/timeline`),
          ]);

        if (!statsRes.ok || !sensorsRes.ok || !timelineRes.ok) {
          throw new Error("One or more API requests failed.");
        }

        const statsData = await statsRes.json();
        const sensorsData = await sensorsRes.json();
        const timelineData = await timelineRes.json();
        console.log(statsData)
        setStats(statsData);
        setSensors(
          Array.isArray(sensorsData)
            ? sensorsData
            : sensorsData.sensors ?? []
        );
        setTimeline(timelineData.timeline ?? []);
      } catch (err) {
        setError(err.message);
      }
    }

    loadDashboard();
  }, []);

  if (error) {
    return (
      <main className="dashboard">
        <h1>Real-Time ML Pipeline</h1>
        <p className="error">
          Could not load dashboard data: {error}
        </p>
        <p>Check that FastAPI and PostgreSQL are running.</p>
      </main>
    );
  }

  if (!stats) {
    return <main className="dashboard">Loading dashboard...</main>;
  }

  const total = Number(stats.total_readings ?? 0);
  const anomalies = Number(stats.anomaly_count ?? 0);
  const average = Number(stats.average_temperature ?? 0);
  const anomalyRate = total ? (anomalies / total) * 100 : 0;

  return (
    <main className="dashboard">
      <header className="header">
        <div>
          <p className="eyebrow">MONITORING SYSTEM</p>
          <h1>Real-Time ML Pipeline</h1>
          <p className="subtitle">
            Sensor activity and anomaly monitoring
          </p>
        </div>
        <span className="status">● API connected</span>
      </header>

      <section className="cards">
        <article className="card">
          <p>Total readings</p>
          <h2>{total.toLocaleString()}</h2>
        </article>
        <article className="card">
          <p>Anomalies</p>
          <h2>{anomalies.toLocaleString()}</h2>
        </article>
        <article className="card">
          <p>Average temperature</p>
          <h2>{average.toFixed(2)}°</h2>
        </article>
        <article className="card">
          <p>Ground-truth anomaly rate</p>
          <h2>{anomalyRate.toFixed(1)}%</h2>
        </article>
      </section>

      <section className="panel">
        <h2>Temperature timeline</h2>
        {timeline.length === 0 ? (
          <p>No timeline data available yet.</p>
        ) : (
          <div className="timeline">
            {timeline.map((point, index) => (
              <div className="timeline-row" key={index}>
                <span>
                  {new Date(point.time_bucket).toLocaleTimeString()}
                </span>
                <div className="bar-track">
                  <div
                    className="bar"
                    style={{
                      width: `${Math.min(
                        100,
                        Math.max(
                          0,
                          Number(point.average_temperature) / 200 * 100
                        )
                      )}%`,
                    }}
                  />
                </div>
                <strong>
                  {Number(point.average_temperature).toFixed(1)}°
                </strong>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="panel">
        <h2>Sensor comparison</h2>
        {sensors.length === 0 ? (
          <p>No sensor data available.</p>
        ) : (
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Sensor</th>
                  <th>Readings</th>
                  <th>Average °C</th>
                  <th>Anomalies</th>
                </tr>
              </thead>
              <tbody>
                {sensors.map((sensor) => (
                  <tr key={sensor.sensor_id}>
                    <td>{sensor.sensor_id}</td>
                    <td>{Number(sensor.total_readings ?? 0)}</td>
                    <td>
                      {Number(sensor.average_temperature ?? 0).toFixed(2)}
                    </td>
                    <td>{Number(sensor.anomaly_count ?? 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <p className="footer">
        Initial dashboard · Refresh the page to reload data
      </p>
    </main>
  );
}

export default App;
