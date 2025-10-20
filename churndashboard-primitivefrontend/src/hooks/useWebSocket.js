import { useState, useEffect } from 'react';

export const useWebSocket = () => {
  const [liveEvent, setLiveEvent] = useState(null);
  const [highRiskAlert, setHighRiskAlert] = useState(null);
  const [wsStatus, setWsStatus] = useState('Connecting...');

  useEffect(() => {
    // Get the base API URL from the environment variable
const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
// Convert the http URL to a ws URL
const wsUrl = apiUrl.replace(/^http/, 'ws');

const ws = new WebSocket(`${wsUrl}/ws/updates`);
    ws.onopen = () => setWsStatus('Connected');
    ws.onclose = () => setWsStatus('Disconnected');
    ws.onerror = () => setWsStatus('Error');

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const payloadWithId = { ...data.payload, id: Date.now() };

        if (data.type === 'new_event') {
          setLiveEvent(payloadWithId);
        } else if (data.type === 'churn_alert') {
          setHighRiskAlert(payloadWithId);
        }
      } catch (error) {
        console.error("Error parsing WebSocket message:", error);
      }
    };
    
    // Cleanup on component unmount
    return () => ws.close();
  }, []);

  return { liveEvent, highRiskAlert, wsStatus };
};

