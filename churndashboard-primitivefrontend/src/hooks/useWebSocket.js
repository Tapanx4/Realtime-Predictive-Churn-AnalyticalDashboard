import { useState, useEffect } from 'react';

export const useWebSocket = () => {
  const [liveEvent, setLiveEvent] = useState(null);
  const [highRiskAlert, setHighRiskAlert] = useState(null);
  const [wsStatus, setWsStatus] = useState('Connecting...');

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/updates');
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

