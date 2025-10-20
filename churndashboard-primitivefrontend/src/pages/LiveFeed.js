import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom'; 
import { useWebSocket } from '../hooks/useWebSocket';
import PageHeader from '../components/shared/PageHeader';
import { Zap, ShieldAlert } from 'lucide-react';
import KpiCard from '../components/dashboard/KpiCard';

const LiveFeed = () => {
    const [events, setEvents] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    
    const { liveEvent, highRiskAlert } = useWebSocket();

    // --- Fetch historical events on initial component load ---
    useEffect(() => {
        const fetchHistory = async () => {
            try {
                const response = await fetch(`${API_URL}/api/events/history?limit=50`);
                if (!response.ok) throw new Error('Failed to fetch history');
                
                const historyData = await response.json();

                const processedHistory = historyData.map(event => {
                    const eventData = event.event_data || {};
                    const isHighRisk = (eventData.churn_probability || 0) > 0.70;
                    
                    return {
                        ...eventData,
                        id: new Date(event.event_timestamp).getTime(),
                        isHighRisk: isHighRisk,
                        user_id: event.user_id || eventData.user_id,
                        event_type: event.event_type || eventData.event_type,
                        timestamp: new Date(event.event_timestamp).getTime() / 1000
                    };
                });

                setEvents(processedHistory);
            } catch (error) {
                console.error("Failed to fetch event history:", error);
            } finally {
                setIsLoading(false);
            }
        };

        fetchHistory();
    }, []);

    // --- useEffect hooks to handle LIVE events from WebSockets ---
    useEffect(() => {
        if (liveEvent) {
            const newEvent = { ...liveEvent, isHighRisk: false };
            setEvents(prevEvents => [newEvent, ...prevEvents].slice(0, 100));
        }
    }, [liveEvent]);

    useEffect(() => {
        if (highRiskAlert) {
            const newAlert = { ...highRiskAlert, isHighRisk: true };
            setEvents(prevEvents => [newAlert, ...prevEvents].slice(0, 100));
        }
    }, [highRiskAlert]);

    // Function to determine border color based on event type
    const getEventStyle = (eventType) => {
        if (eventType?.includes('downgrade')) return 'border-red-500';
        if (eventType?.includes('upgrade')) return 'border-green-500';
        if (eventType?.includes('cancelled')) return 'border-orange-500';
        if (eventType?.includes('ticket')) return 'border-yellow-500';
        return 'border-slate-600';
    };

    return (
        <div className="animate-fade-in space-y-8 ">
            <PageHeader 
                title="Global Live Feed"
                subtitle="A real-time stream of all user events. High-risk churn alerts are highlighted with a glow."
            />
             <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                 <KpiCard title="Events/sec (Live)" value="~0.3/sec" icon="events" />
                 <KpiCard title="Avg. Processing Latency" value="~150ms" icon="events" />
                 <KpiCard title="Dropped Events (24h)" value="0.01%" icon="risk" />
             </div>
            <div className="bg-navy-light rounded-xl border border-slate-700 shadow-lg">
                <h3 className="p-4 text-lg font-semibold text-white border-b border-slate-700">Real-Time Event Stream</h3>
                <div className="p-4 space-y-3 max-h-[60vh] overflow-y-auto">
                    {isLoading ? (
                        <div className="text-center py-24 text-slate-400">
                            <Zap size={48} className="mx-auto mb-4 opacity-50 animate-pulse" />
                            <p>Loading recent event history...</p>
                        </div>
                    ) : events.length === 0 ? (
                        <div className="text-center py-24 text-slate-400">
                            <Zap size={48} className="mx-auto mb-4 opacity-50" />
                            <p>No recent history. Waiting for new live events...</p>
                        </div>
                    ) : (
                        events.map((event) => (
                            <div 
                                key={event.id} 
                                className={`p-3 bg-navy rounded-md border-l-4 transition-shadow duration-300 ${getEventStyle(event.event_type)} ${
                                    event.isHighRisk ? 'high-risk-glow' : ''
                                }`}
                            >
                                <div className="flex justify-between items-center">
                                    <div className="flex items-center gap-3">
                                        {event.isHighRisk ? (
                                            <ShieldAlert size={20} className="text-red-400 flex-shrink-0" />
                                        ) : (
                                            <Zap size={20} className="text-slate-400 flex-shrink-0" />
                                        )}
                                        <div>
                                            <p className="font-semibold text-basic text-white capitalize">
                                                {event.event_type?.replace(/_/g, ' ') || 'Unknown Event'}
                                                {event.isHighRisk && (
                                                    <span className="ml-2 text-xs font-bold text-red-300 bg-red-900/50 px-2 py-0.5 rounded-full">
                                                        High Churn Risk
                                                    </span>
                                                )}
                                            </p>
                                            <p className="text-base text-slate-400">
                                                {/* 2. Wrap the user ID in a Link component */}
                                                User:{" "}
                                                <Link
                                                    to={`/customer/${event.user_id}`}
                                                    className="font-mono text-brand-light hover:underline hover:text-brand-lighter transition-colors"
                                                >
                                                    {event.user_id}
                                                </Link>
                                                {event.isHighRisk && (
                                                    <span className="ml-3">
                                                        Score: <span className="font-bold text-red-400">{(event.churn_probability * 100).toFixed(1)}%</span>
                                                    </span>
                                                )}
                                            </p>
                                        </div>
                                    </div>
                                    <p className="text-basic font-mono text-slate-100">{new Date(event.timestamp * 1000).toLocaleTimeString()}</p>
                                </div>
                            </div>
                        ))
                    )}
                </div>
            </div>
        </div>
    );
};

export default LiveFeed;