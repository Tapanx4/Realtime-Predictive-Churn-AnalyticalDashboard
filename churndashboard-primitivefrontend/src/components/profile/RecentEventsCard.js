import React from 'react';
import { History } from 'lucide-react';

const RecentEventsCard = ({ events }) => {
    
    // 1. Add the helper function to determine border color
    const getEventStyle = (eventType) => {
        if (eventType?.includes('downgrade')) return 'border-red-500';
        if (eventType?.includes('upgrade')) return 'border-green-500';
        if (eventType?.includes('cancelled')) return 'border-orange-500';
        if (eventType?.includes('ticket')) return 'border-yellow-500';
        return 'border-slate-600'; // Default color for other events
    };

    return (
        <div className="bg-navy-light p-6 rounded-xl shadow-lg h-full">
            <h3 className="text-xl font-bold text-white mb-4 flex items-center">
                <History size={20} className="mr-2 text-brand-light" />
                Recent Events
            </h3>
            <div className="space-y-3 max-h-96 overflow-y-auto">
                {(events || []).length > 0 ? (
                    (events || []).map((event) => (
                        // 2. Apply the dynamic border style here
                        <div 
                            key={event.event_id || event.id} 
                            className={`p-3 bg-navy rounded-md border-l-4 ${getEventStyle(event.event_type)}`}
                        >
                            <p className="font-semibold text-lg text-white capitalize">{event.event_type.replace(/_/g, ' ')}</p>
                            <p className="text-basic text-slate-500">
                                {new Date(event.event_timestamp).toLocaleString('en-IN', {
                                    day: 'numeric',
                                    month: 'short',
                                    year: 'numeric',
                                    hour: 'numeric',
                                    minute: '2-digit',
                                    hour12: true
                                })}
                            </p>
                        </div>
                    ))
                ) : (
                    <p className="text-slate-500">No recent events found.</p>
                )}
            </div>
        </div>
    );
};

export default RecentEventsCard;