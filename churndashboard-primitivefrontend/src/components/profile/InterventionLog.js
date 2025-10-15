import React from 'react';
import { Clock } from 'lucide-react';

const InterventionLog = ({ logs = [] }) => {
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A';
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div className="bg-navy-light p-6 rounded-xl shadow-lg">
      <h3 className="text-xl font-bold text-white mb-4">Intervention Log</h3>

      <div className="mt-4">
        {logs.length === 0 ? (
          <div className="text-center py-8 text-slate-400">
            No interventions performed.
          </div>
        ) : (
          <div className="space-y-2 text-sm max-h-64 overflow-y-auto">
            {logs.map((log, index) => (
              <div
                key={log.log_id || index}
                className="border-b border-slate-700 pb-3 last:border-b-0"
              >
                <div className="flex items-start justify-between mb-1">
                  <span className="flex items-center text-slate-300 font-medium">
                    <Clock size={14} className="mr-1.5 text-slate-500" />
                    {formatTimestamp(log.log_timestamp)}
                  </span>
                  <span className="px-2 py-0.5 bg-slate-700 text-slate-300 rounded text-xs">
                    Intervention
                  </span>
                </div>
                <p className="text-slate-400 pl-5">
                  {log.action_taken || 'No description'}
                </p>
                {log.agent_id && (
                  <p className="text-slate-500 text-xs mt-1 pl-5">
                    by {log.agent_id}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default InterventionLog;
