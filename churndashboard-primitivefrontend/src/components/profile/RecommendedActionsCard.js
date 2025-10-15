
import React, { useState } from 'react';
import { Lightbulb, Check, Loader2 } from 'lucide-react';
import { RecommendationRules } from './RecommendationRules';

const RecommendedActionsCard = ({ details }) => {
  const [loggingAction, setLoggingAction] = useState(null);
  const [loggedIndexes, setLoggedIndexes] = useState([]);

  const generateRecommendations = () => {
    if (!details) return ["No data available for recommendations."];

    const actions = RecommendationRules
      .filter(rule => rule.condition(details))
      .map(rule => rule.message);

    if (actions.length === 0) {
      actions.push("Schedule a routine customer satisfaction call.");
    }

    return actions;
  };

  const recommendations = generateRecommendations();

  const handleLogAction = async (action, index) => {
    if (!details?.customerid && !details?.id) {
      alert('Customer ID not found');
      return;
    }

    setLoggingAction(index);

    try {
      const response = await fetch(`http://localhost:8000/api/customer/${details.customerid || details.id}/log-intervention`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customer_id: details.customerid || details.id,
          intervention_type: 'recommended_action',
          description: action,
          agent: 'System', // Or get from auth context
          timestamp: new Date().toISOString()
        })
      });

      if (!response.ok) {
        throw new Error('Failed to log action');
      }

      // Update state to show logged feedback
      setLoggedIndexes(prev => [...prev, index]);
      setTimeout(() => {
        setLoggedIndexes(prev => prev.filter(i => i !== index));
      }, 2000);

      console.log('Action logged successfully:', action);
    } catch (error) {
      console.error('Error logging action:', error);
      alert('Failed to log action. Please try again.');
    } finally {
      setLoggingAction(null);
    }
  };

  return (
    <div className="bg-navy-light p-6 rounded-xl shadow-lg">
      <h3 className="text-xl font-bold text-white mb-4 flex items-center">
        <Lightbulb size={20} className="mr-2 text-yellow-300" />
        Recommended Actions
      </h3>
      <ul className="space-y-3 max-h-96 overflow-y-auto scrollbar-thin">
        {recommendations.map((rec, index) => (
          <li
            key={index}
            className="bg-brand/10 p-3 rounded-lg flex justify-between items-center text-sm border-l-4 border-brand-light"
          >
            <span className="text-white">{rec}</span>
            <button
              id={`log-btn-${index}`}
              onClick={() => handleLogAction(rec, index)}
              disabled={loggingAction === index}
              className={`text-white text-basic px-3 py-1 rounded-full transition-colors flex items-center space-x-1 disabled:opacity-50 disabled:cursor-not-allowed min-w-[70px] justify-center ${
                loggedIndexes.includes(index) ? 'bg-green-700' : 'bg-green-600 hover:bg-green-700'
              }`}
            >
              {loggingAction === index ? (
                <>
                  <Loader2 size={14} className="animate-spin" />
                  <span>...</span>
                </>
              ) : loggedIndexes.includes(index) ? (
                <>
                  <Check size={14} />
                  <span>Logged!</span>
                </>
              ) : (
                <>
                  <Check size={14} />
                  <span>Log</span>
                </>
              )}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default RecommendedActionsCard;
