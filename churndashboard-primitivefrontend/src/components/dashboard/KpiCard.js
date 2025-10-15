import React from 'react';
import { Users, AlertTriangle, TrendingUp, Zap } from 'lucide-react';

const KpiCard = ({ title, value, change, changeType, icon }) => {
    const changeClasses = changeType === 'positive' ? 'text-green-400' : 'text-red-400';
    const arrowIcon = changeType === 'positive' ? '↑' : '↓';

    const   icons = {
        users: <Users className="text-brand-light" />,
        risk: <AlertTriangle className="text-red-400" />,
        mrr: <TrendingUp className="text-green-400" />,
        events: <Zap className="text-yellow-400" />
    };

    return (
        <div className="bg-navy-light p-6 rounded-xl shadow-lg">
            <div className="flex justify-between items-start">
                <h3 className="text-slate-400 text-sm font-semibold">{title}</h3>
                {icons[icon]}
            </div>
            <p className="text-4xl font-extrabold text-white mt-2">{value}</p>
            {change && (
                <div className={`text-sm flex items-center ${changeClasses} mt-2`}>
                    <span className="mr-1 font-bold">{arrowIcon}</span>
                    <span>{change} (30m)</span>
                </div>
            )}
        </div>
    );
};

export default KpiCard;
