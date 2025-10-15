import React from 'react';
import { ShieldAlert, ShieldCheck, Shield } from 'lucide-react';

const RiskBadge = ({ score, showText = true }) => {
    let colorClass, text, Icon;
    if (score >= 0.85) { colorClass = 'text-red-400'; text = 'Critical'; Icon = ShieldAlert; }
    else if (score > 0.70) { colorClass = 'text-orange-400'; text = 'High'; Icon = ShieldAlert; }
    else if (score > 0.40) { colorClass = 'text-yellow-400'; text = 'Medium'; Icon = Shield; }
    else { colorClass = 'text-green-400'; text = 'Low'; Icon = ShieldCheck; }

    return (
        <span className={`flex items-center font-semibold text-sm ${colorClass}`}>
            <Icon size={16} className="mr-1.5" />
            {showText && text}
        </span>
    );
};

export default RiskBadge;
