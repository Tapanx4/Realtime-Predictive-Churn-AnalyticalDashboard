import React from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

const ChurnProbabilityTrend = ({ data }) => {
    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            const dataPoint = payload[0].payload;
            const value = payload[0].value;
            return (
                <div className="bg-slate-900/95 backdrop-blur-sm border-2 border-red-500/30 rounded-xl p-4 shadow-2xl">
                    <div className="flex items-center gap-2 mb-2">
                        <div className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
                        <p className="text-white font-bold text-sm">{label}</p>
                    </div>
                    <div className="space-y-1">
                        <div className="flex items-baseline gap-2">
                            <span className="text-slate-400 text-xs">Avg Probability:</span>
                            <span className="text-red-400 font-bold text-xl">
                                {(value * 100).toFixed(2)}%
                            </span>
                        </div>
                        {dataPoint.count && (
                            <p className="text-slate-500 text-xs">
                                Based on {dataPoint.count} customers
                            </p>
                        )}
                    </div>
                </div>
            );
        }
        return null;
    };

    // Format Y-axis to show percentages
    const formatYAxis = (value) => `${(value * 100).toFixed(0)}%`;

    // Custom dot for data points
    const CustomDot = (props) => {
        const { cx, cy, payload } = props;
        return (
            <circle
                cx={cx}
                cy={cy}
                r={4}
                fill="#ef4444"
                stroke="#fee2e2"
                strokeWidth={2}
                style={{ filter: 'drop-shadow(0 0 4px rgba(239, 68, 68, 0.6))' }}
            />
        );
    };

    return (
        <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={data} margin={{ top: 10, right: 30, left: 10, bottom: 0 }}>
                <defs>
                    <linearGradient id="colorProbability" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#ef4444" stopOpacity={0.6}/>
                        <stop offset="50%" stopColor="#dc2626" stopOpacity={0.3}/>
                        <stop offset="100%" stopColor="#991b1b" stopOpacity={0.05}/>
                    </linearGradient>
                    <filter id="glow">
                        <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                        <feMerge>
                            <feMergeNode in="coloredBlur"/>
                            <feMergeNode in="SourceGraphic"/>
                        </feMerge>
                    </filter>
                </defs>
                <CartesianGrid 
                    strokeDasharray="3 3" 
                    stroke="#334155" 
                    opacity={0.2}
                    vertical={false}
                />
                <XAxis 
                    dataKey="time" 
                    stroke="#64748b"
                    fontSize={17}
                    tick={{ fill: '#94a3b8', fontWeight: 500 }}
                    tickLine={false}
                    axisLine={{ stroke: '#475569', strokeWidth: 1 }}
                />
                <YAxis 
                    stroke="#64748b"
                    fontSize={17}
                    tickFormatter={formatYAxis}
                    tick={{ fill: '#94a3b8', fontWeight: 500 }}
                    tickLine={false}
                    axisLine={{ stroke: '#475569', strokeWidth: 1 }}
                    domain={[0, 1]}
                />
                <Tooltip content={<CustomTooltip />} cursor={{ stroke: '#ef4444', strokeWidth: 1, strokeDasharray: '5 5' }} />
                <Area 
                    type="monotone" 
                    dataKey="probability" 
                    stroke="#ef4444"
                    strokeWidth={3}
                    fillOpacity={1} 
                    fill="url(#colorProbability)"
                    animationDuration={800}
                    animationEasing="ease-in-out"
                    dot={<CustomDot />}
                    activeDot={{ 
                        r: 6, 
                        fill: '#ef4444',
                        stroke: '#fee2e2',
                        strokeWidth: 3,
                        style: { filter: 'drop-shadow(0 0 8px rgba(239, 68, 68, 0.8))' }
                    }}
                />
            </AreaChart>
        </ResponsiveContainer>
    );
};

export default ChurnProbabilityTrend;