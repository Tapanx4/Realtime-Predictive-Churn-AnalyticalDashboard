import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

const HighVsLowRiskChart = ({ data }) => {
    const { critical = 0, high = 0, medium = 0, low = 0, total = 0 } = data;

    const chartData = [
        { name: 'Critical Risk', value: critical, gradient: 'criticalGradient' },
        { name: 'High Risk', value: high, gradient: 'highGradient' },
        { name: 'Medium Risk', value: medium, gradient: 'mediumGradient' },
        { name: 'Low Risk', value: low, gradient: 'lowGradient' },
    ].filter(item => item.value > 0);

    const COLORS = {
        'Critical Risk': '#dc2626',
        'High Risk': '#ef4444',
        'Medium Risk': '#f59e0b',
        'Low Risk': '#22c55e'
    };

    // Percentage of High + Critical Risk combined
    const highCriticalTotal = critical + high;
    const highCriticalPercent = total > 0 ? ((highCriticalTotal / total) * 100).toFixed(1) : '0.0';

    // Custom Tooltip with enhanced styling
    const CustomTooltip = ({ active, payload }) => {
        if (active && payload && payload.length) {
            const data = payload[0];
            const percentage = total > 0 ? ((data.value / total) * 100).toFixed(1) : '0.0';
            return (
                <div className="bg-slate-900/95 backdrop-blur-sm border-2 border-slate-600 rounded-xl p-4 shadow-2xl">
                    <div className="flex items-center gap-2 mb-2">
                        <div 
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: COLORS[data.name] }}
                        />
                        <p className="text-white font-bold text-sm">{data.name}</p>
                    </div>
                    <p className="text-slate-100 font-semibold text-lg">{data.value.toLocaleString()}</p>
                    <p className="text-slate-400 text-xs">customers ({percentage}%)</p>
                </div>
            );
        }
        return null;
    };

    // Custom Legend with modern card design
    const CustomLegend = () => {
        return (
            <div className="grid grid-cols-2 gap-3 mt-6 px-4">
                {chartData.map((entry, index) => {
                    const percent = total > 0 ? ((entry.value / total) * 100).toFixed(1) : '0.0';
                    return (
                        <div 
                            key={`legend-${index}`}
                            className="bg-slate-800/50 backdrop-blur-sm rounded-lg p-3 border border-slate-700/50 hover:border-slate-600 transition-all hover:scale-105 cursor-pointer"
                        >
                            <div className="flex items-center gap-2 mb-1">
                                <div 
                                    className="w-3 h-3 rounded-full shadow-lg"
                                    style={{ 
                                        backgroundColor: COLORS[entry.name],
                                        boxShadow: `0 0 10px ${COLORS[entry.name]}40`
                                    }}
                                />
                                <span className="text-slate-300 text-xl font-medium">{entry.name}</span>
                            </div>
                            <div className="flex items-baseline gap-2 ml-5">
                                <span className="text-white font-bold text-lg">{entry.value.toLocaleString()}</span>
                                <span className="text-slate-200 text-xl">({percent}%)</span>
                            </div>
                        </div>
                    );
                })}
            </div>
        );
    };

    return (
        <div className="relative">
            <ResponsiveContainer width="100%" height={320}>
                <PieChart>
                    <defs>
                        {/* Gradient definitions for a modern look */}
                        <radialGradient id="criticalGradient">
                            <stop offset="0%" stopColor="#dc2626" stopOpacity={1} />
                            <stop offset="100%" stopColor="#991b1b" stopOpacity={0.9} />
                        </radialGradient>
                        <radialGradient id="highGradient">
                            <stop offset="0%" stopColor="#ef4444" stopOpacity={1} />
                            <stop offset="100%" stopColor="#dc2626" stopOpacity={0.9} />
                        </radialGradient>
                        <radialGradient id="mediumGradient">
                            <stop offset="0%" stopColor="#f59e0b" stopOpacity={1} />
                            <stop offset="100%" stopColor="#d97706" stopOpacity={0.9} />
                        </radialGradient>
                        <radialGradient id="lowGradient">
                            <stop offset="0%" stopColor="#22c55e" stopOpacity={1} />
                            <stop offset="100%" stopColor="#16a34a" stopOpacity={0.9} />
                        </radialGradient>
                    </defs>
                    <Tooltip content={<CustomTooltip />} />
                    <Pie
                        data={chartData}
                        cx="50%"
                        cy="45%"
                        innerRadius={70}
                        outerRadius={110}
                        paddingAngle={3}
                        dataKey="value"
                        nameKey="name"
                        animationBegin={0}
                        animationDuration={800}
                    >
                        {chartData.map((entry, index) => (
                            <Cell 
                                key={`cell-${index}`} 
                                fill={`url(#${entry.gradient})`}
                                stroke="#1e293b"
                                strokeWidth={2}
                            />
                        ))}
                    </Pie>
                    {/* Center text with enhanced styling */}
                    <text
                        x="50%"
                        y="42%"
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill="#ffffff"
                        fontSize="40"
                        fontWeight="900"
                        style={{ filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.3))' }}
                    >
                        {highCriticalPercent}%
                    </text>
                    <text
                        x="50%"
                        y="52%"
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill="#94a3b8"
                        fontSize="15"
                        fontWeight="900"
                        letterSpacing="0.5"
                    >
                        HIGH + CRITICAL
                    </text>
                    <text
                        x="50%"
                        y="58%"
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fill="#64748b"
                        fontSize="12"
                        fontWeight="700"
                    >
                        RISK LEVEL
                    </text>
                </PieChart>
            </ResponsiveContainer>
            <CustomLegend />
        </div>
    );
};

export default HighVsLowRiskChart;