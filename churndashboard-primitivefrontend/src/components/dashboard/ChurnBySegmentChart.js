import React from 'react';
import {
  CartesianGrid,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
} from 'recharts';

const ChurnBySegmentChart = ({ data }) => {
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900/95 backdrop-blur-sm border-2 border-red-500/30 rounded-xl p-4 shadow-2xl">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 rounded-full bg-red-500" />
            <p className="text-white font-bold text-sm capitalize">{label}</p>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-slate-400 text-xs">High-Risk:</span>
            <span className="text-red-400 font-bold text-xl">
              {payload[0].value.toLocaleString()}
            </span>
          </div>
          <p className="text-slate-500 text-xs mt-1">customers</p>
        </div>
      );
    }
    return null;
  };

  const getBarColor = (value, maxValue) => {
    const intensity = value / maxValue;
    if (intensity > 0.7) return '#dc2626';
    if (intensity > 0.4) return '#ef4444';
    return '#f87171';
  };

  const maxValue = Math.max(...(data || []).map((d) => d.count || 0));

  return (
    // ✅ Fixed height so chart doesn't grow infinitely
    <div style={{ width: '100%', height: '500px' }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          margin={{ top: 20, right: 30, left: 0, bottom: 20 }}
          barGap={8}
        >
          <defs>
            <linearGradient id="barGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#ef4444" stopOpacity={1} />
              <stop offset="100%" stopColor="#dc2626" stopOpacity={0.8} />
            </linearGradient>
            <filter id="barShadow">
              <feDropShadow
                dx="0"
                dy="4"
                stdDeviation="3"
                floodColor="#ef4444"
                floodOpacity="0.3"
              />
            </filter>
          </defs>

          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#334155"
            opacity={0.2}
            vertical={false}
          />
          <XAxis
            dataKey="contract"
            stroke="#64748b"
            fontSize={20}
            tick={{ fill: '#94a3b8', fontWeight: 600 }}
            tickLine={false}
            axisLine={{ stroke: '#475569', strokeWidth: 1 }}
          />
          <YAxis
            stroke="#64748b"
            fontSize={17}
            tick={{ fill: '#94a3b8', fontWeight: 500 }}
            tickLine={false}
            axisLine={{ stroke: '#475569', strokeWidth: 1 }}
          />
          <Tooltip
            content={<CustomTooltip />}
            cursor={{ fill: 'rgba(239, 68, 68, 0.05)', radius: 8 }}
          />
          <Legend
            wrapperStyle={{
              fontSize: '20px',
              fontWeight: 600,
              paddingTop: '10px',
            }}
            iconType="circle"
          />
          <Bar
            dataKey="count"
            fill="url(#barGradient)"
            name="High-Risk Customers"
            radius={[8, 8, 0, 0]}
            animationDuration={800}
            animationEasing="ease-out"
            style={{ filter: 'url(#barShadow)' }}
          >
            {(data || []).map((entry, index) => (
              <Cell
                key={`cell-${index}`}
                fill={getBarColor(entry.count, maxValue)}
                style={{
                  filter: 'drop-shadow(0 4px 6px rgba(239, 68, 68, 0.3))',
                }}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

export default ChurnBySegmentChart;
