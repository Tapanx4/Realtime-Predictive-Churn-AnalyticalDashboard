import React, { useState, useEffect, useMemo } from 'react';
import { Chart } from 'react-google-charts';
import { Activity } from 'lucide-react';
import { useWebSocket } from '../hooks/useWebSocket';

// Helper function to determine customer lifecycle stage based on tenure
const getLifecycleStage = (tenure) => {
    if (tenure <= 3) return 'New Customers';
    if (tenure >= 4 && tenure <= 12) return 'Early Lifecycle';
    if (tenure >= 13 && tenure <= 24) return 'Mid Lifecycle';
    if (tenure > 24) return 'Mature Customers';
    return 'Unknown';
};

// Helper function to determine churn risk band based on probability
const getRiskBand = (probability) => {
    if (probability >= 0 && probability <= 0.25) return 'Low Risk';
    if (probability > 0.25 && probability <= 0.5) return 'Medium Risk';
    if (probability > 0.5 && probability <= 0.75) return 'High Risk';
    if (probability > 0.75 && probability <= 1.0) return 'Critical Risk';
    return 'Unknown';
};

const Analytics = () => {
    const [alertsHistory, setAlertsHistory] = useState([]);
    const [shapData, setShapData] = useState([]);
    const { highRiskAlert, wsStatus } = useWebSocket();
    const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    // Fetch initial history of customer alerts
    useEffect(() => {
        fetch(`${API_URL}/api/churn-alerts-history`, { cache: 'no-store' })
        .then(res => res.json())
        .then(data => setAlertsHistory(data))
        .catch(err => console.error("Error fetching alerts:", err));
    }, []);

    // Fetch SHAP data periodically
    useEffect(() => {
        const fetchShapData = () => {
            fetch(`${API_URL}/api/shap-summary`, { cache: 'no-store' })
                .then(res => res.json())
                .then(data => setShapData(data))
                .catch(err => console.error("Error fetching SHAP data:", err));
        };
        fetchShapData();
        const interval = setInterval(fetchShapData, 30000);
        return () => clearInterval(interval);
    }, []);

    // Update alerts history with live data
    useEffect(() => {
        if (highRiskAlert) {
            setAlertsHistory(prev => [...prev.slice(-499), highRiskAlert]);
        }
    }, [highRiskAlert]);

    // --- CHART DATA PROCESSING ---

    const lifecycleChartData = useMemo(() => {
        const stages = {
            'New Customers': { 'Low Risk': 0, 'Medium Risk': 0, 'High Risk': 0, 'Critical Risk': 0, total: 0 },
            'Early Lifecycle': { 'Low Risk': 0, 'Medium Risk': 0, 'High Risk': 0, 'Critical Risk': 0, total: 0 },
            'Mid Lifecycle': { 'Low Risk': 0, 'Medium Risk': 0, 'High Risk': 0, 'Critical Risk': 0, total: 0 },
            'Mature Customers': { 'Low Risk': 0, 'Medium Risk': 0, 'High Risk': 0, 'Critical Risk': 0, total: 0 },
        };
        alertsHistory.forEach(customer => {
            if (typeof customer.tenure !== 'number' || typeof customer.churn_probability !== 'number') return;
            const stage = getLifecycleStage(customer.tenure);
            const riskBand = getRiskBand(customer.churn_probability);
            if (stages[stage] && stages[stage][riskBand] !== undefined) {
                stages[stage][riskBand]++;
                stages[stage].total++;
            }
        });
        const riskColors = { 'Low Risk': '#22c55e', 'Medium Risk': '#f59e0b', 'High Risk': '#ef4444', 'Critical Risk': '#b91c1c' };
        return Object.entries(stages).map(([stageName, data]) => ({
            stage: stageName,
            total: data.total,
            risks: Object.entries(data).filter(([key]) => key !== 'total').map(([riskName, count]) => ({
                name: riskName,
                count,
                percentage: data.total > 0 ? ((count / data.total) * 100) : 0,
                color: riskColors[riskName]
            }))
        }));
    }, [alertsHistory]);

    const maxTotalCustomers = useMemo(() => Math.max(...lifecycleChartData.map(stage => stage.total), 1), [lifecycleChartData]);

    const shapChartData = useMemo(() => {
        if (!Array.isArray(shapData) || shapData.length === 0) {
            return [['Feature', 'Predictive Impact', { role: 'style' }]];
        }
        const headers = ['Feature', 'Predictive Impact', { role: 'style' }];
        const prettifyFeatureName = (name) => {
            return name.replace(/_/g, ' ')
                       .replace('Contract Month-to-month', 'Month-to-Month Contract')
                       .replace('InternetService Fiber optic', 'Fiber Optic Internet')
                       .replace('PaymentMethod Electronic check', 'Electronic Check')
                       .replace(/\b\w/g, l => l.toUpperCase());
        };
        const dataRows = shapData
            .slice(0, 8)
            .sort((a, b) => a.importance - b.importance)
            .map(item => [prettifyFeatureName(item.feature), item.importance, '#0ea5e9']);
        return [headers, ...dataRows];
    }, [shapData]);

    // REMOVED: The 'sankeyLegend' constant was here.

    const sankeyData = useMemo(() => {
        const links = new Map();
        alertsHistory.forEach(customer => {
            if (typeof customer.tenure !== 'number' || !customer.contract_type || typeof customer.churn_probability !== 'number') return;
            const stage = getLifecycleStage(customer.tenure);
            const contract = `Contract: ${customer.contract_type.replace(/-/g, ' ')}`;
            const risk = `Risk: ${getRiskBand(customer.churn_probability)}`;
            const key1 = `${stage},${contract}`;
            links.set(key1, (links.get(key1) || 0) + 1);
            const key2 = `${contract},${risk}`;
            links.set(key2, (links.get(key2) || 0) + 1);
        });
        const data = [['From', 'To', 'Weight']];
        links.forEach((weight, key) => {
            const [from, to] = key.split(',');
            data.push([from, to, weight]);
        });
        return data.length > 1 ? data : [];
    }, [alertsHistory]);
    
    const heatmapData = useMemo(() => {
        if (alertsHistory.length === 0) return [];
        const features = ['tenure', 'contract_type', 'monthly_charges'];
        const riskLevels = ['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk'];
        const bounds = {};
        features.forEach(f => {
            if (f !== 'contract_type') {
                const values = alertsHistory.map(a => a[f]).filter(v => typeof v === 'number');
                bounds[f] = { min: Math.min(...values), max: Math.max(...values) };
            }
        });
        const heatmap = riskLevels.map(risk => {
            const customersInRisk = alertsHistory.filter(a => getRiskBand(a.churn_probability) === risk);
            const featureAverages = { risk };
            features.forEach(feature => {
                if (feature === 'contract_type') {
                    const counts = customersInRisk.reduce((acc, curr) => {
                        acc[curr.contract_type] = (acc[curr.contract_type] || 0) + 1;
                        return acc;
                    }, {});
                    const mostCommon = Object.keys(counts).reduce((a, b) => counts[a] > counts[b] ? a : b, 'N/A').replace(/-/g, ' ');
                    featureAverages[feature] = mostCommon;
                } else {
                    const avg = customersInRisk.reduce((sum, a) => sum + (a[feature] || 0), 0) / (customersInRisk.length || 1);
                    const normalized = (avg - bounds[feature].min) / (bounds[feature].max - bounds[feature].min || 1);
                    featureAverages[feature] = { value: avg, intensity: normalized };
                }
            });
            return featureAverages;
        });
        return heatmap;
    }, [alertsHistory]);


    return (
        <div className="min-h-screen bg-slate-900 text-slate-200 p-6">
            <div className="flex justify-between items-center mb-8">
                <div>
                    <h1 className="text-3xl font-bold text-white">Real-Time Churn Analytics</h1>
                    <div className="flex items-center gap-2 text-sm text-slate-400">
                        <Activity size={20} className={wsStatus === 'Connected' ? 'text-green-400 animate-pulse' : 'text-red-400'} />
                        <span>Live Feed: {wsStatus}</span>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Customer Lifecycle Risk Chart */}
                <div className="bg-slate-800/50 rounded-lg border border-slate-700 p-6">
                    <h2 className="text-xl font-semibold text-white mb-4">Customer Lifecycle Risk</h2>
                      {alertsHistory.length > 0 ? (
                        <div className="space-y-6">
                            {lifecycleChartData.map((stageData) => (
                                <div key={stageData.stage}>
                                    <div className="flex justify-between items-baseline mb-2 text-sm">
                                        <h3 className="font-medium text-slate-200">{stageData.stage}</h3>
                                        <span className="text-slate-400 font-medium">{stageData.total} Customers</span>
                                    </div>
                                    <div className="bg-slate-700/50 rounded-full h-8 flex overflow-hidden" style={{ width: `${(stageData.total / maxTotalCustomers) * 100}%` }}>
                                        {stageData.risks.map((risk, idx) => (
                                            risk.count > 0 && (
                                                <div key={idx} className="h-full flex items-center justify-center text-white text-sm font-bold" style={{ width: `${risk.percentage}%`, backgroundColor: risk.color }} title={`${risk.name}: ${risk.count} (${risk.percentage.toFixed(1)}%)`}>
                                                    {risk.count}
                                                </div>
                                            )
                                        ))}
                                    </div>
                                </div>
                            ))}
                        </div>
                    ) : <p className="text-slate-500 text-center mt-16">Waiting for customer data...</p>}
                </div>

                {/* Top Churn Predictors (SHAP) */}
                <div className="bg-slate-800/50 rounded-lg border border-slate-700 p-6 h-[480px] flex flex-col">
                    <h2 className="text-xl font-semibold text-white mb-4">Top Churn Predictors</h2>
                    <div className="flex-grow w-full h-full">
                        {shapChartData.length > 1 ? (
                            <Chart
                                chartType="BarChart"
                                width="100%"
                                height="100%"
                                data={shapChartData}
                                options={{
                                    backgroundColor: 'transparent',
                                    legend: { position: 'none' },
                                    hAxis: { textStyle: { color: '#e2e8f0', fontSize: 15 }, gridlines: { color: '#475569', count: 5 }, title: 'Mean Absolute SHAP Value', titleTextStyle: { color: '#94a3b8', italic: false, fontSize: 12 } },
                                    vAxis: { textStyle: { color: '#e2e8f0', fontSize: 17, bold: false } },
                                    chartArea: { left: 220, top: 20, width: '60%', height: '85%' },
                                    tooltip: { textStyle: { color: '#1e293b' }, showColorCode: true },
                                }}
                            />
                        ) : <p className="text-slate-500 text-center mt-16">Loading model explanations...</p>}
                    </div>
                </div>
                
                {/* Feature Importance Heatmap */}
                <div className="lg:col-span-2 bg-slate-800/50 rounded-lg border border-slate-700 p-6">
                     <h2 className="text-xl font-semibold text-white mb-4">Risk Profile Heatmap</h2>
                     {heatmapData.length > 0 ? (
                        <div className="grid grid-cols-4 gap-3 text-center text-basic">
                            <div className="font-semibold text-slate-400 text-left">Risk Level</div>
                            <div className="font-semibold text-slate-400">Avg. Tenure</div>
                            <div className="font-semibold text-slate-400">Avg. Monthly Charges</div>
                            <div className="font-semibold text-slate-400">Dominant Contract</div>
                            {heatmapData.map((row) => (
                                <React.Fragment key={row.risk}>
                                    <div className="text-left font-semibold text-slate-200 p-3">{row.risk}</div>
                                    <div className="p-3 rounded-md text-white font-bold" style={{ backgroundColor: `rgba(245, 158, 11, ${row.tenure.intensity * 0.8 + 0.2})` }}>{row.tenure.value.toFixed(1)} mo</div>
                                    <div className="p-3 rounded-md text-white font-bold" style={{ backgroundColor: `rgba(239, 68, 68, ${row.monthly_charges.intensity * 0.8 + 0.2})` }}>${row.monthly_charges.value.toFixed(2)}</div>
                                    <div className="p-3 rounded-md bg-sky-800/60 text-sky-200 font-semibold capitalize">{row.contract_type}</div>
                                </React.Fragment>
                            ))}
                        </div>
                     ) : <p className="text-slate-500 text-center mt-8">Waiting for data...</p>}
                </div>

                
                <div className="lg:col-span-2 bg-slate-800/50 rounded-lg border border-slate-700 p-6 min-h-[500px]">
                    <h2 className="text-xl font-semibold text-white mb-2">Customer Flow Analysis</h2>
                
                    {/* REMOVED: The custom legend JSX was here. */}

                    {sankeyData.length > 1 ? (
                        <Chart
                            chartType="Sankey"
                            width="100%"
                            height="400px"
                            data={sankeyData}
                            options={{
                                backgroundColor: 'transparent',
                                sankey: {
                                    node: {
                                        label: { color: '#e2e8f0', fontSize: 14, bold: true },
                                        labelPadding: 10,
                                        nodePadding: 25,
                                        width: 20,
                                        // REMOVED: The 'colors' array that depended on sankeyLegend.
                                        // The chart will now use its default color palette.
                                    },
                                    link: {
                                        colorMode: 'source',
                                        color: { fillOpacity: 0.6 },
                                    }
                                }
                            }}
                        />
                    ) : <p className="text-slate-500 text-center mt-16">Not enough data to render customer flow.</p>}
                </div>
            </div>
        </div>
    );
};

export default Analytics;