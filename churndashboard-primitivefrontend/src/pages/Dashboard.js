import React, { useState, useEffect } from 'react';
import { Loader2 } from 'lucide-react';
import PageHeader from '../components/shared/PageHeader';
import KpiCard from '../components/dashboard/KpiCard';
import ChurnBySegmentChart from '../components/dashboard/ChurnBySegmentChart';
import HighVsLowRiskChart from '../components/dashboard/HighVsLowRiskChart';
import ChurnProbabilityTrend from '../components/dashboard/ChurnProbabilityTrend';
import { useWebSocket } from '../hooks/useWebSocket';

const Dashboard = () => {
  const [kpis, setKpis] = useState(null);
  const [loading, setLoading] = useState(true);
  const { highRiskAlert, wsStatus } = useWebSocket();
  const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const response = await fetch(`${API_URL}/api/dashboard-kpis`);
        if (!response.ok) {
          throw new Error('Failed to fetch dashboard KPIs');
        }
        const data = await response.json();
        setKpis(data);
      } catch (error) {
        console.error("Failed to fetch KPIs", error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [highRiskAlert]);

  // 🔹 fallback trend data (in case API doesn't return it)
  const fallbackTrendData = [
    { time: '12:00', probability: 0.12 },
    { time: '12:05', probability: 0.15 },
    { time: '12:10', probability: 0.14 },
    { time: '12:15', probability: 0.18 },
    { time: '12:20', probability: 0.22 },
  ];

  // 🧠 pick trend data from backend if available
  // Transform the backend data to match the format the chart component expects.
const trendData = (kpis?.churn_trend || []).map(item => ({
    // Rename 'hour' to 'time' and format it for better readability
    time: new Date(item.hour).toLocaleTimeString('en-IN', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: true
    }),
    
    // Rename 'avg_prob' to 'probability'
    probability: item.avg_prob 
}));
  const avgChurnProbability = kpis?.overall_avg_churn_probability ?? 0;
  if (loading) {
    return (
      <div className="flex justify-center items-center h-full">
        <Loader2 className="animate-spin text-brand" size={48} />
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      <PageHeader title="Main Dashboard (Overview)">
        <span className="text-sm text-slate-400">
          Last updated: {new Date().toLocaleTimeString()}
        </span>
      </PageHeader>

      {/* KPI CARDS */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6">
        <KpiCard
          title="Total Active Customers"
          value={(kpis?.total_active_customers || 0).toLocaleString()}
          icon="users"
        />
        <KpiCard
          title="High & Critical Risk Customers"
          value={(
            (kpis?.high_risk_customers || 0) +
            (kpis?.critical_risk_customers || 0)
          ).toLocaleString()}
          icon="risk"
        />
        <KpiCard
          title="MRR At Risk"
          value={`$${(kpis?.mrr_at_risk || 0).toLocaleString('en-US', {
            maximumFractionDigits: 0,
          })}`}
          icon="mrr"
        />
        <KpiCard
          title="Avg Churn Probability"
          value={`${(avgChurnProbability * 100).toFixed(2)}%`}
          icon="trend"
        />
        <KpiCard title="WS Status" value={wsStatus} icon="events" />
      </div>

      {/* CHARTS */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-3 bg-navy-light p-6 rounded-xl shadow-md">
          <h3 className="text-xl font-semibold mb-4 text-white">Churn Risk by Segment</h3>
          <ChurnBySegmentChart data={kpis?.churn_by_segment || []} />
        </div>

        <div className="lg:col-span-2 bg-navy-light p-6 rounded-xl shadow-md">
          <h3 className="text-xl font-semibold mb-4 text-white">High vs. Low Risk Customers</h3>
          <HighVsLowRiskChart
            data={{
              critical: kpis?.critical_risk_customers || 0,
              high: kpis?.high_risk_customers || 0,
              medium: kpis?.medium_risk_customers || 0,
              low: kpis?.low_risk_customers || 0,
              total: kpis?.total_active_customers || 0,
            }}
          />
        </div>
      </div>

      {/* TREND CHART */}
      <div className="bg-navy-light p-6 rounded-xl shadow-md">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-semibold text-white">
            Churn Probability Over Time (Last N minutes)
          </h3>
          <span className="text-sm text-slate-400">
            Avg: {(avgChurnProbability * 100).toFixed(2)}%
          </span>
        </div>
        <ChurnProbabilityTrend data={trendData} />
      </div>
    </div>
  );
};

export default Dashboard;
