import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, AlertCircle } from 'lucide-react';
import PageHeader from '../components/shared/PageHeader';
import CustomerDetailsCard from '../components/profile/CustomerDetailsCard';
import RecentEventsCard from '../components/profile/RecentEventsCard';
import RecommendedActionsCard from '../components/profile/RecommendedActionsCard';
import InterventionLog from '../components/profile/InterventionLog';
import Loader from '../components/shared/Loader';

const CustomerProfile = () => {
    const { customerId } = useParams();
    const [customerData, setCustomerData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                const response = await fetch(`${API_URL}/api/customer/${customerId}`);
                const data = await response.json();
                if (data.error) throw new Error(data.error);
                setCustomerData(data);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [customerId]);

    if (loading) return <Loader />;
    if (error || !customerData) return <div className="text-red-500"><AlertCircle /> Error loading data.</div>;

    const latestPrediction = customerData.details?.churn_probability || 0;


    return (
        <div className="space-y-8 animate-fade-in">
             <Link to="/watchlist" className="flex items-center space-x-2 text-brand-light font-bold hover:underline">
                <ArrowLeft size={20} />
                <span>Back to Watchlist</span>
            </Link>
            <PageHeader title={`Customer Profile: ${customerId}`} />
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                <div className="lg:col-span-1 space-y-8">
                    <CustomerDetailsCard details={customerData.details} score={latestPrediction} />
                    <RecommendedActionsCard details={customerData.details} />
                </div>
                <div className="lg:col-span-2 space-y-8">
                    <RecentEventsCard events={customerData.recent_events} />
                    <InterventionLog logs={customerData.intervention_log} />

                </div>
            </div>
        </div>
    );
};

export default CustomerProfile;
