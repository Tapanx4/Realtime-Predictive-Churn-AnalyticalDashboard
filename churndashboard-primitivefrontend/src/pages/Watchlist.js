import React, { useState, useEffect, useCallback } from 'react';
import PageHeader from '../components/shared/PageHeader';
import FilterPanel from '../components/watchlist/FilterPanel';
import WatchlistTable from '../components/watchlist/WatchlistTable';
import Loader from '../components/shared/Loader';
import { AlertCircle } from 'lucide-react';

const Watchlist = () => {
    const [data, setData] = useState({ users: [], total_pages: 1, total_users: 0 });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [page, setPage] = useState(1);
    const [filters, setFilters] = useState({
        searchTerm: '',
        risk: [],
        contract: [],
    });

    const handleFilterChange = useCallback((name, value) => {
        setFilters(prev => ({ ...prev, [name]: value }));
        setPage(1);
    }, []);

    useEffect(() => {
        const fetchUsers = async () => {
            try {
                setLoading(true);
                setError(null);
                const params = new URLSearchParams({ page, search: filters.searchTerm });
                filters.risk.forEach(r => params.append('risk', r));
                filters.contract.forEach(c => params.append('contract', c));

                const response = await fetch(`${API_URL}/api/watchlist?${params.toString()}`);
                if (!response.ok) throw new Error('Network response was not ok');
                const result = await response.json();
                if (result.error) throw new Error(result.error);
                setData(result);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };
        const timer = setTimeout(() => fetchUsers(), 300);
        return () => clearTimeout(timer);
    }, [page, filters]);

    return (
        <div className="space-y-8 animate-fade-in">
            <PageHeader title="High-Risk Customer Watchlist">
                <span className="text-sm text-slate-400">Last updated: {new Date().toLocaleTimeString()}</span>
            </PageHeader>
            <div className="flex flex-col lg:flex-row gap-8">
                <FilterPanel filters={filters} onFilterChange={handleFilterChange} />
                <div className="flex-1">
                    {loading && <div className="h-96 flex items-center justify-center bg-navy-light rounded-xl"><Loader /></div>}
                    {error && <div className="h-96 flex items-center justify-center bg-navy-light rounded-xl text-red-400"><AlertCircle className="mr-2"/>{error}</div>}
                    {!loading && !error && (
                        <WatchlistTable 
                            users={data.users} 
                            page={page}
                            totalPages={data.total_pages}
                            totalUsers={data.total_users}
                            onPageChange={setPage}
                        />
                    )}
                </div>
            </div>
        </div>
    );
};

export default Watchlist;
