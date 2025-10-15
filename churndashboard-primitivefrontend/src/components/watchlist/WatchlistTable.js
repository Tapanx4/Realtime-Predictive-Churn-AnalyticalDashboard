
import React from 'react';
import { Link } from 'react-router-dom';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import RiskBadge from '../shared/RiskBadge';

const WatchlistTable = ({ users, page, totalPages, onPageChange }) => (
    <div className="bg-navy-light rounded-xl border border-slate-700 shadow-lg">
        <div className="overflow-x-auto">
            <table className="min-w-full">
                <thead className="border-b border-slate-700">
                    <tr>
                        <th className="px-6 py-4 text-left text-base font-semibold text-slate-400 uppercase tracking-wider">Customer ID</th>
                        <th className="px-6 py-4 text-left text-base font-semibold text-slate-400 uppercase tracking-wider">Risk Score</th>
                        <th className="px-6 py-4 text-left text-base font-semibold text-slate-400 uppercase tracking-wider">Tenure</th>
                        <th className="px-6 py-4 text-left text-base font-semibold text-slate-400 uppercase tracking-wider">Contract Type</th>
                        <th className="px-6 py-4 text-left text-base font-semibold text-slate-400 uppercase tracking-wider">LTV ($)</th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-slate-700">
                    {(users || []).map(user => (
                        <tr key={user.customerid} className="hover:bg-navy transition-colors">
                            <td className="px-6 py-4">
                                <Link 
                                    to={`/customer/${user.customerid}`} 
                                    className="font-mono text-brand-light font-semibold hover:underline"
                                >
                                    {user.customerid}
                                </Link>
                            </td>
                            <td className="px-6 py-4">
                                <RiskBadge score={user.risk_score} />
                            </td>
                            <td className="px-6 py-4 text-slate-300 text-sm">
                                {user.tenure ? `${user.tenure} months` : 'N/A'}
                            </td>
                            <td className="px-6 py-4 text-slate-300 text-sm">
                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-700 text-slate-200">
                                    {user.contract || user.contract_type || 'N/A'}
                                </span>
                            </td>
                            
                            <td className="px-6 py-4 font-mono font-semibold text-white">
                                ${(user.totalcharges || user.total_charges || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
        <div className="flex items-center justify-between p-4 border-t border-slate-700">
            <button 
                onClick={() => onPageChange(p => Math.max(1, p - 1))} 
                disabled={page <= 1} 
                className="flex items-center px-3 py-1.5 text-sm font-semibold bg-navy rounded-md disabled:opacity-50 hover:bg-slate-700 transition-colors"
            >
                <ChevronLeft size={16} className="mr-1" /> Previous
            </button>
            <span className="text-sm text-slate-400">
                Page {page} of {totalPages}
            </span>
            <button 
                onClick={() => onPageChange(p => Math.min(totalPages, p + 1))} 
                disabled={page >= totalPages} 
                className="flex items-center px-3 py-1.5 text-sm font-semibold bg-navy rounded-md disabled:opacity-50 hover:bg-slate-700 transition-colors"
            >
                Next <ChevronRight size={16} className="ml-1" />
            </button>
        </div>
    </div>
);

export default WatchlistTable;