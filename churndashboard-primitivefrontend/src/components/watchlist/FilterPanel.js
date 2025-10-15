import React from 'react';
import { SlidersHorizontal, Shield, FileText } from 'lucide-react';

const FilterPanel = ({ filters, onFilterChange }) => {
    const handleCheckboxChange = (e) => {
        const { name, value, checked } = e.target;
        const currentValues = filters[name] || [];
        const newValues = checked
            ? [...currentValues, value]
            : currentValues.filter(item => item !== value);
        onFilterChange(name, newValues);
    };

    const riskLevels = [
        { value: 'Critical', color: 'bg-red-600', border: 'border-red-500', glow: 'shadow-red-500/20' },
        { value: 'High', color: 'bg-red-500', border: 'border-red-400', glow: 'shadow-red-400/20' },
        { value: 'Medium', color: 'bg-amber-500', border: 'border-amber-400', glow: 'shadow-amber-400/20' },
        { value: 'Low', color: 'bg-green-500', border: 'border-green-400', glow: 'shadow-green-400/20' }
    ];

    const contractTypes = [
        { value: 'Month-to-month', icon: '📅' },
        { value: 'One year', icon: '📆' },
        { value: 'Two year', icon: '🗓️' }
    ];

    return (
        <aside className="w-full lg:w-72 bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm p-6 rounded-2xl border border-slate-700/50 shadow-2xl flex-shrink-0">
            {/* Header */}
            <div className="mb-6 pb-4 border-b border-slate-700/50">
                <h3 className="text-xl font-bold text-white flex items-center">
                    <div className="p-2 bg-cyan-500/10 rounded-lg mr-3 border border-cyan-500/20">
                        <SlidersHorizontal size={20} className="text-cyan-400" />
                    </div>
                    Filters
                </h3>
            </div>

            <div className="space-y-6">
                {/* Risk Level Section */}
                <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-3">
                        <Shield size={16} className="text-slate-400" />
                        <label className="text-base font-bold text-slate-200 uppercase tracking-wide">
                            Risk Level
                        </label>
                    </div>
                    <div className="space-y-2.5">
                        {riskLevels.map(({ value, color, border, glow }) => {
                            const isChecked = (filters.risk || []).includes(value);
                            return (
                                <label 
                                    key={value} 
                                    className={`
                                        flex items-center justify-between p-3 rounded-xl cursor-pointer
                                        transition-all duration-200 group
                                        ${isChecked 
                                            ? `bg-slate-700/50 border-2 ${border} shadow-lg ${glow}` 
                                            : 'bg-slate-800/30 border border-slate-700/50 hover:bg-slate-700/30 hover:border-slate-600'
                                        }
                                    `}
                                >
                                    <div className="flex items-center gap-3">
                                        <div className={`w-3 h-3 rounded-full ${color} ${isChecked ? 'animate-pulse' : ''}`} />
                                        <span className={`font-medium ${isChecked ? 'text-white' : 'text-slate-300'}`}>
                                            {value}
                                        </span>
                                    </div>
                                    <input 
                                        type="checkbox" 
                                        name="risk"
                                        value={value}
                                        checked={isChecked}
                                        onChange={handleCheckboxChange}
                                        className="w-5 h-5 rounded-md bg-slate-700 border-slate-600 text-cyan-500 focus:ring-2 focus:ring-cyan-500 focus:ring-offset-0 cursor-pointer" 
                                    />
                                </label>
                            );
                        })}
                    </div>
                </div>

                {/* Contract Type Section */}
                <div className="space-y-3 pt-2">
                    <div className="flex items-center gap-2 mb-3">
                        <FileText size={16} className="text-slate-400" />
                        <label className="text-base font-bold text-slate-200 uppercase tracking-wide">
                            Contract Type
                        </label>
                    </div>
                    <div className="space-y-2.5">
                        {contractTypes.map(({ value, icon }) => {
                            const isChecked = (filters.contract || []).includes(value);
                            return (
                                <label 
                                    key={value} 
                                    className={`
                                        flex items-center justify-between p-3 rounded-xl cursor-pointer
                                        transition-all duration-200 group
                                        ${isChecked 
                                            ? 'bg-slate-700/50 border-2 border-cyan-500 shadow-lg shadow-cyan-500/20' 
                                            : 'bg-slate-800/30 border border-slate-700/50 hover:bg-slate-700/30 hover:border-slate-600'
                                        }
                                    `}
                                >
                                    <div className="flex items-center gap-3">
                                        <span className="text-lg">{icon}</span>
                                        <span className={`font-medium ${isChecked ? 'text-white' : 'text-slate-300'}`}>
                                            {value}
                                        </span>
                                    </div>
                                    <input 
                                        type="checkbox"
                                        name="contract"
                                        value={value}
                                        checked={isChecked}
                                        onChange={handleCheckboxChange}
                                        className="w-5 h-5 rounded-md bg-slate-700 border-slate-600 text-cyan-500 focus:ring-2 focus:ring-cyan-500 focus:ring-offset-0 cursor-pointer" 
                                    />
                                </label>
                            );
                        })}
                    </div>
                </div>

                {/* Active Filters Count */}
                {(filters.risk?.length > 0 || filters.contract?.length > 0) && (
                    <div className="mt-6 pt-4 border-t border-slate-700/50">
                        <div className="bg-cyan-500/10 border border-cyan-500/20 rounded-lg p-3">
                            <div className="flex items-center justify-between">
                                <span className="text-base font-semibold text-cyan-400 uppercase">Active Filters</span>
                                <span className="bg-cyan-500 text-white text-sm font-bold px-2 py-1 rounded-full">
                                    {(filters.risk?.length || 0) + (filters.contract?.length || 0)}
                                </span>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </aside>
    );
};

export default FilterPanel;
