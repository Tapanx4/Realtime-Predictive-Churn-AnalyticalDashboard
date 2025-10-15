import React from 'react';
import { NavLink } from 'react-router-dom';
// Note: lucide-react icons are no longer needed for the navigation
import { useWebSocket } from '../../hooks/useWebSocket';

const Sidebar = () => {
    const { wsStatus } = useWebSocket();

    // --- NEW: Data structure for navigation links ---
    const navItems = [
        { name: 'Dashboard', path: 'dashboard', icon: '📊' },
        { name: 'Watchlist', path: 'watchlist', icon: '🚨' },
        { name: 'Customer Profile', path: 'profile', icon: '👤', customerId: 'Select a User from Watchlist' }, // Using a placeholder ID
        { name: 'Analytics', path: 'analytics', icon: '📈' },
        { name: 'Live Feed', path: 'live-feed', icon: '⚡' },
    ];

    const getStatusIndicator = () => {
        switch (wsStatus) {
            case 'Connected': return <div className="w-2.5 h-2.5 rounded-full bg-green-400"></div>;
            case 'Disconnected': return <div className="w-2.5 h-2.5 rounded-full bg-yellow-400"></div>;
            default: return <div className="w-2.5 h-2.5 rounded-full bg-red-400"></div>;
        }
    };

    return (
        <aside className="w-60 bg-navy text-slate-400 flex flex-col border-r border-navy-light flex-shrink-0">
            <div className="p-6 text-center border-b border-navy-light">
                <h1 className="text-2xl font-bold text-white">ChurnPulse</h1>
            </div>
            <nav className="flex-grow p-4 space-y-1.5">
                {/* --- MODIFIED: Links are now generated dynamically --- */}
                {navItems.map((item) => {
                    // Handle dynamic path for customer profile
                    const toPath = item.path === 'profile' 
                        ? `/customer/${item.customerId}` 
                        : `/${item.path}`;

                    return (
                        <NavLink
                            key={item.name}
                            to={toPath}
                            className={({ isActive }) =>
                                `flex items-center space-x-3 px-3 py-2.5 rounded-lg transition-colors text-basic font-semibold ${
                                    isActive
                                        ? 'bg-brand text-white shadow-lg'
                                        : 'hover:bg-navy-light hover:text-white'
                                }`
                            }
                        >
                            {/* Using emoji as icon */}
                            <span className="text-xl w-5 text-center">{item.icon}</span>
                            <span>{item.name}</span>
                        </NavLink>
                    );
                })}
            </nav>
            <div className="p-4 border-t border-navy-light">
                <div className="flex items-center justify-between text-xs font-semibold px-2 py-1.5 bg-navy-light rounded-md">
                    <span className="text-slate-400">WS Status:</span>
                    <div className="flex items-center space-x-1.5 text-white">
                        {getStatusIndicator()}
                        <span>{wsStatus}</span>
                    </div>
                </div>
            </div>
        </aside>
    );
};

export default Sidebar;
