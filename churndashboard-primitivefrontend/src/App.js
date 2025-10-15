// import React, { useState, useEffect } from 'react';
// import AtRiskCustomers from './components/AtRiskCustomers';
// import EventFeed from './components/EventFeed';
// import Metrics from './components/Metrics';

// function App() {
//   const [events, setEvents] = useState([]);
//   const [atRiskCustomers, setAtRiskCustomers] = useState([]);
//   const [metrics, setMetrics] = useState({ logins: 0, upgrades: 0, tickets: 0 });

//   useEffect(() => {
//     const ws = new WebSocket('ws://localhost:8000/ws/churn-updates');

//     ws.onopen = () => console.log('WebSocket connection established');
//     ws.onclose = () => console.log('WebSocket connection closed');
//     ws.onerror = (error) => console.error('WebSocket error:', error);

//     ws.onmessage = (message) => {
//         try {
//             // Data from FastAPI is a string, replace single quotes for valid JSON
//             const data = JSON.parse(message.data.replace(/'/g, '"'));

//             // The backend only sends high-risk alerts. We use this single message
//             // to update both the "at-risk" list and the "event feed".
//             const newEvent = { ...data, id: Date.now() };

//             // Update the live event feed
//             setEvents(prevEvents => [newEvent, ...prevEvents].slice(0, 50));

//             // Update the list of at-risk customers
//             setAtRiskCustomers(prevCustomers => {
//                 // Avoid duplicates by checking user_id
//                 const existingCustomerIndex = prevCustomers.findIndex(c => c.user_id === newEvent.user_id);
//                 if (existingCustomerIndex !== -1) {
//                     // Update existing customer's risk
//                     const updatedCustomers = [...prevCustomers];
//                     updatedCustomers[existingCustomerIndex] = newEvent;
//                     return updatedCustomers.sort((a, b) => b.churn_probability - a.churn_probability);
//                 } else {
//                     // Add new customer and sort by risk
//                     return [...prevCustomers, newEvent].sort((a, b) => b.churn_probability - a.churn_probability);
//                 }
//             });
            
//             // Update the metrics based on the event type
//             if (data.event_type === 'login') {
//                 setMetrics(prev => ({...prev, logins: prev.logins + 1}));
//             } else if (data.event_type === 'upgrade_plan') {
//                 setMetrics(prev => ({...prev, upgrades: prev.upgrades + 1}));
//             } else if (data.event_type === 'create_support_ticket') {
//                 setMetrics(prev => ({...prev, tickets: prev.tickets + 1}));
//             }

//         } catch (error) {
//             console.error('Failed to parse incoming message:', error);
//         }
//     };

//     // Clean up the connection when the component unmounts
//     return () => {
//       ws.close();
//     };
//   }, []);

//   return (
//     <div className="min-h-screen bg-gray-900 text-gray-200 p-4 sm:p-6 lg:p-8">
//       <div className="max-w-7xl mx-auto">
//         <header className="mb-8">
//           <h1 className="text-4xl font-bold text-white text-center">
//             Real-Time Customer Churn Dashboard
//           </h1>
//           <p className="text-center text-gray-400 mt-2">
//             Live predictions powered by Kafka, FastAPI, and React
//           </p>
//         </header>

//         <main className="flex flex-col gap-8">
//           <section>
//             <Metrics metrics={metrics} />
//           </section>

//           <section className="grid grid-cols-1 lg:grid-cols-3 gap-8 h-[60vh]">
//             <div className="lg:col-span-2">
//               <AtRiskCustomers customers={atRiskCustomers} />
//             </div>
//             <div className="lg:col-span-1">
//               <EventFeed events={events} />
//             </div>
//           </section>
//         </main>
//       </div>
//     </div>
//   );
// }

// export default App;

import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Sidebar from './components/layout/SideBar';
import Dashboard from './pages/Dashboard';
import Watchlist from './pages/Watchlist';
import CustomerProfile from './pages/CustomerProfile';
import Analytics from './pages/Analytics';
import LiveFeed from './pages/LiveFeed';

import './App.css';

function App() {
  return (
    <div className="flex h-screen bg-navy-dark text-slate-300 font-sans">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <main className="flex-1 overflow-x-hidden overflow-y-auto bg-navy-dark p-6 md:p-8">
          <Routes>
            <Route path="/" element={<Navigate replace to="/dashboard" />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/watchlist" element={<Watchlist />} />
            <Route path="/customer/:customerId" element={<CustomerProfile />} />
            <Route path="/analytics" element={<Analytics />} />
            <Route path="/live-feed" element={<LiveFeed />} />
            
          </Routes>
        </main>
      </div>
    </div>
  );
}

export default App;
