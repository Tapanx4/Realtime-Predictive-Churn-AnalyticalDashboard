import React from 'react';
import RiskBadge from '../shared/RiskBadge';

const DetailItem = ({ label, value }) => (
    <div className="flex justify-between border-b border-navy-light py-2">
        <p className="text-sm text-slate-400">{label}</p>
        <p className="text-sm font-semibold text-white">{value}</p>
    </div>
);

const CustomerDetailsCard = ({ details, score }) => {
    if (!details) return null;

    return (
        <div className="bg-navy-light p-6 rounded-xl shadow-lg">
            <div className="flex justify-between items-start mb-4">
                <div>
                    <h3 className="text-xl font-bold text-white">Customer Details</h3>
                    <p className="font-mono text-brand-light">{details.customerid}</p>
                </div>
                <RiskBadge score={score} showText={true} />
            </div>
            <div className="space-y-1">
                <DetailItem label="Gender" value={details.gender} />
                <DetailItem label="Senior Citizen" value={details.seniorcitizen ? 'Yes' : 'No'} />
                <DetailItem label="Contract" value={details.contract} />
                <DetailItem label="Tenure" value={`${details.tenure} months`} />
                <DetailItem label="Monthly Charges" value={`$${details.monthlycharges}`} />
                <DetailItem label="Total Charges" value={`$${details.totalcharges}`} />
                <DetailItem label="Internet Service" value={details.internetservice} />
            </div>
        </div>
    );
};

export default CustomerDetailsCard;
