import React from 'react';

const PageHeader = ({ title, children }) => (
    <div className="flex flex-col md:flex-row justify-between md:items-center mb-8">
        <h1 className="text-3xl font-bold text-white">{title}</h1>
        <div className="mt-4 md:mt-0">{children}</div>
    </div>
);

export default PageHeader;
