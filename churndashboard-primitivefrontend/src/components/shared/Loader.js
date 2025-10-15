import React from 'react';
import { Loader2 } from 'lucide-react';

const Loader = ({ text = "Loading..." }) => (
    <div className="flex flex-col items-center justify-center h-full text-slate-400">
        <Loader2 className="animate-spin mb-2" size={32} />
        <p>{text}</p>
    </div>
);

export default Loader;
