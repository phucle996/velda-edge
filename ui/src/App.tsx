import React from 'react';
import { Activity, Server, Shuffle, ShieldCheck } from 'lucide-react';

export default function App() {
  return (
    <div className="flex h-screen bg-slate-950 text-slate-100">
      {/* Sidebar */}
      <aside className="w-64 border-r border-slate-800 p-6 flex flex-col justify-between">
        <div>
          <div className="flex items-center gap-3 mb-8">
            <div className="h-8 w-8 rounded-lg bg-indigo-600 flex items-center justify-center font-bold text-white shadow-lg shadow-indigo-500/30">
              V
            </div>
            <span className="font-semibold text-lg tracking-tight">Velda Edge</span>
          </div>

          <nav className="space-y-1">
            <a href="#overview" className="flex items-center gap-3 px-3 py-2 rounded-md bg-slate-900 text-indigo-400 font-medium text-sm">
              <Activity className="h-4 w-4" /> Overview
            </a>
            <a href="#routes" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-900/60 text-slate-400 hover:text-slate-200 font-medium text-sm transition-colors">
              <Shuffle className="h-4 w-4" /> Routes
            </a>
            <a href="#upstreams" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-900/60 text-slate-400 hover:text-slate-200 font-medium text-sm transition-colors">
              <Server className="h-4 w-4" /> Upstreams
            </a>
            <a href="#certificates" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-900/60 text-slate-400 hover:text-slate-200 font-medium text-sm transition-colors">
              <ShieldCheck className="h-4 w-4" /> Certificates
            </a>
          </nav>
        </div>

        <div className="text-xs text-slate-500">
          Velda Edge Control Plane v0.1.0
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto">
        <header className="flex justify-between items-center mb-8">
          <div>
            <h1 className="text-2xl font-bold text-slate-100">Cluster Overview</h1>
            <p className="text-sm text-slate-400">High-performance edge routing and data plane telemetry.</p>
          </div>
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
              Data Plane Active
            </span>
          </div>
        </header>

        {/* Metric Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <div className="p-6 rounded-xl bg-slate-900/50 border border-slate-800">
            <span className="text-xs font-medium text-slate-400">Total Routes</span>
            <div className="text-2xl font-bold mt-2 text-slate-100">12</div>
          </div>
          <div className="p-6 rounded-xl bg-slate-900/50 border border-slate-800">
            <span className="text-xs font-medium text-slate-400">Upstream Clusters</span>
            <div className="text-2xl font-bold mt-2 text-slate-100">4</div>
          </div>
          <div className="p-6 rounded-xl bg-slate-900/50 border border-slate-800">
            <span className="text-xs font-medium text-slate-400">P99 Latency</span>
            <div className="text-2xl font-bold mt-2 text-indigo-400">0.42 ms</div>
          </div>
          <div className="p-6 rounded-xl bg-slate-900/50 border border-slate-800">
            <span className="text-xs font-medium text-slate-400">Throughput</span>
            <div className="text-2xl font-bold mt-2 text-emerald-400">14.2k req/s</div>
          </div>
        </div>
      </main>
    </div>
  );
}
