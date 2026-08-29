import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { TopBar } from './components/TopBar'
import { Sidebar } from './components/Sidebar'
import { Overview } from './pages/Overview'
import { IncidentExplorer } from './pages/IncidentExplorer'
import { Topology } from './pages/Topology'
import { KnowledgeBase } from './pages/KnowledgeBase'
import { GenAIReports } from './pages/GenAIReports'
import { Security } from './pages/Security'
import { MinioBrowser } from './pages/MinioBrowser'

function App() {
  return (
    <BrowserRouter>
      <div className="h-screen flex flex-col bg-bg-primary text-text-primary">
        {/* Global Top Bar */}
        <TopBar />

        <div className="flex flex-1 overflow-hidden">
          {/* Left Sidebar */}
          <Sidebar />

          {/* Main Content — 24-col grid */}
          <main className="flex-1 overflow-y-auto">
            <Routes>
              <Route path="/" element={<Overview />} />
              <Route path="/incidents" element={<IncidentExplorer />} />
              <Route path="/topology" element={<Topology />} />
              <Route path="/knowledge" element={<KnowledgeBase />} />
              <Route path="/reports" element={<GenAIReports />} />
              <Route path="/security" element={<Security />} />
              <Route path="/storage" element={<MinioBrowser />} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  )
}

export default App
