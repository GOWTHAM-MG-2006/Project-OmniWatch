import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { TopBar } from './components/TopBar'
import { Sidebar } from './components/Sidebar'
import { LandingRoute, RequireAuth, RequireWorkspace } from './components/RequireAuth'
import { Overview } from './pages/Overview'
import { IncidentExplorer } from './pages/IncidentExplorer'
import { Topology } from './pages/Topology'
import { KnowledgeBase } from './pages/KnowledgeBase'
import { GenAIReports } from './pages/GenAIReports'
import { Security } from './pages/Security'
import { MinioBrowser } from './pages/MinioBrowser'
import { DataExplorer } from './pages/DataExplorer'
import { GraphExplorer } from './pages/GraphExplorer'
import ModelSettings from './pages/ModelSettings'
import { Login } from './pages/Login'
import { Register } from './pages/Register'
import { Workspaces } from './pages/Workspaces'
import { WorkspaceOnboarding } from './pages/WorkspaceOnboarding'

function DashboardShell() {
  return (
    <div className="h-screen flex flex-col bg-bg-primary text-text-primary">
      {/* Global Top Bar */}
      <TopBar />

      <div className="flex flex-1 overflow-hidden">
        {/* Left Sidebar */}
        <Sidebar />

        {/* Main Content — 24-col grid */}
        <main className="flex-1 overflow-y-auto">
          <Routes>
            <Route
              path="/"
              element={
                <LandingRoute>
                  <Overview />
                </LandingRoute>
              }
            />
            <Route path="/incidents" element={<RequireWorkspace><IncidentExplorer /></RequireWorkspace>} />
            <Route path="/topology" element={<RequireWorkspace><Topology /></RequireWorkspace>} />
            <Route path="/knowledge" element={<RequireWorkspace><KnowledgeBase /></RequireWorkspace>} />
            <Route path="/reports" element={<RequireWorkspace><GenAIReports /></RequireWorkspace>} />
            <Route path="/security" element={<RequireWorkspace><Security /></RequireWorkspace>} />
            <Route path="/storage" element={<RequireWorkspace><MinioBrowser /></RequireWorkspace>} />
            <Route path="/data" element={<RequireWorkspace><DataExplorer /></RequireWorkspace>} />
            <Route path="/graph" element={<RequireWorkspace><GraphExplorer /></RequireWorkspace>} />
            <Route path="/settings/model" element={<RequireWorkspace><ModelSettings /></RequireWorkspace>} />
          </Routes>
        </main>
      </div>
    </div>
  )
}

function EntryShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-screen flex flex-col bg-bg-primary text-text-primary">
      <TopBar />
      <main className="flex-1 overflow-y-auto">
        {children}
      </main>
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<EntryShell><Login /></EntryShell>} />
        <Route path="/register" element={<EntryShell><Register /></EntryShell>} />
        <Route
          path="/workspaces"
          element={
            <EntryShell>
              <RequireAuth>
                <Workspaces />
              </RequireAuth>
            </EntryShell>
          }
        />
        <Route
          path="/workspaces/:id/onboarding"
          element={
            <EntryShell>
              <RequireAuth>
                <WorkspaceOnboarding />
              </RequireAuth>
            </EntryShell>
          }
        />
        <Route path="/*" element={<DashboardShell />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
