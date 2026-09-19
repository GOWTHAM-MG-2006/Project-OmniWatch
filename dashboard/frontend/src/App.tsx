import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
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

function EntryShell() {
  return (
    <div className="h-screen flex flex-col bg-bg-primary text-text-primary">
      <TopBar />
      <main className="flex-1 overflow-y-auto">
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          <Route
            path="/workspaces"
            element={
              <RequireAuth>
                <Workspaces />
              </RequireAuth>
            }
          />
          <Route
            path="/workspaces/:id/onboarding"
            element={
              <RequireAuth>
                <WorkspaceOnboarding />
              </RequireAuth>
            }
          />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </main>
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<EntryShell />} />
        <Route path="/register" element={<EntryShell />} />
        <Route path="/workspaces/*" element={<EntryShell />} />
        <Route path="/*" element={<DashboardShell />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
