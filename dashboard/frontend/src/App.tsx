import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { useState, useCallback } from 'react'
import { TopBar } from './components/TopBar'
import { Sidebar } from './components/Sidebar'
import { RightDrawer } from './components/RightDrawer'
import { Overview } from './pages/Overview'
import { IncidentExplorer } from './pages/IncidentExplorer'
import { Topology } from './pages/Topology'
import { KnowledgeBase } from './pages/KnowledgeBase'
import { GenAIReports } from './pages/GenAIReports'
import { Security } from './pages/Security'

function App() {
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerTitle, setDrawerTitle] = useState('')
  const [drawerContent, setDrawerContent] = useState<React.ReactNode>(null)

  const openDrawer = useCallback((title: string, content: React.ReactNode) => {
    setDrawerTitle(title)
    setDrawerContent(content)
    setDrawerOpen(true)
  }, [])

  const handleSearch = useCallback((query: string) => {
    openDrawer('CoPilot', (
      <div className="space-y-4">
        <div className="text-text-muted text-sm">Query: <span className="text-text-primary">{query}</span></div>
        <div className="text-text-muted text-xs">Results will appear here...</div>
      </div>
    ))
  }, [openDrawer])

  return (
    <BrowserRouter>
      <div className="h-screen flex flex-col bg-bg-primary text-text-primary">
        {/* Global Top Bar */}
        <TopBar onSearch={handleSearch} />

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
            </Routes>
          </main>
        </div>

        {/* Right Drawer (entity detail) */}
        <RightDrawer
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          title={drawerTitle}
        >
          {drawerContent}
        </RightDrawer>
      </div>
    </BrowserRouter>
  )
}

export default App
