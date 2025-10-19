import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { useEffect } from "react";
import BurstPage from "./pages/BurstPage";
import BinaryVotingPage from "./pages/BinaryVotingPage";
import SliderVotingPage from "./pages/SliderVotingPage";
import LoginPage from "./pages/LoginPage";
import Header from "./components/Header";
import FrequencyDashboardPage from "./pages/FrequencyDashboardPage";
import HomePage from "./pages/HomePage";

function AppRoutes() {
  const location = useLocation();
  const hideHeader = location.pathname.startsWith("/login") || location.pathname.startsWith("/vote");

  // Update document title based on current route
  useEffect(() => {
    const getPageTitle = () => {
      const path = location.pathname;
      if (path === "/") return "Science N-grams - Academic Burstiness Analysis";
      if (path === "/frequency") return "Frequency Dashboard - Science N-grams";
      if (path.startsWith("/bursts")) return "Burst Detection - Science N-grams";
      if (path === "/vote") return "Binary Voting - Science N-grams";
      if (path === "/vote/slider") return "Slider Voting - Science N-grams";
      if (path === "/login") return "Login - Science N-grams";
      return "Science N-grams - Academic Burstiness Analysis";
    };

    document.title = getPageTitle();
  }, [location.pathname]);

  return (
    <div className="min-h-screen bg-gray-50">
      {!hideHeader && <Header />}
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/frequency" element={<FrequencyDashboardPage />} />

        {/* ✅ main burst route */}
        <Route path="/bursts/:method" element={<BurstPage />} />
        {/* ✅ convenience redirect */}
        <Route path="/bursts" element={<Navigate to="/bursts/macd" replace />} />

        {/* ✅ Binary Voting */}
        <Route path="/vote" element={<BinaryVotingPage />} />
        
        {/* ✅ Slider Voting after Binary Voting */}
        <Route path="/vote/slider" element={<SliderVotingPage />} />

        <Route path="/login" element={<LoginPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  );
}

export default function App() {
  // Set default title on app load
  useEffect(() => {
    document.title = "Science N-grams - Academic Burstiness Analysis";
    
    // Optional: Set favicon programmatically (chart emoji)
    const favicon = document.querySelector("link[rel='icon']") as HTMLLinkElement;
    if (favicon) {
      // You can replace this with an actual icon file later
      favicon.href = "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📊</text></svg>";
    }
  }, []);

  return (
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  );
}