// src/components/Header.tsx
import { Link, useLocation } from 'react-router-dom';

function Header() {
  const location = useLocation();
  const isActive = (path: string) => location.pathname === path;
  const isActivePrefix = (prefix: string) => location.pathname.startsWith(prefix);

  const linkClass = (active: boolean) => {
    const base = "px-3 py-2 rounded-md text-sm font-medium transition-colors";
    return active
      ? `${base} bg-blue-600 text-white`
      : `${base} text-blue-600 hover:bg-blue-50 hover:text-blue-800`;
  };

  return (
    <header className="bg-white shadow-sm border-b">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center">
            <Link to="/" className="text-xl font-bold text-gray-900 hover:text-gray-700">
              Science N-grams
            </Link>
          </div>

          <nav className="flex items-center space-x-1">
            <Link to="/frequency" className={linkClass(isActive('/frequency'))}>
              📊 Frequency Dashboard
            </Link>

            {/* ✅ point to the existing route */}
            <Link
              to="/bursts/macd"
              className={linkClass(isActivePrefix('/bursts'))}
            >
              🔥 Bursts
            </Link>
          </nav>

          <div className="flex items-center">
            <Link to="/login" className={linkClass(isActive('/login'))}>
              Login
            </Link>
          </div>
        </div>
      </div>
    </header>
  );
}

export default Header;
