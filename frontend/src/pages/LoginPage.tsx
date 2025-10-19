import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { login, LoginResponse } from "../api/client";

const STORAGE_KEY = "ngram_user";

function LoginPage() {
  const [username, setUsername] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const navigate = useNavigate();

  const onSubmit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    if (!username.trim()) return;
    setBusy(true);
    setErr(null);
    try {
      const res: LoginResponse = await login(username.trim());  // Call login API
      localStorage.setItem(STORAGE_KEY, JSON.stringify(res));   // Store response in localStorage
      navigate("/vote");  // Navigate to voting page after successful login
    } catch (error: any) {
      setErr(error?.response?.data?.detail || "Login failed");  // Handle login error
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="max-w-md mx-auto px-4 py-10">
      <h1 className="text-2xl font-bold mb-4">Enter your username</h1>
      <form onSubmit={onSubmit} className="space-y-3">
        <input
          type="text"
          placeholder="e.g., Joseph Stefani"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          className="w-full border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
          autoFocus
        />
        {err && <p className="text-red-600 text-sm">{err}</p>}
        <button
          type="submit"
          disabled={busy || !username.trim()}
          className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white font-semibold rounded-lg px-4 py-2"
        >
          {busy ? "Starting…" : "Start"}
        </button>
      </form>
    </div>
  );
}

export default LoginPage;
