import { useEffect, useMemo, useState, useRef, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import FrequencyChart from "../components/FrequencyChart";
import { getNextBinaryPair, submitBinaryVote, getFrequency, NextPair } from "../api/client";

type XY = { date: string; count: number };
const safeDiv = (a: number, b: number) => (b === 0 ? 0 : a / b);

/** Normalize both series by the same max across the pair → [0,1] */
function normalizePair(left: XY[], right: XY[]): [XY[], XY[]] {
  const maxL = Math.max(0, ...left.map((p) => p.count));
  const maxR = Math.max(0, ...right.map((p) => p.count));
  const maxBoth = Math.max(maxL, maxR, 0) || 1;
  return [
    left.map((p) => ({ ...p, count: safeDiv(p.count, maxBoth) })),
    right.map((p) => ({ ...p, count: safeDiv(p.count, maxBoth) })),
  ];
}

const STORAGE_KEY = "ngram_user";

type StoredUser = { user_id: number; username: string };
type Choice = "left" | "right";

type PairEntry = {
  index: number;
  total: number;
  left_id: number;
  right_id: number;
  leftData: XY[] | null;
  rightData: XY[] | null;
  leftNorm?: XY[];
  rightNorm?: XY[];
  choice?: Choice;
};

const BinaryVotingPage = () => {
  const navigate = useNavigate();
  const user: StoredUser | null = useMemo(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }, []);

  const [history, setHistory] = useState<PairEntry[]>([]);
  const [pos, setPos] = useState(-1);
  const [done, setDone] = useState(false);
  const [loading, setLoading] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const rtStartRef = useRef<number>(0);

  // Get the localStorage key for this user's history
  const getHistoryKey = useCallback(() => {
    return user ? `history_${user.user_id}` : null;
  }, [user]);

  // Save history to localStorage with current state
  const saveHistoryToLocalStorage = useCallback((currentHistory: PairEntry[]) => {
    const key = getHistoryKey();
    if (key) {
      try {
        localStorage.setItem(key, JSON.stringify(currentHistory));
      } catch (error) {
        console.error('Failed to save history to localStorage:', error);
      }
    }
  }, [getHistoryKey]);

  // Load history from localStorage
  const loadHistoryFromLocalStorage = useCallback((): PairEntry[] => {
    const key = getHistoryKey();
    if (!key) return [];
    
    try {
      const storedHistory = localStorage.getItem(key);
      return storedHistory ? JSON.parse(storedHistory) : [];
    } catch (error) {
      console.error('Failed to load history from localStorage:', error);
      return [];
    }
  }, [getHistoryKey]);

  // Save position to localStorage
  const savePositionToLocalStorage = useCallback((position: number) => {
    const key = user ? `position_${user.user_id}` : null;
    if (key) {
      try {
        localStorage.setItem(key, position.toString());
      } catch (error) {
        console.error('Failed to save position to localStorage:', error);
      }
    }
  }, [user]);

  // Load position from localStorage
  const loadPositionFromLocalStorage = useCallback((): number => {
    const key = user ? `position_${user.user_id}` : null;
    if (!key) return -1;
    
    try {
      const storedPosition = localStorage.getItem(key);
      return storedPosition ? parseInt(storedPosition, 10) : -1;
    } catch (error) {
      console.error('Failed to load position from localStorage:', error);
      return -1;
    }
  }, [user]);

  // Redirect if no user
  useEffect(() => {
    if (!user) navigate("/login", { replace: true });
  }, [user, navigate]);

  // Load saved data when user is available
  useEffect(() => {
    if (!user || historyLoaded) return;

    const savedHistory = loadHistoryFromLocalStorage();
    const savedPosition = loadPositionFromLocalStorage();
    
    if (savedHistory.length > 0) {
      setHistory(savedHistory);
      setPos(Math.min(savedPosition, savedHistory.length - 1));
      rtStartRef.current = Date.now();
    } else {
      // No saved history, start fresh
      setPos(-1);
    }
    
    setHistoryLoaded(true);
  }, [user, historyLoaded, loadHistoryFromLocalStorage, loadPositionFromLocalStorage]);

  // Fetch next pair when needed
  useEffect(() => {
    if (!user || !historyLoaded) return;
    
    // If we don't have any history or we're at the end and need a new pair
    if (history.length === 0 || (pos === history.length - 1 && history[pos]?.choice)) {
      fetchNext();
    }
  }, [user, historyLoaded, history.length, pos]);

  async function fetchNext() {
    if (!user) return;
    setLoading(true);
    try {
      const nxt: NextPair = await getNextBinaryPair(user.user_id);
      if (nxt.done) {
        setDone(true);
        return;
      }

      const [left, right] = await Promise.all([
        getFrequency(nxt.left_id),
        getFrequency(nxt.right_id),
      ]);

      const [leftNorm, rightNorm] = normalizePair(left.frequency_data, right.frequency_data);

      const entry: PairEntry = {
        index: nxt.index,
        total: nxt.total,
        left_id: nxt.left_id,
        right_id: nxt.right_id,
        leftData: left.frequency_data,
        rightData: right.frequency_data,
        leftNorm,
        rightNorm,
      };

      setHistory((prevHistory) => {
        const newHistory = [...prevHistory.slice(0, pos + 1), entry];
        saveHistoryToLocalStorage(newHistory);
        return newHistory;
      });
      
      setPos((prevPos) => {
        const newPos = prevPos + 1;
        savePositionToLocalStorage(newPos);
        return newPos;
      });
      
      rtStartRef.current = Date.now();
    } catch (error) {
      console.error('Failed to fetch next pair:', error);
    } finally {
      setLoading(false);
    }
  }

  const current = pos >= 0 ? history[pos] : undefined;

  function selectChoice(choice: Choice) {
    if (!current) return;
    
    setHistory((prevHistory) => {
      const newHistory = [...prevHistory];
      const prev = newHistory[pos]?.choice;
      newHistory[pos] = { 
        ...newHistory[pos], 
        choice: prev === choice ? undefined : choice 
      };
      saveHistoryToLocalStorage(newHistory);
      return newHistory;
    });
  }

  function onPrev() {
    if (pos > 0) {
      const newPos = pos - 1;
      setPos(newPos);
      savePositionToLocalStorage(newPos);
      rtStartRef.current = Date.now();
    }
  }

  async function onNext() {
    const cur = current;
    if (!user || !cur || !cur.choice) return;

    const rt_ms = Date.now() - rtStartRef.current;

    try {
      await submitBinaryVote({
        user_id: user.user_id,
        pair_index: cur.index,
        left_id: cur.left_id,
        right_id: cur.right_id,
        choice: cur.choice,
        rt_ms,
      });

      if (pos === history.length - 1) {
        await fetchNext();
      } else {
        const newPos = pos + 1;
        setPos(newPos);
        savePositionToLocalStorage(newPos);
        rtStartRef.current = Date.now();
      }
    } catch (error) {
      console.error('Failed to submit vote:', error);
    }
  }

  if (!historyLoaded) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-10">
        <div className="bg-white border rounded-lg p-6 shadow">Loading saved data...</div>
      </div>
    );
  }

  if (done) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-10">
        <h1 className="text-2xl font-bold mb-2">All done — thank you!</h1>
        <p className="text-gray-600 mb-6">
          You completed {history.length} / {history.length} pairs.
        </p>
        <div className="flex gap-3">
          <button
            onClick={() => navigate("/vote/slider")}
            className="inline-flex items-center px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-semibold"
          >
            Proceed to Slider Voting
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      <div className="mb-4">
        <h1 className="text-xl font-bold">Which term looks hotter?</h1>
        <p className="text-gray-600">
          Select the chart that shows a more "bursty / hot" trend.
          {current && <> Pair {current.index} of {current.total}.</>}
        </p>
      </div>

      {loading && (
        <div className="bg-white border rounded-lg p-6 shadow">Loading…</div>
      )}

      {!loading && current && current.leftNorm && current.rightNorm && (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            {/* LEFT */}
            <button
              type="button"
              onClick={() => selectChoice("left")}
              className={`text-left rounded-xl focus:outline-none ring-offset-2 ${current.choice === "left" ? "ring-2 ring-blue-500" : ""}`}
            >
              <div className={`${current.choice === "right" ? "opacity-70 blur-[1px] grayscale" : ""} transition`}>
                <FrequencyChart
                  data={current.leftNorm}
                  hideTitle
                  showTooltip={false}
                  showAxes={false}
                  height={380}
                />
              </div>
            </button>

            {/* RIGHT */}
            <button
              type="button"
              onClick={() => selectChoice("right")}
              className={`text-left rounded-xl focus:outline-none ring-offset-2 ${current.choice === "right" ? "ring-2 ring-blue-500" : ""}`}
            >
              <div className={`${current.choice === "left" ? "opacity-70 blur-[1px] grayscale" : ""} transition`}>
                <FrequencyChart
                  data={current.rightNorm}
                  hideTitle
                  showTooltip={false}
                  showAxes={false}
                  height={380}
                />
              </div>
            </button>
          </div>

          <div className="mt-6 flex items-center justify-center gap-3">
            <button
              onClick={onPrev}
              className="px-4 py-2 rounded-lg border border-gray-300 hover:bg-gray-50 font-semibold disabled:opacity-50"
              disabled={pos <= 0}
            >
              ⬅️ Previous
            </button>
            <button
              onClick={onNext}
              className="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-semibold disabled:opacity-50"
              disabled={!current.choice}
            >
              Next ➡️
            </button>
          </div>
        </>
      )}
    </div>
  );
};

export default BinaryVotingPage;