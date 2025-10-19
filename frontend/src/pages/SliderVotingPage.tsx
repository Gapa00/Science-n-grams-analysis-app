import { useEffect, useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { getSliderData, submitSliderVote, getFrequency } from "../api/client";
import FrequencyChart from "../components/FrequencyChart";

const STORAGE_KEY = "ngram_user";

type StoredUser = { user_id: number; username: string };

type SliderItem = {
  id: number;
  text: string;
  frequency_data: { date: string; count: number }[];
};

const SliderVotingPage = () => {
  const navigate = useNavigate();
  
  const user: StoredUser | null = useMemo(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }, []);

  const [sliderData, setSliderData] = useState<SliderItem[]>([]);
  const [sliderValues, setSliderValues] = useState<Record<number, number>>({});
  const [loading, setLoading] = useState(true);
  const [submitted, setSubmitted] = useState(false);
  const [enlargedItem, setEnlargedItem] = useState<number | null>(null);

  // Load saved slider values from localStorage
  const loadSliderValues = () => {
    if (!user) return {};
    try {
      const saved = localStorage.getItem(`slider_values_${user.user_id}`);
      return saved ? JSON.parse(saved) : {};
    } catch {
      return {};
    }
  };

  // Save slider values to localStorage
  const saveSliderValues = (values: Record<number, number>) => {
    if (!user) return;
    try {
      localStorage.setItem(`slider_values_${user.user_id}`, JSON.stringify(values));
    } catch (error) {
      console.error('Failed to save slider values:', error);
    }
  };

  useEffect(() => {
    if (!user) {
      navigate("/login", { replace: true });
      return;
    }

    async function fetchSliderData() {
      try {
        const data = await getSliderData();
        
        // Fetch frequency data for each ngram
        const dataWithFrequency = await Promise.all(
          data.map(async (item: any) => {
            try {
              const frequencyResponse = await getFrequency(item.id);
              return {
                ...item,
                frequency_data: frequencyResponse.frequency_data
              };
            } catch (error) {
              console.error(`Failed to fetch frequency for ngram ${item.id}:`, error);
              return {
                ...item,
                frequency_data: [] // Empty array as fallback
              };
            }
          })
        );
        
        setSliderData(dataWithFrequency);
        
        // Load saved values or initialize with defaults
        const savedValues = loadSliderValues();
        const initialSliderValues = dataWithFrequency.reduce((acc: Record<number, number>, item: SliderItem) => {
          acc[item.id] = savedValues[item.id] ?? 0;
          return acc;
        }, {});
        
        setSliderValues(initialSliderValues);
      } catch (error) {
        console.error("Failed to fetch slider data:", error);
      } finally {
        setLoading(false);
      }
    }
    
    fetchSliderData();
  }, [user, navigate]);

  const handleSliderChange = (ngramId: number, value: number) => {
    const newValues = {
      ...sliderValues,
      [ngramId]: value,
    };
    setSliderValues(newValues);
    saveSliderValues(newValues);
  };

  const handleItemClick = (itemId: number) => {
    setEnlargedItem(enlargedItem === itemId ? null : itemId);
  };

  const handleSubmitAll = async () => {
    if (!user) return;
    
    try {
      // Submit ALL slider values (including 0s) for all items
      for (const item of sliderData) {
        const sliderValue = sliderValues[item.id] ?? 0; // Default to 0 if not set
        await submitSliderVote({
          user_id: user.user_id,
          ngram_id: item.id,
          slider_value: sliderValue,
        });
      }
      setSubmitted(true);
    } catch (error) {
      console.error("Error submitting slider votes:", error);
    }
  };

  if (submitted) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-10 text-center">
        <h1 className="text-2xl font-bold mb-4">Thank you for your feedback!</h1>
        <p className="text-gray-600 mb-6">
          You submitted ratings for {sliderData.length} frequency patterns.
        </p>
        <button
          onClick={() => navigate("/")}
          className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-semibold"
        >
          Back to Homepage
        </button>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-10">
        <div className="bg-white border rounded-lg p-6 shadow text-center">
          Loading frequency data...
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold mb-2">Rate the Burstiness</h1>
        <p className="text-gray-600 mb-4">
          Click on any graph to enlarge it. Rate each frequency pattern using the sliders, depending on how bursty characteristic you deem it shows. 
          A value of 0 means no burstiness, higher values indicate more bursty behavior.
          All slider values (including 0s) will be submitted when you click Submit.
        </p>
      </div>

      {/* Enlarged view overlay */}
      {enlargedItem !== null && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
          onClick={() => setEnlargedItem(null)}
        >
          <div 
            className="bg-white rounded-xl p-6 max-w-4xl w-full max-h-[90vh] overflow-auto"
            onClick={(e) => e.stopPropagation()}
          >
            {(() => {
              const item = sliderData.find(d => d.id === enlargedItem);
              if (!item) return null;
              
              return (
                <div>
                  <button
                    onClick={() => setEnlargedItem(null)}
                    className="absolute top-4 right-4 text-gray-500 hover:text-gray-700 text-2xl"
                  >
                    ×
                  </button>
                  
                  <div className="bg-white rounded-xl shadow p-4">
                    <FrequencyChart 
                      data={item.frequency_data} 
                      height={400}
                      hideTitle={true}
                    />
                  </div>
                  
                  <div className="mt-6">
                    <input
                      type="range"
                      min="0"
                      max="100"
                      step="0.01"
                      value={sliderValues[item.id] || 0}
                      className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider"
                      onChange={(e) => handleSliderChange(item.id, parseFloat(e.target.value))}
                    />
                  </div>
                </div>
              );
            })()}
          </div>
        </div>
      )}

      {/* Grid of items */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4 mb-8">
        {sliderData.map((item) => {
          return (
            <div 
              key={item.id} 
              className="bg-white rounded-lg shadow-md overflow-hidden cursor-pointer transition-all duration-200 hover:shadow-lg hover:scale-105"
              onClick={() => handleItemClick(item.id)}
            >
              <div className="p-3">
                <FrequencyChart 
                  data={item.frequency_data} 
                  height={120}
                  hideTitle={true}
                />
                
                <input
                  type="range"
                  min="0"
                  max="100"
                  step="0.01"
                  value={sliderValues[item.id] || 0}
                  className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider-small mt-3"
                  onChange={(e) => {
                    e.stopPropagation();
                    handleSliderChange(item.id, parseFloat(e.target.value));
                  }}
                  onClick={(e) => e.stopPropagation()}
                />
              </div>
            </div>
          );
        })}
      </div>

      {/* Action buttons */}
      <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
        <button
          onClick={handleSubmitAll}
          className="px-8 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-semibold shadow-md transition-colors"
        >
          Submit All Ratings ({sliderData.length} patterns)
        </button>
        
        <button
          onClick={() => navigate("/")}
          className="px-6 py-3 bg-gray-600 text-white rounded-lg hover:bg-gray-700 font-semibold"
        >
          Back to Homepage
        </button>
      </div>

      {/* Custom slider styles */}
      <style>{`
        .slider::-webkit-slider-thumb {
          appearance: none;
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: #2563eb;
          cursor: pointer;
          box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        
        .slider::-moz-range-thumb {
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: #2563eb;
          cursor: pointer;
          border: none;
          box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        
        .slider-small::-webkit-slider-thumb {
          appearance: none;
          height: 12px;
          width: 12px;
          border-radius: 50%;
          background: #2563eb;
          cursor: pointer;
        }
        
        .slider-small::-moz-range-thumb {
          height: 12px;
          width: 12px;
          border-radius: 50%;
          background: #2563eb;
          cursor: pointer;
          border: none;
        }
      `}</style>
    </div>
  );
};

export default SliderVotingPage;