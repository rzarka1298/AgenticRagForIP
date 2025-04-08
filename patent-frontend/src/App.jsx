import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

function App() {
  const [query, setQuery] = useState("");
  const [response, setResponse] = useState("");
  const [loading, setLoading] = useState(false);
  const [queryHistory, setQueryHistory] = useState([]);
  const [selectedHistoryItem, setSelectedHistoryItem] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;
    
    setLoading(true);
    setResponse("");
    setSelectedHistoryItem(null);

    try {
      const res = await fetch("http://localhost:8000/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ user_query: query }),
      });
      const data = await res.json();
      const cleanResponse = data.response
        .replaceAll("<inference>", "")
        .replace(/\n+/g, " ")
        .trim();
      setResponse(cleanResponse);
      
      setQueryHistory(prev => [...prev, {
        id: Date.now(),
        query: query,
        response: cleanResponse,
        timestamp: new Date().toLocaleString()
      }]);
    } catch (err) {
      setResponse("Error contacting backend.");
    } finally {
      setLoading(false);
      setQuery("");
    }
  };

  const handleHistoryClick = (item) => {
    setSelectedHistoryItem(item);
    setQuery(item.query);
    setResponse(item.response);
  };

  return (
    <div className="flex h-screen bg-white">
      {/* Sidebar */}
      <motion.div 
        initial={{ x: -300 }}
        animate={{ x: 0 }}
        className="w-80 bg-gray-50 border-r border-gray-200 p-6 overflow-y-auto shadow-lg"
      >
        <h2 className="text-2xl font-bold text-gray-800 mb-6">Query History</h2>
        <div className="space-y-4">
          {queryHistory.map((item) => (
            <motion.div
              key={item.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={() => handleHistoryClick(item)}
              className={`p-4 rounded-lg cursor-pointer transition-all duration-200 ${
                selectedHistoryItem?.id === item.id
                  ? "bg-gray-200 shadow-md"
                  : "bg-white hover:bg-gray-100 shadow-sm"
              }`}
            >
              <p className="text-gray-800 text-sm font-medium truncate">{item.query}</p>
              <p className="text-gray-500 text-xs mt-1">{item.timestamp}</p>
            </motion.div>
          ))}
        </div>
      </motion.div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col">
        {/* Response Area */}
        <div className="flex-1 overflow-y-auto p-8">
          <AnimatePresence mode="wait">
            {response && (
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                className="bg-white border border-gray-200 rounded-xl p-8 shadow-lg"
              >
                <div className="mb-4">
                  <h3 className="text-lg font-semibold text-gray-800 mb-2">Query:</h3>
                  <p className="text-gray-600">{selectedHistoryItem?.query || query}</p>
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-800 mb-2">Response:</h3>
                  <p className="text-gray-700 leading-relaxed">{response}</p>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Search Bar */}
        <div className="border-t border-gray-200 p-6 bg-white shadow-lg">
          <form onSubmit={handleSubmit} className="max-w-3xl mx-auto">
            <div className="relative">
              <textarea
                rows={1}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Ask your patent-related question..."
                className="w-full p-4 pr-12 text-gray-800 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-gray-300 focus:border-transparent resize-none shadow-sm"
              />
              <motion.button
                type="submit"
                disabled={loading}
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                className="absolute right-2 bottom-2 bg-gray-800 hover:bg-gray-700 text-white px-6 py-2 rounded-lg transition disabled:opacity-50 disabled:cursor-not-allowed shadow-md"
              >
                {loading ? "Searching..." : "Search"}
              </motion.button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

export default App;
