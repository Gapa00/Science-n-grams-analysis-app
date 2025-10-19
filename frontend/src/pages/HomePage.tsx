export default function HomePage() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-10">
      <h1 className="text-3xl font-bold mb-3">Welcome to Science N-grams</h1>
      <p className="text-gray-700 mb-6">
        Help us evaluate how “bursty” scientific terms are over time by choosing the hotter trend from pairs of charts and assigning burstiness scores to individual charts.
      </p>

      <ol className="list-decimal ml-6 space-y-3 text-gray-800">
        <li>
          Go to <span className="font-semibold">Login</span> or <span className="font-semibold">Start voting</span> and enter your username (no password needed).
        </li>
        <li>
          You’ll be redirected to the <span className="font-semibold">Voting</span> page and shown two time-series frequency charts at the same time for binary voting.
        </li>
        <li>
          To submit a binary vote, click on the chart that shows a more <span className="font-semibold">bursty / buzzwordy</span> characteristic, then press <span className="font-semibold">Next</span>.
        </li>
        <li>
          You can also review your previous votes by clicking the <span className="font-semibold">Previous</span> button.
        </li>
        <li>
          Binary voting includes <span className="font-semibold">100 chart pairs</span> in total.
        </li>
        <li>
          After finishing the binary voting, you will proceed to the <span className="font-semibold">Slider Evaluation</span> page.
        </li>
        <li>
          On the slider page, you will see <span className="font-semibold">50 individual charts</span>. Assign a score via slider to each chart depending on how bursty you deem the term’s frequency trend over time.
        </li>
        <li>
          You can click on a chart to enlarge it for a better view while adjusting the slider.
        </li>
        <li>
          Once you assign values to all charts, press the <span className="font-semibold">Submit All Ratings</span> button to save your feedback.
        </li>
        <li>
          Please note that all the votes are purely based on subjective opinion and there are no wrong answers.
        </li>
      </ol>

      <p className="mt-5 text-gray-700 italic">
        <span className="font-semibold">NOTE: </span>
        Terms that are <span className="font-semibold">"bursty"</span> or <span className="font-semibold">"buzzwordy"</span> show a steep frequency hill, which usually happens just once in a timeline. Think of words like "Covid" or "pandemic" in 2019 or "World Wide Web" and "Website" in the nineties. 
        Binary voting evaluates <span className="font-semibold">relative burstiness between two charts</span>, while slider evaluation lets you assign an <span className="font-semibold">absolute burstiness score</span> to individual charts.
      </p>

      <div className="mt-8 flex gap-3">
        <a
          href="/login"
          className="inline-flex items-center px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-semibold"
        >
          Start Voting
        </a>
        <a
          href="/frequency"
          className="inline-flex items-center px-4 py-2 rounded-lg bg-gray-100 hover:bg-gray-200 text-gray-900 font-semibold"
        >
          Explore Frequency Dashboard
        </a>
      </div>
    </div>
  );
}
