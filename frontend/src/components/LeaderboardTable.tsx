// File: src/components/LeaderboardTable.tsx
import React from "react";

export type NgramRow = {
  id: number;
  text: string;
  domain: string;
  field: string;
  subfield: string;
  domain_id: number;
  field_id: number;
  subfield_id: number;
  n_words: number;
  df_ngram: number;
  df_ngram_subfield: number;
};

type Props = {
  data: NgramRow[];
  onSelectNgram: (ngram: NgramRow) => void;
  showHidden?: boolean;
  sortBy: string;
  sortOrder: 'asc' | 'desc';
  onSortChange: (field: string) => void;
};

const LeaderboardTable: React.FC<Props> = ({
  data,
  onSelectNgram,
  showHidden = false,
  sortBy,
  sortOrder,
  onSortChange,
}) => {
  const renderSortIcon = (field: string) => {
    if (sortBy !== field) return null;
    return sortOrder === 'asc' ? '▲' : '▼';
  };

  return (
    <div className="overflow-x-auto bg-white rounded-xl shadow p-4">
      <table className="min-w-full table-auto text-sm">
        <thead className="bg-gray-100 text-left">
          <tr>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('text')}>
              N-gram {renderSortIcon('text')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('subfield')}>
              Subfield {renderSortIcon('subfield')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('field')}>
              Field {renderSortIcon('field')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('domain')}>
              Domain {renderSortIcon('domain')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('n_words')}>
              N-words {renderSortIcon('n_words')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('df_ngram')}>
              DF (All) {renderSortIcon('df_ngram')}
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange('df_ngram_subfield')}>
              DF (Subfield) {renderSortIcon('df_ngram_subfield')}
            </th>
            {showHidden && (
              <>
                <th className="p-2">Domain ID</th>
                <th className="p-2">Field ID</th>
                <th className="p-2">Subfield ID</th>
                <th className="p-2">Ngram ID</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {data.map((ngram) => (
            <tr
              key={ngram.id}
              className="hover:bg-gray-50 cursor-pointer"
              onClick={() => onSelectNgram(ngram)}
            >
              <td className="p-2">{ngram.text}</td>
              <td className="p-2">{ngram.subfield}</td>
              <td className="p-2">{ngram.field}</td>
              <td className="p-2">{ngram.domain}</td>
              <td className="p-2">{ngram.n_words}</td>
              <td className="p-2">{ngram.df_ngram}</td>
              <td className="p-2">{ngram.df_ngram_subfield}</td>
              {showHidden && (
                <>
                  <td className="p-2">{ngram.domain_id}</td>
                  <td className="p-2">{ngram.field_id}</td>
                  <td className="p-2">{ngram.subfield_id}</td>
                  <td className="p-2">{ngram.id}</td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default LeaderboardTable;


// // File: src/components/LeaderboardTable.tsx
// import React from "react";

// type NgramRow = {
//   id: number;
//   text: string;
//   domain: string;
//   field: string;
//   subfield: string;
//   domain_id: number;
//   field_id: number;
//   subfield_id: number;
//   n_words: number;
//   df_ngram: number;
//   df_ngram_subfield: number;
// };

// type Props = {
//   data: NgramRow[];
//   onSelectNgram: (ngram: NgramRow) => void;
//   showHidden?: boolean; // If true, show hidden ID columns
// };

// const LeaderboardTable: React.FC<Props> = ({ data, onSelectNgram, showHidden = false }) => {
//   return (
//     <div className="overflow-x-auto bg-white rounded-xl shadow p-4">
//       <table className="min-w-full table-auto text-sm">
//         <thead className="bg-gray-100 text-left">
//           <tr>
//             <th className="p-2">N-gram</th>
//             <th className="p-2">Domain</th>
//             <th className="p-2">Field</th>
//             <th className="p-2">Subfield</th>
//             <th className="p-2">N-words</th>
//             <th className="p-2">DF (All)</th>
//             <th className="p-2">DF (Subfield)</th>
//             {showHidden && (
//               <>
//                 <th className="p-2">Domain ID</th>
//                 <th className="p-2">Field ID</th>
//                 <th className="p-2">Subfield ID</th>
//                 <th className="p-2">Ngram ID</th>
//               </>
//             )}
//           </tr>
//         </thead>
//         <tbody>
//           {data.map((ngram) => (
//             <tr
//               key={ngram.id}
//               className="hover:bg-gray-50 cursor-pointer"
//               onClick={() => onSelectNgram(ngram)}
//             >
//               <td className="p-2">{ngram.text}</td>
//               <td className="p-2">{ngram.domain}</td>
//               <td className="p-2">{ngram.field}</td>
//               <td className="p-2">{ngram.subfield}</td>
//               <td className="p-2">{ngram.n_words}</td>
//               <td className="p-2">{ngram.df_ngram}</td>
//               <td className="p-2">{ngram.df_ngram_subfield}</td>
//               {showHidden && (
//                 <>
//                   <td className="p-2">{ngram.domain_id}</td>
//                   <td className="p-2">{ngram.field_id}</td>
//                   <td className="p-2">{ngram.subfield_id}</td>
//                   <td className="p-2">{ngram.id}</td>
//                 </>
//               )}
//             </tr>
//           ))}
//         </tbody>
//       </table>
//     </div>
//   );
// };

// export default LeaderboardTable;