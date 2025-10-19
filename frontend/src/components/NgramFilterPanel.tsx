// File: src/components/NgramFilterPanel.tsx
import { useEffect, useRef, useState } from "react";
import api from "../api/client";

type NgramOption = { id: number; text: string };

export type FilterState = {
  domainId: number | null;
  fieldId: number | null;
  subfieldId: number | null;
  nWords: number | null;
  ngram: NgramOption | null;

  // optional names if you need them elsewhere
  domainName?: string | null;
  fieldName?: string | null;
  subfieldName?: string | null;
};

type Props = {
  onFilterChange: (filters: FilterState) => void;
  showTimePicker?: boolean;
  onTimeChange?: (start: string | null, end: string | null) => void;
  initialStart?: string | null;
  initialEnd?: string | null;
};

function NgramFilterPanel({
  onFilterChange,
  showTimePicker = false,
  onTimeChange,
  initialStart = null,
  initialEnd = null,
}: Props) {
  const [hierarchy, setHierarchy] = useState<any[]>([]);
  const [allFields, setAllFields] = useState<any[]>([]);
  const [allSubfields, setAllSubfields] = useState<any[]>([]);
  const [nWordsList, setNWordsList] = useState<number[]>([]);
  const [filters, setFilters] = useState<FilterState>({
    domainId: null,
    fieldId: null,
    subfieldId: null,
    nWords: null,
    ngram: null,
    domainName: null,
    fieldName: null,
    subfieldName: null,
  });

  // time picker (local until Apply)
  const [bounds, setBounds] = useState<{ min: string | null; max: string | null }>({ min: null, max: null });
  const [start, setStart] = useState<string | null>(initialStart);
  const [end, setEnd] = useState<string | null>(initialEnd);

  // ambiguity helpers
  const [isSubfieldAmbiguous, setIsSubfieldAmbiguous] = useState(false);
  const [ambiguousSubfieldInfo, setAmbiguousSubfieldInfo] = useState<{
    subfieldName: string;
    availableFields: any[];
  } | null>(null);

  // autocomplete
  const [searchText, setSearchText] = useState("");
  const [autocompleteOptions, setAutocompleteOptions] = useState<NgramOption[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [isLoadingAutocomplete, setIsLoadingAutocomplete] = useState(false);

  const debounceTimer = useRef<number | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // fetch filters + time bounds
  useEffect(() => {
    api.get("/v1/filters/hierarchy").then((res) => {
      const data = res.data;
      setHierarchy(data);

      const fields = data.flatMap((d: any) =>
        d.fields.map((f: any) => ({ ...f, domain_id: d.id, domain_name: d.name }))
      );
      setAllFields(fields);

      const subfields = fields.flatMap((f: any) =>
        f.subfields.map((s: any) => ({
          ...s,
          field_id: f.id,
          field_name: f.name,
          domain_id: f.domain_id,
          domain_name: f.domain_name,
        }))
      );
      setAllSubfields(subfields);
    });
    api.get("/v1/filters/n_words").then((res) => setNWordsList(res.data));

    if (showTimePicker) {
      api
        .get("/v1/bursts/time-bounds")
        .then((res) => setBounds({ min: res.data?.min ?? null, max: res.data?.max ?? null }))
        .catch(() => setBounds({ min: null, max: null }));
    }
  }, [showTimePicker]);

  // autocomplete fetch (debounced) — works independently of Apply
  useEffect(() => {
    if (debounceTimer.current) clearTimeout(debounceTimer.current);

    if (searchText.length < 2) {
      setAutocompleteOptions([]);
      setShowDropdown(false);
      return;
    }

    setIsLoadingAutocomplete(true);

    debounceTimer.current = window.setTimeout(() => {
      api
        .get("/v1/filters/ngram-text", {
          params: {
            q: searchText,
            subfield_id: filters.subfieldId || undefined,
            limit: 10,
          },
        })
        .then((res) => {
          const items: NgramOption[] = res.data || [];
          setAutocompleteOptions(items);
          setShowDropdown(items.length > 0);
        })
        .catch(() => {
          setAutocompleteOptions([]);
          setShowDropdown(false);
        })
        .finally(() => setIsLoadingAutocomplete(false));
    }, 300);

    return () => {
      if (debounceTimer.current) clearTimeout(debounceTimer.current);
    };
  }, [searchText, filters.subfieldId]);

  // close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (inputRef.current && !inputRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const resetFilters = () => {
    setFilters({
      domainId: null,
      fieldId: null,
      subfieldId: null,
      nWords: null,
      ngram: null,
      domainName: null,
      fieldName: null,
      subfieldName: null,
    });
    setSearchText("");
    setAutocompleteOptions([]);
    setShowDropdown(false);
    setIsSubfieldAmbiguous(false);
    setAmbiguousSubfieldInfo(null);
    if (showTimePicker) {
      setStart(null);
      setEnd(null);
    }
    // NOTE: not auto-applying; user still needs to click Apply
  };

  const applyFilters = () => {
    if (isSubfieldAmbiguous) return; // safety
    onFilterChange(filters);
    if (showTimePicker && onTimeChange) {
      onTimeChange(start, end);
    }
  };

  const handleNgramSelect = (option: NgramOption) => {
    setSearchText(option.text);
    setShowDropdown(false);
    setFilters((prev) => ({ ...prev, ngram: option }));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && searchText.trim()) {
      const ngramOption = { id: 0, text: searchText.trim() };
      setFilters((prev) => ({ ...prev, ngram: ngramOption }));
      setShowDropdown(false);
    } else if (e.key === "Escape") {
      setShowDropdown(false);
    }
  };

  const handleClearSearch = () => {
    setSearchText("");
    setShowDropdown(false);
    setFilters((prev) => ({ ...prev, ngram: null }));
  };

  const visibleFields = allFields.filter((f) => {
    if (isSubfieldAmbiguous && ambiguousSubfieldInfo) {
      return ambiguousSubfieldInfo.availableFields.some((af) => af.id === f.id);
    }
    return filters.domainId ? f.domain_id === filters.domainId : true;
  });

  const visibleSubfields = allSubfields.filter((s) => {
    if (filters.fieldId) return s.field_id === filters.fieldId;
    if (filters.domainId) return s.domain_id === filters.domainId;
    return true;
  });

  // quick time buttons (local only)
  const setMaxRange = () => {
    if (bounds.min && bounds.max) {
      setStart(bounds.min);
      setEnd(bounds.max);
    }
  };
  const setLastYear = () => {
    if (!bounds.max) return;
    const max = new Date(bounds.max);
    const lastYear = new Date(max);
    lastYear.setFullYear(max.getFullYear() - 1);
    const iso = (d: Date) => d.toISOString().slice(0, 10);
    setStart(iso(lastYear));
    setEnd(iso(max));
  };
  const setYTD = () => {
    const now = new Date();
    const ytdStart = new Date(now.getFullYear(), 0, 1);
    const iso = (d: Date) => d.toISOString().slice(0, 10);
    setStart(iso(ytdStart));
    setEnd(iso(now));
  };

  return (
    <div className="bg-white rounded-xl shadow p-6 mb-8 space-y-4">
      <div className="flex justify-between items-center">
        <h2 className="text-xl font-semibold">Filter N-grams</h2>
        <div className="flex items-center gap-2">
          <button
            onClick={resetFilters}
            className="text-sm px-3 py-1 bg-gray-100 rounded hover:bg-gray-200 transition"
            type="button"
          >
            Reset
          </button>
          <button
            onClick={applyFilters}
            disabled={isSubfieldAmbiguous}
            className={`text-sm px-3 py-1 rounded transition ${
              isSubfieldAmbiguous
                ? "bg-blue-300 text-white cursor-not-allowed"
                : "bg-blue-600 text-white hover:bg-blue-700"
            }`}
            type="button"
            title={isSubfieldAmbiguous ? "Select a Field to resolve ambiguous Subfield" : "Apply filters"}
          >
            Apply
          </button>
        </div>
      </div>

      {/* Grid — give Search and Time Window more room on small/medium screens */}
      <div className={`grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-6 gap-4`}>
        {/* DOMAIN */}
        <select
          className="border border-gray-300 rounded p-2"
          value={filters.domainId ?? ""}
          onChange={(e) => {
            const val = e.target.value ? Number(e.target.value) : null;
            const selected = hierarchy.find((d: any) => d.id === val) || null;
            setFilters({
              domainId: val,
              fieldId: null,
              subfieldId: null,
              nWords: filters.nWords,
              ngram: filters.ngram,
              domainName: selected ? selected.name : null,
              fieldName: null,
              subfieldName: null,
            });
            setIsSubfieldAmbiguous(false);
            setAmbiguousSubfieldInfo(null);
          }}
        >
          <option value="">Select Domain</option>
          {hierarchy.map((d) => (
            <option key={d.id} value={d.id}>
              {d.name}
            </option>
          ))}
        </select>

        {/* FIELD */}
        <select
          className="border border-gray-300 rounded p-2"
          value={filters.fieldId ?? ""}
          onChange={(e) => {
            const val = e.target.value ? Number(e.target.value) : null;
            const selected = allFields.find((f) => f.id === val) || null;

            if (isSubfieldAmbiguous && ambiguousSubfieldInfo && selected) {
              const correctSubfield = allSubfields.find(
                (s) => s.field_id === selected.id && s.name === ambiguousSubfieldInfo.subfieldName
              );
              setFilters((prev) => ({
                ...prev,
                fieldId: val,
                fieldName: selected.name,
                domainId: selected.domain_id,
                domainName: selected.domain_name,
                subfieldId: correctSubfield ? correctSubfield.id : prev.subfieldId,
                subfieldName: correctSubfield ? correctSubfield.name : prev.subfieldName,
              }));
              setIsSubfieldAmbiguous(false);
              setAmbiguousSubfieldInfo(null);
            } else {
              setFilters((prev) => ({
                ...prev,
                fieldId: val,
                fieldName: selected ? selected.name : null,
                domainId: selected ? selected.domain_id : prev.domainId,
                domainName: selected ? selected.domain_name : prev.domainName,
                subfieldId: null,
                subfieldName: null,
              }));
              setIsSubfieldAmbiguous(false);
              setAmbiguousSubfieldInfo(null);
            }
          }}
        >
          <option value="">Select Field</option>
          {visibleFields.map((f) => (
            <option key={f.id} value={f.id}>
              {f.name}
            </option>
          ))}
        </select>

        {/* SUBFIELD */}
        <select
          className="border border-gray-300 rounded p-2"
          value={filters.subfieldId ?? ""}
          onChange={(e) => {
            const val = e.target.value ? Number(e.target.value) : null;
            const selected = allSubfields.find((s) => s.id === val) || null;

            if (selected) {
              const sameNameSubfields = allSubfields.filter((s) => s.name === selected.name);
              const ambiguous = sameNameSubfields.length > 1 && !filters.fieldId;
              if (ambiguous) {
                const fieldsWithThisSubfield = sameNameSubfields
                  .map((s) => ({ id: s.field_id, name: s.field_name, domain_name: s.domain_name }))
                  .filter((field, idx, self) => idx === self.findIndex((f) => f.id === field.id));

                setFilters((prev) => ({
                  ...prev,
                  subfieldId: val,
                  subfieldName: selected.name,
                  domainId: selected.domain_id,
                  domainName: selected.domain_name,
                }));
                setIsSubfieldAmbiguous(true);
                setAmbiguousSubfieldInfo({
                  subfieldName: selected.name,
                  availableFields: fieldsWithThisSubfield,
                });
                return;
              }
            }

            setFilters((prev) => ({
              ...prev,
              subfieldId: val,
              subfieldName: selected ? selected.name : null,
              fieldId: selected ? selected.field_id : prev.fieldId,
              fieldName: selected ? selected.field_name : prev.fieldName,
              domainId: selected ? selected.domain_id : prev.domainId,
              domainName: selected ? selected.domain_name : prev.domainName,
            }));
            setIsSubfieldAmbiguous(false);
            setAmbiguousSubfieldInfo(null);
          }}
        >
          <option value="">Select Subfield</option>
          {visibleSubfields.map((s) => (
            <option key={s.id} value={s.id}>
              {s.name} {s.field_name ? `(${s.field_name})` : ""}
            </option>
          ))}
        </select>

        {/* N_WORDS */}
        <select
          className="border border-gray-300 rounded p-2"
          value={filters.nWords ?? ""}
          onChange={(e) => setFilters({ ...filters, nWords: e.target.value ? Number(e.target.value) : null })}
        >
          <option value="">All Lengths</option>
          {nWordsList.map((n) => (
            <option key={n} value={n}>
              {n} words
            </option>
          ))}
        </select>

        {/* AUTOCOMPLETE — wider cell */}
        <div className="relative md:col-span-2 xl:col-span-2" ref={inputRef}>
          <div className="flex">
            <input
              type="text"
              placeholder="Search N-gram (type 2+ chars)…"
              className="border border-gray-300 rounded-l p-2 flex-1 min-w-0"
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              onKeyDown={handleKeyDown}
              onFocus={() => {
                if (autocompleteOptions.length > 0) setShowDropdown(true);
              }}
            />
            {(searchText || filters.ngram) && (
              <button
                onClick={handleClearSearch}
                className="border border-l-0 border-gray-300 rounded-r px-2 hover:bg-gray-50"
                title="Clear search"
                type="button"
              >
                ✕
              </button>
            )}
          </div>

          {filters.ngram && !showDropdown && (
            <div className="text-xs text-green-600 mt-1">Selected: {filters.ngram.text}</div>
          )}

          {showDropdown && (
            <div className="absolute z-10 bg-white border border-gray-300 rounded w-full mt-1 shadow-lg">
              {isLoadingAutocomplete ? (
                <div className="p-2 text-sm text-gray-500">Loading…</div>
              ) : autocompleteOptions.length > 0 ? (
                <>
                  {autocompleteOptions.map((opt, idx) => (
                    <div
                      key={`${opt.text}-${idx}`}
                      className="p-2 hover:bg-gray-100 cursor-pointer text-sm border-b last:border-b-0"
                      onClick={() => handleNgramSelect(opt)}
                    >
                      {opt.text}
                    </div>
                  ))}
                  <div className="p-2 text-xs text-gray-400 border-t bg-gray-50">
                    Press Enter to search for “{searchText}”
                  </div>
                </>
              ) : (
                <div className="p-2 text-sm text-gray-500">
                  No matches found. Press Enter to search for “{searchText}”.
                </div>
              )}
            </div>
          )}
        </div>

        {/* TIME PICKER — local changes only until Apply */}
        {showTimePicker && (
          <div className="border border-gray-200 rounded p-2 md:col-span-2 xl:col-span-2 min-w-0">
            <div className="text-xs font-medium text-gray-600 mb-1">Time Window</div>
            <div className="flex flex-wrap items-center gap-2">
              <input
                type="date"
                className="border border-gray-300 rounded p-1 text-sm w-36 sm:w-40"
                value={start ?? ""}
                onChange={(e) => setStart(e.target.value || null)}
                min={bounds.min ?? undefined}
                max={bounds.max ?? undefined}
              />
              <span className="text-gray-400">→</span>
              <input
                type="date"
                className="border border-gray-300 rounded p-1 text-sm w-36 sm:w-40"
                value={end ?? ""}
                onChange={(e) => setEnd(e.target.value || null)}
                min={bounds.min ?? undefined}
                max={bounds.max ?? undefined}
              />
            </div>
            <div className="flex flex-wrap gap-2 mt-2">
              <button className="text-xs px-2 py-1 bg-gray-100 rounded hover:bg-gray-200" onClick={setMaxRange} type="button">
                Max
              </button>
              <button className="text-xs px-2 py-1 bg-gray-100 rounded hover:bg-gray-200" onClick={setLastYear} type="button">
                1Y
              </button>
              <button className="text-xs px-2 py-1 bg-gray-100 rounded hover:bg-gray-200" onClick={setYTD} type="button">
                YTD
              </button>
            </div>
            {bounds.min && bounds.max && (
              <div className="text-[10px] text-gray-500 mt-1">
                Data: {bounds.min} → {bounds.max}
              </div>
            )}
          </div>
        )}
      </div>

      {isSubfieldAmbiguous && ambiguousSubfieldInfo && (
        <div className="mt-4 p-4 bg-amber-50 border border-amber-200 rounded-lg">
          <div className="flex items-start gap-3">
            <svg className="h-5 w-5 text-amber-600 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path
                fillRule="evenodd"
                d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
                clipRule="evenodd"
              />
            </svg>
            <div className="flex-1">
              <h3 className="text-amber-800 font-medium">Multiple fields contain “{ambiguousSubfieldInfo.subfieldName}”</h3>
              <p className="text-amber-700 text-sm mt-1">Select a field to continue:</p>
              <ul className="text-amber-700 text-sm mt-2 list-disc list-inside">
                {ambiguousSubfieldInfo.availableFields.map((field) => (
                  <li key={field.id}>
                    <strong>{field.name}</strong> (in {field.domain_name})
                  </li>
                ))}
              </ul>
              <p className="text-amber-600 text-xs mt-2">⚡ Data will update once you select a field and click Apply.</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default NgramFilterPanel;
