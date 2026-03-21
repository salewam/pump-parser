"""Structured prompts for Vision AI — task-specific, JSON output.

Each prompt is designed for a specific extraction task:
- page_classify: determine page type
- table_type: determine table format
- extract_flat: extract flat table data
- extract_matrix: extract Q-H matrix data
- extract_graph: read Q-H curves from graph images
"""

# ─── Page Classification ─────────────────────────────────────────────────────

PROMPT_PAGE_CLASSIFY = """Analyze this pump catalog page image and classify it.

Return ONLY a JSON object with these fields:
{
  "page_type": one of ["data_table", "data_graph", "cover", "toc", "dimensions", "model_code", "model_range", "materials", "installation", "other"],
  "confidence": float 0.0-1.0,
  "has_table": boolean,
  "has_graph": boolean,
  "series_names": list of pump series names found (e.g. ["CMI", "NBS"]),
  "description": brief description of page content (max 20 words)
}

Rules:
- data_table: page with numeric specifications (Q, H, P values) in table form
- data_graph: page with Q-H performance curves/charts
- model_range: page listing model names with basic specs (ranges, not full data)
- dimensions: page with physical dimensions drawings/tables
- cover/toc/materials/installation: self-explanatory
- If page has BOTH table AND graph, classify as data_table

Output ONLY valid JSON, no other text."""

# ─── Table Type Detection ─────────────────────────────────────────────────────

PROMPT_TABLE_TYPE = """Analyze the table structure on this pump catalog page.

Return ONLY a JSON object:
{
  "table_type": one of ["flat_table", "qh_matrix", "curve_table", "transposed", "list_format", "unknown"],
  "confidence": float 0.0-1.0,
  "description": brief explanation (max 15 words)
}

Table type definitions:
- flat_table: one row per pump model, columns are parameters (Model, Q, H, P)
- qh_matrix: column headers are Q flow values (0, 5, 10, 15...), cells contain H head values
- curve_table: shared Q row at top, then model + H values in rows below
- transposed: parameters in rows, models in columns (rotated flat table)
- list_format: sequential listing of models with specs, minimal structure

Output ONLY valid JSON."""

# ─── Flat Table Extraction ────────────────────────────────────────────────────

PROMPT_EXTRACT_FLAT = """Extract pump specifications from this catalog page table.

The table has one row per pump model. Extract ALL models.

Return ONLY a JSON object:
{
  "pumps": [
    {
      "model": "model name string",
      "article": "article/part number or empty string",
      "q_nom": nominal flow in m³/h (float),
      "h_nom": nominal head in meters (float),
      "power_kw": power in kW (float),
      "rpm": speed in RPM (integer, 0 if not shown),
      "stages": number of stages (integer, 0 if not shown),
      "dn": pipe diameter DN in mm (integer, 0 if not shown)
    }
  ],
  "units": {
    "q": "m3/h or l/min or gpm",
    "h": "m or bar or psi",
    "p": "kW or HP"
  }
}

Rules:
- Convert ALL values to standard units: Q in m³/h, H in meters, P in kW
- If Q is in l/min, divide by 60. If in l/s, multiply by 3.6
- If H is in bar, multiply by 10.2. If in kPa, divide by 9.81
- If P is in HP, multiply by 0.746
- Use 0 for missing values, never null
- Extract EVERY row, don't skip any models

Output ONLY valid JSON."""

# ─── Q-H Matrix Extraction ───────────────────────────────────────────────────

PROMPT_EXTRACT_MATRIX = """Extract pump Q-H curve data from this catalog page.

The table has Q (flow) values in column headers and H (head) values in cells.
Each row is a different pump model.

Return ONLY a JSON object:
{
  "q_values": [list of Q values from headers, in m³/h],
  "pumps": [
    {
      "model": "model name",
      "power_kw": power in kW (float),
      "h_values": [H values corresponding to each Q value, in meters],
      "q_nom": nominal Q (at ~65% of max Q),
      "h_nom": H at nominal Q
    }
  ]
}

Rules:
- Q values should be in m³/h. Convert from l/min if needed (÷60)
- H values should be in meters. Convert from bar if needed (×10.2)
- Use null for missing/empty H values (pump can't reach that flow)
- Extract ALL models shown

Output ONLY valid JSON."""

# ─── Graph/Curve Reading ─────────────────────────────────────────────────────

PROMPT_EXTRACT_GRAPH = """Read the pump performance curves from this graph image.

The graph shows Q-H curves (flow vs head) for one or more pump models.
X-axis is Q (flow), Y-axis is H (head).

For EACH curve shown, extract coordinate points by reading the graph carefully.

Return ONLY a JSON object:
{
  "x_axis": {"label": "Q", "unit": "m³/h or l/min", "min": float, "max": float},
  "y_axis": {"label": "H", "unit": "m or bar", "min": float, "max": float},
  "curves": [
    {
      "model": "model name from label",
      "q_points": [list of Q values read from curve, in m³/h],
      "h_points": [list of H values at each Q point, in meters],
      "q_nom": estimated nominal Q (at ~65% of max Q),
      "h_nom": H at nominal Q
    }
  ]
}

Rules:
- Read 5-8 points per curve, evenly spaced along Q axis
- Start from Q=0 (or leftmost point) to max Q (rightmost/where curve meets X axis)
- Convert l/min to m³/h (÷60), bar to meters (×10.2)
- If curve labels are partial (e.g. "-14G/2"), prefix with series shown on page
- Read ALL curves visible on the graph

Output ONLY valid JSON."""

# ─── Prompt Registry ─────────────────────────────────────────────────────────

PROMPTS = {
    "page_classify": PROMPT_PAGE_CLASSIFY,
    "table_type": PROMPT_TABLE_TYPE,
    "extract_flat": PROMPT_EXTRACT_FLAT,
    "extract_matrix": PROMPT_EXTRACT_MATRIX,
    "extract_graph": PROMPT_EXTRACT_GRAPH,
}


def get_prompt(key: str) -> str:
    """Get prompt by key. Raises KeyError if not found."""
    if key not in PROMPTS:
        raise KeyError(f"Unknown prompt key: {key}. Available: {list(PROMPTS.keys())}")
    return PROMPTS[key]
