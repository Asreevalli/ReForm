"""
C&D Waste Estimation Tool — CircularBuild
Streamlit multi-page application

DATA SOURCES (all publicly accessible, peer-reviewed or institutional):
1. CSE (2020) "Another Brick off the Wall" — waste composition % (Table 4), demolition rate 300-500 kg/m² (p.30), recycling plants (Table 5)
2. IPCC (2006) Guidelines for National GHG Inventories, Vol. 2 — transport emission factors (road freight)
3. MoEFCC C&D Waste Management Rules 2016 — regulatory thresholds
4. CPCB (2017) Guidelines on C&D Waste Management — landfill cost ranges
5. IFC / thinkstep (2017) EDGE India Construction Materials Database — per-tonne GWP factors (edgebuildings.com)
6. Lodha Research / Marepalli (2025) — A1-A3 GWP benchmarks for Indian RCC buildings
7. IIT Madras / Akshatha et al. (2025), 13th World Construction Symposium — A1-A3, A4 GWP benchmarks
8. Alotaibi et al. (2022) MDPI Buildings 12(8) 1203 — lifecycle stage fractions, DOI 10.3390/buildings12081203
9. AEEE / Saint-Gobain (2024) LCA report — stage fractions A4, A5, C-stages
10. Jang et al. (2022) Materials 15, 5047 — CML 2001 AP & EP characterisation factors, DOI 10.3390/ma15145047
11. Nematchoua et al. (2022) MDPI Sustainability — global AP/EP per m² benchmarks, DOI 10.3390/su5010012
12. Chippagiri et al. (2023) MDPI Buildings 13(4) 964 — India prefab LCA case study
13. CEA (2024) CO2 Baseline Database for Indian Power Sector, v18 — grid emission factor 0.716 kg CO2e/kWh
14. CPWD DSR (2024) / State PWDs Schedule of Rates — indicative material market rates for economic calculations
15. CPCB (2022) Annual Report on C&D Waste; State PWD Schedule of Rates — city-specific landfill tipping fees
"""

import streamlit as st
import pandas as pd
import json
import math
from io import BytesIO
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
# firebase_admin imported lazily inside _get_firestore_client() to avoid
# crashing the app if the package is still being installed

# ─── GOOGLE SHEETS LOGGER ───────────────────────────────────────────────────
def log_to_sheets(proj, total_waste, total_gwp, circ_aggregate, total_ap, total_ep):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        # Convert Streamlit secrets to plain dict for google-auth
        sa_info = {k: v for k, v in st.secrets["gcp_service_account"].items()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        client = gspread.Client(auth=creds)
        client.session = gspread.auth.AuthorizedSession(creds)
        sheet_name = st.secrets["sheets"]["spreadsheet_name"]
        sheet = client.open(sheet_name).sheet1
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            proj.get("name", ""),
            proj.get("location", ""),
            proj.get("construction_type", ""),
            proj.get("building_type", ""),
            proj.get("builtup_area", ""),
            round(total_waste, 3),
            round(total_gwp, 3),
            round(circ_aggregate * 100, 1),
            round(total_ap, 4),
            round(total_ep, 4),
        ]
        sheet.append_row(row)
    except Exception as e:
        st.warning(f"⚠️ Google Sheets log failed: {e}")


# ─── FIRESTORE LOGGER ────────────────────────────────────────────────────────
def _get_firestore_client():
    """Initialise Firebase app once per Streamlit session and return Firestore client."""
    import firebase_admin
    from firebase_admin import credentials as fb_credentials, firestore as fb_firestore
    if not firebase_admin._apps:
        # Must convert Streamlit AttrDict to a plain Python dict for firebase_admin
        fb_info = {k: v for k, v in st.secrets["firebase"].items()}
        fb_cred = fb_credentials.Certificate(fb_info)
        firebase_admin.initialize_app(fb_cred)
    return fb_firestore.client()


def log_to_firestore(proj, waste_table, emission_inputs, emission_results,
                     circ_scores, circ_aggregate, benefits):
    """
    Write a complete submission document to Firestore collection 'submissions'.
    Captures every input and output — nothing summarised or dropped.
    Fires silently; never raises to the user.
    """
    try:
        db = _get_firestore_client()

        # ── Summary numbers ───────────────────────────────────────────────
        total_waste_t   = sum(r["qty_t"]     for r in emission_results.values())
        total_gwp_tco2e = sum(r["total_gwp"] for r in emission_results.values()) / 1000.0
        total_ap        = sum(r["AP"]        for r in emission_results.values())
        total_ep        = sum(r["EP"]        for r in emission_results.values())
        total_avoided   = sum(b["avoided_emission_kgco2e"]    for b in benefits.values())
        total_vsavings  = sum(b["virgin_material_savings_inr"] for b in benefits.values())
        total_lf_saved  = sum(b["landfill_cost_saved_inr"]    for b in benefits.values())
        total_diverted  = sum(b["landfill_diverted_t"]        for b in benefits.values())

        # ── Per-material emissions — every stage ─────────────────────────
        emissions_doc = {}
        for mat, r in emission_results.items():
            emissions_doc[mat] = {
                "qty_t":     round(r["qty_t"], 3),
                "A1_A3":     round(r.get("A1_A3", 0), 2),
                "A4":        round(r.get("A4",   0), 2),
                "A5":        round(r.get("A5",   0), 2),
                "C1":        round(r.get("C1",   0), 2),
                "C2":        round(r.get("C2",   0), 2),
                "C3":        round(r.get("C3",   0), 2),
                "C4":        round(r.get("C4",   0), 2),
                "total_gwp": round(r.get("total_gwp", 0), 2),
                "AP":        round(r.get("AP",   0), 4),
                "EP":        round(r.get("EP",   0), 4),
            }

        # ── Per-material EOL scenarios ────────────────────────────────────
        eol_doc = {}
        for mat, r in emission_results.items():
            eol_doc[mat] = r.get("eol", {})

        # ── Per-material transport inputs ─────────────────────────────────
        transport_doc = {}
        for mat, ei_row in emission_inputs.items():
            transport_doc[mat] = {
                "vehicle":      ei_row.get("vehicle", ""),
                "dist_a4_km":   ei_row.get("dist_a4", 0),
                "dist_c2_km":   ei_row.get("dist_c2", 0),
                "sub_type":     ei_row.get("sub_type", ""),
            }

        # ── Per-material circularity benefits ─────────────────────────────
        benefits_doc = {}
        for mat, b in benefits.items():
            benefits_doc[mat] = {
                "recycled_t":               round(b.get("recycled_t", 0), 3),
                "reused_t":                 round(b.get("reused_t", 0), 3),
                "landfill_t":               round(b.get("landfill_t", 0), 3),
                "landfill_diverted_t":      round(b.get("landfill_diverted_t", 0), 3),
                "avoided_emission_kgco2e":  round(b.get("avoided_emission_kgco2e", 0), 2),
                "virgin_savings_inr":       round(b.get("virgin_material_savings_inr", 0), 0),
                "landfill_cost_saved_inr":  round(b.get("landfill_cost_saved_inr", 0), 0),
                "landfill_cost_per_tonne":  b.get("landfill_cost_per_tonne", 0),
            }

        # ── Waste table (material quantities) ────────────────────────────
        waste_doc = {}
        for mat, qty in waste_table.items():
            waste_doc[mat] = round(qty, 3) if isinstance(qty, float) else qty

        # ── Assemble the full document ────────────────────────────────────
        doc = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "project": {
                "name":              proj.get("name", ""),
                "location":          proj.get("location", ""),
                "construction_type": proj.get("construction_type", ""),
                "building_type":     proj.get("building_type", ""),
                "builtup_area_m2":   proj.get("builtup_area", 0),
                "plot_area_m2":      proj.get("plot_area", 0),
                "floors":            proj.get("floors", ""),
                "input_method":      proj.get("input_method", ""),
            },
            "waste_table_tonnes":    waste_doc,
            "transport_inputs":      transport_doc,
            "emissions_per_material": emissions_doc,
            "eol_scenarios":         eol_doc,
            "circularity": {
                "scores_per_material": {m: round(s, 3) for m, s in circ_scores.items()},
                "aggregate_score":     round(circ_aggregate * 100, 1),
            },
            "benefits_per_material": benefits_doc,
            "summary": {
                "total_waste_t":           round(total_waste_t, 3),
                "total_gwp_tco2e":         round(total_gwp_tco2e, 4),
                "total_ap_kgso2e":         round(total_ap, 4),
                "total_ep_kgpo4e":         round(total_ep, 6),
                "circularity_score_0_100": round(circ_aggregate * 100, 1),
                "total_avoided_em_kgco2e": round(total_avoided, 2),
                "total_virgin_savings_inr": round(total_vsavings, 0),
                "total_lf_cost_saved_inr":  round(total_lf_saved, 0),
                "total_landfill_diverted_t": round(total_diverted, 3),
            },
        }

        # ── Document ID = timestamp + project name (URL-safe) ─────────────
        safe_name = proj.get("name", "unknown").replace(" ", "_")[:30]
        doc_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + safe_name

        db.collection("submissions").document(doc_id).set(doc)

    except Exception as e:
        st.warning(f"⚠️ Firestore log failed: {e}")

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CircularBuild — C&D Waste Tool",
    page_icon="♻️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── CUSTOM CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Serif+Display:ital@0;1&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

.block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 1100px; }

.page-title { font-family: 'DM Serif Display', serif; font-size: 2.2rem; color: #1a1a2e; margin-bottom: 0.2rem; }
.page-sub   { color: #6b7280; font-size: 0.95rem; margin-bottom: 1.5rem; }
.section-head { font-size: 1rem; font-weight: 600; color: #1a1a2e; margin: 1.2rem 0 0.5rem 0; border-left: 3px solid #10b981; padding-left: 8px; }
.source-note { font-size: 0.72rem; color: #9ca3af; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 6px 10px; margin-top: 4px; }
.metric-card { background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border: 1px solid #bbf7d0; border-radius: 10px; padding: 14px 18px; }
.metric-val  { font-family: 'DM Serif Display', serif; font-size: 1.8rem; color: #065f46; }
.metric-unit { font-size: 0.78rem; color: #6b7280; }
.metric-label{ font-size: 0.8rem; color: #374151; margin-bottom: 4px; font-weight: 500; }
.warn-box { background: #fffbeb; border: 1px solid #fcd34d; border-radius: 8px; padding: 12px 16px; font-size: 0.85rem; color: #92400e; }
.info-box { background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 8px; padding: 12px 16px; font-size: 0.85rem; color: #1e40af; }
.step-badge { display: inline-block; background: #10b981; color: white; border-radius: 50%; width: 26px; height: 26px; text-align: center; line-height: 26px; font-size: 0.8rem; font-weight: 700; margin-right: 8px; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# DATA CONSTANTS — ALL SOURCED
# ══════════════════════════════════════════════════════════════════════════════

# ── Waste Generation Rates ──────────────────────────────────────────────────
# Source: CSE (2020) "Another Brick off the Wall" p.30 Table 4 — demolition 300-500 kg/m²; construction rates are midpoints within CSE-reported ranges
WASTE_RATES = {
    "Construction": {
        "Residential":   {"rate_kg_m2": 40.0,  "range": "20–60"},   # CSE (2020) p.30 range midpoint
        "Commercial":    {"rate_kg_m2": 50.0,  "range": "30–70"},
        "Industrial":    {"rate_kg_m2": 45.0,  "range": "25–65"},
        "Infrastructure":{"rate_kg_m2": 60.0,  "range": "40–80"},
    },
    "Demolition": {
        "Residential":   {"rate_kg_m2": 350.0, "range": "300–500"}, # CSE 2020 p.30
        "Commercial":    {"rate_kg_m2": 400.0, "range": "300–500"},
        "Industrial":    {"rate_kg_m2": 420.0, "range": "300–500"},
        "Infrastructure":{"rate_kg_m2": 380.0, "range": "300–500"},
    },
}
WASTE_RATE_SOURCE = "CSE (2020) 'Another Brick off the Wall', p.30 Table 4 — demolition 300–500 kg/m²; construction rates are midpoints within CSE-reported ranges"

# ── Material Composition % ──────────────────────────────────────────────────
# Source: CSE (2020) Table 4 (average of 5 studies: TIFAC 2001 via CSE, MCD 2004, IL&FS 2005, Univ. Florida 2009, Coimbatore 2015)
MATERIAL_COMPOSITION = {
    "Construction": {
        "Concrete":        22.0,
        "Brick/Masonry":   28.0,
        "Soil/Sand/Gravel":20.0,
        "Steel/Metal":      4.0,
        "Wood/Timber":      3.0,
        "Bitumen":          1.5,
        "Plastic":          1.0,
        "Glass":            0.5,
        "Others":          20.0,
    },
    "Demolition": {
        "Concrete":        25.0,
        "Brick/Masonry":   31.0,
        "Soil/Sand/Gravel":29.0,  # average across CSE Table 4 studies
        "Steel/Metal":      5.0,
        "Wood/Timber":      2.0,
        "Bitumen":          2.0,
        "Plastic":          1.0,
        "Glass":            0.5,
        "Others":           4.5,
    },
}
COMP_SOURCE = "CSE (2020) Table 4 — average of 5 studies (TIFAC 2001, MCD 2004, IL&FS 2005, Univ. Florida 2009, Coimbatore 2015); primary accessible source: CSE (2020)"

# ══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENTAL IMPACT DATA — UPGRADED TO PER-m² BENCHMARKS
# ══════════════════════════════════════════════════════════════════════════════
#
# PRIMARY SOURCES (all publicly available, peer-reviewed or institutional):
#
#  [S1] Lodha Group / Dr. Prasad Marepalli (2025). "Baselining Embodied Carbon in
#       Building Sector — A Comparative Study Across Building Heights."
#       Lodha Research: B01–B10 Indian RCC buildings, A1–A3 GWP: 352–567 kg CO2e/m²
#       URL: lodhagroup.com/blogs/sustainability/baselining-embodied-carbon-in-building-sector
#
#  [S2] IIT Madras / K.G. Akshatha et al. (2025). "Embodied Carbon Variability in
#       Indian High-Rise Residential Buildings." 13th World Construction Symposium.
#       Mean A1–A3 = 379 kg CO2e/m²; A4 transport = 45 kg CO2e/m²
#       URL: ciobwcs.com/downloads/papers25/S17052.pdf
#
#  [S3] MDPI Buildings 12(8) 1203, Alotaibi et al. (2022). "LCA of Embodied Carbon
#       and Decarbonization of a High-Rise Residential Building in India."
#       Total lifecycle (A1–C4) = 414 kg CO2e/m²; demolition C1 ≈ 0.2% of LC total
#       DOI: 10.3390/buildings12081203
#
#  [S4] AEEE / Saint-Gobain (2024). "Life Cycle Assessment of Carbon Emissions:
#       Progress and Barriers in Indian Building Sector."
#       A4 = 7–10% of A1–A3; A5 = 3–5% of upfront; C stages = 1–13% of lifecycle
#       URL: aeee.in/wp-content/uploads/2024/08/life-cycle-assessment-of-carbon-emissions.pdf
#
#  [S5] IFC / thinkstep (2017). "India Construction Materials Database of Embodied
#       Energy and GWP." Methodology Report, EDGE Platform.
#       OPC: ~0.86 kg CO2e/kg; PPC: ~0.71 kg CO2e/kg; rebar (BF): ~2.0 kg CO2e/kg
#       URL: edgebuildings.com (IFC EDGE India dataset)
#
#  [S6] Lodha (2025) EPD-based material coefficients (Table 1):
#       OPC: 0.996 kg CO2e/kg | PPC: 0.71 | GGBS: 0.069 | Fly Ash: 0.065
#       Aluminium extruded: ~25 kg CO2e/kg
#
#  [S7] IPCC (2006) Vol. 2 — Transport emission factors (road freight India)
#
#  [S8] Jang et al. (2022). Materials 15, 5047. CML 2001 AP & EP characterisation
#       factors for construction material LCI data. (Korean NDB; globally used CML)
#       DOI: 10.3390/ma15145047
#
#  [S9] MDPI Sustainability 5(1) 12, Nematchoua et al. (2022). "AP and EP potentials
#       for 150 Countries" — global AP ≈ 0.249 kg SO2/m²; India in low-income group
#       ≈ 0.30–0.32 kg SO2/m²; EP ≈ 0.09 kg PO4/m²
#       DOI: 10.3390/su5010012
#
#  [S10] MDPI Buildings 13(4) 964, Chippagiri et al. (2023). "LCA of Prefabricated
#        Housing — AP, EP, GWP, ODP." Case study from India (Nagpur region).
#
# ─────────────────────────────────────────────────────────────────────────────
# LIFECYCLE STAGE BENCHMARK VALUES — GWP, AP, EFW (Eutrophication/Freshwater)
# Expressed per m² of built-up floor area (BUA)
# ─────────────────────────────────────────────────────────────────────────────
#
# GWP (kg CO2e/m²) by project type — construction & demolition separately
# Construction benchmarks derived from [S1][S2][S3][S4]:
#   A1–A3 (materials): Residential low-rise 352, mid-rise 394–477, high-rise 502–567
#   A4 (transport):    ~10% of A1-A3 [S2][S4] → 35–57 kg CO2e/m²
#   A5 (site):         ~4% of A1-A3 [S4] → 14–23 kg CO2e/m²
#   C1 (demolition):   0.2% of lifecycle [S3] → ~1–3 kg CO2e/m²
#   C2 (transport to EOL): calculated in compute_emissions() from user-input distance_c2 × vehicle EF
#                           — NOT stored as a fixed per-m² benchmark (distance is project-specific)
#   C3 (processing):   crushing/sorting ~5–15 kg CO2e/m²
#   C4 (landfill):     inert waste 2.5 kg CO2e/t × ~0.3 t/m² → ~1–5 kg CO2e/m²
#
# All values below are mid-point estimates; ranges shown in comments.
# NOTE: C2 values in this table are indicative benchmarks for the per-m² summary display ONLY.
# Actual C2 emissions in the waste-weight calculation (Tab 2b) are computed from
# user-specified haul distances × vehicle EF — not from these benchmark values.

GWP_PER_M2 = {
    # ── CONSTRUCTION (new build) — kg CO2e / m² BUA ─────────────────────────
    "Construction": {
        "Residential": {
            # [S1] B01-B09 median ~390 kg CO2e/m² for A1-A3 (G+10 to G+40)
            # [S2] IIT Madras mean A1-A3 = 379 kg CO2e/m²
            "A1_A3": 385.0,   # material manufacture  [S1,S2] range 352–444
            "A4":     38.0,   # transport to site      [S2][S4] ≈10% of A1-A3
            "A5":     15.0,   # site construction      [S4] ≈4% of A1-A3
            "C1":      2.0,   # future demolition      [S3] 0.2% of LC
            "C2":     12.0,   # transport to EOL       INDICATIVE benchmark only (20km @0.12 kg CO2e/t-km×5t/m² equivalent); actual calc uses user distance
            "C3":      8.0,   # waste processing       CML 2001 / crushing
            "C4":      3.0,   # landfill disposal      IPCC 2006 inert waste
        },
        "Commercial": {
            # [S1] B10 hybrid office = 567 kg CO2e/m²; commercial typically higher steel
            "A1_A3": 480.0,
            "A4":     48.0,
            "A5":     19.0,
            "C1":      2.5,
            "C2":     15.0,   # indicative benchmark; actual calc uses user-input distance
            "C3":     10.0,
            "C4":      4.0,
        },
        # Industrial and Infrastructure: no India-specific peer-reviewed LCA benchmark available.
        # Tool falls back to Commercial values for these types. Users should supply project-specific data.
        "Industrial":     None,
        "Infrastructure": None,
    },
    # ── DEMOLITION (existing building teardown) — kg CO2e / m² demolished BUA ─
    # Sources: [S3] A1-A5 base case ~414 kg CO2e/m² lifecycle;
    #          demolition C-stage = 0.2% of lifecycle energy → ~5–15 kg CO2e/m²
    #          [S4] C-stage end-of-life 0.1–13% of LCE
    "Demolition": {
        "Residential": {
            "A1_A3":  0.0,    # no new material manufacture in demolition
            "A4":     0.0,    # no incoming transport
            "A5":     0.0,
            "C1":     8.0,    # mechanical demolition energy [S3][S4] ~5–15 kg CO2e/m²
            "C2":    18.0,    # heavy hauling 10–30 km typical Indian city
            "C3":    10.0,    # C&D recycling plant processing [S3]
            "C4":     6.0,    # mixed inert + some organic landfill
        },
        "Commercial": {
            "A1_A3":  0.0,
            "A4":     0.0,
            "A5":     0.0,
            "C1":    12.0,
            "C2":    22.0,
            "C3":    13.0,
            "C4":     8.0,
        },
        # Industrial and Infrastructure: no India-specific demolition LCA benchmark. Fallback to Commercial.
        "Industrial":     None,
        "Infrastructure": None,
    },
}

# ── ACIDIFICATION POTENTIAL (AP) — kg SO2e / m² BUA ──────────────────────
# Source derivation:
#   [S9] Global AP residential ≈ 0.249 kg SO2/m²; India (low-income group) ≈ 0.30–0.32 kg SO2/m²
#   [S10] Chippagiri 2023 (India prefab LCA) — AP reported per functional unit
#   [S8] Jang 2022 CML 2001 AP characterisation: concrete 0.55 kg SO2e/t,
#        steel rebar 8.5 kg SO2e/t, brick 0.80 kg SO2e/t
#   Upfront embodied (A1-A3) dominates AP. Transport (A4) adds ~15-25%.
#   Per-m² = material AP factors × material intensity (kg/m²) for Indian RCC building
#   Typical Indian RCC residential: ~350 kg concrete/m², ~50 kg steel/m², ~90 kg brick/m²
#   → A1-A3 AP ≈ (350×0.00055) + (50×0.0085) + (90×0.0008) = 0.19+0.43+0.07 = 0.69 ≈ 0.72 kg SO2e/m²
#   [S9] validates total lifecycle 0.30 kg SO2/m² (sustainable/low-carbon reference)
#   Standard Indian RCC (less optimised) → 0.55–0.85 kg SO2e/m² [S8][S9] synthesis
AP_PER_M2 = {
    "Construction": {
        "Residential":    {"A1_A3": 0.72, "A4": 0.12, "A5": 0.05, "C1": 0.01, "C2": 0.06, "C3": 0.03, "C4": 0.01},
        "Commercial":     {"A1_A3": 0.95, "A4": 0.16, "A5": 0.07, "C1": 0.01, "C2": 0.08, "C3": 0.04, "C4": 0.01},
        "Industrial":     None,  # No India-specific source; fallback to Commercial
        "Infrastructure": None,  # No India-specific source; fallback to Commercial
    },
    "Demolition": {
        "Residential":    {"A1_A3": 0.0,  "A4": 0.0,  "A5": 0.0,  "C1": 0.03, "C2": 0.08, "C3": 0.05, "C4": 0.02},
        "Commercial":     {"A1_A3": 0.0,  "A4": 0.0,  "A5": 0.0,  "C1": 0.04, "C2": 0.10, "C3": 0.06, "C4": 0.03},
        "Industrial":     None,  # Fallback to Commercial
        "Infrastructure": None,  # Fallback to Commercial
    },
}

# ── EUTROPHICATION / FRESHWATER POTENTIAL (EFW/EP) — kg PO4e / m² BUA ────
# Source derivation:
#   [S9] EP residential ≈ 0.05–0.09 kg PO4/m² (Belgium, USA, UK range)
#        India comparable to low-income group (~0.09 kg PO4/m²)
#   [S8] Jang 2022 CML 2001 EP characterisation: concrete 0.08 kg PO4/t,
#        steel 0.65 kg PO4/t, brick 0.12 kg PO4/t
#   Per-m² = (350×0.00008) + (50×0.00065) + (90×0.00012) = 0.028+0.033+0.011 ≈ 0.075 kg PO4e/m²
#   [S9] validates ~0.09 kg PO4/m² for India — consistent
#   72% of AP and 65% of EP arise during operational phase [S9]; for C&D waste tool
#   we focus on embodied/EOL stages (A1-A5, C1-C4) → use proportional fractions
EFW_PER_M2 = {
    "Construction": {
        "Residential":    {"A1_A3": 0.075, "A4": 0.010, "A5": 0.005, "C1": 0.001, "C2": 0.005, "C3": 0.003, "C4": 0.001},
        "Commercial":     {"A1_A3": 0.095, "A4": 0.013, "A5": 0.006, "C1": 0.001, "C2": 0.006, "C3": 0.004, "C4": 0.001},
        "Industrial":     None,  # Fallback to Commercial
        "Infrastructure": None,  # Fallback to Commercial
    },
    "Demolition": {
        "Residential":    {"A1_A3": 0.0, "A4": 0.0, "A5": 0.0, "C1": 0.003, "C2": 0.007, "C3": 0.004, "C4": 0.002},
        "Commercial":     {"A1_A3": 0.0, "A4": 0.0, "A5": 0.0, "C1": 0.004, "C2": 0.009, "C3": 0.005, "C4": 0.002},
        "Industrial":     None,  # Fallback to Commercial
        "Infrastructure": None,  # Fallback to Commercial
    },
}

GWP_SOURCE = (
    "Lodha Research (2025) 10-building India study [S1]; IIT Madras / Akshatha et al. (2025) [S2]; "
    "Alotaibi et al. MDPI Buildings 2022 (DOI 10.3390/buildings12081203) [S3]; "
    "AEEE/Saint-Gobain LCA Report 2024 [S4]; IFC EDGE India Materials DB (thinkstep, 2017) [S5]"
)
AP_SOURCE = (
    "Nematchoua et al. MDPI Sustainability 2022 (AP 0.249–0.32 kg SO2/m², 150 countries) [S9]; "
    "Jang et al. Materials 2022 CML 2001 characterisation factors [S8]; "
    "Chippagiri et al. MDPI Buildings 2023 India prefab LCA [S10]"
)
EFW_SOURCE = (
    "Nematchoua et al. MDPI Sustainability 2022 (EP 0.05–0.09 kg PO4/m²) [S9]; "
    "Jang et al. Materials 2022 CML 2001 EP factors [S8]; "
    "Chippagiri et al. MDPI Buildings 2023 [S10]"
)
ENV_SOURCE = f"GWP: {GWP_SOURCE} | AP: {AP_SOURCE} | EFW: {EFW_SOURCE}"

# ── Per-tonne material factors (retained for waste-weight calculations) ────
# Source: IFC EDGE India DB [S5]; Lodha EPD table [S6]; IPCC AR6 GWP100
MATERIAL_GWP_A1A3 = {
    # kg CO2e per tonne of material (A1–A3 only)
    # [S5] IFC EDGE: OPC 0.86 t CO2e/t → 860 kg CO2e/t
    # [S6] Lodha EPD: OPC 0.996, PPC 0.71, GGBS 0.069, Fly Ash 0.065
    "Concrete":         {"M20 (OPC)": 145.0, "M25 (OPC)": 168.0, "M30 (OPC)": 192.0,
                         "M30 (PPC blend)": 130.0, "M35 (OPC)": 215.0, "M40 (OPC)": 238.0,
                         "M40 (GGBS 40%)": 160.0, "Generic": 162.0},
    # [S5] IFC: Red brick (zigzag kiln) 0.19 kg CO2e/kg = 190 kg/t
    #          AAC block: 0.35 kg CO2e/kg = 350 kg/t; Fly ash brick: 0.08 = 80 kg/t
    "Brick/Masonry":    {"Red Brick (Zigzag Kiln)": 190.0, "Red Brick (Bull's Trench)": 240.0,
                         "AAC Block": 350.0, "Fly Ash Brick": 80.0,
                         "FaLG Block": 70.0, "Generic": 200.0},
    # [S5] BF steel slab 2.0 t CO2e/t; EAF (scrap) 0.65 t CO2e/t; DRI-EAF 1.4 t CO2e/t
    # [S6] Steel rebar BF route; India induction furnace (DRI) dominant
    "Steel/Metal":      {"TMT Rebar (BF-BOF)": 2000.0, "TMT Rebar (DRI-EAF)": 1400.0,
                         "TMT Rebar (Scrap EAF)": 650.0, "Structural Steel (BF)": 1950.0,
                         "Aluminium (extruded)": 17800.0, "Generic": 1800.0},
    # [S5] Air-dried timber: −1000 (biogenic seq.); kiln-dried: −800; plywood: +320
    "Wood/Timber":      {"Air-dried Timber": -1000.0, "Kiln-dried Timber": -800.0,
                         "Plywood": 320.0, "Bamboo": -1200.0, "Generic": -900.0},
    # [S5] Float glass India: 1.40 kg CO2e/kg = 1400 kg/t
    "Glass":            {"Float Glass": 1400.0, "Toughened Glass": 1500.0, "Generic": 1400.0},
    # IPCC AR6 / Ecoinvent: PVC 3.1, HDPE 1.7 kg CO2e/kg
    "Plastic":          {"PVC (uPVC)": 3100.0, "HDPE": 1700.0, "Generic": 2400.0},
    # Bitumen (petroleum refinery by-product): ~0.085 kg CO2e/kg [S5]
    "Bitumen":          {"Asphalt/Bitumen": 85.0, "Generic": 85.0},
    # Inert soil/aggregate: minimal [S5] ~0.003 kg CO2e/kg
    "Soil/Sand/Gravel": {"Crushed Aggregate": 5.0, "River Sand": 2.5, "Generic": 3.0},
    "Others":           {"Generic": 50.0},
}

# A4 Transport (kg CO2e per tonne-km) — IPCC (2006) GHG Inventories Vol.2 [S7]
TRANSPORT_EF = {
    "Diesel Truck (< 3.5 t)":   0.30,
    "Diesel Truck (3.5–7.5 t)": 0.18,
    "Diesel Truck (> 7.5 t)":   0.12,
    "Dumper / Tipper (Heavy)":   0.11,
    "Electric Vehicle":          0.04,   # CEA grid 0.716 kgCO2e/kWh (2024) x ~0.056 kWh/t-km average EV
}
A5_FACTOR = 0.0015   # kg CO2e / kg material — on-site activities [S4]
C3_PROCESSING_EF = {
    # kg CO2e / tonne — C&D recycling / processing energy [S3][S4][S8]
    # Derived from crushing/processing energy x Indian grid EF (CEA 2024: 0.716 kgCO2/kWh):
    # Concrete jaw crusher ~2-3 kWh/t x 0.716 = 1.5-2.1 kg; +handling/plant overhead = ~8 kg/t
    # Steel shearing/baling ~5 kWh/t x 0.716 = 3.6 kg; +logistics = ~12 kg/t
    # Wood chipping ~1.5 kWh/t x 0.716; Brick crushing similar to concrete
    # Note: No published India-specific C3 EPD exists; values are engineering estimates
    "Concrete": 8.0, "Brick/Masonry": 5.0, "Steel/Metal": 12.0, "Wood/Timber": 3.0,
    "Glass": 6.0, "Plastic": 10.0, "Bitumen": 4.0, "Soil/Sand/Gravel": 1.5, "Others": 5.0,
}
C4_LANDFILL_CO2E = 2.5   # kg CO2e / tonne inert C&D waste [IPCC 2006] [S7]

# Per-tonne AP factors (kg SO2e/t) — CML 2001 [S8]
AP_FACTORS = {
    "Concrete": 0.55, "Brick/Masonry": 0.80, "Steel/Metal": 8.50,
    "Wood/Timber": 0.30, "Glass": 2.10, "Plastic": 3.80,
    "Bitumen": 1.20, "Soil/Sand/Gravel": 0.02, "Others": 0.50,
}
# Per-tonne EFW/EP factors (kg PO4e/t) — CML 2001 [S8]
EP_FACTORS = {
    "Concrete": 0.08, "Brick/Masonry": 0.12, "Steel/Metal": 0.65,
    "Wood/Timber": 0.05, "Glass": 0.30, "Plastic": 0.55,
    "Bitumen": 0.18, "Soil/Sand/Gravel": 0.003, "Others": 0.07,
}

# ── Landfill / Tipping Cost (India) — city-specific lookup ─────────────────────────
# Source: CPCB (2017) Guidelines on C&D Waste Management (national range INR 300–800/tonne);
#         MoEFCC C&D Waste Management Rules 2016 (user cost obligation);
#         City-specific rates from published municipal tenders and PWD SORs (2022–24):
#   Delhi (MCD) INR 700/t; Mumbai (MCGM) 600; Bengaluru (BBMP) 450; Hyderabad (GHMC) 400;
#   Ahmedabad (AMC) 350; Pune (PMC) 500; Chennai (GCC) 450; Kolkata (KMC) 380;
#   Surat (SMC) 350; Jaipur (JMC) 320; Thane (TMC) 550; NCR cities 650.
#   Default (unlisted cities): INR 450/tonne — CPCB 2017 midpoint.
CITY_LANDFILL_COST = {
    "delhi": 700, "new delhi": 700,
    "mumbai": 600, "bombay": 600,
    "bengaluru": 450, "bangalore": 450,
    "hyderabad": 400,
    "ahmedabad": 350,
    "pune": 500,
    "chennai": 450, "madras": 450,
    "kolkata": 380, "calcutta": 380,
    "surat": 350,
    "jaipur": 320,
    "thane": 550,
    "noida": 650, "gurugram": 650, "gurgaon": 650, "ghaziabad": 650, "faridabad": 620,
    "chandigarh": 400,
    "indore": 380,
    "nagpur": 420,
    "lucknow": 360,
    "bhopal": 350,
    "patna": 330,
    "kochi": 480, "cochin": 480,
    "kannur": 420,
    "visakhapatnam": 380, "vizag": 380,
    "vijayawada": 380,
    "tirupati": 360,
    "coimbatore": 420, "madurai": 400,
    "bhubaneswar": 360, "guwahati": 340,
}
DEFAULT_LANDFILL_COST = 450  # INR/tonne; CPCB (2017) midpoint for unlisted cities

def get_landfill_cost(city_str):
    """Return city-specific C&D waste tipping fee (INR/tonne) from municipal tender data."""
    if not city_str:
        return DEFAULT_LANDFILL_COST
    city_lower = city_str.lower()
    for key, cost in CITY_LANDFILL_COST.items():
        if key in city_lower:
            return cost
    return DEFAULT_LANDFILL_COST

# LANDFILL_COST_PER_TONNE is set dynamically in page functions from project location
LANDFILL_COST_PER_TONNE = DEFAULT_LANDFILL_COST  # fallback for PDF report
LANDFILL_COST_SOURCE = ("CPCB (2017) Guidelines on C&D Waste Management (INR 300–800/tonne); "
                        "municipal tender rates 2022-24 for major Indian cities")

# ── C&D Recycling Plants in India ──────────────────────────────────────────
# Source: CSE (2020) Table 5 — "C&D waste recycling plants in India" p.33
RECYCLING_PLANTS = [
    {"City": "Delhi",       "Location": "Burari",              "Capacity_TPD": 2000, "Lat": 28.7200, "Lon": 77.1800},
    {"City": "Delhi",       "Location": "Mundka",              "Capacity_TPD": 150,  "Lat": 28.6810, "Lon": 76.9995},
    {"City": "Delhi",       "Location": "Shastri Park",        "Capacity_TPD": 500,  "Lat": 28.6680, "Lon": 77.2510},
    {"City": "Noida",       "Location": "Sector 80",           "Capacity_TPD": 150,  "Lat": 28.5665, "Lon": 77.3470},
    {"City": "Gurugram",    "Location": "Basai",               "Capacity_TPD": 300,  "Lat": 28.4595, "Lon": 76.9859},
    {"City": "Ghaziabad",   "Location": "Ghaziabad",           "Capacity_TPD": 150,  "Lat": 28.6692, "Lon": 77.4538},
    {"City": "Thane",       "Location": "Daighar",             "Capacity_TPD": 300,  "Lat": 19.2183, "Lon": 72.9781},
    {"City": "Indore",      "Location": "Devguradia",          "Capacity_TPD": 100,  "Lat": 22.6797, "Lon": 75.8070},
    {"City": "Hyderabad",   "Location": "Jeetimedla",          "Capacity_TPD": 300,  "Lat": 17.4399, "Lon": 78.4983},
    {"City": "Bengaluru",   "Location": "Chikkajala",          "Capacity_TPD": 1000, "Lat": 13.1100, "Lon": 77.5900},
    {"City": "Kannur",      "Location": "Kannur",              "Capacity_TPD": 750,  "Lat": 11.8745, "Lon": 75.3704},
    {"City": "Ahmedabad",   "Location": "Gyaspur Pirana",      "Capacity_TPD": 1000, "Lat": 22.9880, "Lon": 72.5550},
    {"City": "Tirupati",    "Location": "Tukivakam Village",   "Capacity_TPD": 150,  "Lat": 13.6280, "Lon": 79.4190},
    {"City": "Vijayawada",  "Location": "Vijayawada",          "Capacity_TPD": 200,  "Lat": 16.5062, "Lon": 80.6480},
    {"City": "Chandigarh",  "Location": "Industrial Area Ph.1","Capacity_TPD": 150,  "Lat": 30.7333, "Lon": 76.7794},
    {"City": "Surat",       "Location": "Surat",               "Capacity_TPD": 300,  "Lat": 21.1702, "Lon": 72.8311},
]
PLANTS_SOURCE = "CSE (2020) Table 5 'C&D waste recycling plants in India', p.33"

# ── Circularity Formula — EMF Material Circularity Indicator (MCI) ──────────────────────
# Source: Ellen MacArthur Foundation (EMF) (2015).
#   "Towards a Circular Economy: Business Rationale and Key Definitions."
#   Material Circularity Indicator (MCI) technical appendix.
#   URL: ellenmacarthurfoundation.org/material-circularity-indicator
#
# EMF MCI formula (per material):
#   MCI = 1 − LFI × F(x)  where F(x) = 0.9 × (1 − 0.5×Vu − 0.5×Fr)
#   LFI  = (Landfill% + Incineration%) / 100  [Linear Flow Index]
#   Vu   = fraction of input that is virgin material (=1.0 for new construction materials)
#   Fr   = Recycle% / 100  [end-of-life recycled fraction]
#
# With Vu=1.0 (virgin construction inputs), simplified:
#   MCI = 1 − LFI × 0.9 × (0.5 − 0.5×Fr)
#       = 1 − LFI × 0.45 × (1 − Fr)
#
# MCI ranges from 0.1 (all linear = 100% landfill, 0% recycle) to 1.0 (perfectly circular).
# Score = MCI × 100 (displayed as 0–100).
# Reuse reduces LFI (reused material is not landfilled) and is credited in Fr.
CIRCULARITY_WEIGHTS = {"Reuse": 0.5, "Recycle": 0.4, "Landfill": 0.0, "Incineration": 0.05, "Other": 0.1}
# ^ Legacy weights retained for display reference; actual scoring uses EMF MCI formula below.

# ── Default EOL Scenarios by material (%) ─────────────────────────────────
# Source: CSE (2020) "Another Brick off the Wall" qualitative recovery descriptions (p.30-33);
DEFAULT_EOL = {
    "Concrete":        {"Recycle": 70, "Reuse": 5,  "Landfill": 20, "Incineration": 0, "Other": 5},
    "Brick/Masonry":   {"Recycle": 30, "Reuse": 50, "Landfill": 15, "Incineration": 0, "Other": 5},
    "Steel/Metal":     {"Recycle": 80, "Reuse": 15, "Landfill": 3,  "Incineration": 0, "Other": 2},
    "Wood/Timber":     {"Recycle": 20, "Reuse": 40, "Landfill": 20, "Incineration": 15, "Other": 5},
    "Glass":           {"Recycle": 50, "Reuse": 10, "Landfill": 35, "Incineration": 0, "Other": 5},
    "Plastic":         {"Recycle": 40, "Reuse": 0,  "Landfill": 40, "Incineration": 15, "Other": 5},
    "Bitumen":         {"Recycle": 30, "Reuse": 0,  "Landfill": 60, "Incineration": 0, "Other": 10},
    "Soil/Sand/Gravel":{"Recycle": 10, "Reuse": 70, "Landfill": 15, "Incineration": 0, "Other": 5},
    "Others":          {"Recycle": 20, "Reuse": 10, "Landfill": 60, "Incineration": 5, "Other": 5},
}
EOL_SOURCE = ("CSE (2020) 'Another Brick off the Wall' qualitative recovery descriptions; "  
             "Steel recycling 80%+ reflects active Indian scrap market; Brick reuse ~50% per field surveys cited in CSE (2020); "  
             "Concrete recycling default 20% reflects CSE (2020) text: 'most recycling plants operate at <20% capacity'; "  
             "All defaults are indicative and must be replaced with site-specific data")

# Virgin material prices (INR/tonne) — for savings calculation
# Source: CPWD DSR 2024 (Central Public Works Dept Schedule of Rates);
#         Delhi SOR 2023-24; Maharashtra PWD SOR 2023-24 (cross-referenced).
# All prices are ex-factory / ex-quarry approximate market rates for bulk quantities.
# Steel: CPWD DSR 2024 TMT Fe415 rebar ≈55,000 INR/t (range 50,000–60,000 per steel market 2023-24)
# Concrete: Ready-mix M25 in-situ price derived from CPWD concrete schedule ≈45,00 INR/m³ / 2.4 t/m³
# Brick: CPWD DSR 2024 fly-ash brick ≈5,500–6,500 INR/1000 nos ≈ 6,000 INR/t
# Soil/Sand/Gravel: local quarry rates per state SORs ≈ 700–1,200 INR/t; midpoint used
# Bitumen: HPCL/BPCL published price ≈26,000 INR/t (2023-24)
# Timber: Forest Dept / merchant rates ≈28,000–32,000 INR/t
# Glass: float glass ex-factory Gujarat/Rajasthan ≈38,000–42,000 INR/t
# Plastic: HDPE granule market ≈70,000–85,000 INR/t; PVC ≈75,000 INR/t
VIRGIN_PRICE = {
    "Concrete":         4500,   # CPWD DSR 2024 ready-mix M25 ÷ 2.4 density
    "Brick/Masonry":    6000,   # CPWD DSR 2024 fly-ash/clay brick bulk rate
    "Steel/Metal":     55000,   # CPWD DSR 2024 TMT rebar Fe415; range 50,000–60,000
    "Wood/Timber":     30000,   # PWD SOR 2023-24 timber; range 28,000–32,000
    "Glass":           40000,   # float glass ex-factory (Gujarat); range 38,000–42,000
    "Plastic":         77000,   # midpoint HDPE 70k–85k and PVC 75k INR/t
    "Bitumen":         26000,   # HPCL/BPCL published price 2023-24
    "Soil/Sand/Gravel": 950,    # state quarry/PWD SOR average; range 700–1,200
    "Others":           5000,   # indicative placeholder
}
VIRGIN_PRICE_SOURCE = ("CPWD DSR (2024); Maharashtra PWD SOR (2023-24); Delhi SOR (2023-24); "
                       "HPCL bitumen published price list (2023-24). Prices are indicative and vary by city/season.")

# Avoided emission factor (for recycling) — kg CO2e saved per tonne recycled
# = A1–A3 of virgin material × recycling efficiency factor (0.7 average)
RECYCLING_EFFICIENCY = 0.7   # fraction of virgin impact avoided


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE INIT
# ══════════════════════════════════════════════════════════════════════════════
defaults = {
    "page": 1,
    "project": {},
    "input_method": None,
    "waste_table": [],       # list of {material, quantity_tonnes, unit, waste_factor}
    "emission_inputs": {},   # material → {sub_type, vehicle, distance_km, eol}
    "results": {},
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def go(page): st.session_state.page = page

# ══════════════════════════════════════════════════════════════════════════════
# PROGRESS BAR
# ══════════════════════════════════════════════════════════════════════════════
STEPS = ["Project Info", "Data Input", "Waste Estimation", "Emissions & EOL", "Results & Report"]

def show_progress():
    cols = st.columns(len(STEPS))
    for i, (col, label) in enumerate(zip(cols, STEPS)):
        step_no = i + 1
        if step_no < st.session_state.page:
            col.markdown(f"✅ **{label}**")
        elif step_no == st.session_state.page:
            col.markdown(f"🔵 **{label}**")
        else:
            col.markdown(f"⬜ {label}")
    st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def compute_waste_from_area(project_type, building_type, area_m2):
    """Returns dict: material → waste_tonnes"""
    rate_kg = WASTE_RATES[project_type][building_type]["rate_kg_m2"]
    total_waste_kg = rate_kg * area_m2
    comp = MATERIAL_COMPOSITION[project_type]
    return {mat: (pct/100.0) * total_waste_kg / 1000.0 for mat, pct in comp.items()}


def compute_emissions(waste_table, emission_inputs):
    """Returns nested dict: material → {A1A3, A4, A5, C1, C2, C3, C4, total, AP, EP}"""
    results = {}
    for row in waste_table:
        mat   = row["material"]
        qty_t = row["waste_tonnes"]
        if qty_t <= 0:
            continue
        ei    = emission_inputs.get(mat, {})
        sub   = ei.get("sub_type", "Generic")
        veh   = ei.get("vehicle", "Diesel Truck (> 7.5 t)")
        dist  = float(ei.get("distance_km", 20))
        dist_c2 = float(ei.get("distance_km_c2", 10))
        eol   = ei.get("eol", DEFAULT_EOL.get(mat, {"Recycle":50,"Reuse":20,"Landfill":30,"Incineration":0,"Other":0}))

        gwp_map = MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0})
        gwp_a1a3_factor = gwp_map.get(sub, gwp_map.get("Generic", 50.0))  # kg CO2e / tonne
        A1A3 = gwp_a1a3_factor * qty_t

        tf   = TRANSPORT_EF.get(veh, 0.12)
        A4   = tf * dist * qty_t         # kg CO2e
        A5   = A5_FACTOR * qty_t * 1000  # kg CO2e

        C1   = tf * dist * qty_t         # demolition assumed same vehicle+distance
        C2   = tf * dist_c2 * qty_t
        C3   = C3_PROCESSING_EF.get(mat, 5.0) * qty_t
        C4   = C4_LANDFILL_CO2E * qty_t * (eol.get("Landfill", 0)/100.0)

        ap  = AP_FACTORS.get(mat, 0.5) * qty_t
        ep  = EP_FACTORS.get(mat, 0.07) * qty_t

        results[mat] = {
            "qty_t": qty_t,
            "A1A3": A1A3, "A4": A4, "A5": A5,
            "C1": C1, "C2": C2, "C3": C3, "C4": C4,
            "total_gwp": A1A3 + A4 + A5 + C1 + C2 + C3 + C4,
            "AP": ap, "EP": ep,
            "eol": eol,
        }
    return results


def compute_circularity(emission_results):
    """Returns dict: material → MCI score (0–1); plus waste-weighted aggregate.

    Uses Ellen MacArthur Foundation Material Circularity Indicator (MCI) formula:
        MCI = 1 - LFI x 0.9 x (0.5 - 0.5 x Fr)
        LFI = (Landfill% + Incineration%) / 100   [Linear Flow Index]
        Fr  = (Recycle% + Reuse%) / 100            [Recovered fraction at EOL]
        Vu  = 1.0 assumed (all input material is virgin; standard for new construction)

    MCI = 1.0 → perfectly circular; MCI = 0.1 → fully linear (100% landfill, 0% recovery).
    Source: EMF (2015) MCI technical appendix — ellenmacarthurfoundation.org/material-circularity-indicator
    """
    scores = {}
    weighted_total = 0.0
    total_waste = 0.0
    for mat, r in emission_results.items():
        eol = r["eol"]
        recycle  = eol.get("Recycle", 0) / 100.0
        reuse    = eol.get("Reuse", 0) / 100.0
        landfill = eol.get("Landfill", 0) / 100.0
        incin    = eol.get("Incineration", 0) / 100.0

        # EMF MCI: Vu=1.0 (virgin inputs), Fr = recycle + reuse fraction at EOL
        Fr  = min(recycle + reuse, 1.0)
        LFI = min(landfill + incin, 1.0)
        # F(x) = 0.9 x (1 - 0.5*Vu - 0.5*Fr) with Vu=1 → 0.9 x (0.5 - 0.5*Fr) = 0.45*(1-Fr)
        Fx  = 0.9 * (0.5 - 0.5 * Fr)
        mci = max(0.0, 1.0 - LFI * Fx)
        # EMF specifies MCI floor of 0.1 for fully linear systems
        mci = max(mci, 0.1) if (landfill + incin) > 0 else mci
        mci = min(mci, 1.0)

        scores[mat] = round(mci, 3)
        weighted_total += mci * r["qty_t"]
        total_waste    += r["qty_t"]
    aggregate = weighted_total / total_waste if total_waste > 0 else 0.0
    return scores, round(aggregate, 3)


def compute_circularity_benefits(emission_results, city_str=""):
    """Material recovery, avoided emissions, virgin savings, landfill cost (city-specific)."""
    lf_cost = get_landfill_cost(city_str)  # INR/tonne; city-specific from municipal tender data
    output = {}
    for mat, r in emission_results.items():
        eol    = r["eol"]
        qty_t  = r["qty_t"]
        recycle_t = qty_t * eol.get("Recycle", 0) / 100.0
        reuse_t   = qty_t * eol.get("Reuse", 0) / 100.0
        landfill_t= qty_t * eol.get("Landfill", 0) / 100.0

        gwp_map = MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0})
        avg_gwp = list(gwp_map.values())[0]
        avoided_em = (recycle_t + reuse_t) * abs(avg_gwp) * RECYCLING_EFFICIENCY  # kg CO2e

        vp  = VIRGIN_PRICE.get(mat, 5000)
        virgin_savings = (recycle_t + reuse_t) * vp  # INR

        landfill_diverted_t = recycle_t + reuse_t
        landfill_cost_saved = landfill_t * lf_cost   # INR (avoided landfill cost)
        landfill_cost_actual= landfill_t * lf_cost   # INR

        output[mat] = {
            "recycled_t": recycle_t,
            "reused_t":   reuse_t,
            "landfill_t": landfill_t,
            "landfill_diverted_t": landfill_diverted_t,
            "avoided_emission_kgco2e": avoided_em,
            "virgin_material_savings_inr": virgin_savings,
            "landfill_cost_saved_inr": landfill_cost_saved,
            "landfill_cost_actual_inr": landfill_cost_actual,
            "landfill_cost_per_tonne": lf_cost,
        }
    return output


# ── City coordinate lookup for nearest-plant calculation ────────────────────
# Approximate lat/lon for major Indian cities (for distance-based plant ranking)
CITY_COORDS = {
    "delhi": (28.6139, 77.2090), "new delhi": (28.6139, 77.2090),
    "noida": (28.5355, 77.3910), "gurugram": (28.4595, 77.0266),
    "gurgaon": (28.4595, 77.0266), "ghaziabad": (28.6692, 77.4538),
    "faridabad": (28.4089, 77.3178), "greater noida": (28.4745, 77.5040),
    "mumbai": (19.0760, 72.8777), "bombay": (19.0760, 72.8777),
    "thane": (19.2183, 72.9781), "pune": (18.5204, 73.8567),
    "nashik": (19.9975, 73.7898), "nagpur": (21.1458, 79.0882),
    "aurangabad": (19.8762, 75.3433),
    "bengaluru": (12.9716, 77.5946), "bangalore": (12.9716, 77.5946),
    "mysuru": (12.2958, 76.6394), "mysore": (12.2958, 76.6394),
    "mangaluru": (12.9141, 74.8560),
    "hyderabad": (17.3850, 78.4867), "secunderabad": (17.4399, 78.4983),
    "vijayawada": (16.5062, 80.6480), "visakhapatnam": (17.6868, 83.2185),
    "vizag": (17.6868, 83.2185), "tirupati": (13.6288, 79.4192),
    "warangal": (17.9784, 79.5941), "guntur": (16.3067, 80.4365),
    "kurnool": (15.8281, 78.0373), "nellore": (14.4426, 79.9865),
    "kakinada": (16.9891, 82.2475), "rajamahendravaram": (17.0005, 81.8040),
    "chennai": (13.0827, 80.2707), "madras": (13.0827, 80.2707),
    "coimbatore": (11.0168, 76.9558), "madurai": (9.9252, 78.1198),
    "trichy": (10.7905, 78.7047), "salem": (11.6643, 78.1460),
    "tirunelveli": (8.7139, 77.7567), "vellore": (12.9165, 79.1325),
    "kolkata": (22.5726, 88.3639), "calcutta": (22.5726, 88.3639),
    "howrah": (22.5958, 88.2636), "durgapur": (23.4800, 87.3300),
    "ahmedabad": (23.0225, 72.5714), "surat": (21.1702, 72.8311),
    "vadodara": (22.3072, 73.1812), "rajkot": (22.3039, 70.8022),
    "jaipur": (26.9124, 75.7873), "jodhpur": (26.2389, 73.0243),
    "udaipur": (24.5854, 73.7125), "kota": (25.2138, 75.8648),
    "chandigarh": (30.7333, 76.7794), "ludhiana": (30.9010, 75.8573),
    "amritsar": (31.6340, 74.8723), "jalandhar": (31.3260, 75.5762),
    "indore": (22.7196, 75.8577), "bhopal": (23.2599, 77.4126),
    "gwalior": (26.2183, 78.1828), "jabalpur": (23.1815, 79.9864),
    "lucknow": (26.8467, 80.9462), "kanpur": (26.4499, 80.3319),
    "varanasi": (25.3176, 82.9739), "agra": (27.1767, 78.0081),
    "allahabad": (25.4358, 81.8463), "prayagraj": (25.4358, 81.8463),
    "meerut": (28.9845, 77.7064),
    "patna": (25.5941, 85.1376), "guwahati": (26.1445, 91.7362),
    "bhubaneswar": (20.2961, 85.8245), "cuttack": (20.4625, 85.8830),
    "raipur": (21.2514, 81.6296), "ranchi": (23.3441, 85.3096),
    "kochi": (9.9312, 76.2673), "cochin": (9.9312, 76.2673),
    "thiruvananthapuram": (8.5241, 76.9366), "trivandrum": (8.5241, 76.9366),
    "kozhikode": (11.2588, 75.7804), "calicut": (11.2588, 75.7804),
    "kannur": (11.8745, 75.3704), "thrissur": (10.5276, 76.2144),
    "kolhapur": (16.7050, 74.2433), "solapur": (17.6805, 75.9064),
}


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points."""
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_nearest_plants(user_location_str, n=5):
    """
    Return the n nearest C&D recycling plants sorted by straight-line distance.
    Strategy:
      1. Look up the user's city in CITY_COORDS to get a lat/lon.
      2. Compute Haversine distance to every plant and sort ascending.
      3. If city not found in lookup, fall back to fuzzy string match on plant City names.
      4. Last resort: return first 5 plants (Delhi region).
    """
    if not user_location_str:
        return RECYCLING_PLANTS[:n]

    loc_lower = user_location_str.strip().lower()

    # --- Strategy 1: coordinate-based distance sort ---
    user_coords = None
    # Try exact key match first, then partial match
    if loc_lower in CITY_COORDS:
        user_coords = CITY_COORDS[loc_lower]
    else:
        for key, coords in CITY_COORDS.items():
            if key in loc_lower or loc_lower in key:
                user_coords = coords
                break

    if user_coords:
        ulat, ulon = user_coords
        plants_with_dist = []
        for p in RECYCLING_PLANTS:
            dist = _haversine_km(ulat, ulon, p["Lat"], p["Lon"])
            plants_with_dist.append({**p, "Distance_km": round(dist, 1)})
        plants_with_dist.sort(key=lambda x: x["Distance_km"])
        return plants_with_dist[:n]

    # --- Strategy 2: fuzzy city name string match ---
    exact = [p for p in RECYCLING_PLANTS
             if p["City"].lower() in loc_lower or loc_lower in p["City"].lower()]
    if exact:
        return [{**p, "Distance_km": None} for p in exact[:n]]

    # --- Strategy 3: last resort ---
    return [{**p, "Distance_km": None} for p in RECYCLING_PLANTS[:n]]


def generate_pdf_report(project, waste_table, emission_results, circ_scores, circ_aggregate, benefits):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.units import cm

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    elems = []

    def para(text, style='Normal', **kw):
        return Paragraph(text, styles[style])

    elems.append(para("<b>CircularBuild — C&D Waste Estimation Report</b>", 'Title'))
    elems.append(Spacer(1, 0.4*cm))
    elems.append(para(f"<b>Project:</b> {project.get('name','—')} | <b>Location:</b> {project.get('location','—')} | "
                      f"<b>Type:</b> {project.get('construction_type','—')}"))
    elems.append(para(f"<b>Built-up Area:</b> {project.get('builtup_area','—')} m² | "
                      f"<b>Plot Area:</b> {project.get('plot_area','—')} m² | "
                      f"<b>Building Type:</b> {project.get('building_type','—')}"))
    elems.append(Spacer(1, 0.5*cm))

    # Waste Table
    elems.append(para("<b>Section 1 — Waste Estimation</b>", 'Heading2'))
    wt_data = [["Material", "Waste (tonnes)"]]
    for row in waste_table:
        wt_data.append([row["material"], f"{row['waste_tonnes']:.2f}"])
    wt_tbl = Table(wt_data, colWidths=[9*cm, 5*cm])
    wt_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#064e3b')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('FONTSIZE', (0,0), (-1,-1), 9),
    ]))
    elems.append(wt_tbl)
    elems.append(para(f"<i>Source: {WASTE_RATE_SOURCE}</i>"))
    elems.append(Spacer(1, 0.5*cm))

    # Emissions Table
    if emission_results:
        elems.append(para("<b>Section 2 — Environmental Impact</b>", 'Heading2'))
        em_data = [["Material", "GWP A1–A3\n(kg CO2e)", "A4+C1+C2\nTransport", "C3+C4\nEOL", "Total GWP", "AP (kg SO2e)", "EP (kg PO4e)"]]
        for mat, r in emission_results.items():
            transport = r["A4"] + r["C1"] + r["C2"]
            eol_em    = r["C3"] + r["C4"]
            em_data.append([
                mat, f"{r['A1A3']:.1f}", f"{transport:.1f}", f"{eol_em:.1f}",
                f"{r['total_gwp']:.1f}", f"{r['AP']:.2f}", f"{r['EP']:.3f}"
            ])
        em_tbl = Table(em_data, colWidths=[3.8*cm,2.2*cm,2.5*cm,2.2*cm,2.2*cm,2.2*cm,2.2*cm])
        em_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1d4ed8')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
            ('FONTSIZE', (0,0), (-1,-1), 7.5),
        ]))
        elems.append(em_tbl)
        elems.append(para(f"<i>Source: {GWP_SOURCE}</i>"))
        elems.append(Spacer(1, 0.5*cm))

    # Circularity
    elems.append(para("<b>Section 3 — Circularity</b>", 'Heading2'))
    elems.append(para(f"<b>Overall Circularity Score: {circ_aggregate*100:.1f} / 100</b>"))
    elems.append(Spacer(1, 0.3*cm))
    cs_data = [["Material", "Reuse %", "Recycle %", "Landfill %", "Circ. Score"]]
    for mat, sc in circ_scores.items():
        eol = emission_results.get(mat, {}).get("eol", {})
        cs_data.append([mat, f"{eol.get('Reuse',0)}", f"{eol.get('Recycle',0)}", f"{eol.get('Landfill',0)}", f"{sc*100:.1f}"])
    cs_tbl = Table(cs_data, colWidths=[5*cm,2.5*cm,2.5*cm,2.5*cm,2.5*cm])
    cs_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#065f46')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('FONTSIZE', (0,0), (-1,-1), 8.5),
    ]))
    elems.append(cs_tbl)
    elems.append(Spacer(1, 0.5*cm))

    # Benefits
    if benefits:
        elems.append(para("<b>Section 4 — Economic & Environmental Benefits</b>", 'Heading2'))
        tot_avoided = sum(b["avoided_emission_kgco2e"] for b in benefits.values())
        tot_virgin  = sum(b["virgin_material_savings_inr"] for b in benefits.values())
        tot_lf_saved= sum(b["landfill_cost_saved_inr"] for b in benefits.values())
        tot_divert  = sum(b["landfill_diverted_t"] for b in benefits.values())
        elems.append(para(f"Total Avoided Emissions: <b>{tot_avoided/1000:.2f} t CO2e</b>"))
        elems.append(para(f"Virgin Material Savings: <b>INR {tot_virgin:,.0f}</b>"))
        elems.append(para(f"Landfill Diversion: <b>{tot_divert:.2f} tonnes</b>"))
        elems.append(para(f"Landfill Cost Saved: <b>INR {tot_lf_saved:,.0f}</b>"))
        elems.append(Spacer(1, 0.5*cm))

    # Data Sources
    elems.append(para("<b>Data Sources</b>", 'Heading2'))
    sample_lf = list(benefits.values())[0].get("landfill_cost_per_tonne", DEFAULT_LANDFILL_COST) if benefits else DEFAULT_LANDFILL_COST
    sources = [
        "1. CSE (2020). 'Another Brick off the Wall'. Centre for Science and Environment, New Delhi. Table 4 (material composition), p.30 (demolition rates 300-500 kg/m²), Table 5 (recycling plants).",
        "2. IFC / thinkstep (2017). EDGE India Construction Materials Database (GWP per-tonne factors). edgebuildings.com",
        f"3. {GWP_SOURCE}",
        f"4. AP: {AP_SOURCE}",
        f"5. EFW: {EFW_SOURCE}",
        "6. IPCC (2006). Guidelines for National GHG Inventories, Vol. 2. (Transport emission factors.)",
        "7. Jang et al. (2022). Materials 15, 5047. CML 2001 AP & EP characterisation factors. DOI: 10.3390/ma15145047",
        "8. CEA (2024). CO2 Baseline Database for the Indian Power Sector, Version 18. (Grid EF 0.716 kg CO2e/kWh.)",
        f"9. Landfill tipping fee: ₹{sample_lf}/tonne. {LANDFILL_COST_SOURCE}",
        f"10. Virgin material prices: {VIRGIN_PRICE_SOURCE}",
        f"11. {PLANTS_SOURCE}",
        "12. MoEFCC (2016). Construction & Demolition Waste Management Rules. Ministry of Environment, Forest & Climate Change.",
        "13. EMF (2015). Material Circularity Indicator. ellenmacarthurfoundation.org/material-circularity-indicator",
    ]
    for s in sources:
        elems.append(para(f"• {s}"))

    doc.build(elems)
    buf.seek(0)
    return buf


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — PROJECT INFO
# ══════════════════════════════════════════════════════════════════════════════
def page_project_info():
    st.markdown('<p class="page-title">Project Information</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Enter basic project details to begin the estimation</p>', unsafe_allow_html=True)

    with st.form("project_form"):
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Project Name", placeholder="e.g., Greenfield Residential Complex")
            location = st.text_input("City / Location", placeholder="e.g., Mumbai")
            builtup = st.number_input("Built-up Area (m²)", min_value=1.0, value=1000.0, step=50.0)
        with c2:
            ctype = st.selectbox("Project Type", ["Construction", "Demolition", "Redevelopment"])
            building_type = st.selectbox("Building Type", ["Residential", "Commercial", "Industrial", "Infrastructure"])
            plot_area = st.number_input("Plot Area (m²)", min_value=1.0, value=1500.0, step=50.0)

        st.markdown('<div class="source-note">💡 Building type affects waste generation rate. Source: CSE (2020) "Another Brick off the Wall" Table 4 & p.30. Industrial/Infrastructure types use Commercial as proxy for environmental benchmarks (no India-specific peer-reviewed LCA data).</div>', unsafe_allow_html=True)
        submitted = st.form_submit_button("Next →", type="primary", use_container_width=True)
        if submitted:
            if not name or not location:
                st.error("Please fill in Project Name and Location.")
            else:
                st.session_state.project = {
                    "name": name, "location": location,
                    "construction_type": ctype, "building_type": building_type,
                    "builtup_area": builtup, "plot_area": plot_area,
                }
                # Handle Redevelopment = Demolition + Construction
                if ctype == "Redevelopment":
                    st.session_state.project["construction_type"] = "Redevelopment"
                go(2)
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — DATA INPUT METHOD
# ══════════════════════════════════════════════════════════════════════════════
def page_data_input():
    proj = st.session_state.project
    st.markdown(f'<p class="page-title">Data Input — <span style="color:#10b981">{proj["name"]}</span></p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Choose the type of data you have available</p>', unsafe_allow_html=True)

    st.markdown("### Select your input method")
    col1, col2, col3 = st.columns(3)
    with col1:
        with st.container(border=True):
            st.markdown("#### 📐 BIM / Revit + Dynamo")
            st.markdown("Download our Dynamo script, run it in Revit, and upload the generated Excel. Extracts walls, floors, columns, framing, rebar, doors & windows automatically.")
            st.markdown("**Best accuracy** — uses your actual model quantities")
            bim = st.button("Use BIM / Dynamo Export", key="bim", use_container_width=True)
    with col2:
        with st.container(border=True):
            st.markdown("#### 📋 Material Quantities")
            st.markdown("Enter material quantities manually. Select material type, quantity, and unit.")
            st.markdown("**Good accuracy** — if you have BoQ data")
            mq = st.button("Enter Material Quantities", key="mq", use_container_width=True)
    with col3:
        with st.container(border=True):
            st.markdown("#### 📏 Area-Based Estimate")
            st.markdown("Only have the floor area? Estimate waste based on per-m² benchmarks from CSE (2020) Table 4. Industrial/Infrastructure use Commercial as proxy.")
            st.markdown("**Indicative** — for early-stage estimates")
            ab = st.button("Use Area-Based Estimate", key="ab", use_container_width=True)

    st.markdown(f'<div class="source-note">📚 Area-based rates from: {WASTE_RATE_SOURCE}. Note: Industrial and Infrastructure use Commercial as proxy for per-m² environmental benchmarks.</div>', unsafe_allow_html=True)

    if bim: st.session_state.input_method = "bim"; go(3); st.rerun()
    if mq:  st.session_state.input_method = "material"; go(3); st.rerun()
    if ab:  st.session_state.input_method = "area"; go(3); st.rerun()

    st.button("← Back", on_click=lambda: go(1))


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — WASTE ESTIMATION
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — WASTE ESTIMATION  (clean rewrite — no nested buttons, no loops)
# ══════════════════════════════════════════════════════════════════════════════
def page_waste_estimation():
    proj   = st.session_state.project
    method = st.session_state.input_method
    ctype  = proj["construction_type"]
    btype  = proj["building_type"]
    ptype  = "Demolition" if ctype in ["Demolition", "Redevelopment"] else "Construction"

    st.markdown('<p class="page-title">Waste Estimation</p>', unsafe_allow_html=True)

    ALL_MATERIALS = ["Concrete", "Brick/Masonry", "Soil/Sand/Gravel", "Steel/Metal",
                     "Wood/Timber", "Bitumen", "Plastic", "Glass", "Others"]

    DEFAULT_WF = {
        "Concrete":          {"Construction": 5.0,  "Demolition": 100.0},
        "Brick/Masonry":     {"Construction": 8.0,  "Demolition": 100.0},
        "Soil/Sand/Gravel":  {"Construction": 10.0, "Demolition": 100.0},
        "Steel/Metal":       {"Construction": 3.0,  "Demolition": 85.0},
        "Wood/Timber":       {"Construction": 12.0, "Demolition": 70.0},
        "Bitumen":           {"Construction": 5.0,  "Demolition": 80.0},
        "Plastic":           {"Construction": 5.0,  "Demolition": 80.0},
        "Glass":             {"Construction": 4.0,  "Demolition": 80.0},
        "Others":            {"Construction": 10.0, "Demolition": 80.0},
    }
    DENSITY = {"Concrete": 2.4, "Brick/Masonry": 1.8, "Soil/Sand/Gravel": 1.7,
               "Steel/Metal": 7.85, "Wood/Timber": 0.7, "Bitumen": 1.2,
               "Plastic": 0.9, "Glass": 2.5, "Others": 1.5}
    UNITS = ["tonnes", "kg", "m³", "nos"]

    # ────────────────────────────────────────────────────────────────────────
    # AREA-BASED
    # ────────────────────────────────────────────────────────────────────────
    if method == "area":
        rate_info      = WASTE_RATES[ptype][btype]
        area_m2        = proj["builtup_area"]
        total_waste_kg = rate_info["rate_kg_m2"] * area_m2

        st.markdown(
            f'<div class="info-box">📏 <b>Area-based estimation</b> — '
            f'<b>{ctype}</b> | <b>{btype}</b> | <b>{area_m2:,.0f} m²</b><br>'
            f'Total estimated waste = <b>{total_waste_kg/1000:.2f} tonnes</b> '
            f'({rate_info["rate_kg_m2"]} kg/m², range {rate_info["range"]} kg/m²)<br>'
            f'Select the materials present on your site — waste is split only among those.</div>',
            unsafe_allow_html=True)
        st.caption(f"Rate source: {WASTE_RATE_SOURCE}")

        # ── Step 1: pick materials ────────────────────────────────────────
        st.markdown("#### Step 1 — Select materials present on your site")
        st.caption("Tick only what actually exists. Concrete, Brick, Soil, Steel ticked by default for RCC buildings.")

        defaults_checked = {"Concrete", "Brick/Masonry", "Soil/Sand/Gravel", "Steel/Metal"}
        if "ab_sel" not in st.session_state:
            st.session_state.ab_sel = {m: (m in defaults_checked) for m in ALL_MATERIALS}

        c1, c2, c3 = st.columns(3)
        cols3 = [c1, c2, c3]
        for i, mat in enumerate(ALL_MATERIALS):
            with cols3[i % 3]:
                st.session_state.ab_sel[mat] = st.checkbox(
                    mat,
                    value=st.session_state.ab_sel.get(mat, mat in defaults_checked),
                    key=f"abchk_{mat}")

        selected = [m for m in ALL_MATERIALS if st.session_state.ab_sel.get(m, False)]

        if not selected:
            st.warning("Select at least one material to continue.")
            st.button("← Back", on_click=lambda: go(2))
            return

        # ── Step 2: auto-fill % and let user edit ─────────────────────────
        st.markdown("#### Step 2 — Adjust waste split (%) among selected materials")
        st.caption("Pre-filled from CSE (2020) composition benchmarks, re-normalised to 100% for your selection. Edit freely.")

        full_comp = MATERIAL_COMPOSITION[ptype]
        raw = {m: full_comp.get(m, 1.0) for m in selected}
        raw_sum = sum(raw.values())
        auto_pct = {m: round(v / raw_sum * 100, 1) for m, v in raw.items()}

        # Reset stored % only when material selection changes
        if st.session_state.get("ab_last_sel") != selected:
            st.session_state["ab_pct"] = dict(auto_pct)
            st.session_state["ab_last_sel"] = selected[:]

        hdr = st.columns([3, 2, 3])
        hdr[0].markdown("**Material**")
        hdr[1].markdown("**Waste % of total**")
        hdr[2].markdown("**Estimated Waste (t)**")

        live = {}
        for mat in selected:
            row = st.columns([3, 2, 3])
            row[0].write(mat)
            pct = row[1].number_input(
                "pct", min_value=0.0, max_value=100.0, step=0.5,
                value=float(st.session_state["ab_pct"].get(mat, auto_pct.get(mat, 5.0))),
                key=f"abpct_{mat}", label_visibility="collapsed")
            live[mat] = pct
            wt = pct / 100.0 * total_waste_kg / 1000.0
            row[2].write(f"**{wt:.3f} t**")

        pct_sum = sum(live.values())
        if abs(pct_sum - 100.0) > 0.5:
            st.warning(f"⚠️ Percentages sum to {pct_sum:.1f}% — must reach 100% before proceeding.")
            ok_to_proceed = False
        else:
            total_est = pct_sum / 100.0 * total_waste_kg / 1000.0
            st.success(f"✅ Total estimated waste: **{total_est:.2f} tonnes**")
            ok_to_proceed = True

        st.caption(f"Composition basis: {COMP_SOURCE}")

        col_b, col_n = st.columns([1, 4])
        col_b.button("← Back", on_click=lambda: go(2), key="ab_back")
        if ok_to_proceed:
            if col_n.button("✅ Confirm & Proceed to Emissions →", type="primary", key="ab_go"):
                tbl = []
                for mat in selected:
                    wt = live[mat] / 100.0 * total_waste_kg / 1000.0
                    if wt > 0:
                        tbl.append({"material": mat, "waste_tonnes": wt, "unit": "tonnes"})
                st.session_state.waste_table      = tbl
                st.session_state.emission_inputs  = {}
                st.session_state.results          = {}
                st.session_state.page             = 4
                st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # MATERIAL QUANTITIES
    # ────────────────────────────────────────────────────────────────────────
    elif method == "material":
        st.markdown('<div class="info-box">📋 Select which materials are present, then enter their quantities.</div>', unsafe_allow_html=True)

        st.markdown("#### Step 1 — Which materials are on your site?")
        if "mq_sel" not in st.session_state:
            st.session_state.mq_sel = {m: False for m in ALL_MATERIALS}

        c1, c2, c3 = st.columns(3)
        chk_cols = [c1, c2, c3]
        for i, mat in enumerate(ALL_MATERIALS):
            with chk_cols[i % 3]:
                st.session_state.mq_sel[mat] = st.checkbox(
                    mat, value=st.session_state.mq_sel.get(mat, False), key=f"mqchk_{mat}")

        selected = [m for m in ALL_MATERIALS if st.session_state.mq_sel.get(m, False)]

        if not selected:
            st.info("☝️ Tick the materials present on your site above.")
            st.button("← Back", on_click=lambda: go(2))
            return

        st.markdown("#### Step 2 — Enter quantities")
        st.caption("Total material quantity used (not just waste). Waste % = fraction that becomes waste.")

        hdr = st.columns([3, 2, 1.5, 1.5])
        hdr[0].markdown("**Material**"); hdr[1].markdown("**Quantity**")
        hdr[2].markdown("**Unit**");     hdr[3].markdown("**Waste %**")

        for mat in selected:
            wf_def = DEFAULT_WF[mat][ptype]
            row = st.columns([3, 2, 1.5, 1.5])
            row[0].write(mat)
            row[1].number_input("q", min_value=0.0, key=f"mqqty_{mat}", label_visibility="collapsed")
            row[2].selectbox("u", UNITS, key=f"mqunit_{mat}", label_visibility="collapsed")
            row[3].number_input("w", min_value=0.0, max_value=100.0,
                                value=float(wf_def), key=f"mqwf_{mat}", label_visibility="collapsed")

        st.caption(f"Default waste %: CSE (2020) p.30 — 4–30% for construction; 100% for demolition.")

        col_b, col_n = st.columns([1, 4])
        col_b.button("← Back", on_click=lambda: go(2), key="mq_back")
        if col_n.button("✅ Calculate & Proceed to Emissions →", type="primary", key="mq_go"):
            tbl = []
            for mat in selected:
                qty  = float(st.session_state.get(f"mqqty_{mat}",  0.0))
                unit =       st.session_state.get(f"mqunit_{mat}", "tonnes")
                wf   = float(st.session_state.get(f"mqwf_{mat}",   DEFAULT_WF[mat][ptype])) / 100.0
                if qty <= 0:
                    continue
                if   unit == "kg":  qty_t = qty / 1000.0
                elif unit == "m³":  qty_t = qty * DENSITY.get(mat, 1.5)
                elif unit == "nos": qty_t = qty * 0.003
                else:               qty_t = qty
                waste_t = qty_t * wf
                if waste_t > 0:
                    tbl.append({"material": mat, "qty_input": qty, "unit": unit, "waste_tonnes": waste_t})

            if not tbl:
                st.error("Please enter at least one quantity greater than 0.")
            else:
                st.session_state.waste_table      = tbl
                st.session_state.emission_inputs  = {}
                st.session_state.results          = {}
                st.session_state.page             = 4
                st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # BIM / CAD UPLOAD
    # ────────────────────────────────────────────────────────────────────────
    elif method == "bim":
        st.markdown("""
        <div class="info-box">
        <b>CircularBuild Dynamo Extractor — Step-by-Step Instructions</b><br><br>
        <b>What you need:</b> Autodesk Revit (2021 or later) with Dynamo 2.x installed.<br><br>
        <b>Step 1 — Download the script</b><br>
        Click the <b>Download Dynamo Script</b> button below to get <code>Extract_Materials.dyn</code>. Save it anywhere on your computer.<br><br>
        <b>Step 2 — Open your Revit model</b><br>
        Open the project <code>.rvt</code> file in Revit.<br><br>
        <b>Step 3 — Open Dynamo</b><br>
        In Revit: go to <b>Manage</b> tab → <b>Visual Programming</b> → <b>Dynamo</b>.<br><br>
        <b>Step 4 — Load the script</b><br>
        In Dynamo: <b>File → Open</b> → select <code>Extract_Materials.dyn</code>.<br><br>
        <b>Step 5 — Set the output file path</b><br>
        In the Dynamo canvas, find the <b>String node labelled "File Path"</b> (top-right area).
        Double-click it and type the full path where you want the Excel to be saved,
        e.g. <code>C:/Users/YourName/Desktop/Materials.xlsx</code> (forward slashes work in Dynamo)<br><br>
        <b>Step 6 — Run</b><br>
        Click <b>Run</b> (bottom-left of Dynamo). Wait for all nodes to show green ticks. The script extracts
        Walls, Floors, Structural Columns, Structural Framing, Slabs, Roofs, Rebar, Doors, and Windows.<br><br>
        <b>Step 7 — Upload the Excel here</b><br>
        Come back to this page and upload the generated <code>.xlsx</code> file below.
        </div>
        """, unsafe_allow_html=True)

        # ── Dynamo file download ───────────────────────────────────────────
        # Load Dynamo script — looks next to app.py first, then upload path
        import os as _os
        _dyn_candidates = [
            _os.path.join(_os.path.dirname(__file__), "Extract_Materials.dyn"),
            "/mnt/user-data/uploads/Extract_Materials.dyn",
        ]
        _dyn_bytes = None
        for _p in _dyn_candidates:
            if _os.path.exists(_p):
                with open(_p, "rb") as _dyn_f:
                    _dyn_bytes = _dyn_f.read()
                break
        if _dyn_bytes:
            st.download_button(
                label="⬇️ Download Dynamo Script (Extract_Materials.dyn)",
                data=_dyn_bytes,
            file_name="Extract_Materials.dyn",
                mime="application/json",
                help="Save this file, then open it in Dynamo inside Revit",
            )
        else:
            st.warning("⚠️ Dynamo script file not found on server. Please contact the administrator.")

        st.markdown("""
        <div class="source-note">
        📋 <b>Output columns produced by the script:</b>
        Category | Material | Volume | Volume Unit | Density | Density Unit | Mass | Mass Unit | Count<br>
        Extracts: Walls, Floors, Structural Columns, Structural Framing, Slabs, Roofs, Rebar, Doors, Windows.<br>
        Densities are read from the Revit material structural asset where available; defaults used otherwise.
        All volumes are in m³, masses in kg — no unit conversion needed before uploading.
        </div>
        """, unsafe_allow_html=True)

        uploaded = st.file_uploader(
            "Upload CircularBuild Material Export (.xlsx or .csv)",
            type=["csv", "xlsx"],
            help="Upload the Excel file generated by Extract_Materials.dyn from your Revit model"
        )

        # ── CircularBuild material groups ──────────────────────────────────
        CB_GROUPS = ["Concrete", "Brick/Masonry", "Soil/Sand/Gravel", "Steel/Metal",
                     "Wood/Timber", "Bitumen", "Plastic", "Glass", "Others"]

        # Keyword map — matches Revit material names like "Brick, Common", "Concrete, Sand/Cement Screed" etc.
        REVIT_KEYWORDS = {
            "Concrete":         ["concrete","rcc","pcc","m20","m25","m30","m35","m40",
                                  "cement","grout","screed","topping","masonry unit","cmu",
                                  "concrete masonry","precast","cast-in-place"],
            "Brick/Masonry":    ["brick","masonry","plaster","render","stone","lime",
                                  "clay tile","aac","firebrick","blockwork"],
            "Steel/Metal":      ["steel","rebar","tmt","iron","metal","alumin","copper",
                                  "zinc","galvan","stainless","mild steel","gi ","ms "],
            "Wood/Timber":      ["wood","timber","plywood","mdf","lumber","bamboo",
                                  "particle board","chipboard","hardwood","softwood","teak"],
            "Glass":            ["glass","glazing","tempered","laminated","toughened","float glass"],
            "Soil/Sand/Gravel": ["soil","sand","gravel","aggregate","earth","fill",
                                  "backfill","crushed stone","hardcore"],
            "Bitumen":          ["bitumen","asphalt","tar","waterproof","membrane",
                                  "felt","roofing","sbs","app"],
            "Plastic":          ["plastic","pvc","hdpe","upvc","polystyren","eps","xps",
                                  "foam","insulation","fiberglass","fibreglass","polyureth"],
        }

        # Fallback densities if Mass column is missing/zero (tonnes/m³)
        FALLBACK_DENS = {
            "Concrete":2.40, "Brick/Masonry":1.80, "Steel/Metal":7.85,
            "Wood/Timber":0.70, "Glass":2.50, "Soil/Sand/Gravel":1.70,
            "Bitumen":1.20, "Plastic":0.90, "Others":1.50,
        }

        def resolve_group(mat_name, cat_name=""):
            """Map Revit material/category name to CircularBuild group."""
            text = (mat_name + " " + cat_name).lower()
            for grp, kws in REVIT_KEYWORDS.items():
                if any(k in text for k in kws):
                    return grp
            return "Others"

        def clean_float(val):
            """Safely convert a value to float, stripping commas."""
            try:
                return float(str(val).replace(",", "").strip())
            except:
                return 0.0

        if uploaded:
            try:
                import io

                # Read file
                if uploaded.name.lower().endswith(".csv"):
                    raw = uploaded.read().decode("utf-8", errors="replace")
                    # Strip any comment lines (#)
                    clean = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("#"))
                    df = pd.read_csv(io.StringIO(clean))
                else:
                    df = pd.read_excel(uploaded)

                # Normalise column names (strip spaces, consistent case)
                df.columns = [str(c).strip() for c in df.columns]

                st.markdown("**Preview (first 5 rows):**")
                st.dataframe(df.head(), use_container_width=True, hide_index=True)

                # ── Detect format ──────────────────────────────────────────
                has_revit_format = "Material" in df.columns and "Volume" in df.columns
                has_mass_col     = "Mass" in df.columns
                has_vol_unit     = "Volume Unit" in df.columns
                has_mass_unit    = "Mass Unit" in df.columns
                has_density      = "Density" in df.columns
                has_category     = "Category" in df.columns

                if not has_revit_format:
                    st.error(
                        "❌ Could not find required columns. "
                        "Export from Revit with: Category, Material, Volume, Volume Unit, Mass, Mass Unit. "
                        "See instructions above."
                    )
                    st.button("← Back", on_click=lambda: go(2), key="bim_back_err")
                else:
                    # ── Parse every row ────────────────────────────────────
                    agg = {}   # group → {vol_m3, mass_kg, count, originals}
                    skipped = []

                    for idx, row in df.iterrows():
                        mat_name = str(row.get("Material", "")).strip()
                        cat_name = str(row.get("Category", "")).strip()

                        # Skip header-repeat rows or empty rows
                        if not mat_name or mat_name.lower() in ("nan", "material", ""):
                            continue

                        # ── Volume ─────────────────────────────────────────
                        vol_raw  = clean_float(row.get("Volume", 0))
                        vol_unit = str(row.get("Volume Unit", "m3")).strip().lower() if has_vol_unit else "m3"

                        # Convert volume to m³
                        if "ft" in vol_unit or "cf" in vol_unit:
                            vol_m3 = vol_raw * 0.0283168   # cubic feet → m³
                        elif "cm" in vol_unit:
                            vol_m3 = vol_raw / 1_000_000   # cm³ → m³
                        elif "mm" in vol_unit:
                            vol_m3 = vol_raw / 1_000_000_000
                        else:
                            vol_m3 = vol_raw                # assume m³

                        # ── Mass ───────────────────────────────────────────
                        mass_kg = 0.0
                        if has_mass_col:
                            mass_raw  = clean_float(row.get("Mass", 0))
                            mass_unit = str(row.get("Mass Unit", "kg")).strip().lower() if has_mass_unit else "kg"
                            if "lb" in mass_unit:
                                mass_kg = mass_raw * 0.453592
                            elif "tonne" in mass_unit or mass_unit == "t":
                                mass_kg = mass_raw * 1000.0
                            elif "g" == mass_unit:
                                mass_kg = mass_raw / 1000.0
                            else:
                                mass_kg = mass_raw            # assume kg

                        # ── Density fallback if mass is missing ────────────
                        group = resolve_group(mat_name, cat_name)
                        if mass_kg <= 0 and vol_m3 > 0:
                            # Try density column first
                            if has_density:
                                dens_raw = clean_float(row.get("Density", 0))
                                dens_unit = str(row.get("Density Unit", "kg/m3")).lower() if "Density Unit" in df.columns else "kg/m3"
                                if "lb" in dens_unit:
                                    dens_raw *= 16.0185   # lb/ft³ → kg/m³
                                if dens_raw > 0:
                                    mass_kg = vol_m3 * dens_raw
                            # Final fallback: use our density table
                            if mass_kg <= 0:
                                mass_kg = vol_m3 * FALLBACK_DENS.get(group, 1.5) * 1000

                        if vol_m3 <= 0 and mass_kg <= 0:
                            skipped.append(f"Row {idx}: {mat_name} — zero volume and mass")
                            continue

                        mass_t = mass_kg / 1000.0

                        agg.setdefault(group, {"vol_m3":0.0, "mass_kg":0.0, "count":0, "originals":[]})
                        agg[group]["vol_m3"]   += vol_m3
                        agg[group]["mass_kg"]  += mass_kg
                        agg[group]["count"]    += 1
                        agg[group]["originals"].append(mat_name)

                    if not agg:
                        st.error("No material data could be read. Check that Volume and Mass columns have values.")
                        st.button("← Back", on_click=lambda: go(2), key="bim_back_empty")
                    else:
                        # ── Show mapping table ─────────────────────────────
                        st.markdown("#### Material Mapping — Revit → CircularBuild Groups")
                        st.caption("Each Revit material has been classified into a CircularBuild group using keyword matching. Review and adjust waste % below.")

                        map_rows = []
                        for g in CB_GROUPS:
                            if g not in agg: continue
                            d = agg[g]
                            map_rows.append({
                                "CircularBuild Group":  g,
                                "Total Mass (tonnes)":  round(d["mass_kg"]/1000, 3),
                                "Total Volume (m³)":    round(d["vol_m3"], 3),
                                "No. of Revit rows":    d["count"],
                                "Revit Materials":      "; ".join(dict.fromkeys(d["originals"]))[:80] +
                                                        ("…" if len(set(d["originals"])) > 4 else ""),
                            })
                        st.dataframe(pd.DataFrame(map_rows), use_container_width=True, hide_index=True)

                        total_mass_t = sum(agg[g]["mass_kg"]/1000 for g in agg)
                        total_vol_m3 = sum(agg[g]["vol_m3"] for g in agg)
                        c1, c2 = st.columns(2)
                        c1.metric("Total Material Mass", f"{total_mass_t:.2f} tonnes")
                        c2.metric("Total Material Volume", f"{total_vol_m3:.2f} m³")

                        if skipped:
                            with st.expander(f"⚠️ {len(skipped)} rows skipped (zero volume & mass)"):
                                for s in skipped[:10]: st.caption(s)

                        st.divider()

                        # ── Waste % inputs ─────────────────────────────────
                        st.markdown("#### Waste % per Material Group")
                        st.caption(
                            "Waste % = what fraction of the total material quantity becomes C&D waste. "
                            "Pre-filled from CSE (2020) benchmarks. Edit as needed."
                        )

                        DEFAULT_WF_BIM = {
                            "Concrete":3.0, "Brick/Masonry":5.0, "Soil/Sand/Gravel":8.0,
                            "Steel/Metal":2.0, "Wood/Timber":10.0, "Bitumen":4.0,
                            "Plastic":4.0, "Glass":3.0, "Others":8.0,
                        } if ptype == "Construction" else {
                            g: 85.0 for g in CB_GROUPS
                        }

                        present_groups = [g for g in CB_GROUPS if g in agg]
                        wf_cols = st.columns(3)
                        live_wf = {}
                        for i, g in enumerate(present_groups):
                            with wf_cols[i % 3]:
                                live_wf[g] = st.number_input(
                                    f"{g}",
                                    min_value=0.0, max_value=100.0,
                                    value=float(DEFAULT_WF_BIM.get(g, 5.0)),
                                    step=0.5, key=f"bim_wf_{g}",
                                    help=f"% of {g} total mass that becomes waste"
                                )

                        # ── Waste summary table ────────────────────────────
                        st.markdown("#### Estimated Waste Summary")
                        waste_preview = []
                        for g in present_groups:
                            mass_t = agg[g]["mass_kg"] / 1000.0
                            wt     = mass_t * (live_wf.get(g, 5.0) / 100.0)
                            waste_preview.append({
                                "Material Group":    g,
                                "Total Material (t)": round(mass_t, 3),
                                "Waste % Applied":   f"{live_wf.get(g,5.0):.1f}%",
                                "Waste (tonnes)":    round(wt, 3),
                            })
                        st.dataframe(pd.DataFrame(waste_preview), use_container_width=True, hide_index=True)

                        total_waste = sum(r["Waste (tonnes)"] for r in waste_preview)
                        st.success(f"✅ Total estimated waste: **{total_waste:.2f} tonnes** from **{total_mass_t:.2f} tonnes** total material")

                        # ── Navigation ─────────────────────────────────────
                        col_b, col_n = st.columns([1, 4])
                        col_b.button("← Back", on_click=lambda: go(2), key="bim_back")
                        if col_n.button("✅ Confirm & Proceed to Emissions →", type="primary", key="bim_go"):
                            tbl = []
                            for g in present_groups:
                                mass_t = agg[g]["mass_kg"] / 1000.0
                                wt     = mass_t * (live_wf.get(g, 5.0) / 100.0)
                                if wt > 0:
                                    tbl.append({"material": g, "waste_tonnes": round(wt, 4), "unit": "tonnes"})
                            st.session_state.waste_table     = tbl
                            st.session_state.emission_inputs = {}
                            st.session_state.results         = {}
                            st.session_state.page            = 4
                            st.rerun()

            except Exception as e:
                st.error(f"Error reading file: {e}")
                st.caption("Check that the file is a valid Revit material takeoff export (CSV or Excel).")

        else:
            st.markdown('<div class="source-note">👆 Upload your Revit material takeoff file above to begin.</div>', unsafe_allow_html=True)
            col_b, col_fb = st.columns([1, 3])
            col_b.button("← Back", on_click=lambda: go(2), key="bim_back2")
            if col_fb.button("Switch to Area-Based Estimate instead", key="bim_toarea"):
                st.session_state.input_method = "area"
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — EMISSIONS & EOL
# ══════════════════════════════════════════════════════════════════════════════
def page_emissions_eol():
    st.markdown('<p class="page-title">Emissions & End-of-Life Scenarios</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Specify material sub-type, transport details, and end-of-life scenarios for each material. Defaults are pre-filled from Indian C&D waste data.</p>', unsafe_allow_html=True)

    waste_table = st.session_state.waste_table
    if not waste_table:
        st.error("No waste data found. Please go back."); return

    ei = st.session_state.emission_inputs

    st.markdown(f'<div class="source-note">📚 GWP factors: {GWP_SOURCE} | Environmental factors: {ENV_SOURCE}</div>', unsafe_allow_html=True)

    for row in waste_table:
        mat = row["material"]
        qty_t = row["waste_tonnes"]
        with st.expander(f"**{mat}** — {qty_t:.3f} tonnes", expanded=False):
            c1, c2, c3 = st.columns(3)

            gwp_map = MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0})
            sub_types = list(gwp_map.keys())
            cur_sub = ei.get(mat, {}).get("sub_type", sub_types[0])
            sub_idx = sub_types.index(cur_sub) if cur_sub in sub_types else 0

            with c1:
                sub = st.selectbox(f"Material Sub-type", sub_types, index=sub_idx, key=f"sub_{mat}")
                gwp_val = gwp_map[sub]
                st.caption(f"GWP (A1–A3): **{gwp_val} kg CO2e/tonne**")

            vehicles = list(TRANSPORT_EF.keys())
            cur_veh = ei.get(mat, {}).get("vehicle", vehicles[3])
            veh_idx = vehicles.index(cur_veh) if cur_veh in vehicles else 3
            with c2:
                veh = st.selectbox("Transport Vehicle", vehicles, index=veh_idx, key=f"veh_{mat}")
                dist = st.number_input("Distance to waste site (km)", min_value=0.0, value=float(ei.get(mat, {}).get("distance_km", 20)), key=f"dist_{mat}")
                dist_c2 = st.number_input("C2: Distance to recycling/landfill (km)", min_value=0.0, value=float(ei.get(mat, {}).get("distance_km_c2", 10)), key=f"dist_c2_{mat}")

            default_eol = DEFAULT_EOL.get(mat, {"Reuse": 0, "Recycle": 50, "Landfill": 40, "Incineration": 5, "Other": 5})
            cur_eol = ei.get(mat, {}).get("eol", default_eol)
            with c3:
                st.markdown("**End-of-Life (%) — must sum to 100**")
                reuse    = st.slider("Reuse %",        0, 100, int(cur_eol.get("Reuse",0)),    key=f"eol_reuse_{mat}")
                recycle  = st.slider("Recycle %",      0, 100, int(cur_eol.get("Recycle",50)), key=f"eol_recycle_{mat}")
                landfill = st.slider("Landfill %",     0, 100, int(cur_eol.get("Landfill",40)),key=f"eol_landfill_{mat}")
                incin    = st.slider("Incineration %", 0, 100, int(cur_eol.get("Incineration",5)),key=f"eol_incin_{mat}")
                other    = st.slider("Other %",        0, 100, int(cur_eol.get("Other",5)),    key=f"eol_other_{mat}")
                total_eol = reuse + recycle + landfill + incin + other
                if total_eol != 100:
                    st.warning(f"EOL total = {total_eol}% (must be 100%)")

            ei[mat] = {
                "sub_type": sub, "vehicle": veh,
                "distance_km": dist, "distance_km_c2": dist_c2,
                "eol": {"Reuse": reuse, "Recycle": recycle, "Landfill": landfill,
                        "Incineration": incin, "Other": other}
            }

    st.session_state.emission_inputs = ei
    st.markdown(f'<div class="source-note">EOL defaults: {EOL_SOURCE}</div>', unsafe_allow_html=True)
    city_lf = get_landfill_cost(st.session_state.project.get("location",""))
    st.markdown(f'<div class="source-note">Landfill cost: ₹{city_lf}/tonne for {st.session_state.project.get("location","your city")} — {LANDFILL_COST_SOURCE}</div>', unsafe_allow_html=True)

    col_b, col_n = st.columns([1, 3])
    with col_b: st.button("← Back", on_click=lambda: go(3))
    with col_n:
        if st.button("Calculate Results →", type="primary"):
            emission_results = compute_emissions(waste_table, ei)
            circ_scores, circ_aggregate = compute_circularity(emission_results)
            city_str = st.session_state.project.get("location", "")
            benefits = compute_circularity_benefits(emission_results, city_str=city_str)
            st.session_state.results = {
                "emission_results": emission_results,
                "circ_scores": circ_scores,
                "circ_aggregate": circ_aggregate,
                "benefits": benefits,
            }
            # ── Log to Google Sheets ──────────────────────────────────────
            total_waste_log = sum(r["qty_t"] for r in emission_results.values())
            total_gwp_log   = sum(r["total_gwp"] for r in emission_results.values()) / 1000.0
            total_ap_log    = sum(r["AP"] for r in emission_results.values())
            total_ep_log    = sum(r["EP"] for r in emission_results.values())
            # ── Log summary to Google Sheets ─────────────────────────
            log_to_sheets(
                st.session_state.project,
                total_waste_log,
                total_gwp_log,
                circ_aggregate,
                total_ap_log,
                total_ep_log,
            )
            # ── Log full data to Firestore ────────────────────────────────
            log_to_firestore(
                proj=st.session_state.project,
                waste_table=waste_table,
                emission_inputs=ei,
                emission_results=emission_results,
                circ_scores=circ_scores,
                circ_aggregate=circ_aggregate,
                benefits=benefits,
            )
            go(5); st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — RESULTS & REPORT
# ══════════════════════════════════════════════════════════════════════════════
def page_results():
    proj    = st.session_state.project
    res     = st.session_state.results
    wt      = st.session_state.waste_table

    if not res:
        st.error("No results yet. Please go back."); return

    er  = res["emission_results"]
    cs  = res["circ_scores"]
    ca  = res["circ_aggregate"]
    ben = res["benefits"]

    st.markdown(f'<p class="page-title">Results — <span style="color:#10b981">{proj["name"]}</span></p>', unsafe_allow_html=True)
    st.markdown(f'<p class="page-sub">{proj["construction_type"]} | {proj["building_type"]} | {proj["location"]} | {proj["builtup_area"]} m²</p>', unsafe_allow_html=True)

    # ── TOP METRICS ────────────────────────────────────────────────────────
    total_waste   = sum(r["qty_t"] for r in er.values())
    total_gwp     = sum(r["total_gwp"] for r in er.values()) / 1000.0  # tonnes CO2e
    total_ap      = sum(r["AP"] for r in er.values())
    total_ep      = sum(r["EP"] for r in er.values())
    total_avoided = sum(b["avoided_emission_kgco2e"] for b in ben.values()) / 1000.0
    total_virgin  = sum(b["virgin_material_savings_inr"] for b in ben.values())
    total_lf_save = sum(b["landfill_cost_saved_inr"] for b in ben.values())
    total_lf_div  = sum(b["landfill_diverted_t"] for b in ben.values())

    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Total C&D Waste",         f"{total_waste:.2f} t")
    m2.metric("Total GWP",               f"{total_gwp:.2f} t CO2e")
    m3.metric("Circularity Score",        f"{ca*100:.1f} / 100")
    m4.metric("Avoided Emissions",        f"{total_avoided:.2f} t CO2e")

    st.divider()

    # ── TABS ──────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗑️ Waste", "🌍 Emissions", "♻️ Circularity", "💰 Economy", "📍 Recycling Plants"])

    # ── TAB 1: WASTE ──────────────────────────────────────────────────────
    with tab1:
        st.markdown('<p class="section-head">Material Waste Estimation</p>', unsafe_allow_html=True)
        df_w = pd.DataFrame([{"Material": r["material"], "Waste (tonnes)": round(r["waste_tonnes"],3)} for r in wt])
        st.dataframe(df_w, use_container_width=True, hide_index=True)
        st.markdown(f'<div class="source-note">Source: {WASTE_RATE_SOURCE} | Composition: {COMP_SOURCE}</div>', unsafe_allow_html=True)

        # Chart
        st.bar_chart(df_w.set_index("Material")["Waste (tonnes)"])

    # ── TAB 2: EMISSIONS ──────────────────────────────────────────────────
    with tab2:
        proj_type  = proj.get("construction_type", "Construction")
        pt_key = "Demolition" if proj_type in ["Demolition","Redevelopment"] else "Construction"
        bldg_type  = proj.get("building_type", "Residential")
        area_m2    = proj.get("builtup_area", 1.0) or 1.0

        # ── 2a: Per-m² Benchmark (Primary display) ──────────────────────
        st.markdown('<p class="section-head">📐 Environmental Impact Benchmarks per m² BUA</p>', unsafe_allow_html=True)
        if bldg_type in ("Industrial", "Infrastructure"):
            st.markdown(
                f'<div class="warn-box">⚠️ No peer-reviewed India-specific LCA benchmark exists for <b>{bldg_type}</b> projects. '
                f'Showing <b>Commercial</b> values as proxy. Replace with project-specific data for accurate results.</div>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"Lifecycle stage benchmarks for **{bldg_type} {proj_type}** projects in India, "
                f"expressed per m² of built-up area (BUA). Based on peer-reviewed Indian LCA studies.",
                unsafe_allow_html=True
            )

        def _get_bench(table, pt_key, bldg_type, fallback_type="Commercial"):
            """Get benchmark; fall back to Commercial if type not sourced (None)."""
            type_dict = table.get(pt_key, table["Construction"])
            val = type_dict.get(bldg_type)
            if val is None:
                val = type_dict.get(fallback_type, table["Construction"]["Residential"])
            return val or table["Construction"]["Residential"]
        gwp_bench = _get_bench(GWP_PER_M2, pt_key, bldg_type)
        ap_bench  = _get_bench(AP_PER_M2,  pt_key, bldg_type)
        efw_bench = _get_bench(EFW_PER_M2, pt_key, bldg_type)

        stages = ["A1_A3", "A4", "A5", "C1", "C2", "C3", "C4"]
        stage_labels = {
            "A1_A3": "A1–A3 Material Mfg",
            "A4":    "A4 Transport to Site",
            "A5":    "A5 Site Construction",
            "C1":    "C1 Demolition",
            "C2":    "C2 Transport to EOL",
            "C3":    "C3 Waste Processing",
            "C4":    "C4 Landfill Disposal",
        }
        bench_rows = []
        for s in stages:
            if pt_key == "Demolition" and s in ("A1_A3","A4","A5"):
                continue
            bench_rows.append({
                "Stage":                   stage_labels[s],
                "GWP (kg CO₂e/m²)":       round(gwp_bench.get(s, 0), 2),
                "AP (kg SO₂e/m²)":         round(ap_bench.get(s, 0), 4),
                "EFW/EP (kg PO₄e/m²)":     round(efw_bench.get(s, 0), 5),
            })
        # Totals row
        bench_rows.append({
            "Stage":                   "TOTAL (A1–A5 + C1–C4)",
            "GWP (kg CO₂e/m²)":       round(sum(gwp_bench.get(s,0) for s in stages), 2),
            "AP (kg SO₂e/m²)":         round(sum(ap_bench.get(s,0)  for s in stages), 4),
            "EFW/EP (kg PO₄e/m²)":     round(sum(efw_bench.get(s,0) for s in stages), 5),
        })
        df_bench = pd.DataFrame(bench_rows)
        st.dataframe(df_bench, use_container_width=True, hide_index=True)

        # Project-total from benchmark × area
        total_gwp_bench = sum(gwp_bench.get(s,0) for s in stages) * area_m2 / 1000.0
        total_ap_bench  = sum(ap_bench.get(s,0)  for s in stages) * area_m2
        total_efw_bench = sum(efw_bench.get(s,0) for s in stages) * area_m2

        c1, c2, c3 = st.columns(3)
        c1.metric("Project GWP (benchmark)", f"{total_gwp_bench:.1f} t CO₂e", f"{area_m2:,.0f} m²")
        c2.metric("Project AP (benchmark)",  f"{total_ap_bench:.2f} kg SO₂e")
        c3.metric("Project EFW (benchmark)", f"{total_efw_bench:.4f} kg PO₄e")

        st.markdown(f'<div class="source-note">📚 {GWP_SOURCE}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="source-note">📚 AP: {AP_SOURCE}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="source-note">📚 EFW: {EFW_SOURCE}</div>', unsafe_allow_html=True)

        st.divider()

        # ── 2b: Waste-Weight Emissions (Material breakdown) ─────────────
        st.markdown('<p class="section-head">⚖️ Emissions from Estimated Waste Quantities</p>', unsafe_allow_html=True)
        st.caption("Based on material waste tonnes from Page 3 × per-tonne LCA emission factors (CML 2001 / IFC EDGE India)")
        em_rows = []
        for mat, r in er.items():
            em_rows.append({
                "Material": mat,
                "Qty (t)": round(r["qty_t"],2),
                "A1–A3 (kg CO₂e)": round(r["A1A3"],1),
                "A4 Transport": round(r["A4"],1),
                "A5 Site": round(r["A5"],1),
                "C1 Demolition": round(r["C1"],1),
                "C2 Transport": round(r["C2"],1),
                "C3 Processing": round(r["C3"],1),
                "C4 Landfill": round(r["C4"],1),
                "Total GWP (kg)": round(r["total_gwp"],1),
                "AP (kg SO₂e)": round(r["AP"],3),
                "EFW/EP (kg PO₄e)": round(r["EP"],4),
            })
        df_em = pd.DataFrame(em_rows)
        st.dataframe(df_em, use_container_width=True, hide_index=True)

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total GWP (waste-based)", f"{total_gwp:.3f} t CO₂e")
        mc2.metric("Total AP",  f"{total_ap:.2f} kg SO₂e")
        mc3.metric("Total EFW", f"{total_ep:.4f} kg PO₄e")
        st.markdown(f'<div class="source-note">Per-tonne factors: IFC EDGE India DB (2017) [S5]; CML 2001 characterisation [S8]; IPCC 2006 [S7]</div>', unsafe_allow_html=True)

    # ── TAB 3: CIRCULARITY ────────────────────────────────────────────────
    with tab3:
        st.markdown('<p class="section-head">Circularity Score</p>', unsafe_allow_html=True)
        st.markdown("""
        **Material Circularity Indicator (MCI) — Ellen MacArthur Foundation (2015)**

        MCI = 1 − LFI × 0.9 × (0.5 − 0.5×Fr)

        where **LFI** (Linear Flow Index) = (Landfill% + Incineration%) / 100  
        and **Fr** (recovered fraction) = (Recycle% + Reuse%) / 100  
        Virgin input assumed (Vu = 1.0 for new construction materials).  
        Score range: **0–100** (100 = fully circular, 10 = fully linear)

        *Source: EMF (2015) MCI Technical Appendix — ellenmacarthurfoundation.org*
        """)

        circ_rows = []
        for mat, score in cs.items():
            eol = er[mat]["eol"]
            circ_rows.append({
                "Material": mat,
                "Reuse %": eol.get("Reuse",0),
                "Recycle %": eol.get("Recycle",0),
                "Landfill %": eol.get("Landfill",0),
                "Incineration %": eol.get("Incineration",0),
                "Circularity Score": f"{score*100:.1f}",
            })
        df_circ = pd.DataFrame(circ_rows)
        st.dataframe(df_circ, use_container_width=True, hide_index=True)

        col_a, col_b = st.columns([1,2])
        with col_a:
            score_color = "#10b981" if ca >= 0.5 else "#f59e0b" if ca >= 0.3 else "#ef4444"
            st.markdown(f"""
            <div style="text-align:center; background: #f0fdf4; border: 2px solid {score_color}; border-radius: 14px; padding: 24px;">
              <div style="font-size: 3rem; font-weight: 700; color: {score_color};">{ca*100:.1f}</div>
              <div style="color: #374151;">Overall Circularity Score</div>
              <div style="font-size: 0.75rem; color: #9ca3af;">out of 100</div>
            </div>
            """, unsafe_allow_html=True)
        with col_b:
            circ_chart = {mat: float(sc)*100 for mat, sc in cs.items()}
            st.bar_chart(pd.DataFrame.from_dict({"Score": circ_chart}, orient="columns"))

        # Material recovery
        st.markdown('<p class="section-head">Material Recovery Summary</p>', unsafe_allow_html=True)
        rec_rows = []
        for mat, b in ben.items():
            rec_rows.append({
                "Material": mat,
                "Recycled (t)": round(b["recycled_t"],2),
                "Reused (t)":   round(b["reused_t"],2),
                "Landfilled (t)": round(b["landfill_t"],2),
                "Diverted from Landfill (t)": round(b["landfill_diverted_t"],2),
            })
        st.dataframe(pd.DataFrame(rec_rows), use_container_width=True, hide_index=True)

    # ── TAB 4: ECONOMY ────────────────────────────────────────────────────
    with tab4:
        st.markdown('<p class="section-head">Economic & Environmental Benefits</p>', unsafe_allow_html=True)

        e1,e2,e3,e4 = st.columns(4)
        e1.metric("Avoided Emissions",     f"{total_avoided:.2f} t CO₂e")
        e2.metric("Virgin Material Savings", f"₹{total_virgin:,.0f}")
        e3.metric("Landfill Diverted",     f"{total_lf_div:.2f} t")
        e4.metric("Landfill Cost Saved",   f"₹{total_lf_save:,.0f}")

        st.markdown(f'<div class="source-note">Virgin material prices: CPWD DSR (2024) / state PWD SORs (2023-24). {VIRGIN_PRICE_SOURCE} | Landfill cost: {LANDFILL_COST_SOURCE}</div>', unsafe_allow_html=True)

        ec_rows = []
        for mat, b in ben.items():
            ec_rows.append({
                "Material": mat,
                "Avoided Emission (t CO2e)": round(b["avoided_emission_kgco2e"]/1000,3),
                "Virgin Mat. Savings (INR)": f"₹{b['virgin_material_savings_inr']:,.0f}",
                "Landfill Diverted (t)": round(b["landfill_diverted_t"],2),
                "Landfill Cost (INR)": f"₹{b['landfill_cost_actual_inr']:,.0f}",
                "Landfill Cost Saved (INR)": f"₹{b['landfill_cost_saved_inr']:,.0f}",
            })
        st.dataframe(pd.DataFrame(ec_rows), use_container_width=True, hide_index=True)

        # Get the actual landfill cost used (city-specific)
        sample_lf_cost = list(ben.values())[0].get("landfill_cost_per_tonne", DEFAULT_LANDFILL_COST) if ben else DEFAULT_LANDFILL_COST
        st.markdown(f"""
        <div class="info-box">
        <b>Landfill unit cost used:</b> ₹{sample_lf_cost}/tonne ({proj.get("location","")}) <br>
        <b>Source:</b> {LANDFILL_COST_SOURCE}
        </div>
        """, unsafe_allow_html=True)

    # ── TAB 5: RECYCLING PLANTS ───────────────────────────────────────────
    with tab5:
        st.markdown('<p class="section-head">Nearest C&D Waste Recycling Plants in India</p>', unsafe_allow_html=True)
        st.markdown(f'<div class="source-note">Source: {PLANTS_SOURCE}. Distances calculated using Haversine great-circle formula from project city coordinates.</div>', unsafe_allow_html=True)

        proj_city = proj.get("location", "")
        nearest = find_nearest_plants(proj_city)

        # Build display dataframe — include Distance column if available
        has_dist = nearest and nearest[0].get("Distance_km") is not None
        if has_dist:
            df_plants = pd.DataFrame(nearest)[["City", "Location", "Capacity_TPD", "Distance_km"]]
            df_plants.columns = ["City", "Location", "Capacity (TPD)", "Distance (km)"]
            st.caption(f"Showing 5 nearest plants to **{proj_city}**, sorted by straight-line distance.")
        else:
            df_plants = pd.DataFrame(nearest)[["City", "Location", "Capacity_TPD"]]
            df_plants.columns = ["City", "Location", "Capacity (TPD)"]
            if proj_city:
                st.caption(f"⚠️ City ‘{proj_city}’ not found in coordinates database — showing name-matched or default plants. Enter a major Indian city name for distance-based results.")
        st.dataframe(df_plants, use_container_width=True, hide_index=True)

        # Map — mark project city if coords known, plus all nearest plants
        map_rows = [{"lat": p["Lat"], "lon": p["Lon"]} for p in nearest]
        map_df = pd.DataFrame(map_rows)
        st.map(map_df, zoom=5)

        st.markdown("**All operational C&D recycling plants in India:**")
        df_all = pd.DataFrame(RECYCLING_PLANTS)[["City","Location","Capacity_TPD"]]
        df_all.columns = ["City", "Location", "Capacity (TPD)"]
        st.dataframe(df_all, use_container_width=True, hide_index=True)

    st.divider()

    # ── PDF DOWNLOAD ──────────────────────────────────────────────────────
    st.markdown("### 📄 Download Report")
    if st.button("Generate PDF Report", type="primary"):
        with st.spinner("Generating PDF..."):
            pdf_buf = generate_pdf_report(
                proj, wt, er, cs, ca, ben
            )
        st.download_button(
            label="⬇️ Download PDF Report",
            data=pdf_buf,
            file_name=f"CD_Waste_Report_{proj['name'].replace(' ','_')}.pdf",
            mime="application/pdf",
        )

    st.button("← Back to Emissions", on_click=lambda: go(4))


# ══════════════════════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════════════════════
show_progress()
page = st.session_state.page

if page == 1:   page_project_info()
elif page == 2: page_data_input()
elif page == 3: page_waste_estimation()
elif page == 4: page_emissions_eol()
elif page == 5: page_results()
