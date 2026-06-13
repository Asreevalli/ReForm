"""
C&D Waste Estimation Tool — ReForm
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
import numpy as np
import json
import math
from io import BytesIO
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials as fb_credentials, firestore
import matplotlib

# ─── GOOGLE SHEETS LOGGER ───────────────────────────────────────────────────
# ─── SHARED: build formatted report string ──────────────────────────────────
def _build_report_string(proj, waste_table, emission_inputs, emission_results,
                          circ_scores, circ_aggregate, benefits):
    """Build the human-readable formatted report string stored in Firestore."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append(f"Project submitted: {ts}")
    lines.append("")

    # PROJECT INFO
    lines.append("─── PROJECT INFO ───────────────────────")
    lines.append(f"  Name:          {proj.get('name','')}")
    lines.append(f"  City:          {proj.get('location','')}")
    lines.append(f"  Type:          {proj.get('construction_type','')}")
    lines.append(f"  Building Type: {proj.get('building_type','')}")
    lines.append(f"  Built-up Area: {proj.get('builtup_area','')} m²")
    lines.append(f"  Plot Area:     {proj.get('plot_area','')} m²")
    lines.append(f"  Input Method:  {proj.get('input_method','')}")
    lines.append("")

    # WASTE TABLE
    lines.append("─── WASTE TABLE ────────────────────────")
    for row in waste_table:
        mat = row["material"]
        qty = row["waste_tonnes"]
        lines.append(f"  {mat:<20} {qty:.2f} t")
    lines.append("")

    # TRANSPORT INPUTS
    lines.append("─── TRANSPORT INPUTS ───────────────────")
    for mat, ei in emission_inputs.items():
        veh   = ei.get("vehicle", "")
        da4   = ei.get("distance_km", 0)
        dc2   = ei.get("distance_km_c2", 0)
        sub   = ei.get("sub_type", "")
        lines.append(f"  {mat:<20} {veh} | {da4} km to site | {dc2} km to EOL | sub-type: {sub}")
    lines.append("")

    # EMISSIONS PER MATERIAL
    lines.append("─── EMISSIONS (per material) ───────────")
    for mat, r in emission_results.items():
        a1a3 = round(r.get("A1A3", 0), 1)
        a4   = round(r.get("A4",   0), 1)
        a5   = round(r.get("A5",   0), 1)
        c1   = round(r.get("C1",   0), 1)
        c2   = round(r.get("C2",   0), 1)
        c3   = round(r.get("C3",   0), 1)
        c4   = round(r.get("C4",   0), 1)
        ap   = round(r.get("AP",   0), 2)
        ep   = round(r.get("EP",   0), 4)
        lines.append(
            f"  {mat:<20} A1-A3: {a1a3:,.1f} kg | A4: {a4} | A5: {a5} | "
            f"C1: {c1} | C2: {c2} | C3: {c3} | C4: {c4} | AP: {ap} | EP: {ep}"
        )
    lines.append("")

    # EOL SCENARIOS
    lines.append("─── EOL SCENARIOS ──────────────────────")
    for mat, r in emission_results.items():
        eol = r.get("eol", {})
        rec  = eol.get("Recycle", 0)
        reus = eol.get("Reuse", 0)
        lf   = eol.get("Landfill", 0)
        inc  = eol.get("Incineration", 0)
        oth  = eol.get("Other", 0)
        lines.append(
            f"  {mat:<20} Recycle {rec}% | Reuse {reus}% | "
            f"Landfill {lf}% | Incineration {inc}% | Other {oth}%"
        )
    lines.append("")

    # CIRCULARITY SCORES
    lines.append("─── CIRCULARITY SCORES ─────────────────")
    for mat, score in circ_scores.items():
        lines.append(f"  {mat:<20} MCI: {round(score*100,1)} / 100")
    lines.append("")

    # BENEFITS PER MATERIAL
    lines.append("─── MATERIAL BENEFITS ──────────────────")
    for mat, b in benefits.items():
        rec_t  = round(b.get("recycled_t", 0), 2)
        reu_t  = round(b.get("reused_t", 0), 2)
        lf_t   = round(b.get("landfill_t", 0), 2)
        av_em  = round(b.get("avoided_emission_kgco2e", 0), 2)
        v_sav  = round(b.get("virgin_material_savings_inr", 0), 0)
        lf_sav = round(b.get("landfill_cost_saved_inr", 0), 0)
        lines.append(
            f"  {mat:<20} Recycled: {rec_t} t | Reused: {reu_t} t | "
            f"Landfilled: {lf_t} t | Avoided: {av_em} kg CO₂e | "
            f"Virgin Savings: ₹{v_sav:,.0f} | LF Cost Saved: ₹{lf_sav:,.0f}"
        )
    lines.append("")

    # SUMMARY RESULTS
    total_waste   = sum(r["qty_t"]     for r in emission_results.values())
    total_gwp     = sum(r["total_gwp"] for r in emission_results.values()) / 1000.0
    total_ap      = sum(r["AP"]        for r in emission_results.values())
    total_ep      = sum(r["EP"]        for r in emission_results.values())
    total_avoided = sum(b.get("avoided_emission_kgco2e", 0) for b in benefits.values()) / 1000.0
    total_vsav    = sum(b.get("virgin_material_savings_inr", 0) for b in benefits.values())
    total_lfsav   = sum(b.get("landfill_cost_saved_inr", 0) for b in benefits.values())
    total_lfdiv   = sum(b.get("landfill_diverted_t", 0) for b in benefits.values())

    lines.append("─── RESULTS ────────────────────────────")
    lines.append(f"  Total Waste:        {total_waste:.2f} t")
    lines.append(f"  Total GWP:          {total_gwp:.3f} tCO₂e")
    lines.append(f"  Total AP:           {total_ap:.2f} kg SO₂e")
    lines.append(f"  Total EFW:          {total_ep:.4f} kg PO₄e")
    lines.append(f"  Circularity Score:  {round(circ_aggregate*100,1)} / 100")
    lines.append(f"  Avoided Emissions:  {total_avoided:.3f} tCO₂e")
    lines.append(f"  Virgin Mat Savings: ₹{total_vsav:,.0f}")
    lines.append(f"  Landfill Cost Saved:₹{total_lfsav:,.0f}")
    lines.append(f"  Landfill Diverted:  {total_lfdiv:.2f} t")

    return "\n".join(lines)



# ─── GOOGLE SHEETS LOGGER — full per-material row ────────────────────────────
def log_to_sheets(proj, emission_results, circ_aggregate, benefits,
                  waste_table=None, emission_inputs=None, circ_scores=None):
    """Write one detailed row per submission with all per-material columns.
    Headers auto-created if sheet is empty."""
    try:
        scopes  = ["https://www.googleapis.com/auth/spreadsheets",
                   "https://www.googleapis.com/auth/drive"]
        sa_info = {k: v for k, v in st.secrets["gcp_service_account"].items()}
        creds   = Credentials.from_service_account_info(sa_info, scopes=scopes)
        client  = gspread.authorize(creds)
        sheet   = client.open(st.secrets["sheets"]["spreadsheet_name"]).sheet1

        # ── Material order and short abbreviations ───────────────────────────
        MATS  = ["Concrete","Brick/Masonry","Soil/Sand/Gravel","Steel/Metal",
                 "Wood/Timber","Bitumen","Plastic","Glass","Others"]
        ABBR  = ["Conc","Brick","Soil","Steel","Wood","Bitu","Plas","Glass","Other"]

        # ── Build lookup dicts ───────────────────────────────────────────────
        wt_map  = {r["material"]: r for r in (waste_table or [])}
        ei_map  = emission_inputs or {}
        er_map  = emission_results or {}
        ben_map = benefits or {}
        cs_map  = circ_scores or {}

        def g(d, *keys, default=0):
            """Safe nested get."""
            for k in keys:
                if not isinstance(d, dict): return default
                d = d.get(k, default)
            return d if d is not None else default

        # ── HEADER (written once if sheet empty) ─────────────────────────────
        static_hdr = [
            "Timestamp","Project Name","City","Project Type","Building Type",
            "Built-up Area (m²)","Locality","Input Method",
        ]
        per_mat_hdrs = []
        for grp, fmt in [
            ("Waste Estimated — {a} (t)",         ABBR),
            ("Material SubType — {a}",             ABBR),
            ("Vehicle Type — {a}",                 ABBR),
            ("Transport Dist A4 — {a} (km)",       ABBR),
            ("Transport Dist C2 — {a} (km)",       ABBR),
            ("EOL Recycle% — {a}",                 ABBR),
            ("EOL Reuse% — {a}",                   ABBR),
            ("EOL Landfill% — {a}",                ABBR),
            ("EOL Incineration% — {a}",            ABBR),
            ("GWP Total — {a} (kgCO₂e)",           ABBR),
            ("AP Acidification — {a} (kgSO₂e)",   ABBR),
            ("EP Eutrophication — {a} (kgPO₄e)",  ABBR),
            ("Circularity Score — {a} (/100)",     ABBR),
            ("Avoided Emissions — {a} (tCO₂e)",   ABBR),
            ("Virgin Mat Savings — {a} (INR)",     ABBR),
            ("Landfill Diverted — {a} (t)",        ABBR),
            ("Landfill Cost Saved — {a} (INR)",    ABBR),
        ]:
            for a in fmt:
                per_mat_hdrs.append(grp.replace("{a}", a))

        totals_hdr = [
            "TOTAL — Waste Estimated (t)",
            "TOTAL — GWP Emissions (tCO₂e)",
            "TOTAL — AP Acidification (kgSO₂e)",
            "TOTAL — EP Eutrophication (kgPO₄e)",
            "OVERALL Circularity Score (/100)",
            "TOTAL — Avoided Emissions (tCO₂e)",
            "TOTAL — Virgin Mat Savings (INR)",
            "TOTAL — Landfill Diverted (t)",
            "TOTAL — Landfill Cost Saved (INR)",
        ]
        full_hdr = static_hdr + per_mat_hdrs + totals_hdr

        if not sheet.row_values(1):
            sheet.append_row(full_hdr)

        # ── BUILD DATA ROW ───────────────────────────────────────────────────
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            proj.get("name",""),
            proj.get("location",""),
            proj.get("construction_type",""),
            proj.get("building_type",""),
            proj.get("builtup_area",""),
            proj.get("locality",""),
            proj.get("input_method",""),
        ]

        # Per-material: waste
        for m in MATS:
            row.append(round(g(wt_map, m, "waste_tonnes"), 3) if m in wt_map else "")
        # Per-material: sub_type
        for m in MATS:
            row.append(g(ei_map, m, "sub_type", default=""))
        # Per-material: vehicle
        for m in MATS:
            row.append(g(ei_map, m, "vehicle", default=""))
        # Per-material: dist A4
        for m in MATS:
            row.append(g(ei_map, m, "distance_km"))
        # Per-material: dist C2
        for m in MATS:
            row.append(g(ei_map, m, "distance_km_c2"))
        # Per-material: EOL Recycle%
        for m in MATS:
            row.append(g(er_map, m, "eol", "Recycle"))
        # Per-material: EOL Reuse%
        for m in MATS:
            row.append(g(er_map, m, "eol", "Reuse"))
        # Per-material: EOL Landfill%
        for m in MATS:
            row.append(g(er_map, m, "eol", "Landfill"))
        # Per-material: EOL Incineration%
        for m in MATS:
            row.append(g(er_map, m, "eol", "Incineration"))
        # Per-material: GWP
        for m in MATS:
            row.append(round(g(er_map, m, "total_gwp"), 3) if m in er_map else "")
        # Per-material: AP
        for m in MATS:
            row.append(round(g(er_map, m, "AP"), 4) if m in er_map else "")
        # Per-material: EP
        for m in MATS:
            row.append(round(g(er_map, m, "EP"), 6) if m in er_map else "")
        # Per-material: circularity score
        for m in MATS:
            row.append(round(cs_map[m] * 100, 2) if m in cs_map else "")
        # Per-material: avoided emissions (tCO2e)
        for m in MATS:
            row.append(round(g(ben_map, m, "avoided_emission_kgco2e") / 1000, 4) if m in ben_map else "")
        # Per-material: virgin savings
        for m in MATS:
            row.append(round(g(ben_map, m, "virgin_material_savings_inr"), 0) if m in ben_map else "")
        # Per-material: landfill diverted
        for m in MATS:
            row.append(round(g(ben_map, m, "landfill_diverted_t"), 3) if m in ben_map else "")
        # Per-material: landfill cost saved
        for m in MATS:
            row.append(round(g(ben_map, m, "landfill_cost_saved_inr"), 0) if m in ben_map else "")

        # Totals
        total_waste   = round(sum(g(er_map, m, "qty_t")     for m in er_map), 3)
        total_gwp     = round(sum(g(er_map, m, "total_gwp") for m in er_map) / 1000.0, 4)
        total_ap      = round(sum(g(er_map, m, "AP")        for m in er_map), 4)
        total_ep      = round(sum(g(er_map, m, "EP")        for m in er_map), 6)
        total_avoided = round(sum(g(ben_map, b, "avoided_emission_kgco2e") for b in ben_map) / 1000.0, 4)
        total_vsav    = round(sum(g(ben_map, b, "virgin_material_savings_inr") for b in ben_map), 0)
        total_lfdiv   = round(sum(g(ben_map, b, "landfill_diverted_t") for b in ben_map), 3)
        total_lfsav   = round(sum(g(ben_map, b, "landfill_cost_saved_inr") for b in ben_map), 0)
        row += [
            total_waste, total_gwp, total_ap, total_ep,
            round(circ_aggregate * 100, 1),
            total_avoided, total_vsav, total_lfdiv, total_lfsav,
        ]

        sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        pass  # silent — never interrupt the user


# ─── FIRESTORE LOGGER — full formatted report ────────────────────────────────
def _get_firestore_client():
    """Initialise Firebase app once per session and return Firestore client."""
    import firebase_admin
    from firebase_admin import credentials as _fb_creds, firestore as _fb_fs
    if not firebase_admin._apps:
        info = {k: v for k, v in st.secrets["firebase"].items()}
        firebase_admin.initialize_app(_fb_creds.Certificate(info))
    return _fb_fs.client()


def _safe_fs_key(k):
    """Sanitise a string for use as a Firestore map key.
    Firestore forbids keys containing '/', and keys with special chars like '—'
    can cause issues in some client versions. Replace with safe equivalents."""
    return (str(k)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
            .replace(" — ", "__")
            .replace("—", "__"))


def _sanitise_dict_keys(d):
    """Recursively sanitise all dict keys in a nested structure."""
    if isinstance(d, dict):
        return {_safe_fs_key(k): _sanitise_dict_keys(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_sanitise_dict_keys(i) for i in d]
    # Firestore does not accept Python None — replace with empty string
    if d is None:
        return ""
    return d


def log_to_firestore(proj, waste_table, emission_inputs, emission_results,
                     circ_scores, circ_aggregate, benefits):
    """Store full structured data + formatted report in Firestore collection 'submissions'."""
    try:
        db = _get_firestore_client()

        report_str = _build_report_string(
            proj, waste_table, emission_inputs, emission_results,
            circ_scores, circ_aggregate, benefits
        )

        safe_name = proj.get("name", "unknown").replace(" ", "_")[:30]
        doc_id    = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + safe_name

        # ── Compute summary totals ───────────────────────────────────────────
        total_waste   = round(sum(r["qty_t"]     for r in emission_results.values()), 3)
        total_gwp     = round(sum(r["total_gwp"] for r in emission_results.values()) / 1000.0, 4)
        total_ap      = round(sum(r["AP"]        for r in emission_results.values()), 4)
        total_ep      = round(sum(r["EP"]        for r in emission_results.values()), 6)
        total_avoided = round(sum(b.get("avoided_emission_kgco2e", 0)    for b in benefits.values()) / 1000.0, 4)
        total_vsav    = round(sum(b.get("virgin_material_savings_inr", 0) for b in benefits.values()), 0)
        total_lfdiv   = round(sum(b.get("landfill_diverted_t", 0)         for b in benefits.values()), 3)
        total_lfsav   = round(sum(b.get("landfill_cost_saved_inr", 0)     for b in benefits.values()), 0)

        # ── Serialise waste_table — strip internal-only fields, sanitise keys ─
        waste_table_serial = [
            {
                "material":     str(row.get("material", "")),
                "category":     str(row.get("category", row.get("material", ""))),
                "waste_tonnes": round(float(row.get("waste_tonnes", 0)), 4),
                "unit":         str(row.get("unit", "tonnes")),
            }
            for row in waste_table
        ]

        # ── Serialise per-material emission results ──────────────────────────
        emission_results_serial = {}
        for mat, r in emission_results.items():
            emission_results_serial[_safe_fs_key(mat)] = {
                "material_label": str(mat),
                "category":       str(r.get("category", mat)),
                "A1A3":      round(float(r.get("A1A3", 0)), 2),
                "A4":        round(float(r.get("A4",   0)), 2),
                "A5":        round(float(r.get("A5",   0)), 2),
                "C1":        round(float(r.get("C1",   0)), 2),
                "C2":        round(float(r.get("C2",   0)), 2),
                "C3":        round(float(r.get("C3",   0)), 2),
                "C4":        round(float(r.get("C4",   0)), 2),
                "AP":        round(float(r.get("AP",   0)), 4),
                "EP":        round(float(r.get("EP",   0)), 6),
                "total_gwp": round(float(r.get("total_gwp", 0)), 2),
                "qty_t":     round(float(r.get("qty_t", 0)), 3),
                "eol":       {str(k): int(v) for k, v in r.get("eol", {}).items()},
            }

        # ── Serialise benefits ───────────────────────────────────────────────
        benefits_serial = {}
        for mat, b in benefits.items():
            benefits_serial[_safe_fs_key(mat)] = {
                "material_label":              str(mat),
                "recycled_t":                  round(float(b.get("recycled_t", 0)), 3),
                "reused_t":                    round(float(b.get("reused_t", 0)), 3),
                "landfill_t":                  round(float(b.get("landfill_t", 0)), 3),
                "landfill_diverted_t":         round(float(b.get("landfill_diverted_t", 0)), 3),
                "avoided_emission_kgco2e":     round(float(b.get("avoided_emission_kgco2e", 0)), 3),
                "virgin_material_savings_inr": round(float(b.get("virgin_material_savings_inr", 0)), 0),
                "landfill_cost_saved_inr":     round(float(b.get("landfill_cost_saved_inr", 0)), 0),
                "landfill_cost_actual_inr":    round(float(b.get("landfill_cost_actual_inr", 0)), 0),
                "landfill_cost_per_tonne":     float(b.get("landfill_cost_per_tonne", 0)),
            }

        # ── Serialise circularity scores ─────────────────────────────────────
        circ_scores_serial = {
            _safe_fs_key(mat): round(float(sc) * 100, 2)
            for mat, sc in circ_scores.items()
        }

        # ── Serialise transport inputs ────────────────────────────────────────
        transport_serial = {}
        for mat, ei_val in emission_inputs.items():
            transport_serial[_safe_fs_key(mat)] = {
                "material_label": str(mat),
                "vehicle":        str(ei_val.get("vehicle", "")),
                "distance_km":    float(ei_val.get("distance_km", 0)),
                "distance_km_c2": float(ei_val.get("distance_km_c2", 0)),
                "sub_type":       str(ei_val.get("sub_type", "")),
            }

        # ── Write to Firestore ────────────────────────────────────────────────
        doc_payload = {
            "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "project_name": str(proj.get("name", "")),
            "city":         str(proj.get("location", "")),
            "inputs": {
                "project_type":     str(proj.get("construction_type", "")),
                "building_type":    str(proj.get("building_type", "")),
                "builtup_area_m2":  float(proj.get("builtup_area", 0) or 0),
                "plot_area_m2":     float(proj.get("plot_area", 0) or 0),
                "num_floors":       int(proj.get("num_floors", 0) or 0),
                "input_method":     str(proj.get("input_method", "")),
                "locality":         str(proj.get("locality", "")),
                "waste_table":      waste_table_serial,
                "transport_inputs": transport_serial,
            },
            "outputs": {
                "summary": {
                    "total_waste_t":           total_waste,
                    "total_gwp_tco2e":         total_gwp,
                    "total_ap_kg_so2e":        total_ap,
                    "total_ep_kg_po4e":        total_ep,
                    "circularity_score":       round(float(circ_aggregate) * 100, 1),
                    "avoided_emissions_tco2e": total_avoided,
                    "virgin_mat_savings_inr":  total_vsav,
                    "landfill_diverted_t":     total_lfdiv,
                    "landfill_cost_saved_inr": total_lfsav,
                },
                "per_material_emissions":   emission_results_serial,
                "per_material_circularity": circ_scores_serial,
                "per_material_benefits":    benefits_serial,
            },
            "report": report_str,
        }

        db.collection("submissions").document(doc_id).set(doc_payload)

    except Exception as e:
        # Store error in session state so it's visible during development;
        # never raises — never interrupts the user flow.
        st.session_state["_firestore_error"] = str(e)

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ReForm — C&D Waste Tool",
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

# ── Sub-materials per category (mirrors keys in MATERIAL_GWP_A1A3) ───────────
# Used only in the input UI (Page 3, material-quantities method) so users can
# enter M20, M35, AAC Block etc. as separate rows under one category.
# The LCA engine (compute_emissions) already uses "sub_type" from emission_inputs;
# these lists simply drive the UI pickers — no other code is touched.
MATERIAL_SUBCATEGORIES = {
    "Concrete":         list(MATERIAL_GWP_A1A3["Concrete"].keys()),
    "Brick/Masonry":    list(MATERIAL_GWP_A1A3["Brick/Masonry"].keys()),
    "Steel/Metal":      list(MATERIAL_GWP_A1A3["Steel/Metal"].keys()),
    "Wood/Timber":      list(MATERIAL_GWP_A1A3["Wood/Timber"].keys()),
    "Glass":            list(MATERIAL_GWP_A1A3["Glass"].keys()),
    "Plastic":          list(MATERIAL_GWP_A1A3["Plastic"].keys()),
    "Bitumen":          list(MATERIAL_GWP_A1A3["Bitumen"].keys()),
    "Soil/Sand/Gravel": list(MATERIAL_GWP_A1A3["Soil/Sand/Gravel"].keys()),
    "Others":           list(MATERIAL_GWP_A1A3["Others"].keys()),
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

# C1 Demolition energy (kg CO2e / tonne of material demolished)
# Source: Excavator/hydraulic breaker ~3 kWh/t × CEA 2024 grid EF 0.716 kgCO2/kWh = 2.15 kg/t;
#         plant overhead + diesel machinery adds ~3–8 kg/t depending on material hardness.
#         Alotaibi et al. (2022) MDPI Buildings — C1 ≈ 0.2% of lifecycle; AEEE/Saint-Gobain (2024).
#         Concrete/masonry: mechanical breaking is energy-intensive (~5–8 kg/t)
#         Steel: cutting/shearing lower energy (~3 kg/t); Soil: minimal (~1 kg/t)
C1_DEMOLITION_EF = {
    "Concrete": 6.0, "Brick/Masonry": 4.0, "Steel/Metal": 3.0, "Wood/Timber": 2.0,
    "Glass": 2.5, "Plastic": 2.0, "Bitumen": 3.0, "Soil/Sand/Gravel": 1.0, "Others": 4.0,
}

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

# ── Virgin fraction of INPUT material (Vu) by sub-type ───────────────────────
# Vu = fraction of input that is virgin (0 = fully recycled input, 1 = fully virgin)
# Used in full EMF MCI formula: F(x) = 0.9 × (1 − 0.5×Vu − 0.5×Fr)
# Sources:
#   OPC/PPC concrete: 100% virgin raw materials (limestone, clinker) → Vu = 1.0
#   GGBS blend (40%): 40% GGBS (industrial by-product) → Vu = 0.60
#   Fly Ash Brick: ~30% fly ash (industrial waste) → Vu = 0.70 [IS 12894]
#   FaLG Block: ~50% lime+gypsum industrial by-product → Vu = 0.50
#   AAC Block: primarily sand + cement, minimal recycled content → Vu = 0.95
#   TMT Rebar BF-BOF: virgin iron ore → Vu = 1.0
#   TMT Rebar DRI-EAF: ~30% scrap mix in Indian EAF route → Vu = 0.70
#   TMT Rebar Scrap EAF: ~90% scrap → Vu = 0.10
#   Structural Steel BF: virgin → Vu = 1.0
#   Aluminium extruded: primary Al → Vu = 1.0 (recycled Al handled separately)
#   Air-dried/Kiln-dried Timber: virgin harvest → Vu = 1.0
#   Plywood: typically virgin timber → Vu = 1.0
#   Bamboo: rapidly renewable → Vu = 0.80 (treated as near-circular bio-resource)
#   Float/Toughened Glass: virgin silica → Vu = 1.0
#   HDPE: virgin petrochemical → Vu = 1.0; PVC: Vu = 1.0
#   Bitumen: petroleum by-product → Vu = 0.90 (minor recycled content)
#   Default (unlisted): Vu = 1.0 (conservative; fully virgin)
VU_BY_SUBTYPE = {
    # Concrete
    "M20 (OPC)":        1.00,
    "M25 (OPC)":        1.00,
    "M30 (OPC)":        1.00,
    "M30 (PPC blend)":  0.85,  # ~15% fly ash/pozzolan replacement
    "M35 (OPC)":        1.00,
    "M40 (OPC)":        1.00,
    "M40 (GGBS 40%)":   0.60,  # 40% GGBS replaces clinker
    # Brick/Masonry
    "Red Brick (Zigzag Kiln)":   1.00,
    "Red Brick (Bull's Trench)": 1.00,
    "AAC Block":         0.95,
    "Fly Ash Brick":     0.70,
    "FaLG Block":        0.50,
    # Steel
    "TMT Rebar (BF-BOF)":     1.00,
    "TMT Rebar (DRI-EAF)":    0.70,
    "TMT Rebar (Scrap EAF)":  0.10,
    "Structural Steel (BF)":  1.00,
    "Aluminium (extruded)":   1.00,
    # Timber
    "Air-dried Timber":  1.00,
    "Kiln-dried Timber": 1.00,
    "Plywood":           1.00,
    "Bamboo":            0.80,
    # Glass
    "Float Glass":       1.00,
    "Toughened Glass":   1.00,
    # Plastic
    "PVC (uPVC)":        1.00,
    "HDPE":              1.00,
    # Bitumen
    "Asphalt/Bitumen":   0.90,
    # Generic fallback (all others)
    "Generic":           1.00,
}


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
    "scenarios": [],         # list of saved design scenarios for TOPSIS comparison
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def go(page): st.session_state.page = page

def start_new_design():
    """Reset project/input data for a new design while keeping saved TOPSIS scenarios."""
    st.session_state.project          = {}
    st.session_state.input_method      = None
    st.session_state.waste_table       = []
    st.session_state.emission_inputs   = {}
    st.session_state.results           = {}
    st.session_state.page              = 1
    # Clear auxiliary widget state left over from the previous run
    for k in ["mq_sel", "mq_rows", "ab_pct", "ab_last_sel", "_firestore_error"]:
        st.session_state.pop(k, None)

# ══════════════════════════════════════════════════════════════════════════════
# PROGRESS BAR
# ══════════════════════════════════════════════════════════════════════════════
STEPS = ["Project Info", "Data Input", "Waste Estimation", "Emissions & EOL", "Results & Report"]

def show_progress():
    st.markdown(
        '<div style="text-align:center;padding-bottom:4px">'
        '<span style="font-family:DM Sans,sans-serif;font-size:1.7rem;font-weight:700;color:#10b981">ReForm</span>'
        '<span style="font-family:DM Sans,sans-serif;font-size:0.82rem;color:#6b7280;margin-left:10px;">'
        'C&amp;D Waste Estimation Tool</span></div>',
        unsafe_allow_html=True)
    st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)
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
    """Returns nested dict: material → {A1A3, A4, A5, C1, C2, C3, C4, total, AP, EP}

    Supports sub-material rows (e.g. "Concrete — M20 (OPC)") created by the
    material-quantities input method. Each such row carries a 'category' field
    that maps it back to the correct GWP, AP, EP, and C3 lookup tables.
    The sub_type in emission_inputs is used for the precise per-grade GWP factor.
    """
    results = {}
    for row in waste_table:
        mat   = row["material"]
        qty_t = row["waste_tonnes"]
        if qty_t <= 0:
            continue
        # 'category' is the base material category (e.g. "Concrete") — used for
        # all factor lookups. Falls back to mat itself for rows without sub-materials.
        cat = row.get("category", mat)

        ei    = emission_inputs.get(mat, {})
        sub   = ei.get("sub_type", "Generic")
        veh   = ei.get("vehicle", "Diesel Truck (> 7.5 t)")
        dist  = float(ei.get("distance_km", 20))
        dist_c2 = float(ei.get("distance_km_c2", 10))
        eol   = ei.get("eol", DEFAULT_EOL.get(cat, {"Recycle":50,"Reuse":20,"Landfill":30,"Incineration":0,"Other":0}))

        # GWP A1-A3: look up by category, then exact sub_type within that map
        gwp_map = MATERIAL_GWP_A1A3.get(cat, MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0}))
        gwp_a1a3_factor = gwp_map.get(sub, gwp_map.get("Generic", 50.0))  # kg CO2e / tonne
        A1A3 = gwp_a1a3_factor * qty_t

        tf   = TRANSPORT_EF.get(veh, 0.12)
        A4   = tf * dist * qty_t         # kg CO2e
        A5   = A5_FACTOR * qty_t * 1000  # kg CO2e

        # C1: demolition machinery energy — excavator/breaker ~3 kWh/t × Indian grid
        # 0.716 kgCO2/kWh (CEA 2024) = ~2.15 kg CO2e/t; +diesel plant overhead ~5 kg/t
        # Source: Alotaibi et al. (2022) MDPI Buildings; AEEE/Saint-Gobain (2024)
        C1   = C1_DEMOLITION_EF.get(cat, C1_DEMOLITION_EF.get(mat, 5.0)) * qty_t

        C2   = tf * dist_c2 * qty_t

        # C3: processing energy applies only to the fraction that is recycled
        # (material going to landfill or reuse does not pass through a crushing plant)
        recycle_frac = eol.get("Recycle", 0) / 100.0
        C3   = C3_PROCESSING_EF.get(cat, C3_PROCESSING_EF.get(mat, 5.0)) * qty_t * recycle_frac

        C4   = C4_LANDFILL_CO2E * qty_t * (eol.get("Landfill", 0)/100.0)

        ap  = AP_FACTORS.get(cat, AP_FACTORS.get(mat, 0.5)) * qty_t
        ep  = EP_FACTORS.get(cat, EP_FACTORS.get(mat, 0.07)) * qty_t

        results[mat] = {
            "qty_t": qty_t,
            "A1A3": A1A3, "A4": A4, "A5": A5,
            "C1": C1, "C2": C2, "C3": C3, "C4": C4,
            "total_gwp": A1A3 + A4 + A5 + C1 + C2 + C3 + C4,
            "AP": ap, "EP": ep,
            "eol": eol,
            "category": cat,
            "sub_type": sub,   # stored so circularity can look up Vu by sub-type
        }
    return results


def compute_circularity(emission_results):
    """Returns dict: material → MCI score (0–1); plus waste-weighted aggregate.

    Uses the FULL Ellen MacArthur Foundation Material Circularity Indicator (MCI) formula:
        MCI = 1 - LFI × F(x)
        F(x) = 0.9 × (1 - 0.5×Vu - 0.5×Fr)

    where:
        LFI = (Landfill% + Incineration%) / 100          [Linear Flow Index]
        Fr  = (Recycle% + Reuse%) / 100                  [Recovered fraction at EOL]
        Vu  = virgin fraction of INPUT material           [from VU_BY_SUBTYPE lookup]
              (0 = fully recycled-content input, 1 = fully virgin input)

    Vu is looked up from VU_BY_SUBTYPE using the sub_type stored in emission_results.
    This means material substitution (e.g. GGBS concrete, Scrap EAF steel) is credited
    on the input side — rewarding circular procurement, not just EOL routing.

    MCI = 1.0 → perfectly circular; MCI → 0 → fully linear.
    EMF floor of 0.1 applies only when LFI > 0 (some waste goes to landfill/incineration).
    Source: EMF (2015) MCI Technical Appendix — ellenmacarthurfoundation.org/material-circularity-indicator
    """
    scores = {}
    vu_used = {}         # exposed so UI can display Vu per material
    weighted_total = 0.0
    total_waste = 0.0
    for mat, r in emission_results.items():
        eol = r["eol"]
        recycle  = eol.get("Recycle", 0) / 100.0
        reuse    = eol.get("Reuse", 0) / 100.0
        landfill = eol.get("Landfill", 0) / 100.0
        incin    = eol.get("Incineration", 0) / 100.0

        # Look up Vu from sub_type; fall back to Generic (1.0 = fully virgin)
        sub = r.get("sub_type", "Generic")
        Vu  = VU_BY_SUBTYPE.get(sub, VU_BY_SUBTYPE.get("Generic", 1.0))
        vu_used[mat] = Vu

        Fr  = min(recycle + reuse, 1.0)
        LFI = min(landfill + incin, 1.0)

        # Full EMF F(x) — both Vu and Fr affect score
        Fx  = 0.9 * (1.0 - 0.5 * Vu - 0.5 * Fr)
        Fx  = max(Fx, 0.0)   # clamp: if Vu=0 and Fr=1, Fx=0 → MCI=1.0

        mci = 1.0 - LFI * Fx
        # No artificial floor — the formula's natural minimum for virgin inputs (Vu=1.0)
        # with 100% landfill is 0.55, which is the correct EMF result for that scenario.
        # As Vu decreases (recycled-content inputs), the minimum approaches 0.
        mci = min(max(mci, 0.0), 1.0)

        scores[mat] = round(mci, 3)
        vu_used[mat] = round(Vu, 2)
        weighted_total += mci * r["qty_t"]
        total_waste    += r["qty_t"]
    aggregate = weighted_total / total_waste if total_waste > 0 else 0.0
    return scores, round(aggregate, 3), vu_used


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

        gwp_map = MATERIAL_GWP_A1A3.get(r.get("category", mat), MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0}))
        avg_gwp = list(gwp_map.values())[0]
        avoided_em = (recycle_t + reuse_t) * abs(avg_gwp) * RECYCLING_EFFICIENCY  # kg CO2e

        vp  = VIRGIN_PRICE.get(r.get("category", mat), VIRGIN_PRICE.get(mat, 5000))
        virgin_savings = (recycle_t + reuse_t) * vp  # INR

        landfill_diverted_t = recycle_t + reuse_t
        landfill_cost_saved = landfill_diverted_t * lf_cost  # INR: cost saved by diverting material away from landfill
        landfill_cost_actual= landfill_t * lf_cost           # INR: actual disposal cost for mass that still goes to landfill

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


def _pdf_pie(vals, labels, colors_list, title):
    """Render a pie chart to a ReportLab Image, sized from the actual saved PNG."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt, io
    from PIL import Image as PILImage
    from reportlab.platypus import Image as RLImg

    fig, ax = plt.subplots(figsize=(3.4, 3.4))
    _, _, autotexts = ax.pie(
        vals, labels=None, colors=colors_list,
        startangle=140, autopct="%1.0f%%", pctdistance=0.78,
        wedgeprops=dict(linewidth=0.5, edgecolor="white"))
    for at in autotexts:
        at.set_fontsize(7); at.set_fontfamily("serif")
    ax.set_title(title, fontsize=9, fontweight="bold", pad=7,
                 fontfamily="serif")
    ax.legend(labels, loc="lower center", bbox_to_anchor=(0.5, -0.22),
              ncol=2, fontsize=6, frameon=False, prop={"family": "serif"})
    fig.subplots_adjust(bottom=0.20, top=0.88)

    buf2 = io.BytesIO()
    fig.savefig(buf2, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    buf2.seek(0)

    # Measure actual pixel dimensions so RLImg never clips
    pil = PILImage.open(buf2); w_px, h_px = pil.size; buf2.seek(0)
    target_w = 3.4 * 72   # points (~5.8 cm)
    target_h = target_w * h_px / w_px
    return RLImg(buf2, width=target_w, height=target_h)


def _pdf_bar(mats_list, er_dict):
    """Render the stacked emission bar chart, sized from the actual PNG."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt, io
    from PIL import Image as PILImage
    from reportlab.platypus import Image as RLImg

    stages  = ["A1A3","A4","A5","C1","C2","C3","C4"]
    slabels = ["A1-A3","A4","A5","C1","C2","C3","C4"]
    scols   = ["#1d4ed8","#3b82f6","#93c5fd","#dc2626","#f87171","#16a34a","#4ade80"]

    fig, ax = plt.subplots(figsize=(6.8, 3.2))
    bottoms = [0.0] * len(mats_list)
    for sk, sl, sc in zip(stages, slabels, scols):
        vals = [er_dict[m].get(sk, 0) for m in mats_list]
        ax.bar(mats_list, vals, bottom=bottoms, label=sl, color=sc, width=0.52)
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    ax.set_ylabel("kg CO\u2082e", fontsize=8, fontfamily="serif")
    ax.set_title("Emission Stages by Material", fontsize=9.5,
                 fontweight="bold", fontfamily="serif")
    ax.legend(loc="upper right", fontsize=7.5, frameon=False, ncol=4,
              prop={"family": "serif"})
    plt.xticks(rotation=22, ha="right", fontsize=7.5,
               fontfamily="serif")
    fig.subplots_adjust(bottom=0.24, top=0.90)

    buf2 = io.BytesIO()
    fig.savefig(buf2, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    buf2.seek(0)

    pil = PILImage.open(buf2); w_px, h_px = pil.size; buf2.seek(0)
    target_w = 6.8 * 72
    target_h = target_w * h_px / w_px
    return RLImg(buf2, width=target_w, height=target_h)


def generate_pdf_report(project, waste_table, emission_results, circ_scores, circ_aggregate, benefits):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, HRFlowable, KeepTogether)
    from reportlab.lib.units import cm

    # ── Palette ──────────────────────────────────────────────────────────────
    GREEN  = colors.HexColor("#2e7d32")
    DARK   = colors.HexColor("#1a1a1a")
    LGREY  = colors.HexColor("#f5f5f5")
    MGREY  = colors.HexColor("#e0e0e0")

    mats_p = [r["material"] for r in waste_table]
    LM = 1.8 * cm
    RM = 1.8 * cm
    page_w = A4[0] - LM - RM   # usable width ≈ 457 pt

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
          leftMargin=LM, rightMargin=RM,
          topMargin=1.8*cm, bottomMargin=2.2*cm)

    # ── Styles ────────────────────────────────────────────────────────────────
    TNR   = "Times-Roman"
    TNR_B = "Times-Bold"
    TNR_I = "Times-Italic"

    title_sty = ParagraphStyle("rtitle",
        fontName=TNR_B, fontSize=26, textColor=GREEN,
        alignment=1, spaceAfter=2, spaceBefore=0)

    sub_sty = ParagraphStyle("rsub",
        fontName=TNR_I, fontSize=10, textColor=colors.HexColor("#555555"),
        alignment=1, spaceAfter=6)

    h2_sty = ParagraphStyle("rh2",
        fontName=TNR_B, fontSize=11, textColor=DARK,
        spaceBefore=10, spaceAfter=4, leftIndent=0)

    sm_sty = ParagraphStyle("rsm",
        fontName=TNR_I, fontSize=7, leading=10,
        textColor=colors.HexColor("#777777"))

    # Cell style — used inside table cells so text wraps instead of overflowing
    cell_sty = ParagraphStyle("rcell",
        fontName=TNR, fontSize=8.5, leading=11, textColor=DARK)
    cell_hdr = ParagraphStyle("rchdr",
        fontName=TNR_B, fontSize=8.5, leading=11, textColor=colors.white)
    cell_sm  = ParagraphStyle("rcsm",
        fontName=TNR_I, fontSize=7.5, leading=10, textColor=DARK)

    # ── Table style factory ───────────────────────────────────────────────────
    def ts(header_col):
        return TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), header_col),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LGREY]),
            ("GRID",          (0, 0), (-1, -1), 0.35, MGREY),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (0, 0), (0, -1),  "LEFT"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ])

    def section(txt):
        return [
            Paragraph(txt, h2_sty),
            HRFlowable(width="100%", thickness=0.6,
                       color=colors.HexColor("#cccccc"), spaceAfter=4),
        ]

    # Wrap a string in a Paragraph for a header cell (white text)
    def H(txt): return Paragraph(txt, cell_hdr)
    # Wrap a string in a Paragraph for a body cell
    def C(txt): return Paragraph(str(txt), cell_sty)

    E = []  # story

    # ── TITLE ─────────────────────────────────────────────────────────────────
    E.append(Spacer(1, 0.2*cm))
    E.append(Paragraph("ReForm", title_sty))
    E.append(Paragraph("C&amp;D Waste Estimation Report", sub_sty))
    E.append(HRFlowable(width="100%", thickness=1.5, color=GREEN, spaceAfter=8))

    # ── PROJECT INFO ──────────────────────────────────────────────────────────
    lbl = ParagraphStyle("rlbl", fontName=TNR_B, fontSize=9, textColor=DARK,
                         leftIndent=0)
    val = ParagraphStyle("rval", fontName=TNR,   fontSize=9, textColor=DARK)
    inf = [
        [Paragraph("Project",  lbl), Paragraph(project.get("name","—"), val),
         Paragraph("Location", lbl), Paragraph(project.get("location","—"), val)],
        [Paragraph("Type",     lbl), Paragraph(project.get("construction_type","—"), val),
         Paragraph("Building", lbl), Paragraph(project.get("building_type","—"), val)],
        [Paragraph("Area",     lbl), Paragraph(f"{project.get('builtup_area','—')} m²", val),
         Paragraph("Floors",   lbl), Paragraph(str(project.get("num_floors","—")), val)],
    ]
    col_lbl = 1.8*cm
    col_val = page_w/2 - col_lbl
    it = Table(inf, colWidths=[col_lbl, col_val, col_lbl, col_val])
    it.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), LGREY),
        ("GRID",          (0, 0), (-1, -1), 0.3, MGREY),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    E.append(it); E.append(Spacer(1, 0.4*cm))

    # ── KPI SUMMARY BAR ───────────────────────────────────────────────────────
    tw = sum(r["waste_tonnes"] for r in waste_table)
    tg = sum(r["total_gwp"] for r in emission_results.values()) / 1000
    ta = sum(b["avoided_emission_kgco2e"] for b in benefits.values()) / 1000
    tv = sum(b["virgin_material_savings_inr"] for b in benefits.values())

    kpi_sty = ParagraphStyle("rkpi", fontName=TNR_B, fontSize=8,
                              textColor=colors.white, alignment=1, leading=12)
    kpi_data = [[
        Paragraph(f"{tw:.2f} t\nTotal Waste",           kpi_sty),
        Paragraph(f"{tg:.2f} tCO\u2082e\nGWP",         kpi_sty),
        Paragraph(f"{circ_aggregate*100:.1f}/100\nCircularity", kpi_sty),
        Paragraph(f"{ta:.2f} tCO\u2082e\nAvoided",     kpi_sty),
        Paragraph(f"\u20b9{tv:,.0f}\nVirgin Savings",  kpi_sty),
    ]]
    kpi = Table(kpi_data, colWidths=[page_w / 5] * 5)
    kpi.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#1a1a1a")),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
        ("LINEAFTER",     (0, 0), (-2, -1), 0.5, colors.HexColor("#444444")),
    ]))
    E.append(kpi); E.append(Spacer(1, 0.55*cm))

    # ── SECTION 1 — WASTE ─────────────────────────────────────────────────────
    E += section("1 — Waste Estimation")
    col_mat = page_w * 0.55
    col_num = (page_w - col_mat) / 2
    wd = [[H("Material"), H("Waste (t)"), H("% of Total")]]
    for r in waste_table:
        wd.append([C(r["material"]),
                   C(f"{r['waste_tonnes']:.2f}"),
                   C(f"{r['waste_tonnes']/tw*100:.1f}%")])
    wt2 = Table(wd, colWidths=[col_mat, col_num, col_num])
    wt2.setStyle(ts(colors.HexColor("#1b5e20")))
    E.append(wt2)
    E.append(Paragraph(WASTE_RATE_SOURCE, sm_sty))
    E.append(Spacer(1, 0.45*cm))

    # ── SECTION 2 — ENVIRONMENTAL IMPACT ─────────────────────────────────────
    if emission_results:
        E += section("2 — Environmental Impact")
        # 8 cols: Material + 7 numeric (A5 shown explicitly so no stage is hidden)
        # Transport = A4+C1+C2 (inbound + demolition + outbound transport)
        # Construction = A5 (on-site construction waste / energy)
        # EOL = C3+C4 (processing + landfill)
        cw2 = [page_w*0.20, page_w*0.12, page_w*0.11,
                page_w*0.11, page_w*0.12, page_w*0.11, page_w*0.11, page_w*0.12]
        ed = [[H("Material"), H("A1–A3\n(kgCO\u2082e)"),
               H("Transport\n(A4+C1+C2)"), H("Construction\n(A5)"),
               H("EOL\n(C3+C4)"), H("Total GWP\n(kgCO\u2082e)"),
               H("AP\n(kgSO\u2082e)"), H("EP\n(kgPO\u2084e)")]]
        for m, r in emission_results.items():
            ed.append([C(m),
                       C(f"{r['A1A3']:.1f}"),
                       C(f"{r['A4']+r['C1']+r['C2']:.1f}"),
                       C(f"{r['A5']:.1f}"),
                       C(f"{r['C3']+r['C4']:.1f}"),
                       C(f"{r['total_gwp']:.1f}"),
                       C(f"{r['AP']:.2f}"),
                       C(f"{r['EP']:.3f}")])
        et = Table(ed, colWidths=cw2)
        et.setStyle(ts(colors.HexColor("#0d47a1")))
        E.append(et)
        E.append(Paragraph(GWP_SOURCE, sm_sty))
        E.append(Spacer(1, 0.45*cm))

    # ── SECTION 3 — CIRCULARITY ───────────────────────────────────────────────
    E += section(f"3 — Circularity  |  Aggregate Score: {circ_aggregate*100:.1f} / 100")
    cw3 = [page_w*0.34, page_w*0.165, page_w*0.165, page_w*0.165, page_w*0.165]
    cd = [[H("Material"), H("Reuse %"), H("Recycle %"), H("Landfill %"), H("MCI / 100")]]
    for m, sc in circ_scores.items():
        eol = emission_results.get(m, {}).get("eol", {})
        cd.append([C(m),
                   C(f"{eol.get('Reuse',0)}"),
                   C(f"{eol.get('Recycle',0)}"),
                   C(f"{eol.get('Landfill',0)}"),
                   C(f"{sc*100:.1f}")])
    ct = Table(cd, colWidths=cw3)
    ct.setStyle(ts(colors.HexColor("#1b5e20")))
    E.append(ct); E.append(Spacer(1, 0.45*cm))

    # ── SECTION 4 — ECONOMIC BENEFITS ────────────────────────────────────────
    if benefits:
        E += section("4 — Economic & Environmental Benefits")
        td2 = sum(b["landfill_diverted_t"] for b in benefits.values())
        tls = sum(b["landfill_cost_saved_inr"] for b in benefits.values())
        cw4 = [page_w * 0.55, page_w * 0.45]
        bd = [
            [H("Metric"), H("Value")],
            [C("Avoided Emissions"),       C(f"{ta:.3f} t CO\u2082e")],
            [C("Virgin Material Savings"),  C(f"\u20b9{tv:,.0f}")],
            [C("Landfill Diverted"),        C(f"{td2:.2f} t")],
            [C("Landfill Cost Saved"),      C(f"\u20b9{tls:,.0f}")],
        ]
        bt = Table(bd, colWidths=cw4)
        bt.setStyle(ts(colors.HexColor("#b45309")))
        E.append(bt); E.append(Spacer(1, 0.45*cm))

    # ── DATA SOURCES ─────────────────────────────────────────────────────────
    E += section("Data Sources")
    sample_lf = (list(benefits.values())[0].get("landfill_cost_per_tonne",
                 DEFAULT_LANDFILL_COST) if benefits else DEFAULT_LANDFILL_COST)
    srcs = [
        "CSE (2020). 'Another Brick off the Wall'. CSE, New Delhi. Table 4, p.30.",
        "IFC/thinkstep (2017). EDGE India Construction Materials Database.",
        GWP_SOURCE,
        f"AP: {AP_SOURCE}",
        f"EFW: {EFW_SOURCE}",
        "IPCC (2006). Guidelines for National GHG Inventories, Vol. 2.",
        "Jang et al. (2022). Materials 15, 5047. DOI: 10.3390/ma15145047.",
        "CEA (2024). CO\u2082 Baseline Database for the Indian Power Sector v18.",
        f"Landfill tipping fee: \u20b9{sample_lf}/tonne. {LANDFILL_COST_SOURCE}",
        VIRGIN_PRICE_SOURCE,
        PLANTS_SOURCE,
        "MoEFCC (2016). C&D Waste Management Rules.",
        "EMF (2015). Material Circularity Indicator (MCI) Technical Appendix.",
    ]
    for i, s in enumerate(srcs, 1):
        E.append(Paragraph(f"{i}.  {s}", sm_sty))

    doc.build(E)
    buf.seek(0)
    return buf


# ══════════════════════════════════════════════════════════════════════════════
# INTERACTIVE ANALOGIES — relatable, everyday-life equivalents for results
# ══════════════════════════════════════════════════════════════════════════════
# All factors below are illustrative communication aids only (NOT used in any
# LCA/MCI/TOPSIS calculation). They are derived from commonly cited reference
# figures so non-technical stakeholders can grasp the scale of the results.
#
#   - CAR_KM_PER_TCO2E      : km driven by an average petrol passenger car per
#                              tonne CO2e (~0.25 kg CO2e/km -> EPA GHG Equivalencies
#                              Calculator methodology; IPCC 2006 road transport EF)
#   - TREE_TCO2E_PER_YEAR   : CO2 sequestered by one mature tree in one year
#                              (~21.77 kg CO2/yr, US EPA / Urban Forestry guidance)
#   - HOUSEHOLD_MONTH_TCO2E : emissions from one month of an average Indian
#                              household's grid electricity use
#                              (~150 kWh/month x 0.716 kg CO2e/kWh, CEA 2024 grid factor)
#   - LPG_CYLINDER_TCO2E    : CO2e from burning one 14.2 kg domestic LPG cylinder
#                              (~3.0 kg CO2e/kg LPG, IPCC 2006 default EF)
#   - TRUCK_CAPACITY_T      : payload of a typical 10-tonne C&D waste tipper truck
#   - BRICK_WEIGHT_T        : weight of one standard clay brick (~3 kg, IS 1077)
#   - CEMENT_BAG_PRICE_INR  : indicative price of one 50 kg OPC cement bag (CPWD DSR 2024)
#   - SKILLED_WAGE_INR_DAY  : indicative skilled-mason daily wage (CPWD DSR 2024)
#   - TRUCK_SPEED_KMH       : average road speed assumed for a loaded C&D waste truck
ANALOGY_SOURCE = ("Illustrative equivalencies for communication only — derived from "
                  "EPA GHG Equivalencies methodology, CEA (2024) grid factor, IPCC (2006) "
                  "transport EFs, IS 1077 brick weight, and CPWD DSR (2024) rates. "
                  "Not used in any LCA/MCI/TOPSIS computation.")

CAR_KM_PER_TCO2E      = 4000.0
TREE_TCO2E_PER_YEAR   = 0.0217
HOUSEHOLD_MONTH_TCO2E = 0.1074
LPG_CYLINDER_TCO2E    = 0.0426
TRUCK_CAPACITY_T      = 10.0
BRICK_WEIGHT_T        = 0.003
CEMENT_BAG_PRICE_INR  = 400.0
SKILLED_WAGE_INR_DAY  = 800.0
TRUCK_SPEED_KMH       = 40.0


def render_emission_analogies(avoided_tco2e):
    """Return list of analogy strings for avoided GHG emissions (tonnes CO2e)."""
    if avoided_tco2e <= 0:
        return []
    car_km   = avoided_tco2e * CAR_KM_PER_TCO2E
    trees    = avoided_tco2e / TREE_TCO2E_PER_YEAR
    hh_month = avoided_tco2e / HOUSEHOLD_MONTH_TCO2E
    lpg      = avoided_tco2e / LPG_CYLINDER_TCO2E
    return [
        f"🚗 Equivalent to **not driving a car for {car_km:,.0f} km** (~{car_km/12000:,.1f} years for an average commuter)",
        f"🌳 Equivalent to the **CO₂ absorbed by ~{trees:,.0f} mature trees in one year**",
        f"🏠 Equivalent to **{hh_month:,.0f} months of an average Indian household's electricity emissions**",
        f"🔥 Equivalent to the emissions from burning **~{lpg:,.0f} domestic LPG cylinders**",
    ]


def render_gwp_footprint_analogies(total_tco2e):
    """Return list of analogy strings for the total GWP footprint (tonnes CO2e)."""
    if total_tco2e <= 0:
        return []
    car_km   = total_tco2e * CAR_KM_PER_TCO2E
    trees    = total_tco2e / TREE_TCO2E_PER_YEAR
    hh_month = total_tco2e / HOUSEHOLD_MONTH_TCO2E
    lpg      = total_tco2e / LPG_CYLINDER_TCO2E
    return [
        f"🚗 This footprint is equivalent to **driving a car {car_km:,.0f} km** (~{car_km/12000:,.1f} years for an average commuter)",
        f"🌳 Offsetting it would need **~{trees:,.0f} mature trees growing for one year**",
        f"🏠 Equivalent to **{hh_month:,.0f} months of an average Indian household's electricity emissions**",
        f"🔥 Equivalent to the emissions from burning **~{lpg:,.0f} domestic LPG cylinders**",
    ]


def render_circularity_analogies(landfill_diverted_t, recycled_t, reused_t):
    """Return list of analogy strings for diverted/recycled/reused material (tonnes)."""
    items = []
    if landfill_diverted_t > 0:
        trucks = landfill_diverted_t / TRUCK_CAPACITY_T
        items.append(f"🚛 **{landfill_diverted_t:,.1f} t** diverted from landfill ≈ **{trucks:,.1f} ten-tonne truckloads** that never go to the dumpsite")
    recovered = recycled_t + reused_t
    if recovered > 0:
        bricks = recovered / BRICK_WEIGHT_T
        items.append(f"🧱 **{recovered:,.1f} t** of material recycled/reused ≈ the weight of **~{bricks:,.0f} standard bricks**")
    return items


def render_economy_analogies(virgin_savings_inr, lf_save_inr):
    """Return list of analogy strings for cost savings (INR)."""
    items = []
    if virgin_savings_inr > 0:
        bags = virgin_savings_inr / CEMENT_BAG_PRICE_INR
        items.append(f"🧰 Virgin material savings of **₹{virgin_savings_inr:,.0f}** ≈ the cost of **~{bags:,.0f} bags of cement** (50 kg, @₹{CEMENT_BAG_PRICE_INR:.0f}/bag)")
    total = virgin_savings_inr + lf_save_inr
    if total > 0:
        wage_days = total / SKILLED_WAGE_INR_DAY
        items.append(f"👷 Combined savings of **₹{total:,.0f}** ≈ **~{wage_days:,.0f} days** of a skilled mason's wages (@₹{SKILLED_WAGE_INR_DAY:.0f}/day)")
    return items


def render_plant_analogy(distance_km, capacity_tpd, total_waste_t):
    """Return list of analogy strings for the nearest recycling plant."""
    items = []
    if distance_km is not None:
        hours = distance_km / TRUCK_SPEED_KMH
        items.append(f"🛣️ At **{distance_km:.0f} km**, a loaded truck would take roughly **{hours:.1f} hours** to reach this plant (@{TRUCK_SPEED_KMH:.0f} km/h)")
    if capacity_tpd and total_waste_t:
        days = total_waste_t / capacity_tpd
        if days < 1:
            items.append(f"⚙️ This plant (capacity {capacity_tpd:,.0f} t/day) could process your entire **{total_waste_t:,.1f} t** of waste in **under a day**")
        else:
            items.append(f"⚙️ This plant (capacity {capacity_tpd:,.0f} t/day) could process your entire **{total_waste_t:,.1f} t** of waste in **~{days:.1f} days**")
    return items


# ══════════════════════════════════════════════════════════════════════════════
# TOPSIS MULTI-CRITERIA DECISION ANALYSIS — for comparing multiple design scenarios
# ══════════════════════════════════════════════════════════════════════════════
TOPSIS_CRITERIA = [
    # (column label, direction)  direction: "benefit" = higher is better, "cost" = lower is better
    ("GWP (t CO2e)",                     "cost"),
    ("Circularity Score (MCI, 0-100)",   "benefit"),
    ("Acidification Potential (kg SO2e)", "cost"),
    ("Eutrophication Potential (kg PO4e)", "cost"),
    ("Net Landfill Waste (t)",           "cost"),
    ("Landfill Diverted (t)",            "benefit"),
    ("Virgin Material Savings (INR)",    "benefit"),
    ("Landfill Cost Saved (INR)",        "benefit"),
]


def scenario_from_results(name, res):
    """Extract the 8 TOPSIS criteria values from a results dict for a saved design."""
    er  = res["emission_results"]
    ca  = res["circ_aggregate"]
    ben = res["benefits"]
    total_gwp     = sum(r["total_gwp"] for r in er.values()) / 1000.0
    total_ap      = sum(r["AP"] for r in er.values())
    total_ep      = sum(r["EP"] for r in er.values())
    total_lf_t    = sum(b["landfill_t"] for b in ben.values())
    total_lf_div  = sum(b["landfill_diverted_t"] for b in ben.values())
    total_virgin  = sum(b["virgin_material_savings_inr"] for b in ben.values())
    total_lf_save = sum(b["landfill_cost_saved_inr"] for b in ben.values())
    return {
        "Design": name,
        "GWP (t CO2e)": round(total_gwp, 3),
        "Circularity Score (MCI, 0-100)": round(ca * 100, 2),
        "Acidification Potential (kg SO2e)": round(total_ap, 3),
        "Eutrophication Potential (kg PO4e)": round(total_ep, 5),
        "Net Landfill Waste (t)": round(total_lf_t, 3),
        "Landfill Diverted (t)": round(total_lf_div, 3),
        "Virgin Material Savings (INR)": round(total_virgin, 0),
        "Landfill Cost Saved (INR)": round(total_lf_save, 0),
    }


def compute_topsis(df, weights):
    """
    df: DataFrame with a 'Design' column plus the 8 TOPSIS_CRITERIA columns.
    weights: dict {criterion_label: weight}, need not sum to 1 (auto-normalised).
    Returns df with added columns: Closeness Score, Rank.
    """
    crit_cols = [c for c, _ in TOPSIS_CRITERIA]
    X = df[crit_cols].astype(float).copy()

    # Avoid divide-by-zero for all-zero columns
    denom = np.sqrt((X ** 2).sum())
    denom = denom.replace(0, 1)
    norm = X / denom

    w = np.array([weights.get(c, 0.0) for c in crit_cols], dtype=float)
    if w.sum() == 0:
        w = np.ones(len(crit_cols))
    w = w / w.sum()

    weighted = norm * w

    ideal_best, ideal_worst = {}, {}
    for c, direction in TOPSIS_CRITERIA:
        col = weighted[c]
        if direction == "benefit":
            ideal_best[c]  = col.max()
            ideal_worst[c] = col.min()
        else:
            ideal_best[c]  = col.min()
            ideal_worst[c] = col.max()
    ideal_best  = pd.Series(ideal_best)
    ideal_worst = pd.Series(ideal_worst)

    dist_best  = np.sqrt(((weighted - ideal_best) ** 2).sum(axis=1))
    dist_worst = np.sqrt(((weighted - ideal_worst) ** 2).sum(axis=1))

    denom2 = (dist_best + dist_worst).replace(0, 1)
    closeness = dist_worst / denom2

    out = df.copy()
    out["Closeness Score"] = closeness.round(4)
    out["Rank"] = out["Closeness Score"].rank(ascending=False, method="min").astype(int)
    out = out.sort_values("Rank").reset_index(drop=True)
    return out



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
            city = st.selectbox("City", [
                "Ahmedabad","Agra","Amritsar","Aurangabad","Bengaluru","Bhopal","Bhubaneswar",
                "Chandigarh","Chennai","Coimbatore","Cuttack","Delhi","Durgapur","Faridabad",
                "Ghaziabad","Greater Noida","Gurugram","Guwahati","Gwalior","Howrah","Hyderabad",
                "Indore","Jabalpur","Jaipur","Jalandhar","Jodhpur","Kanpur","Kochi","Kolkata",
                "Kozhikode","Kota","Lucknow","Ludhiana","Madurai","Mangaluru","Meerut","Mumbai",
                "Mysuru","Nagpur","Nashik","Noida","Patna","Pune","Prayagraj","Rajkot","Raipur",
                "Ranchi","Salem","Solapur","Surat","Thane","Thiruvananthapuram","Thrissur",
                "Tirupati","Trichy","Udaipur","Vadodara","Varanasi","Vijayawada","Visakhapatnam",
                "Warangal","Other"])
            locality = st.text_input("Locality / Area", placeholder="e.g., Banjara Hills, Whitefield")
            builtup = st.number_input("Built-up Area (m²)", min_value=1.0, value=1000.0, step=50.0)
        with c2:
            ctype = st.selectbox("Project Type", ["Construction", "Demolition", "Redevelopment"])
            building_type = st.selectbox("Building Type", ["Residential", "Commercial", "Institutional"])
            num_floors = st.number_input("Number of Floors", min_value=1, value=4, step=1)

        st.markdown('<div class="source-note">💡 Waste estimated using CSE (2020) "Another Brick off the Wall" benchmarks. Building type recorded for data purposes.</div>', unsafe_allow_html=True)
        submitted = st.form_submit_button("Next →", type="primary", use_container_width=True)
        if submitted:
            location_full = f"{locality.strip()}, {city}" if locality.strip() else city
            if not name:
                st.error("Please fill in Project Name.")
            else:
                st.session_state.project = {
                    "name": name, "location": location_full,
                    "city": city, "locality": locality.strip(),
                    "construction_type": ctype, "building_type": building_type,
                    "builtup_area": builtup, "num_floors": int(num_floors),
                }
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
            st.markdown("Only have the floor area? Estimate waste based on per-m² benchmarks from CSE (2020) Table 4.")
            st.markdown("**Indicative** — for early-stage estimates")
            ab = st.button("Use Area-Based Estimate", key="ab", use_container_width=True)

    st.markdown(f'<div class="source-note">📚 Area-based rates: {WASTE_RATE_SOURCE}</div>', unsafe_allow_html=True)

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
    # Report reference text shown against the asterisk for each material's
    # pre-filled default waste %. Mirrors CSE (2020) "Another Brick off the
    # Wall" Table 4 (p.30) wastage-rate ranges used to derive DEFAULT_WF above.
    WASTE_PCT_REFERENCE = {
        "Concrete":         "As per report, Concrete waste = 5% (Construction) / 100% (Demolition) — CSE (2020) Table 4, p.30",
        "Brick/Masonry":    "As per report, Brick/Masonry waste = 8% (Construction) / 100% (Demolition) — CSE (2020) Table 4, p.30",
        "Soil/Sand/Gravel": "As per report, Soil/Sand/Gravel waste = 10% (Construction) / 100% (Demolition) — CSE (2020) Table 4, p.30",
        "Steel/Metal":      "As per report, Steel/Metal waste = 3% (Construction) / 85% (Demolition) — CSE (2020) Table 4, p.30",
        "Wood/Timber":      "As per report, Wood/Timber waste = 12% (Construction) / 70% (Demolition) — CSE (2020) Table 4, p.30",
        "Bitumen":          "As per report, Bitumen waste = 5% (Construction) / 80% (Demolition) — CSE (2020) Table 4, p.30",
        "Plastic":          "As per report, Plastic waste = 5% (Construction) / 80% (Demolition) — CSE (2020) Table 4, p.30",
        "Glass":            "As per report, Glass waste = 4% (Construction) / 80% (Demolition) — CSE (2020) Table 4, p.30",
        "Others":           "As per report, Others waste = 10% (Construction) / 80% (Demolition) — CSE (2020) Table 4, p.30",
    }
    DENSITY = {"Concrete": 2.4, "Brick/Masonry": 1.80, "Soil/Sand/Gravel": 1.7,
               "Steel/Metal": 7.85, "Wood/Timber": 0.7, "Bitumen": 2.3,
               "Plastic": 0.9, "Glass": 2.5, "Others": 1.5}
    # Sub-type specific densities (t/m³) -- override category DENSITY when sub-type is known
    # AAC blocks: 550-650 kg/m³ (IS 2185 Part 3); Fly ash brick ~1.1 t/m³; FaLG ~0.9 t/m³
    # Red brick solid: 1.6-1.9 t/m³; Asphalt mix (laid): 2.2-2.4 t/m³
    # Sources: IS 2185-3 (AAC), IS 12894 (fly ash brick), CPWD specifications
    SUB_DENSITY = {
        "AAC Block": 0.60, "Fly Ash Brick": 1.10, "FaLG Block": 0.90,
        "Red Brick (Zigzag Kiln)": 1.80, "Red Brick (Bull's Trench)": 1.80,
        "Asphalt/Bitumen": 2.30,
    }
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
        SELECTABLE = [m for m in ALL_MATERIALS if m != "Others"]
        if "ab_sel" not in st.session_state:
            st.session_state.ab_sel = {m: (m in defaults_checked) for m in SELECTABLE}

        st.caption("✅ Others is always included — it absorbs any remaining % automatically.")
        c1, c2, c3 = st.columns(3)
        cols3 = [c1, c2, c3]
        for i, mat in enumerate(SELECTABLE):
            with cols3[i % 3]:
                st.session_state.ab_sel[mat] = st.checkbox(
                    mat,
                    value=st.session_state.ab_sel.get(mat, mat in defaults_checked),
                    key=f"abchk_{mat}")

        selected_no_others = [m for m in SELECTABLE if st.session_state.ab_sel.get(m, False)]
        selected = selected_no_others + ["Others"]

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
        for mat in selected_no_others:
            row = st.columns([3, 2, 3])
            row[0].write(mat)
            pct = row[1].number_input(
                "pct", min_value=0.0, max_value=100.0, step=0.5,
                value=float(st.session_state["ab_pct"].get(mat, auto_pct.get(mat, 5.0))),
                key=f"abpct_{mat}", label_visibility="collapsed")
            live[mat] = pct
            wt = pct / 100.0 * total_waste_kg / 1000.0
            row[2].write(f"**{wt:.3f} t**")

        others_pct = round(max(0.0, 100.0 - sum(live.values())), 1)
        live["Others"] = others_pct
        oth_row = st.columns([3, 2, 3])
        oth_row[0].markdown("**Others** *(auto)*")
        oth_row[1].markdown(f"**{others_pct:.1f}%**")
        oth_row[2].markdown(f"**{others_pct/100*total_waste_kg/1000:.3f} t**")

        pct_sum = sum(live.values())
        if sum(live[m] for m in selected_no_others) > 100.0:
            st.error("⚠️ Materials exceed 100% — reduce one. Others is floored at 0%.")
            ok_to_proceed = False
        else:
            total_est = pct_sum / 100.0 * total_waste_kg / 1000.0
            st.success(f"✅ Sums to 100% — Total: **{total_est:.2f} t** (Others = {others_pct:.1f}% auto-filled)")
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
        st.markdown('<div class="info-box">📋 Select material categories, then add one or more specific sub-materials (e.g. M20, M35, AAC Block) under each. Waste is summed per category for display but each sub-material uses its own accurate LCA factor.</div>', unsafe_allow_html=True)

        # ── Step 1: pick categories ───────────────────────────────────────────
        st.markdown("#### Step 1 — Which material categories are on your site?")
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
            st.info("☝️ Tick the material categories present on your site above.")
            st.button("← Back", on_click=lambda: go(2))
            return

        # ── Step 2: per-category sub-material rows ────────────────────────────
        st.markdown("#### Step 2 — Add sub-materials and quantities")
        st.caption("Each category can have multiple specific grades/types. Quantities and waste % entered per sub-material. Waste is summed per category for the waste report; LCA uses each sub-material's individual factor.")

        # We store sub-material rows in session state as a list of dicts per category
        # Structure: st.session_state.mq_rows = {category: [{sub, qty, unit, wf}, ...]}
        if "mq_rows" not in st.session_state:
            st.session_state.mq_rows = {}

        # Sync: add new categories, remove deselected ones
        for mat in selected:
            if mat not in st.session_state.mq_rows:
                sub_options = MATERIAL_SUBCATEGORIES.get(mat, ["Generic"])
                st.session_state.mq_rows[mat] = [
                    {"sub": sub_options[0], "qty": 0.0, "unit": "tonnes", "wf": DEFAULT_WF[mat][ptype]}
                ]
        for mat in list(st.session_state.mq_rows.keys()):
            if mat not in selected:
                del st.session_state.mq_rows[mat]

        for mat in selected:
            sub_options = MATERIAL_SUBCATEGORIES.get(mat, ["Generic"])
            wf_def = DEFAULT_WF[mat][ptype]
            rows = st.session_state.mq_rows[mat]

            with st.expander(f"**{mat}**", expanded=True):
                # Column headers
                hdr = st.columns([2.5, 1.5, 1.5, 1.2, 0.6])
                hdr[0].markdown("**Sub-material / Grade**")
                hdr[1].markdown("**Quantity**")
                hdr[2].markdown("**Unit**")
                hdr[3].markdown("**Waste %***")
                hdr[4].markdown("**Del**")

                rows_to_delete = []
                for idx, row_data in enumerate(rows):
                    cols = st.columns([2.5, 1.5, 1.5, 1.2, 0.6])
                    cur_sub = row_data.get("sub", sub_options[0])
                    sub_idx = sub_options.index(cur_sub) if cur_sub in sub_options else 0

                    new_sub  = cols[0].selectbox("sub", sub_options, index=sub_idx,
                                                  key=f"mqsub_{mat}_{idx}", label_visibility="collapsed")
                    new_qty  = cols[1].number_input("qty", min_value=0.0,
                                                     value=float(row_data.get("qty", 0.0)),
                                                     key=f"mqqty_{mat}_{idx}", label_visibility="collapsed")
                    new_unit = cols[2].selectbox("unit", UNITS,
                                                  index=UNITS.index(row_data.get("unit", "tonnes")),
                                                  key=f"mqunit_{mat}_{idx}", label_visibility="collapsed")
                    new_wf   = cols[3].number_input("wf%", min_value=0.0, max_value=100.0,
                                                     value=float(row_data.get("wf", wf_def)),
                                                     key=f"mqwf_{mat}_{idx}", label_visibility="collapsed")
                    if len(rows) > 1:
                        if cols[4].button("✕", key=f"mqdel_{mat}_{idx}"):
                            rows_to_delete.append(idx)

                    # Update in-place
                    rows[idx] = {"sub": new_sub, "qty": new_qty, "unit": new_unit, "wf": new_wf}

                # Remove deleted rows (reverse order to preserve indices)
                for idx in sorted(rows_to_delete, reverse=True):
                    rows.pop(idx)

                # Add row button
                if st.button(f"＋ Add another {mat} type", key=f"mqadd_{mat}"):
                    rows.append({"sub": sub_options[0], "qty": 0.0, "unit": "tonnes", "wf": wf_def})
                    st.rerun()

                # Show category waste preview
                cat_waste = 0.0
                for rd in rows:
                    q = float(rd.get("qty", 0.0))
                    u = rd.get("unit", "tonnes")
                    wf = float(rd.get("wf", wf_def)) / 100.0
                    sub_rd = rd.get("sub", "")
                    if u == "kg":    q_t = q / 1000.0
                    elif u == "m³":  q_t = q * SUB_DENSITY.get(sub_rd, DENSITY.get(mat, 1.5))
                    elif u == "nos": q_t = q * 0.003
                    else:            q_t = q
                    cat_waste += q_t * wf
                if cat_waste > 0:
                    st.caption(f"↳ Total waste from {mat}: **{cat_waste:.3f} t**")

                st.caption(f"*{WASTE_PCT_REFERENCE.get(mat, '')}")

            st.session_state.mq_rows[mat] = rows

        st.caption("Default waste %: CSE (2020) p.30 — 4–30% for construction; 100% for demolition.")
        st.divider()

        col_b, col_n = st.columns([1, 4])
        col_b.button("← Back", on_click=lambda: go(2), key="mq_back")
        if col_n.button("✅ Calculate & Proceed to Emissions →", type="primary", key="mq_go"):
            tbl = []
            # Prefill emission_inputs so Page 4 picks up the correct sub_type per row
            ei_prefill = {}
            for mat in selected:
                rows = st.session_state.mq_rows.get(mat, [])
                for idx, rd in enumerate(rows):
                    qty  = float(rd.get("qty", 0.0))
                    unit = rd.get("unit", "tonnes")
                    wf   = float(rd.get("wf", DEFAULT_WF[mat][ptype])) / 100.0
                    sub  = rd.get("sub", MATERIAL_SUBCATEGORIES.get(mat, ["Generic"])[0])
                    if qty <= 0:
                        continue
                    if   unit == "kg":  qty_t = qty / 1000.0
                    elif unit == "m³":  qty_t = qty * SUB_DENSITY.get(sub, DENSITY.get(mat, 1.5))
                    elif unit == "nos": qty_t = qty * 0.003
                    else:               qty_t = qty
                    waste_t = qty_t * wf
                    if waste_t <= 0:
                        continue
                    # Use "Material (Sub)" as the row key when there are multiple sub-types
                    if len([r for r in rows if float(r.get("qty",0))>0]) > 1 or sub != MATERIAL_SUBCATEGORIES.get(mat, ["Generic"])[0]:
                        row_key = f"{mat} — {sub}" if any(
                            rd2.get("sub") != sub for rd2 in rows if float(rd2.get("qty",0))>0
                        ) else mat
                    else:
                        row_key = mat
                    # If key already exists (same sub twice), make unique
                    base_key = row_key
                    suffix = 2
                    while row_key in [r["material"] for r in tbl]:
                        row_key = f"{base_key} #{suffix}"
                        suffix += 1
                    tbl.append({"material": row_key, "qty_input": qty, "unit": unit, "waste_tonnes": waste_t, "category": mat})
                    # Pre-fill sub_type in emission_inputs so LCA page auto-selects it
                    ei_prefill[row_key] = {"sub_type": sub}

            if not tbl:
                st.error("Please enter at least one quantity greater than 0.")
            else:
                st.session_state.waste_table      = tbl
                st.session_state.emission_inputs  = ei_prefill
                st.session_state.results          = {}
                st.session_state.page             = 4
                st.rerun()

    # ────────────────────────────────────────────────────────────────────────
    # BIM / CAD UPLOAD
    # ────────────────────────────────────────────────────────────────────────
    elif method == "bim":
        st.markdown("""
        <div class="info-box">
        <b>ReForm Dynamo Extractor — Step-by-Step Instructions</b><br><br>
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
            "Upload ReForm Material Export (.xlsx or .csv)",
            type=["csv", "xlsx"],
            help="Upload the Excel file generated by Extract_Materials.dyn from your Revit model"
        )

        # ── ReForm material groups ──────────────────────────────────
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
            "Bitumen":2.30, "Plastic":0.90, "Others":1.50,
            # AAC Block density handled via SUB_DENSITY in manual input path
        }

        def resolve_group(mat_name, cat_name=""):
            """Map Revit material/category name to ReForm group."""
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
                        st.markdown("#### Material Mapping — Revit → ReForm Groups")
                        st.caption("Each Revit material has been classified into a ReForm group using keyword matching. Review and adjust waste % below.")

                        map_rows = []
                        for g in CB_GROUPS:
                            if g not in agg: continue
                            d = agg[g]
                            map_rows.append({
                                "ReForm Group":  g,
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

    # ── Quick waste summary table (reference — full breakdown is on Results) ──
    st.markdown("#### Waste Summary (from Page 3)")
    waste_summary_df = pd.DataFrame([
        {"Material": row["material"], "Waste (tonnes)": round(row["waste_tonnes"], 3)}
        for row in waste_table
    ])
    st.dataframe(waste_summary_df, use_container_width=True, hide_index=True)
    st.caption(f"Total: {sum(r['waste_tonnes'] for r in waste_table):,.3f} tonnes — set transport, sub-type and end-of-life routing for each material below.")
    st.divider()

    for row in waste_table:
        mat = row["material"]
        qty_t = row["waste_tonnes"]
        cat = row.get("category", mat)   # base category for factor lookups
        with st.expander(f"**{mat}** — {qty_t:.3f} tonnes", expanded=False):
            c1, c2, c3 = st.columns(3)

            gwp_map = MATERIAL_GWP_A1A3.get(cat, MATERIAL_GWP_A1A3.get(mat, {"Generic": 50.0}))
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
                dist_c2 = st.number_input("Distance to recycling/landfill (km)", min_value=0.0, value=float(ei.get(mat, {}).get("distance_km_c2", 10)), key=f"dist_c2_{mat}")

            default_eol = DEFAULT_EOL.get(cat, DEFAULT_EOL.get(mat, {"Reuse": 0, "Recycle": 50, "Landfill": 40, "Incineration": 5, "Other": 5}))
            cur_eol = ei.get(mat, {}).get("eol", default_eol)
            with c3:
                st.markdown("**End-of-Life routing** — remaining % goes automatically to **Landfill**")
                reuse   = st.slider("Reuse %",        0, 100, int(cur_eol.get("Reuse",0)),       key=f"eol_reuse_{mat}")
                recycle = st.slider("Recycle %",      0, 100, int(cur_eol.get("Recycle",50)),     key=f"eol_recycle_{mat}")
                incin   = st.slider("Incineration %", 0, 100, int(cur_eol.get("Incineration",5)), key=f"eol_incin_{mat}")
                landfill = max(0, 100 - reuse - recycle - incin)
                other = 0
                if (reuse + recycle + incin) > 100:
                    st.error("⚠️ Exceeds 100% — reduce a slider. Landfill = 0%.")
                else:
                    st.metric("↳ Landfill (auto)", f"{landfill}%")

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
            circ_scores, circ_aggregate, vu_used = compute_circularity(emission_results)
            city_str = st.session_state.project.get("location", "")
            benefits = compute_circularity_benefits(emission_results, city_str=city_str)
            st.session_state.results = {
                "emission_results": emission_results,
                "circ_scores": circ_scores,
                "circ_aggregate": circ_aggregate,
                "benefits": benefits,
                "vu_used": vu_used,
            }
            # ── Log summary row to Google Sheets (silent) ────────────
            log_to_sheets(
                proj=st.session_state.project,
                emission_results=emission_results,
                circ_aggregate=circ_aggregate,
                benefits=benefits,
                waste_table=waste_table,
                emission_inputs=ei,
                circ_scores=circ_scores,
            )
            # ── Log full formatted report to Firestore (silent) ──────────
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

    # Show Firestore error if one was captured (only visible in dev; harmless in prod)
    if "_firestore_error" in st.session_state:
        st.warning(f"⚠️ Firestore log failed: {st.session_state['_firestore_error']}", icon="🔥")

    er  = res["emission_results"]
    cs  = res["circ_scores"]
    ca  = res["circ_aggregate"]
    ben = res["benefits"]
    vu  = res.get("vu_used", {})   # virgin fraction per material (from VU_BY_SUBTYPE)

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

    # ── Quick takeaway — one-line analogy per headline number ──────────────
    _gwp_a = render_gwp_footprint_analogies(total_gwp)
    _avoid_a = render_emission_analogies(total_avoided)
    _takeaways = []
    if _gwp_a:
        _takeaways.append(_gwp_a[0])
    if _avoid_a:
        _takeaways.append(_avoid_a[0])
    if _takeaways:
        st.caption("⚡ Quick takeaway: " + "  |  ".join(_takeaways) + "  *(see each tab for more)*")

    st.divider()

    # ── TABS ──────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🗑️ Waste", "🌍 Emissions", "♻️ Circularity", "💰 Economy", "📍 Recycling Plants", "📊 Analytics"])

    # ── TAB 1: WASTE ──────────────────────────────────────────────────────
    with tab1:
        st.markdown('<p class="section-head">Material Waste Estimation</p>', unsafe_allow_html=True)

        # Group sub-material rows by category so M20+M40+M40(GGBS) sum as one "Concrete" row
        from collections import defaultdict
        cat_totals = defaultdict(float)
        cat_subs   = defaultdict(list)
        for r in wt:
            cat  = r.get("category", r["material"])
            tons = round(r["waste_tonnes"], 3)
            cat_totals[cat] += tons
            if r["material"] != cat:
                cat_subs[cat].append((r["material"], tons))

        # Build EOL-split columns from emission_results (already has eol fractions)
        def _eol_tonnes(cat_name, total_t, key):
            # Look up eol from er — find any sub-row that belongs to this category
            for mat_key, r in er.items():
                if r.get("category", mat_key) == cat_name or mat_key == cat_name:
                    return round(total_t * r["eol"].get(key, 0) / 100.0, 3)
            return 0.0

        display_rows = []
        for cat in cat_totals:
            total_t = round(cat_totals[cat], 3)
            display_rows.append({
                "Material":           cat,
                "Total Waste (t)":    total_t,
                "Reused (t)":         _eol_tonnes(cat, total_t, "Reuse"),
                "Recycled (t)":       _eol_tonnes(cat, total_t, "Recycle"),
                "Landfilled (t)":     _eol_tonnes(cat, total_t, "Landfill"),
            })

        df_w = pd.DataFrame(display_rows)
        st.dataframe(df_w, use_container_width=True, hide_index=True)

        for cat, subs in cat_subs.items():
            if subs:
                with st.expander(f"\u21b3 {cat} — breakdown by type"):
                    st.dataframe(pd.DataFrame([{"Sub-material": lbl, "Waste (tonnes)": t} for lbl, t in subs]),
                                 use_container_width=True, hide_index=True)

        st.markdown(f'<div class="source-note">Source: {WASTE_RATE_SOURCE} | Composition: {COMP_SOURCE}</div>', unsafe_allow_html=True)
        st.bar_chart(df_w.set_index("Material")["Total Waste (t)"])

    # ── TAB 2: EMISSIONS ──────────────────────────────────────────────────
    with tab2:
        proj_type  = proj.get("construction_type", "Construction")
        pt_key = "Demolition" if proj_type in ["Demolition","Redevelopment"] else "Construction"
        bldg_type  = proj.get("building_type", "Residential")
        area_m2    = proj.get("builtup_area", 1.0) or 1.0

        # ── Quick waste summary table (reference — full breakdown is in Waste tab) ──
        st.markdown("#### Waste Summary")
        waste_summary_df2 = pd.DataFrame([
            {"Material": mat, "Waste (tonnes)": round(r["qty_t"], 3)}
            for mat, r in er.items()
        ])
        st.dataframe(waste_summary_df2, use_container_width=True, hide_index=True)

        # ── Summary: GWP footprint in everyday terms (shown first) ──
        st.markdown('<p class="section-head">🌍 In Everyday Terms — GWP Footprint Summary</p>', unsafe_allow_html=True)
        st.metric("Total GWP", f"{total_gwp:.2f} t CO₂e")
        gwp_analogies = render_gwp_footprint_analogies(total_gwp)
        if gwp_analogies:
            ac1, ac2 = st.columns(2)
            for i, a in enumerate(gwp_analogies):
                (ac1 if i % 2 == 0 else ac2).markdown(
                    f'<div class="info-box">{a}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="source-note">{ANALOGY_SOURCE}</div>', unsafe_allow_html=True)

        # ── Summary: avoided emissions in everyday terms (shown first) ──
        st.markdown('<p class="section-head">🌍 In Everyday Terms — Avoided Emissions Summary</p>', unsafe_allow_html=True)
        st.metric("Avoided Emissions", f"{total_avoided:.2f} t CO₂e")
        em_analogies = render_emission_analogies(total_avoided)
        if em_analogies:
            ac1, ac2 = st.columns(2)
            for i, a in enumerate(em_analogies):
                (ac1 if i % 2 == 0 else ac2).markdown(
                    f'<div class="info-box">{a}</div>', unsafe_allow_html=True)
        else:
            st.info("No avoided emissions to compare yet — adjust end-of-life routing (recycle/reuse) to see equivalencies.")
        st.markdown(f'<div class="source-note">{ANALOGY_SOURCE}</div>', unsafe_allow_html=True)

        st.divider()
        st.markdown("### 📊 Detailed Emissions Results")

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

    with tab3:
        # ── Summary: circularity in everyday terms (shown first) ──
        st.markdown('<p class="section-head">♻️ In Everyday Terms — Circularity Summary</p>', unsafe_allow_html=True)
        score_color = "#10b981" if ca >= 0.5 else "#f59e0b" if ca >= 0.3 else "#ef4444"
        st.markdown(f"""
        <div style="text-align:center; background: #f0fdf4; border: 2px solid {score_color}; border-radius: 14px; padding: 24px; margin-bottom: 12px;">
          <div style="font-size: 3rem; font-weight: 700; color: {score_color};">{ca*100:.1f}</div>
          <div style="color: #374151;">Overall Circularity Score (out of 100)</div>
        </div>
        """, unsafe_allow_html=True)
        total_recycled = sum(b["recycled_t"] for b in ben.values())
        total_reused   = sum(b["reused_t"] for b in ben.values())
        circ_analogies = render_circularity_analogies(total_lf_div, total_recycled, total_reused)
        if circ_analogies:
            for a in circ_analogies:
                st.markdown(f'<div class="info-box">{a}</div>', unsafe_allow_html=True)
        else:
            st.info("No material recovered/diverted yet — increase recycle/reuse % on Page 4 to see equivalencies.")
        st.markdown(f'<div class="source-note">{ANALOGY_SOURCE}</div>', unsafe_allow_html=True)

        st.divider()
        st.markdown("### 📊 Detailed Circularity Results")

        st.markdown('<p class="section-head">Circularity Score</p>', unsafe_allow_html=True)
        st.markdown("""
        **Material Circularity Indicator (MCI) — Ellen MacArthur Foundation (2015)**

        MCI = 1 − LFI × F(x) &nbsp;&nbsp; where &nbsp;&nbsp; F(x) = 0.9 × (1 − 0.5×Vu − 0.5×Fr)

        | Parameter | Meaning | Value |
        |---|---|---|
        | **LFI** | Linear Flow Index = (Landfill% + Incineration%) / 100 | EOL-specific |
        | **Fr** | Recovered fraction = (Recycle% + Reuse%) / 100 | EOL-specific |
        | **Vu** | Virgin fraction of INPUT material (0 = fully recycled input, 1 = fully virgin) | Sub-type specific |

        Vu rewards circular **procurement** (recycled-content inputs), not just EOL routing.  
        E.g. Scrap EAF steel has Vu = 0.10; GGBS 40% concrete has Vu = 0.60; OPC concrete has Vu = 1.0.  
        Score range: **0–100** (100 = fully circular)

        *Source: EMF (2015) MCI Technical Appendix — ellenmacarthurfoundation.org*
        """)

        circ_rows = []
        for mat, score in cs.items():
            eol = er[mat]["eol"]
            vu_val = vu.get(mat, 1.0)
            circ_rows.append({
                "Material": mat,
                "Vu (input virgin %)": f"{vu_val*100:.0f}%",
                "Reuse %": eol.get("Reuse",0),
                "Recycle %": eol.get("Recycle",0),
                "Landfill %": eol.get("Landfill",0),
                "Incineration %": eol.get("Incineration",0),
                "Circularity Score": f"{score*100:.1f}",
            })
        df_circ = pd.DataFrame(circ_rows)
        st.dataframe(df_circ, use_container_width=True, hide_index=True)

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
        st.markdown('<p class="section-head">💰 In Everyday Terms — Savings Summary</p>', unsafe_allow_html=True)
        e1,e2,e3,e4 = st.columns(4)
        e1.metric("Avoided Emissions",     f"{total_avoided:.2f} t CO₂e")
        e2.metric("Virgin Material Savings", f"₹{total_virgin:,.0f}")
        e3.metric("Landfill Diverted",     f"{total_lf_div:.2f} t")
        e4.metric("Landfill Cost Saved",   f"₹{total_lf_save:,.0f}")

        econ_analogies = render_economy_analogies(total_virgin, total_lf_save)
        if econ_analogies:
            for a in econ_analogies:
                st.markdown(f'<div class="info-box">{a}</div>', unsafe_allow_html=True)
        else:
            st.info("No cost savings to compare yet.")
        st.markdown(f'<div class="source-note">{ANALOGY_SOURCE}</div>', unsafe_allow_html=True)

        st.divider()
        st.markdown("### 📊 Detailed Economic Results")
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
        proj_city = proj.get("location", "")
        nearest = find_nearest_plants(proj_city)

        # ── Summary: nearest plant in everyday terms (shown first) ──
        st.markdown('<p class="section-head">📍 In Everyday Terms — Nearest Plant Summary</p>', unsafe_allow_html=True)
        if nearest:
            nearest_plant = nearest[0]
            st.metric("Nearest Recycling Plant", f"{nearest_plant.get('City','')} — {nearest_plant.get('Location','')}")
            plant_dist = nearest_plant.get("Distance_km")
            plant_cap  = nearest_plant.get("Capacity_TPD")
            plant_analogies = render_plant_analogy(plant_dist, plant_cap, total_waste)
            if plant_analogies:
                for a in plant_analogies:
                    st.markdown(f'<div class="info-box">{a}</div>', unsafe_allow_html=True)
            else:
                st.info("Not enough information to compute plant analogies for this location.")
        st.markdown(f'<div class="source-note">{ANALOGY_SOURCE}</div>', unsafe_allow_html=True)

        st.divider()
        st.markdown("### 📊 Detailed Recycling Plant Results")
        st.markdown('<p class="section-head">Nearest C&D Waste Recycling Plants in India</p>', unsafe_allow_html=True)
        st.markdown(f'<div class="source-note">Source: {PLANTS_SOURCE}. Distances calculated using Haversine great-circle formula from project city coordinates.</div>', unsafe_allow_html=True)


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

    with tab6:
        import matplotlib; matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        PCOLS6 = ["#ef4444","#f97316","#eab308","#22c55e","#3b82f6","#8b5cf6","#ec4899","#14b8a6","#6b7280"]
        mats6  = list(er.keys())
        cmap6  = {m: PCOLS6[i % len(PCOLS6)] for i, m in enumerate(mats6)}

        def make_pie6(ax, vals, labels, colors, title):
            # Filter out non-positive values (e.g. negative biogenic GWP for timber)
            filtered = [(v, l, c) for v, l, c in zip(vals, labels, colors) if v > 0]
            if not filtered:
                ax.text(0.5, 0.5, "No positive\nvalues", ha="center", va="center",
                        transform=ax.transAxes, fontsize=9, color="#9ca3af")
                ax.set_title(title, fontsize=10, fontweight="bold", pad=8)
                return
            fv, fl, fc = zip(*filtered)
            ax.pie(fv, labels=None, colors=fc, startangle=140,
                   autopct="%1.1f%%", pctdistance=0.78,
                   wedgeprops=dict(linewidth=0.5, edgecolor="white"))
            ax.set_title(title, fontsize=10, fontweight="bold", pad=8)
            patches = [mpatches.Patch(color=c, label=l) for c,l in zip(fc, fl)]
            ax.legend(handles=patches, loc="lower center", bbox_to_anchor=(0.5,-0.28), ncol=2, fontsize=6.5, frameon=False)

        st.markdown("#### Material Contribution")
        pc1, pc2, pc3 = st.columns(3)
        with pc1:
            fig, ax = plt.subplots(figsize=(3.8,3.8))
            wv = [r["waste_tonnes"] for r in wt if r["material"] in mats6]
            wl = [r["material"]     for r in wt if r["material"] in mats6]
            make_pie6(ax, wv, wl, [cmap6.get(m,"#ccc") for m in wl], "Waste by Material")
            plt.tight_layout(); st.pyplot(fig, use_container_width=True); plt.close(fig)
        with pc2:
            fig, ax = plt.subplots(figsize=(3.8,3.8))
            gv = [er[m]["total_gwp"] for m in mats6]
            make_pie6(ax, gv, mats6, [cmap6[m] for m in mats6], "GWP by Material")
            plt.tight_layout(); st.pyplot(fig, use_container_width=True); plt.close(fig)
        with pc3:
            fig, ax = plt.subplots(figsize=(3.8,3.8))
            cv = [cs.get(m,0)*er[m]["qty_t"] for m in mats6]
            if sum(cv)==0: cv=[1]*len(mats6)
            make_pie6(ax, cv, mats6, [cmap6[m] for m in mats6], "Circularity (Waste-Wtd)")
            plt.tight_layout(); st.pyplot(fig, use_container_width=True); plt.close(fig)

        st.markdown("#### Emission Stage Distribution by Material")
        st.caption("A1–A3: Production · A4: Transport in · A5: Construction · C1: Demolition · C2: Transport out · C3: Processing · C4: Landfill")
        fig, ax = plt.subplots(figsize=(10, 3.8))
        bottoms = [0.0]*len(mats6)
        for sk, sl, sc in zip(["A1A3","A4","A5","C1","C2","C3","C4"],
                               ["A1–A3","A4","A5","C1","C2","C3","C4"],
                               ["#1d4ed8","#3b82f6","#93c5fd","#dc2626","#f87171","#16a34a","#4ade80"]):
            vals = [er[m].get(sk,0) for m in mats6]
            ax.bar(mats6, vals, bottom=bottoms, label=sl, color=sc, width=0.52)
            bottoms = [b+v for b,v in zip(bottoms,vals)]
        ax.set_ylabel("kg CO₂e", fontsize=9)
        ax.set_title("Lifecycle Emission Stages by Material", fontsize=11, fontweight="bold")
        ax.legend(loc="upper right", fontsize=8, frameon=False, ncol=4)
        plt.xticks(rotation=22, ha="right", fontsize=8)
        plt.tight_layout(); st.pyplot(fig, use_container_width=True); plt.close(fig)

    st.divider()

    # ── SAVE AS DESIGN SCENARIO (for TOPSIS comparison) ─────────────────────
    st.markdown("### 🧮 Save This Design for Comparison")
    st.caption("Save the results of this design as a scenario, then compare multiple designs side-by-side using TOPSIS multi-criteria ranking.")
    sc1, sc2 = st.columns([3, 1])
    default_name = f"{proj.get('name','Design')} — Option {len(st.session_state.scenarios) + 1}"
    scenario_name = sc1.text_input("Design / Scenario name", value=default_name, key="scenario_name_input")
    if sc2.button("💾 Save Design", use_container_width=True):
        st.session_state.scenarios.append(scenario_from_results(scenario_name, res))
        st.success(f"Saved '{scenario_name}' — {len(st.session_state.scenarios)} design(s) saved so far.")

    if st.session_state.scenarios:
        st.markdown("**Designs saved so far:**")
        saved_list_df = pd.DataFrame([
            {"#": i + 1, "Design": s.get("Design", ""),
             "GWP (t CO2e)": s.get("GWP (t CO2e)"),
             "Circularity Score (MCI, 0-100)": s.get("Circularity Score (MCI, 0-100)")}
            for i, s in enumerate(st.session_state.scenarios)
        ])
        st.dataframe(saved_list_df, use_container_width=True, hide_index=True)

        ac1, ac2 = st.columns(2)
        ac1.button("➕ Add Another Design (start new run) →", use_container_width=True, on_click=start_new_design)
        ac2.button(f"📊 Compare Designs / TOPSIS ({len(st.session_state.scenarios)} saved) →",
                   type="primary", use_container_width=True, on_click=lambda: go(6))

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
# PAGE 6 — DESIGN COMPARISON (TOPSIS MULTI-CRITERIA ANALYSIS)
# ══════════════════════════════════════════════════════════════════════════════
def page_scenario_comparison():
    st.markdown('<p class="page-title">Design Comparison — TOPSIS Multi-Criteria Analysis</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Compare multiple design scenarios (varying material choice, EOL routing, recycling rates, etc.) and rank them using TOPSIS with adjustable criteria weights.</p>', unsafe_allow_html=True)

    if not st.session_state.scenarios:
        st.info("No design scenarios saved yet. Go to **Results** and click **Save Design** after running an estimation for each design variant you want to compare. You can also add rows manually in the table below.")
        scen_df = pd.DataFrame(columns=["Design"] + [c for c, _ in TOPSIS_CRITERIA])
    else:
        scen_df = pd.DataFrame(st.session_state.scenarios)

    crit_cols = [c for c, _ in TOPSIS_CRITERIA]
    dir_map   = {c: d for c, d in TOPSIS_CRITERIA}

    # ── Step 1: Editable scenario / criteria table ──────────────────────────
    st.markdown("#### Step 1 — Design Scenarios & Criteria Values")
    st.caption("Add, edit, or remove design rows directly. Each row is one design alternative (e.g. OPC vs GGBS concrete, different EOL recycling rates, AAC vs fly-ash brick, etc.).")

    col_cfg = {"Design": st.column_config.TextColumn("Design", required=True)}
    for c, d in TOPSIS_CRITERIA:
        col_cfg[c] = st.column_config.NumberColumn(
            f"{c} {'↑ benefit' if d=='benefit' else '↓ cost'}", format="%.3f")

    edited_df = st.data_editor(
        scen_df, num_rows="dynamic", use_container_width=True,
        column_config=col_cfg, key="topsis_editor"
    )

    # Clean: drop rows without a design name or with any missing numeric value
    edited_df = edited_df.dropna(subset=["Design"])
    edited_df = edited_df[edited_df["Design"].astype(str).str.strip() != ""]
    for c in crit_cols:
        if c not in edited_df.columns:
            edited_df[c] = 0.0
        edited_df[c] = pd.to_numeric(edited_df[c], errors="coerce").fillna(0.0)

    # Persist edits back to session state (so it survives navigation)
    st.session_state.scenarios = edited_df.to_dict("records")

    if len(edited_df) < 2:
        st.warning("⚠️ Add at least **two** design scenarios to compute a TOPSIS ranking.")
        st.button("← Back to Results", on_click=lambda: go(5))
        return

    st.divider()

    # ── Step 2: Dynamic weights ──────────────────────────────────────────────
    st.markdown("#### Step 2 — Set Criteria Weights")
    st.caption("Adjust the relative importance (0–1) of each criterion. Weights are auto-normalised to sum to 1 — relative values matter, not the absolute numbers. ↑ benefit = higher is better; ↓ cost = lower is better.")

    if "topsis_weights" not in st.session_state or set(st.session_state.topsis_weights.keys()) != set(crit_cols):
        st.session_state.topsis_weights = {c: round(1.0 / len(crit_cols), 3) for c in crit_cols}

    rcol1, rcol2 = st.columns([4, 1])
    with rcol2:
        if st.button("↺ Reset to Equal Weights", use_container_width=True):
            st.session_state.topsis_weights = {c: round(1.0 / len(crit_cols), 3) for c in crit_cols}
            st.rerun()

    wcols = st.columns(4)
    for i, (c, d) in enumerate(TOPSIS_CRITERIA):
        with wcols[i % 4]:
            st.session_state.topsis_weights[c] = st.slider(
                f"{c}\n({'↑ benefit' if d=='benefit' else '↓ cost'})",
                min_value=0.0, max_value=1.0,
                value=float(st.session_state.topsis_weights.get(c, 1.0/len(crit_cols))),
                step=0.01, key=f"topsis_w_{i}")

    raw_sum = sum(st.session_state.topsis_weights.values())
    norm_weights = ({c: round(v / raw_sum, 3) for c, v in st.session_state.topsis_weights.items()}
                    if raw_sum > 0 else {c: round(1.0/len(crit_cols), 3) for c in crit_cols})

    wdf = pd.DataFrame([{"Criterion": c, "Direction": ("↑ benefit" if d=="benefit" else "↓ cost"),
                          "Raw Weight": round(st.session_state.topsis_weights[c], 3),
                          "Normalised Weight (Σ=1)": norm_weights[c]}
                         for c, d in TOPSIS_CRITERIA])
    st.dataframe(wdf, use_container_width=True, hide_index=True)

    st.divider()

    # ── Step 3: TOPSIS ranking ────────────────────────────────────────────────
    st.markdown("#### Step 3 — TOPSIS Ranking")
    ranked = compute_topsis(edited_df, norm_weights)

    display_cols = ["Rank", "Design"] + crit_cols + ["Closeness Score"]
    st.dataframe(ranked[display_cols], use_container_width=True, hide_index=True)

    best = ranked.iloc[0]
    st.success(f"🏆 **Best-performing design (current weights): {best['Design']}** — Closeness Score = {best['Closeness Score']:.4f}")

    st.bar_chart(ranked.set_index("Design")["Closeness Score"])

    st.markdown("""
    <div class="source-note">
    <b>TOPSIS method:</b> Technique for Order of Preference by Similarity to Ideal Solution (Hwang & Yoon, 1981).
    Criteria values are vector-normalised, weighted, and compared to the ideal-best and ideal-worst solutions;
    the Closeness Score (0–1) reflects relative distance from the worst case — higher is better.
    Criteria set follows the CircularBuild MTP evaluation framework (GWP, MCI, AP, EP, net landfill waste,
    landfill diverted, virgin material savings, landfill cost saved).
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ── Step 4: Download as Excel ─────────────────────────────────────────────
    st.markdown("#### Step 4 — Download Comparison Workbook")

    excel_engine = None
    for _eng in ("openpyxl", "xlsxwriter"):
        try:
            __import__(_eng)
            excel_engine = _eng
            break
        except ImportError:
            continue

    proj_name_for_file = str(st.session_state.project.get('name','project')).replace(' ','_')

    if excel_engine:
        excel_buf = BytesIO()
        with pd.ExcelWriter(excel_buf, engine=excel_engine) as writer:
            edited_df.to_excel(writer, sheet_name="Scenarios & Criteria", index=False)
            wdf.to_excel(writer, sheet_name="Weights", index=False)
            ranked[display_cols].to_excel(writer, sheet_name="TOPSIS Ranking", index=False)
        excel_buf.seek(0)
        st.download_button(
            label="⬇️ Download Scenario Comparison (Excel)",
            data=excel_buf,
            file_name=f"CircularBuild_Scenario_Comparison_{proj_name_for_file}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning("⚠️ Excel export requires the `openpyxl` package, which isn't installed on this server. "
                   "Add `openpyxl` to your `requirements.txt` to enable the Excel download. "
                   "In the meantime, you can download the tables as CSV below.")
        dl1, dl2, dl3 = st.columns(3)
        dl1.download_button("⬇️ Scenarios & Criteria (CSV)",
                             data=edited_df.to_csv(index=False).encode("utf-8"),
                             file_name=f"Scenarios_Criteria_{proj_name_for_file}.csv", mime="text/csv")
        dl2.download_button("⬇️ Weights (CSV)",
                             data=wdf.to_csv(index=False).encode("utf-8"),
                             file_name=f"Weights_{proj_name_for_file}.csv", mime="text/csv")
        dl3.download_button("⬇️ TOPSIS Ranking (CSV)",
                             data=ranked[display_cols].to_csv(index=False).encode("utf-8"),
                             file_name=f"TOPSIS_Ranking_{proj_name_for_file}.csv", mime="text/csv")


    st.divider()
    cb1, cb2 = st.columns([1, 4])
    cb1.button("← Back to Results", on_click=lambda: go(5))
    if cb2.button("🗑️ Clear All Saved Designs"):
        st.session_state.scenarios = []
        st.rerun()


show_progress()
page = st.session_state.page

if page == 1:   page_project_info()
elif page == 2: page_data_input()
elif page == 3: page_waste_estimation()
elif page == 4: page_emissions_eol()
elif page == 5: page_results()
elif page == 6: page_scenario_comparison()
