import os
import json
import asyncio
import asyncpg
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None


# ── Schema kept minimal — carriers table already exists with Census columns ──
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number TEXT NOT NULL,
    title TEXT NOT NULL,
    decided TEXT,
    category TEXT,
    date_fetched TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);

CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    plan TEXT NOT NULL DEFAULT 'Free' CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active TEXT DEFAULT 'Never',
    ip_address TEXT,
    is_online BOOLEAN DEFAULT false,
    is_blocked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocked_ips (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason TEXT,
    blocked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    blocked_by TEXT
);

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Indexes for Census carriers table
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_status_code ON carriers(status_code);
CREATE INDEX IF NOT EXISTS idx_carriers_phy_state ON carriers(phy_state);
CREATE INDEX IF NOT EXISTS idx_carriers_legal_name_trgm ON carriers USING gin (legal_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number_trgm ON carriers USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_email_address ON carriers(email_address);
CREATE INDEX IF NOT EXISTS idx_carriers_power_units ON carriers(power_units);
CREATE INDEX IF NOT EXISTS idx_carriers_hm_ind ON carriers(hm_ind);
CREATE INDEX IF NOT EXISTS idx_carriers_temp ON carriers(temp);
CREATE INDEX IF NOT EXISTS idx_carriers_legal_name ON carriers(legal_name);
CREATE INDEX IF NOT EXISTS idx_carriers_cargo_gin ON carriers USING gin (cargo);
CREATE INDEX IF NOT EXISTS idx_carriers_insurance_gin ON carriers USING gin (insurance);
CREATE INDEX IF NOT EXISTS idx_carriers_dockets_gin ON carriers USING gin (dockets);

CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();

CREATE TABLE IF NOT EXISTS new_ventures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    dot_number TEXT,
    prefix TEXT,
    docket_number TEXT,
    status_code TEXT,
    carship TEXT,
    carrier_operation TEXT,
    name TEXT,
    name_dba TEXT,
    add_date TEXT,
    chgn_date TEXT,
    common_stat TEXT,
    contract_stat TEXT,
    broker_stat TEXT,
    common_app_pend TEXT,
    contract_app_pend TEXT,
    broker_app_pend TEXT,
    common_rev_pend TEXT,
    contract_rev_pend TEXT,
    broker_rev_pend TEXT,
    property_chk TEXT,
    passenger_chk TEXT,
    hhg_chk TEXT,
    private_auth_chk TEXT,
    enterprise_chk TEXT,
    operating_status TEXT,
    operating_status_indicator TEXT,
    phy_str TEXT,
    phy_city TEXT,
    phy_st TEXT,
    phy_zip TEXT,
    phy_country TEXT,
    phy_cnty TEXT,
    mai_str TEXT,
    mai_city TEXT,
    mai_st TEXT,
    mai_zip TEXT,
    mai_country TEXT,
    mai_cnty TEXT,
    phy_undeliv TEXT,
    mai_undeliv TEXT,
    phy_phone TEXT,
    phy_fax TEXT,
    mai_phone TEXT,
    mai_fax TEXT,
    cell_phone TEXT,
    email_address TEXT,
    company_officer_1 TEXT,
    company_officer_2 TEXT,
    genfreight TEXT,
    household TEXT,
    metalsheet TEXT,
    motorveh TEXT,
    drivetow TEXT,
    logpole TEXT,
    bldgmat TEXT,
    mobilehome TEXT,
    machlrg TEXT,
    produce TEXT,
    liqgas TEXT,
    intermodal TEXT,
    passengers TEXT,
    oilfield TEXT,
    livestock TEXT,
    grainfeed TEXT,
    coalcoke TEXT,
    meat TEXT,
    garbage TEXT,
    usmail TEXT,
    chem TEXT,
    drybulk TEXT,
    coldfood TEXT,
    beverages TEXT,
    paperprod TEXT,
    utility TEXT,
    farmsupp TEXT,
    construct TEXT,
    waterwell TEXT,
    cargoothr TEXT,
    cargoothr_desc TEXT,
    hm_ind TEXT,
    bipd_req TEXT,
    cargo_req TEXT,
    bond_req TEXT,
    bipd_file TEXT,
    cargo_file TEXT,
    bond_file TEXT,
    owntruck TEXT,
    owntract TEXT,
    owntrail TEXT,
    owncoach TEXT,
    ownschool_1_8 TEXT,
    ownschool_9_15 TEXT,
    ownschool_16 TEXT,
    ownbus_16 TEXT,
    ownvan_1_8 TEXT,
    ownvan_9_15 TEXT,
    ownlimo_1_8 TEXT,
    ownlimo_9_15 TEXT,
    ownlimo_16 TEXT,
    trmtruck TEXT,
    trmtract TEXT,
    trmtrail TEXT,
    trmcoach TEXT,
    trmschool_1_8 TEXT,
    trmschool_9_15 TEXT,
    trmschool_16 TEXT,
    trmbus_16 TEXT,
    trmvan_1_8 TEXT,
    trmvan_9_15 TEXT,
    trmlimo_1_8 TEXT,
    trmlimo_9_15 TEXT,
    trmlimo_16 TEXT,
    trptruck TEXT,
    trptract TEXT,
    trptrail TEXT,
    trpcoach TEXT,
    trpschool_1_8 TEXT,
    trpschool_9_15 TEXT,
    trpschool_16 TEXT,
    trpbus_16 TEXT,
    trpvan_1_8 TEXT,
    trpvan_9_15 TEXT,
    trplimo_1_8 TEXT,
    trplimo_9_15 TEXT,
    trplimo_16 TEXT,
    total_trucks TEXT,
    total_buses TEXT,
    total_pwr TEXT,
    fleetsize TEXT,
    inter_within_100 TEXT,
    inter_beyond_100 TEXT,
    total_inter_drivers TEXT,
    intra_within_100 TEXT,
    intra_beyond_100 TEXT,
    total_intra_drivers TEXT,
    total_drivers TEXT,
    avg_tld TEXT,
    total_cdl TEXT,
    review_type TEXT,
    review_id TEXT,
    review_date TEXT,
    recordable_crash_rate TEXT,
    mcs150_mileage TEXT,
    mcs151_mileage TEXT,
    mcs150_mileage_year TEXT,
    mcs150_date TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    arber TEXT,
    smartway TEXT,
    tia TEXT,
    tia_phone TEXT,
    tia_contact_name TEXT,
    tia_tool_free TEXT,
    tia_fax TEXT,
    tia_email TEXT,
    tia_website TEXT,
    phy_ups_store TEXT,
    mai_ups_store TEXT,
    phy_mail_box TEXT,
    mai_mail_box TEXT,
    raw_data JSONB,
    scrape_date TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dot_number, add_date)
);

CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_number ON new_ventures(dot_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_number ON new_ventures(docket_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_add_date ON new_ventures(add_date);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name ON new_ventures(name);
CREATE INDEX IF NOT EXISTS idx_new_ventures_phy_st ON new_ventures(phy_st);
CREATE INDEX IF NOT EXISTS idx_new_ventures_operating_status ON new_ventures(operating_status);
CREATE INDEX IF NOT EXISTS idx_new_ventures_created_at ON new_ventures(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_new_ventures_email ON new_ventures(email_address);
CREATE INDEX IF NOT EXISTS idx_new_ventures_hm_ind ON new_ventures(hm_ind);
CREATE INDEX IF NOT EXISTS idx_new_ventures_carrier_op ON new_ventures(carrier_operation);

CREATE INDEX IF NOT EXISTS idx_new_ventures_name_trgm ON new_ventures USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_dba_trgm ON new_ventures USING gin (name_dba gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_trgm ON new_ventures USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_trgm ON new_ventures USING gin (docket_number gin_trgm_ops);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;
"""


async def connect_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    try:
        async with _pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)
        print("[DB] Connected to PostgreSQL, schema initialized, pool created")
    except Exception as e:
        print(f"[DB] Connected to PostgreSQL, pool created (schema init skipped: {e})")


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
    _pool = None
    print("[DB] PostgreSQL connection pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database not connected. Call connect_db() first.")
    return _pool


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_jsonb(value) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _to_jsonb(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


def _clean_phone(raw: Optional[str]) -> Optional[str]:
    """Convert '2074255451.0' → '2074255451', strip junk."""
    if not raw:
        return None
    s = str(raw).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.replace(" ", "").replace("-", "")
    return s if s and s != "0" else None


def _extract_mc_number(dockets) -> Optional[str]:
    """Pull the first MC docket number (numeric part only) from the dockets JSONB array."""
    if not dockets:
        return None
    lst = _parse_jsonb(dockets) if isinstance(dockets, str) else dockets
    if not isinstance(lst, list):
        return None
    for d in lst:
        s = str(d).strip().upper()
        if s.startswith("MC"):
            return s[2:]  # strip the "MC" prefix, return numeric part
    return None


def _cargo_list_from_jsonb(cargo_raw) -> list[str]:
    """
    cargo JSONB looks like:
    {"General Freight": "X", "Fresh Produce": "X", "crgo_cargoothr_desc": "POTATOES"}
    Return list of human-readable cargo type names (keys where value == 'X', skip meta keys).
    """
    if not cargo_raw:
        return []
    obj = _parse_jsonb(cargo_raw) if isinstance(cargo_raw, str) else cargo_raw
    if not isinstance(obj, dict):
        return []
    skip_keys = {"crgo_cargoothr_desc"}
    return [k for k, v in obj.items() if v == "X" and k not in skip_keys]


def _classdef_to_operation_classification(classdef: Optional[str]) -> list[str]:
    """
    classdef: "AUTHORIZED FOR HIRE;EXEMPT FOR HIRE"
    Maps Census classdef segments to the human-readable classification labels
    the UI expects.
    """
    if not classdef:
        return []
    _MAP = {
        "AUTHORIZED FOR HIRE": "Auth. For Hire",
        "EXEMPT FOR HIRE": "Exempt For Hire",
        "PRIVATE(PROPERTY)": "Private(Property)",
        "PRIVATE(PASSENGER)": "Private(Passenger)",
        "MIGRANT": "Migrant",
        "U.S. MAIL": "U.S. Mail",
        "FEDERAL GOVERNMENT": "Federal Government",
        "STATE GOVERNMENT": "State Government",
        "LOCAL GOVERNMENT": "Local Government",
        "INDIAN TRIBE": "Indian Tribe",
    }
    parts = [p.strip().upper() for p in classdef.split(";") if p.strip()]
    result = []
    for p in parts:
        result.append(_MAP.get(p, p.title()))
    return result


def _derive_carrier_operation(row: dict) -> list[str]:
    """
    Use interstate_*/intrastate_* driver counts to derive operating territory tags.
    """
    ops = []
    def _nonzero(val):
        try:
            return float(val or 0) > 0
        except (ValueError, TypeError):
            return False

    inter = _nonzero(row.get("interstate_beyond_100_miles")) or _nonzero(row.get("interstate_within_100_miles"))
    intra_hm = _nonzero(row.get("intrastate_beyond_100_miles"))
    intra_non = _nonzero(row.get("intrastate_within_100_miles"))

    if inter:
        ops.append("Interstate")
    if intra_hm:
        ops.append("Intrastate Only (HM)")
    if intra_non and not intra_hm:
        ops.append("Intrastate Only (Non-HM)")
    return ops


def _derive_entity_type(classdef: Optional[str]) -> str:
    """Broker vs Carrier from classdef."""
    if classdef and "BROKER" in classdef.upper():
        return "BROKER"
    return "CARRIER"


def _derive_status(status_code: Optional[str]) -> str:
    """Map status_code to human-readable status string the UI uses."""
    mapping = {
        "A": "AUTHORIZED FOR HIRE",
        "I": "NOT AUTHORIZED",
        "X": "REVOKED",
        "N": "NOT AUTHORIZED",
    }
    return mapping.get((status_code or "").upper(), "NOT AUTHORIZED")


def _format_mcs150_date(raw: Optional[str]) -> Optional[str]:
    """
    mcs150_date stored as '20250606 1457' or '20250606'.
    Return 'YYYY-MM-DD' so JS Date() can parse it for yearsInBusiness calc.
    """
    if not raw:
        return None
    s = str(raw).strip().split(" ")[0]  # take date part only
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _format_add_date(raw: Optional[str]) -> Optional[str]:
    """add_date stored as '19740601' → '1974-06-01'."""
    if not raw:
        return None
    s = str(raw).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _format_insurance_from_jsonb(raw_insurance) -> list[dict]:
    """
    Convert the `insurance` JSONB column (list of policy dicts) into the
    InsuranceHistoryFiling shape the frontend expects.
    """
    if not raw_insurance:
        return []
    lst = _parse_jsonb(raw_insurance) if isinstance(raw_insurance, str) else raw_insurance
    if not isinstance(lst, list):
        return []
    results = []
    for row in lst:
        raw_amount = str(row.get("max_cov_amount") or "").strip()
        try:
            amount_int = int(float(raw_amount)) * 1000
            coverage = f"${amount_int:,}"
        except (ValueError, TypeError):
            coverage = raw_amount or "N/A"
        cancl = str(row.get("cancl_effective_date") or "").strip()
        results.append({
            "type": str(row.get("ins_type_desc") or "").strip(),
            "coverageAmount": coverage,
            "policyNumber": str(row.get("policy_no") or "").strip(),
            "effectiveDate": str(row.get("effective_date") or "").strip(),
            "carrier": str(row.get("name_company") or "").strip(),
            "formCode": str(row.get("ins_form_code") or "").strip(),
            "transDate": str(row.get("trans_date") or "").strip(),
            "underlLimAmount": str(row.get("underl_lim_amount") or "").strip(),
            "canclEffectiveDate": cancl,
            "status": "Cancelled" if cancl else "Active",
        })
    return results


def _carrier_row_to_dict(row) -> dict:
    """
    Convert a raw Census carriers table row into the API response shape
    that the frontend (supabaseClient.ts mapping) expects.
    """
    d = dict(row)

    # Extract MC number from dockets JSONB array
    mc_number = _extract_mc_number(d.get("dockets"))

    # Build physical address string
    parts = [
        d.get("phy_street") or "",
        d.get("phy_city") or "",
        d.get("phy_state") or "",
        d.get("phy_zip") or "",
    ]
    physical_address = ", ".join(p for p in parts if p).strip(", ")

    # Build mailing address string
    mail_parts = [
        d.get("carrier_mailing_street") or "",
        d.get("carrier_mailing_city") or "",
        d.get("carrier_mailing_state") or "",
        d.get("carrier_mailing_zip") or "",
    ]
    mailing_address = ", ".join(p for p in mail_parts if p).strip(", ")

    # Company representative: use company_officer_1 / company_officer_2
    company_rep_parts = [d.get("company_officer_1") or "", d.get("company_officer_2") or ""]
    company_rep = " ".join(p for p in company_rep_parts if p).strip() or None

    classdef = d.get("classdef")
    status_code = d.get("status_code")
    cargo_raw = d.get("cargo")
    insurance_raw = d.get("insurance")

    return {
        # ── IDs ──────────────────────────────────────────────────────────
        "mc_number": mc_number,
        "dot_number": str(d.get("dot_number") or "").strip(),
        "duns_number": d.get("dun_bradstreet_no"),

        # ── Identity ─────────────────────────────────────────────────────
        "legal_name": d.get("legal_name") or "",
        "dba_name": d.get("dba_name"),
        "entity_type": _derive_entity_type(classdef),
        "status": _derive_status(status_code),

        # ── Contact ──────────────────────────────────────────────────────
        "email": d.get("email_address"),
        "phone": _clean_phone(d.get("phone")),
        "company_rep": company_rep,

        # ── Location ─────────────────────────────────────────────────────
        "physical_address": physical_address or None,
        "mailing_address": mailing_address or None,
        "phy_state": d.get("phy_state"),

        # ── Compliance ───────────────────────────────────────────────────
        "mcs150_date": _format_mcs150_date(d.get("mcs150_date")),
        "mcs150_mileage": d.get("mcs150_mileage"),
        "add_date": _format_add_date(d.get("add_date")),
        "date_scraped": _format_add_date(d.get("add_date")),  # best proxy

        # ── Operations ───────────────────────────────────────────────────
        "operation_classification": _classdef_to_operation_classification(classdef),
        "carrier_operation": _derive_carrier_operation(d),
        "cargo_carried": _cargo_list_from_jsonb(cargo_raw),
        "hm_ind": d.get("hm_ind") or "N",

        # ── Fleet ────────────────────────────────────────────────────────
        "power_units": d.get("power_units"),
        "drivers": d.get("total_drivers"),

        # ── Safety (enriched later) ───────────────────────────────────────
        "safety_rating": None,
        "safety_rating_date": None,
        "basic_scores": None,
        "oos_rates": None,
        "out_of_service_date": None,
        "state_carrier_id": None,
        "inspections": None,
        "crashes": None,

        # ── Insurance (from carriers.insurance JSONB) ─────────────────────
        "insurance_history_filings": _format_insurance_from_jsonb(insurance_raw),
    }


# ── Census carrier query ──────────────────────────────────────────────────────

# Maps cargo filter label → key in cargo JSONB
_CARGO_KEY_MAP = {
    "General Freight": "General Freight",
    "Household Goods": "Household Goods",
    "Metal: Sheets, Coils, Rolls": "Metal: Sheets, Coils, Rolls",
    "Motor Vehicles": "Motor Vehicles",
    "Drive/Tow Away": "Drive/Tow Away",
    "Logs, Poles, Beams, Lumber": "Logs, Poles, Beams, Lumber",
    "Building Materials": "Building Materials",
    "Mobile Homes": "Mobile Homes",
    "Machinery, Large Objects": "Machinery, Large Objects",
    "Fresh Produce": "Fresh Produce",
    "Liquids/Gases": "Liquids/Gases",
    "Intermodal Cont.": "Intermodal Cont.",
    "Passengers": "Passengers",
    "Oilfield Equipment": "Oilfield Equipment",
    "Livestock": "Livestock",
    "Grain, Feed, Hay": "Grain, Feed, Hay",
    "Coal/Coke": "Coal/Coke",
    "Meat": "Meat",
    "Garbage/Refuse": "Garbage/Refuse",
    "US Mail": "US Mail",
    "Chemicals": "Chemicals",
    "Commodities Dry Bulk": "Commodities Dry Bulk",
    "Refrigerated Food": "Refrigerated Food",
    "Beverages": "Beverages",
    "Paper Products": "Paper Products",
    "Utilities": "Utilities",
    "Agricultural/Farm Supplies": "Agricultural/Farm Supplies",
    "Construction": "Construction",
    "Water Well": "Water Well",
    "Other": "Other",
}

# Maps classdef fragment → filter-friendly label
_CLASSDEF_FILTER_MAP = {
    "Auth. For Hire": "AUTHORIZED FOR HIRE",
    "Exempt For Hire": "EXEMPT FOR HIRE",
    "Private(Property)": "PRIVATE(PROPERTY)",
    "Private(Passenger)": "PRIVATE(PASSENGER)",
    "Migrant": "MIGRANT",
    "U.S. Mail": "U.S. MAIL",
    "Federal Government": "FEDERAL GOVERNMENT",
    "State Government": "STATE GOVERNMENT",
    "Local Government": "LOCAL GOVERNMENT",
    "Indian Tribe": "INDIAN TRIBE",
}

# Maps insurance type filter → ins_type_desc pattern inside carriers.insurance
_INS_TYPE_PATTERN = {
    "BI&PD": "BIPD",
    "CARGO": "CARGO",
    "BOND": "SURETY",
    "TRUST FUND": "TRUST FUND",
}

_INSURANCE_COMPANY_PATTERNS: dict[str, list[str]] = {
    "GREAT WEST CASUALTY": ["GREAT WEST"],
    "UNITED FINANCIAL CASUALTY": ["UNITED FINANCIAL"],
    "GEICO MARINE": ["GEICO MARINE"],
    "NORTHLAND INSURANCE": ["NORTHLAND"],
    "ARTISAN & TRUCKERS": ["ARTISAN", "TRUCKERS CASUALTY"],
    "CANAL INSURANCE": ["CANAL INS"],
    "PROGRESSIVE": ["PROGRESSIVE"],
    "BERKSHIRE HATHAWAY": ["BERKSHIRE"],
    "OLD REPUBLIC": ["OLD REPUBLIC"],
    "SENTRY": ["SENTRY"],
    "TRAVELERS": ["TRAVELERS"],
}


async def upsert_carrier(record: dict) -> bool:
    """
    Upsert a carrier record from the scraper into the Census carriers table.
    Maps the scraped field names to Census column names where they differ.
    Uses dot_number as the conflict key since the Census table is keyed on it.
    """
    pool = get_pool()
    dot = str(record.get("dot_number") or "").strip()
    if not dot:
        return False

    # Build the insurance JSONB from insurance_policies if present
    insurance = record.get("insurance_policies") or record.get("insurance")

    try:
        await pool.execute(
            """
            INSERT INTO carriers (
                dot_number, legal_name, dba_name, status_code, classdef,
                email_address, phone, power_units, total_drivers,
                phy_street, phy_city, phy_state, phy_zip,
                carrier_mailing_street, carrier_mailing_city, carrier_mailing_state, carrier_mailing_zip,
                mcs150_date, mcs150_mileage, dun_bradstreet_no, hm_ind,
                dockets, cargo, insurance
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16, $17,
                $18, $19, $20, $21,
                $22::jsonb, $23::jsonb, $24::jsonb
            )
            ON CONFLICT (dot_number) DO UPDATE SET
                legal_name = COALESCE(EXCLUDED.legal_name, carriers.legal_name),
                dba_name = COALESCE(EXCLUDED.dba_name, carriers.dba_name),
                status_code = COALESCE(EXCLUDED.status_code, carriers.status_code),
                classdef = COALESCE(EXCLUDED.classdef, carriers.classdef),
                email_address = COALESCE(EXCLUDED.email_address, carriers.email_address),
                phone = COALESCE(EXCLUDED.phone, carriers.phone),
                power_units = COALESCE(EXCLUDED.power_units, carriers.power_units),
                total_drivers = COALESCE(EXCLUDED.total_drivers, carriers.total_drivers),
                mcs150_date = COALESCE(EXCLUDED.mcs150_date, carriers.mcs150_date),
                mcs150_mileage = COALESCE(EXCLUDED.mcs150_mileage, carriers.mcs150_mileage),
                insurance = COALESCE(EXCLUDED.insurance, carriers.insurance)
            """,
            dot,
            record.get("legal_name"),
            record.get("dba_name"),
            # Map status string back to status_code
            "A" if str(record.get("status", "")).upper().startswith("AUTHORIZED") else "I",
            record.get("entity_type"),  # store in classdef column
            record.get("email"),
            record.get("phone"),
            record.get("power_units"),
            record.get("drivers"),
            # Address — split if possible, else store whole in phy_street
            record.get("physical_address"),
            None, None, None,
            record.get("mailing_address"),
            None, None, None,
            record.get("mcs150_date"),
            record.get("mcs150_mileage"),
            record.get("duns_number"),
            "Y" if record.get("cargo_carried") and any(
                "haz" in c.lower() for c in (record.get("cargo_carried") or [])
            ) else "N",
            _to_jsonb(record.get("mc_number") and [f"MC{record['mc_number']}"] or []),
            _to_jsonb({}),
            _to_jsonb(insurance),
        )
        return True
    except Exception as e:
        print(f"[DB] Error upserting carrier DOT {dot}: {e}")
        return False


async def fetch_carriers(filters: dict) -> dict:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # ── Text search ──────────────────────────────────────────────────────────

    if filters.get("mc_number"):
        # dockets is a JSONB array like ["MC143680"]. Search for the MC value.
        val = filters["mc_number"].strip().upper()
        # Strip leading MC if user typed it
        numeric = val.lstrip("MC").strip()
        conditions.append(f"dockets::text ILIKE ${idx}")
        params.append(f"%MC{numeric}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number = ${idx}")
        params.append(str(filters["dot_number"]).strip())
        idx += 1

    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    # ── Entity type (CARRIER vs BROKER) ──────────────────────────────────────
    entity_type = filters.get("entity_type")
    if entity_type:
        et = entity_type.upper()
        if et == "BROKER":
            conditions.append("UPPER(classdef) LIKE '%BROKER%'")
        elif et == "CARRIER":
            conditions.append("(classdef IS NULL OR UPPER(classdef) NOT LIKE '%BROKER%')")

    # ── Active / Inactive ────────────────────────────────────────────────────
    active = filters.get("active")
    if active == "true":
        conditions.append("status_code = 'A'")
    elif active == "false":
        conditions.append("(status_code IS NULL OR status_code != 'A')")

    # ── State ────────────────────────────────────────────────────────────────
    if filters.get("state"):
        states = filters["state"].split("|")
        or_clauses = []
        for s in states:
            or_clauses.append(f"phy_state = ${idx}")
            params.append(s.strip().upper())
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Has email ────────────────────────────────────────────────────────────
    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    # ── Company rep (has company_officer_1) ──────────────────────────────────
    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("company_officer_1 IS NOT NULL AND company_officer_1 != ''")
    elif has_company_rep == "false":
        conditions.append("(company_officer_1 IS NULL OR company_officer_1 = '')")

    # ── BOC-3 — no direct Census column, skip gracefully ────────────────────
    # (BOC-3 was only in the scraped data; Census doesn't have it)

    # ── Years in Business (based on add_date stored as 'YYYYMMDD') ──────────
    if filters.get("years_in_business_min"):
        yrs = int(filters["years_in_business_min"])
        conditions.append(
            f"add_date IS NOT NULL AND add_date ~ '^[0-9]{{8}}$' "
            f"AND (NOW()::date - TO_DATE(add_date, 'YYYYMMDD')) >= ${idx} * 365"
        )
        params.append(yrs)
        idx += 1
    if filters.get("years_in_business_max"):
        yrs = int(filters["years_in_business_max"])
        conditions.append(
            f"add_date IS NOT NULL AND add_date ~ '^[0-9]{{8}}$' "
            f"AND (NOW()::date - TO_DATE(add_date, 'YYYYMMDD')) <= ${idx} * 365"
        )
        params.append(yrs)
        idx += 1

    # ── Operation classification (classdef text contains fragment) ───────────
    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        or_clauses = []
        for c in classifications:
            key = _CLASSDEF_FILTER_MAP.get(c.strip(), c.strip().upper())
            or_clauses.append(f"UPPER(classdef) LIKE ${idx}")
            params.append(f"%{key}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Carrier operation (interstate/intrastate fields) ─────────────────────
    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        or_clauses = []
        for op in ops:
            op = op.strip()
            if op == "Interstate":
                or_clauses.append(
                    "(NULLIF(interstate_beyond_100_miles,'')::numeric > 0 "
                    "OR NULLIF(interstate_within_100_miles,'')::numeric > 0)"
                )
            elif op == "Intrastate Only (HM)":
                or_clauses.append(
                    "NULLIF(intrastate_beyond_100_miles,'')::numeric > 0"
                )
            elif op == "Intrastate Only (Non-HM)":
                or_clauses.append(
                    "NULLIF(intrastate_within_100_miles,'')::numeric > 0"
                )
        if or_clauses:
            conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Hazmat ───────────────────────────────────────────────────────────────
    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("hm_ind = 'Y'")
    elif hazmat == "false":
        conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    # ── Power units ──────────────────────────────────────────────────────────
    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(power_units,'')::numeric >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(power_units,'')::numeric <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    # ── Drivers ──────────────────────────────────────────────────────────────
    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers,'')::numeric >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers,'')::numeric <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    # ── Cargo (JSONB key exists with value 'X') ───────────────────────────────
    if filters.get("cargo"):
        cargo_list = filters["cargo"]
        if isinstance(cargo_list, str):
            cargo_list = cargo_list.split(",")
        or_clauses = []
        for c in cargo_list:
            key = _CARGO_KEY_MAP.get(c.strip(), c.strip())
            or_clauses.append(f"cargo->${idx} = '\"X\"'::jsonb")
            params.append(key)
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Insurance filters — operate on carriers.insurance JSONB array ────────
    # Helper: EXISTS subquery against jsonb_array_elements(insurance)

    def _ins_exists(field_sql: str, op: str, param_placeholder: str) -> str:
        return (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance,'[]'::jsonb)) AS ins "
            f"WHERE {field_sql} {op} {param_placeholder} "
            f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = ''))"
        )

    def _ins_exists_raw(where_clause: str) -> str:
        return (
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance,'[]'::jsonb)) AS ins "
            f"WHERE {where_clause})"
        )

    # Insurance required types
    if filters.get("insurance_required"):
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_clauses = []
        for itype in ins_types:
            pattern = _INS_TYPE_PATTERN.get(itype.strip(), itype.strip())
            or_clauses.append(
                _ins_exists_raw(
                    f"UPPER(ins->>'ins_type_desc') LIKE ${idx} "
                    f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
                )
            )
            params.append(f"{pattern}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # BIPD on file
    bipd_on_file = filters.get("bipd_on_file")
    if bipd_on_file == "1":
        conditions.append(
            _ins_exists_raw(
                f"UPPER(ins->>'ins_type_desc') LIKE 'BIPD%' "
                f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )
    elif bipd_on_file == "0":
        conditions.append(
            "NOT " + _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') LIKE 'BIPD%' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )

    # Cargo on file
    cargo_on_file = filters.get("cargo_on_file")
    if cargo_on_file == "1":
        conditions.append(
            _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') = 'CARGO' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )
    elif cargo_on_file == "0":
        conditions.append(
            "NOT " + _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') = 'CARGO' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )

    # Bond on file
    bond_on_file = filters.get("bond_on_file")
    if bond_on_file == "1":
        conditions.append(
            _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') LIKE 'SURETY%' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )
    elif bond_on_file == "0":
        conditions.append(
            "NOT " + _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') LIKE 'SURETY%' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )

    # Trust fund on file
    trust_fund_on_file = filters.get("trust_fund_on_file")
    if trust_fund_on_file == "1":
        conditions.append(
            _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') LIKE 'TRUST FUND%' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )
    elif trust_fund_on_file == "0":
        conditions.append(
            "NOT " + _ins_exists_raw(
                "UPPER(ins->>'ins_type_desc') LIKE 'TRUST FUND%' "
                "AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '')"
            )
        )

    # BIPD coverage amount (max_cov_amount stored in thousands)
    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        conditions.append(
            _ins_exists_raw(
                f"UPPER(ins->>'ins_type_desc') LIKE 'BIPD%' "
                f"AND NULLIF(REPLACE(ins->>'max_cov_amount',',',''),'')::numeric >= ${idx}"
            )
        )
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        conditions.append(
            _ins_exists_raw(
                f"UPPER(ins->>'ins_type_desc') LIKE 'BIPD%' "
                f"AND NULLIF(REPLACE(ins->>'max_cov_amount',',',''),'')::numeric <= ${idx}"
            )
        )
        params.append(compare_max)
        idx += 1

    # Insurance effective date (MM/DD/YYYY stored)
    if filters.get("ins_effective_date_from"):
        parts = filters["ins_effective_date_from"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            _ins_exists_raw(
                f"ins->>'effective_date' IS NOT NULL "
                f"AND ins->>'effective_date' LIKE '%/%/%' "
                f"AND TO_DATE(ins->>'effective_date','MM/DD/YYYY') >= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1
    if filters.get("ins_effective_date_to"):
        parts = filters["ins_effective_date_to"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            _ins_exists_raw(
                f"ins->>'effective_date' IS NOT NULL "
                f"AND ins->>'effective_date' LIKE '%/%/%' "
                f"AND TO_DATE(ins->>'effective_date','MM/DD/YYYY') <= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1

    # Insurance cancellation date
    if filters.get("ins_cancellation_date_from"):
        parts = filters["ins_cancellation_date_from"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            _ins_exists_raw(
                f"ins->>'cancl_effective_date' IS NOT NULL "
                f"AND ins->>'cancl_effective_date' != '' "
                f"AND ins->>'cancl_effective_date' LIKE '%/%/%' "
                f"AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') >= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        parts = filters["ins_cancellation_date_to"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            _ins_exists_raw(
                f"ins->>'cancl_effective_date' IS NOT NULL "
                f"AND ins->>'cancl_effective_date' != '' "
                f"AND ins->>'cancl_effective_date' LIKE '%/%/%' "
                f"AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') <= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1

    # Insurance company filter
    if filters.get("insurance_company"):
        companies = filters["insurance_company"]
        if isinstance(companies, str):
            companies = companies.split(",")
        or_clauses = []
        for company in companies:
            company_upper = company.strip().upper()
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company_upper, [company_upper])
            for pattern in patterns:
                or_clauses.append(
                    _ins_exists_raw(
                        f"UPPER(ins->>'name_company') LIKE ${idx} "
                        f"AND (ins->>'cancl_effective_date' IS NULL "
                        f"OR ins->>'cancl_effective_date' = '' "
                        f"OR (ins->>'cancl_effective_date' LIKE '%/%/%' "
                        f"AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') >= CURRENT_DATE))"
                    )
                )
                params.append(f"%{pattern}%")
                idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # Renewal policy months
    def _renewal_case(eff_field: str) -> str:
        return f"""
          CASE
            WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                 EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                 LEAST(EXTRACT(DAY FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                   EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                     EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int))
                 >= CURRENT_DATE
            THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                 EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                 LEAST(EXTRACT(DAY FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                   EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                     EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int))
            ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
                 EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                 LEAST(EXTRACT(DAY FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int,
                   EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
                     EXTRACT(MONTH FROM TO_DATE({eff_field},'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int))
          END
        """

    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        eff = "ins->>'effective_date'"
        conditions.append(
            _ins_exists_raw(
                f"{eff} IS NOT NULL AND {eff} LIKE '%/%/%' "
                f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '' "
                f"OR (ins->>'cancl_effective_date' LIKE '%/%/%' AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') >= CURRENT_DATE)) "
                f"AND {_renewal_case(eff)} BETWEEN CURRENT_DATE "
                f"AND (DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
            )
        )
        params.append(months)
        idx += 1

    if filters.get("renewal_date_from"):
        parts = filters["renewal_date_from"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        eff = "ins->>'effective_date'"
        conditions.append(
            _ins_exists_raw(
                f"{eff} IS NOT NULL AND {eff} LIKE '%/%/%' "
                f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '' "
                f"OR (ins->>'cancl_effective_date' LIKE '%/%/%' AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') >= CURRENT_DATE)) "
                f"AND {_renewal_case(eff)} >= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1

    if filters.get("renewal_date_to"):
        parts = filters["renewal_date_to"].split("-")
        date_db = f"{parts[1]}/{parts[2]}/{parts[0]}"
        eff = "ins->>'effective_date'"
        conditions.append(
            _ins_exists_raw(
                f"{eff} IS NOT NULL AND {eff} LIKE '%/%/%' "
                f"AND (ins->>'cancl_effective_date' IS NULL OR ins->>'cancl_effective_date' = '' "
                f"OR (ins->>'cancl_effective_date' LIKE '%/%/%' AND TO_DATE(ins->>'cancl_effective_date','MM/DD/YYYY') >= CURRENT_DATE)) "
                f"AND {_renewal_case(eff)} <= TO_DATE(${idx},'MM/DD/YYYY')"
            )
        )
        params.append(date_db)
        idx += 1

    # ── Build final query ─────────────────────────────────────────────────────
    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    limit_val = int(filters.get("limit", 200))
    offset_val = int(filters.get("offset", 0))

    # ORDER BY: only sort by legal_name when filtered (trgm index helps).
    # When unfiltered, use dot_number (b-tree indexed) — far faster on 4.5M rows.
    if is_filtered:
        order_by = "ORDER BY legal_name ASC"
    else:
        order_by = "ORDER BY dot_number ASC"

    query = f"""
        SELECT
            dot_number, legal_name, dba_name, status_code, classdef,
            email_address, phone, company_officer_1, company_officer_2,
            phy_street, phy_city, phy_state, phy_zip,
            carrier_mailing_street, carrier_mailing_city, carrier_mailing_state, carrier_mailing_zip,
            mcs150_date, mcs150_mileage, add_date,
            power_units, total_drivers,
            interstate_beyond_100_miles, interstate_within_100_miles,
            intrastate_beyond_100_miles, intrastate_within_100_miles,
            hm_ind, dun_bradstreet_no,
            dockets, cargo, insurance
        FROM carriers
        WHERE {where}
        {order_by}
        LIMIT {limit_val} OFFSET {offset_val}
    """

    # Only run COUNT when filters are active — counting 4.5M rows every page load is too slow.
    # When unfiltered, return 0 and let the frontend use the cached total from /api/carriers/count.
    count_query = f"SELECT COUNT(*) AS cnt FROM carriers WHERE {where}" if is_filtered else None

    try:
        async with pool.acquire() as conn:
            # 25-second statement timeout — fail fast instead of hanging forever
            await conn.execute("SET statement_timeout = '25000'")

            if count_query:
                rows, count_row = await asyncio.gather(
                    conn.fetch(query, *params),
                    conn.fetchrow(count_query, *params),
                )
                filtered_count = count_row["cnt"] if count_row else 0
            else:
                rows = await conn.fetch(query, *params)
                filtered_count = 0  # frontend uses cached total

        return {
            "data": [_carrier_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}


async def delete_carrier(mc_number: str) -> bool:
    """Delete by dot_number (mc_number is now the dot_number proxy for deletions)."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE dot_number = $1", mc_number
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier {mc_number}: {e}")
        return False


async def get_carrier_count() -> int:
    """Use pg_class reltuples for instant approximate count on large tables."""
    pool = get_pool()
    try:
        # reltuples gives the planner's estimate — updated by ANALYZE, accurate within ~1%
        row = await pool.fetchrow(
            "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"
        )
        approx = row["cnt"] if row else 0
        # If estimate is 0 (table never analyzed), fall back to exact count
        if approx <= 0:
            row = await pool.fetchrow("SELECT COUNT(*) AS cnt FROM carriers")
            return row["cnt"] if row else 0
        return approx
    except Exception as e:
        print(f"[DB] Error getting carrier count: {e}")
        return 0


async def get_carrier_dashboard_stats() -> dict:
    pool = get_pool()
    empty = {
        "total": 0, "active_carriers": 0, "brokers": 0,
        "with_email": 0, "email_rate": "0",
        "with_safety_rating": 0, "with_insurance": 0,
        "with_inspections": 0, "with_crashes": 0,
        "not_authorized": 0, "other": 0,
    }
    try:
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = '20000'")
            # Use a single scan with conditional counts — much faster than separate queries
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status_code = 'A') AS active_carriers,
                    COUNT(*) FILTER (WHERE UPPER(classdef) LIKE '%BROKER%') AS brokers,
                    COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                    COUNT(*) FILTER (WHERE status_code IS DISTINCT FROM 'A') AS not_authorized,
                    COUNT(*) FILTER (WHERE insurance IS NOT NULL AND jsonb_array_length(insurance) > 0) AS with_insurance
                FROM carriers
            """)
        if not row:
            return empty
        total = row["total"]
        active = row["active_carriers"]
        not_auth = row["not_authorized"]
        with_email = row["with_email"]
        email_rate = f"{(with_email / total * 100):.1f}" if total > 0 else "0"
        return {
            "total": total,
            "active_carriers": active,
            "brokers": row["brokers"],
            "with_email": with_email,
            "email_rate": email_rate,
            "with_safety_rating": 0,
            "with_insurance": row["with_insurance"],
            "with_inspections": 0,
            "with_crashes": 0,
            "not_authorized": not_auth,
            "other": max(0, total - active - not_auth),
        }
    except Exception as e:
        print(f"[DB] Error getting dashboard stats: {e}")
        return empty


async def update_carrier_insurance(dot_number: str, policies: list) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "UPDATE carriers SET insurance = $1 WHERE dot_number = $2",
            _to_jsonb(policies),
            dot_number,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating insurance for DOT {dot_number}: {e}")
        return False


async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    """Safety data (rating, basic scores etc.) are not in the Census table.
    This is a no-op placeholder kept for API compatibility."""
    print(f"[DB] update_carrier_safety called for DOT {dot_number} — Census table has no safety columns")
    return False


async def get_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT dot_number, legal_name, dba_name, status_code, classdef,
                email_address, phone, company_officer_1, company_officer_2,
                phy_street, phy_city, phy_state, phy_zip,
                carrier_mailing_street, carrier_mailing_city, carrier_mailing_state, carrier_mailing_zip,
                mcs150_date, mcs150_mileage, add_date,
                power_units, total_drivers,
                interstate_beyond_100_miles, interstate_within_100_miles,
                intrastate_beyond_100_miles, intrastate_within_100_miles,
                hm_ind, dun_bradstreet_no, dockets, cargo, insurance
            FROM carriers
            WHERE temp >= $1 AND temp <= $2
            ORDER BY temp ASC
            """,
            int(start),
            int(end),
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []


async def fetch_insurance_history(mc_number: str) -> list[dict]:
    """
    Insurance is now in carriers.insurance JSONB, not a separate table.
    Fetch by dot_number or by MC number via the dockets column.
    """
    pool = get_pool()
    try:
        # Try to find via dockets column
        row = await pool.fetchrow(
            """
            SELECT insurance FROM carriers
            WHERE dockets::text ILIKE $1
            LIMIT 1
            """,
            f"%MC{mc_number}%",
        )
        if not row:
            return []
        return _format_insurance_from_jsonb(row["insurance"])
    except Exception as e:
        print(f"[DB] Error fetching insurance history for MC {mc_number}: {e}")
        return []


# ── FMCSA Register ────────────────────────────────────────────────────────────

async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    batch_size = 500
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = [
                        (
                            entry["number"],
                            entry.get("title", ""),
                            entry.get("decided", "N/A"),
                            entry.get("category", ""),
                            extracted_date,
                        )
                        for entry in batch
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO fmcsa_register (number, title, decided, category, date_fetched)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (number, date_fetched) DO UPDATE SET
                            title = EXCLUDED.title,
                            decided = EXCLUDED.decided,
                            category = EXCLUDED.category,
                            updated_at = NOW()
                        """,
                        args,
                    )
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving FMCSA entries: {e}")

    return {"success": True, "saved": saved, "skipped": len(entries) - saved}


async def fetch_fmcsa_register_by_date(
    extracted_date: str,
    category: Optional[str] = None,
    search_term: Optional[str] = None,
) -> list[dict]:
    pool = get_pool()
    conditions = ["date_fetched = $1"]
    params: list = [extracted_date]
    idx = 2

    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1
    if search_term:
        conditions.append(f"(title ILIKE ${idx} OR number ILIKE ${idx})")
        params.append(f"%{search_term}%")
        idx += 1

    where = " AND ".join(conditions)
    rows = await pool.fetch(
        f"SELECT number, title, decided, category, date_fetched FROM fmcsa_register WHERE {where} ORDER BY number LIMIT 10000",
        *params,
    )
    return [dict(row) for row in rows]


async def get_fmcsa_extracted_dates() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT date_fetched FROM fmcsa_register ORDER BY date_fetched DESC"
    )
    return [row["date_fetched"] for row in rows]


async def get_fmcsa_categories() -> list[str]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT category FROM fmcsa_register WHERE category IS NOT NULL ORDER BY category"
        )
        return [row["category"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching FMCSA categories: {e}")
        return []


async def delete_fmcsa_entries_before_date(date: str) -> int:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM fmcsa_register WHERE date_fetched < $1", date
        )
        parts = result.split(" ")
        return int(parts[-1]) if len(parts) > 1 else 0
    except Exception as e:
        print(f"[DB] Error deleting FMCSA entries: {e}")
        return 0


# ── Users ─────────────────────────────────────────────────────────────────────

def _user_row_to_dict(row) -> dict:
    d = dict(row)
    d.pop("password_hash", None)
    for key in ("created_at", "updated_at", "blocked_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def fetch_users() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users ORDER BY created_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching users: {e}")
        return []


async def fetch_user_by_email(email: str) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users WHERE email = $1",
            email.lower(),
        )
        return _user_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching user by email: {e}")
        return None


async def create_user(user_data: dict) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO users (user_id, name, email, password_hash, role, plan,
                               daily_limit, records_extracted_today, last_active,
                               ip_address, is_online, is_blocked)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
            """,
            user_data.get("user_id"),
            user_data.get("name"),
            user_data.get("email", "").lower(),
            user_data.get("password_hash"),
            user_data.get("role", "user"),
            user_data.get("plan", "Free"),
            user_data.get("daily_limit", 50),
            user_data.get("records_extracted_today", 0),
            user_data.get("last_active", "Never"),
            user_data.get("ip_address"),
            user_data.get("is_online", False),
            user_data.get("is_blocked", False),
        )
        return _user_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return None


async def update_user(user_id: str, user_data: dict) -> bool:
    pool = get_pool()
    _ALLOWED = {
        "name", "role", "plan", "daily_limit",
        "records_extracted_today", "last_active",
        "ip_address", "is_online", "is_blocked",
    }
    columns = {k: v for k, v in user_data.items() if k in _ALLOWED}
    if not columns:
        return False
    set_clauses = []
    values = []
    for i, (col, val) in enumerate(columns.items(), start=1):
        set_clauses.append(f"{col} = ${i}")
        values.append(val)
    values.append(user_id)
    query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ${len(values)}"
    try:
        result = await pool.execute(query, *values)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating user {user_id}: {e}")
        return False


async def delete_user(user_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM users WHERE user_id = $1", user_id)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting user {user_id}: {e}")
        return False


async def get_user_password_hash(email: str) -> Optional[str]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT password_hash FROM users WHERE email = $1", email.lower()
        )
        return row["password_hash"] if row and row["password_hash"] else None
    except Exception as e:
        print(f"[DB] Error fetching password hash: {e}")
        return None


async def fetch_blocked_ips() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch("SELECT * FROM blocked_ips ORDER BY blocked_at DESC")
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching blocked IPs: {e}")
        return []


async def block_ip(ip_address: str, reason: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            "INSERT INTO blocked_ips (ip_address, reason) VALUES ($1, $2) ON CONFLICT (ip_address) DO NOTHING",
            ip_address,
            reason or "No reason provided",
        )
        return True
    except Exception as e:
        print(f"[DB] Error blocking IP {ip_address}: {e}")
        return False


async def unblock_ip(ip_address: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM blocked_ips WHERE ip_address = $1", ip_address)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error unblocking IP {ip_address}: {e}")
        return False


async def is_ip_blocked(ip_address: str) -> bool:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT ip_address FROM blocked_ips WHERE ip_address = $1", ip_address
        )
        return row is not None
    except Exception as e:
        print(f"[DB] Error checking IP block: {e}")
        return False


# ── New Ventures ─────────────────────────────────────────────────────────────

_NV_COLUMNS = [
    "dot_number", "prefix", "docket_number", "status_code", "carship",
    "carrier_operation", "name", "name_dba", "add_date", "chgn_date",
    "common_stat", "contract_stat", "broker_stat",
    "common_app_pend", "contract_app_pend", "broker_app_pend",
    "common_rev_pend", "contract_rev_pend", "broker_rev_pend",
    "property_chk", "passenger_chk", "hhg_chk", "private_auth_chk", "enterprise_chk",
    "operating_status", "operating_status_indicator",
    "phy_str", "phy_city", "phy_st", "phy_zip", "phy_country", "phy_cnty",
    "mai_str", "mai_city", "mai_st", "mai_zip", "mai_country", "mai_cnty",
    "phy_undeliv", "mai_undeliv",
    "phy_phone", "phy_fax", "mai_phone", "mai_fax", "cell_phone", "email_address",
    "company_officer_1", "company_officer_2",
    "genfreight", "household", "metalsheet", "motorveh", "drivetow", "logpole",
    "bldgmat", "mobilehome", "machlrg", "produce", "liqgas", "intermodal",
    "passengers", "oilfield", "livestock", "grainfeed", "coalcoke", "meat",
    "garbage", "usmail", "chem", "drybulk", "coldfood", "beverages",
    "paperprod", "utility", "farmsupp", "construct", "waterwell",
    "cargoothr", "cargoothr_desc",
    "hm_ind", "bipd_req", "cargo_req", "bond_req", "bipd_file", "cargo_file", "bond_file",
    "owntruck", "owntract", "owntrail", "owncoach",
    "ownschool_1_8", "ownschool_9_15", "ownschool_16", "ownbus_16",
    "ownvan_1_8", "ownvan_9_15", "ownlimo_1_8", "ownlimo_9_15", "ownlimo_16",
    "trmtruck", "trmtract", "trmtrail", "trmcoach",
    "trmschool_1_8", "trmschool_9_15", "trmschool_16", "trmbus_16",
    "trmvan_1_8", "trmvan_9_15", "trmlimo_1_8", "trmlimo_9_15", "trmlimo_16",
    "trptruck", "trptract", "trptrail", "trpcoach",
    "trpschool_1_8", "trpschool_9_15", "trpschool_16", "trpbus_16",
    "trpvan_1_8", "trpvan_9_15", "trplimo_1_8", "trplimo_9_15", "trplimo_16",
    "total_trucks", "total_buses", "total_pwr", "fleetsize",
    "inter_within_100", "inter_beyond_100", "total_inter_drivers",
    "intra_within_100", "intra_beyond_100", "total_intra_drivers",
    "total_drivers", "avg_tld", "total_cdl",
    "review_type", "review_id", "review_date", "recordable_crash_rate",
    "mcs150_mileage", "mcs151_mileage", "mcs150_mileage_year", "mcs150_date",
    "safety_rating", "safety_rating_date",
    "arber", "smartway", "tia", "tia_phone", "tia_contact_name",
    "tia_tool_free", "tia_fax", "tia_email", "tia_website",
    "phy_ups_store", "mai_ups_store", "phy_mail_box", "mai_mail_box",
]


def _new_venture_row_to_dict(row) -> dict:
    d = dict(row)
    if "raw_data" in d:
        d["raw_data"] = _parse_jsonb(d["raw_data"])
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def save_new_venture_entries(entries: list[dict], scrape_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    cols = _NV_COLUMNS + ["raw_data", "scrape_date"]
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
    col_names = ", ".join(cols)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in _NV_COLUMNS + ["raw_data"]
    )

    saved = 0
    skipped = 0
    batch_size = 500

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = []
                    for entry in batch:
                        row_vals = [entry.get(c) for c in _NV_COLUMNS]
                        row_vals.append(_to_jsonb(entry.get("raw_data")))
                        row_vals.append(scrape_date)
                        args.append(tuple(row_vals))
                    await conn.executemany(
                        f"""
                        INSERT INTO new_ventures ({col_names})
                        VALUES ({placeholders})
                        ON CONFLICT (dot_number, add_date) DO UPDATE SET
                            {update_set},
                            updated_at = NOW()
                        """,
                        args,
                    )
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error saving new ventures: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_new_ventures(filters: dict) -> list[dict]:
    pool = get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("scrape_date"):
        conditions.append(f"scrape_date = ${idx}")
        params.append(filters["scrape_date"])
        idx += 1
    if filters.get("search_term"):
        conditions.append(f"(name ILIKE ${idx} OR docket_number ILIKE ${idx} OR dot_number ILIKE ${idx})")
        params.append(f"%{filters['search_term']}%")
        idx += 1
    if filters.get("status_code"):
        conditions.append(f"status_code = ${idx}")
        params.append(filters["status_code"])
        idx += 1
    if filters.get("state"):
        conditions.append(f"phy_st = ${idx}")
        params.append(filters["state"])
        idx += 1
    if filters.get("has_email") == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    if filters.get("hm_ind"):
        conditions.append(f"hm_ind = ${idx}")
        params.append(filters["hm_ind"])
        idx += 1
    if filters.get("carrier_operation"):
        conditions.append(f"carrier_operation ILIKE ${idx}")
        params.append(f"%{filters['carrier_operation']}%")
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"
    limit_val = int(filters.get("limit", 500))
    offset_val = int(filters.get("offset", 0))

    try:
        rows = await pool.fetch(
            f"SELECT * FROM new_ventures WHERE {where} ORDER BY created_at DESC LIMIT {limit_val} OFFSET {offset_val}",
            *params,
        )
        return [_new_venture_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new ventures: {e}")
        return []


async def get_new_venture_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM new_ventures")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting new venture count: {e}")
        return 0


async def get_new_venture_scraped_dates() -> list[str]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT scrape_date FROM new_ventures ORDER BY scrape_date DESC"
        )
        return [row["scrape_date"] for row in rows]
    except Exception as e:
        print(f"[DB] Error getting scrape dates: {e}")
        return []


async def fetch_new_venture_by_id(record_id: str) -> dict | None:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM new_ventures WHERE id = $1", record_id)
        return _new_venture_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching new venture by ID: {e}")
        return None


async def delete_new_venture(record_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM new_ventures WHERE id = $1", record_id)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting new venture {record_id}: {e}")
        return False
