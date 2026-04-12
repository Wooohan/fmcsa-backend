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


_SCHEMA_SQL = """
-- ── Tables (carriers already exists from Census import — DO NOT recreate) ──

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

-- ── Indexes on carriers (Census columns) ───────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_status_code ON carriers(status_code);
CREATE INDEX IF NOT EXISTS idx_carriers_phy_state ON carriers(phy_state);
CREATE INDEX IF NOT EXISTS idx_carriers_carrier_op ON carriers(carrier_operation);
CREATE INDEX IF NOT EXISTS idx_carriers_hm_ind ON carriers(hm_ind);
CREATE INDEX IF NOT EXISTS idx_carriers_temp ON carriers(temp);
CREATE INDEX IF NOT EXISTS idx_carriers_add_date ON carriers(add_date);
CREATE INDEX IF NOT EXISTS idx_carriers_email ON carriers(email_address);

-- Trigram indexes for fast ILIKE text search
CREATE INDEX IF NOT EXISTS idx_carriers_legal_name_trgm ON carriers USING gin (legal_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number_trgm ON carriers USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_dba_name_trgm ON carriers USING gin (dba_name gin_trgm_ops);

-- GIN JSONB indexes for dockets, cargo, insurance
CREATE INDEX IF NOT EXISTS idx_carriers_dockets_gin ON carriers USING gin (dockets jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_cargo_gin ON carriers USING gin (cargo jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_insurance_gin ON carriers USING gin (insurance jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

-- ── Timestamp triggers ──────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_fmcsa_register_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER update_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_fmcsa_register_updated_at();

CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();

-- ── New Ventures table (ALL BrokerSnapshot CSV columns) ──────────────────────
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

-- Trigram indexes for fast ILIKE text search on new_ventures
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_trgm ON new_ventures USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_dba_trgm ON new_ventures USING gin (name_dba gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_trgm ON new_ventures USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_trgm ON new_ventures USING gin (docket_number gin_trgm_ops);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

-- ── Default admin user ──────────────────────────────────────────────────────
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


# ── FMCSA Register functions ────────────────────────────────────────────────

async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    skipped = 0
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
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


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
    query = f"""
        SELECT number, title, decided, category, date_fetched
        FROM fmcsa_register
        WHERE {where}
        ORDER BY number
        LIMIT 10000
    """

    rows = await pool.fetch(query, *params)
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


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_jsonb(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


def _parse_jsonb(value) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


# ── Carrier helpers (Census schema) ─────────────────────────────────────────

def _format_embedded_insurance(raw_insurance) -> list[dict]:
    """Format the embedded insurance JSONB array from Census data."""
    parsed = _parse_jsonb(raw_insurance)
    if not parsed or not isinstance(parsed, list):
        return []
    results = []
    for row in parsed:
        if not isinstance(row, dict):
            continue
        raw_amount = str(row.get("max_cov_amount") or "").strip()
        try:
            amount_int = int(raw_amount) * 1000
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


def _build_mc_number(row) -> str:
    """Extract MC number from dockets JSONB or temp column."""
    dockets = _parse_jsonb(row.get("dockets"))
    if dockets and isinstance(dockets, list):
        for d in dockets:
            ds = str(d) if d else ""
            if ds.upper().startswith("MC"):
                return ds[2:]
    temp = row.get("temp")
    if temp is not None:
        return str(temp)
    return ""


def _build_carrier_operation_display(code: str) -> str:
    """Convert single-char carrier_operation to display string."""
    if not code:
        return ""
    code = code.strip().upper()
    mapping = {"A": "Interstate", "B": "Intrastate Only (HM)", "C": "Intrastate Only (Non-HM)"}
    return mapping.get(code, code)


def _build_cargo_list(cargo_jsonb) -> list[str]:
    """Convert Census cargo JSONB object to list of cargo type names."""
    parsed = _parse_jsonb(cargo_jsonb)
    if not parsed or not isinstance(parsed, dict):
        return []
    _CARGO_MAP = {
        "genfreight": "General Freight", "household": "Household Goods",
        "metalsheet": "Metal: Sheets, Coils, Rolls", "motorveh": "Motor Vehicles",
        "drivetow": "Drive/Tow Away", "logpole": "Logs, Poles, Beams, Lumber",
        "bldgmat": "Building Materials", "mobilehome": "Mobile Homes",
        "machlrg": "Machinery, Large Objects", "produce": "Fresh Produce",
        "liqgas": "Liquids/Gases", "intermodal": "Intermodal Cont.",
        "passengers": "Passengers", "oilfield": "Oilfield Equipment",
        "livestock": "Livestock", "grainfeed": "Grain, Feed, Hay",
        "coalcoke": "Coal/Coke", "meat": "Meat",
        "garbage": "Garbage/Refuse", "usmail": "US Mail",
        "chem": "Chemicals", "drybulk": "Commodities Dry Bulk",
        "coldfood": "Refrigerated Food", "beverages": "Beverages",
        "paperprod": "Paper Products", "utility": "Utilities",
        "farmsupp": "Agricultural/Farm Supplies", "construct": "Construction",
        "waterwell": "Water Well", "cargoothr": "Other",
    }
    result = []
    for key, val in parsed.items():
        if val and str(val).strip().upper() == "X":
            result.append(_CARGO_MAP.get(key, key))
    return result


def _build_classdef_list(classdef: str) -> list[str]:
    """Convert semicolon-separated classdef string to list."""
    if not classdef:
        return []
    return [c.strip() for c in classdef.split(";") if c.strip()]


def _carrier_row_to_dict(row) -> dict:
    """Convert a Census carriers row to the API response dict."""
    d = dict(row)

    mc_number = _build_mc_number(d)
    cargo_list = _build_cargo_list(d.get("cargo"))
    classdef_list = _build_classdef_list(d.get("classdef"))
    carrier_op_display = _build_carrier_operation_display(d.get("carrier_operation"))
    insurance_filings = _format_embedded_insurance(d.get("insurance"))

    # Build physical address string
    parts = []
    if d.get("phy_street"):
        parts.append(d["phy_street"].strip())
    city_state = []
    if d.get("phy_city"):
        city_state.append(d["phy_city"].strip())
    if d.get("phy_state"):
        city_state.append(d["phy_state"].strip())
    if city_state:
        parts.append(", ".join(city_state))
    if d.get("phy_zip"):
        parts.append(d["phy_zip"].strip())
    physical_address = ", ".join(parts) if parts else ""

    # Build mailing address string
    mparts = []
    if d.get("mai_street"):
        mparts.append(d["mai_street"].strip())
    mcity_state = []
    if d.get("mai_city"):
        mcity_state.append(d["mai_city"].strip())
    if d.get("mai_state"):
        mcity_state.append(d["mai_state"].strip())
    if mcity_state:
        mparts.append(", ".join(mcity_state))
    if d.get("mai_zip"):
        mparts.append(d["mai_zip"].strip())
    mailing_address = ", ".join(mparts) if mparts else ""

    # Company rep from company_rep_1 / company_rep_2
    reps = []
    if d.get("company_rep_1"):
        reps.append(d["company_rep_1"].strip())
    if d.get("company_rep_2"):
        reps.append(d["company_rep_2"].strip())
    company_rep = "; ".join(reps) if reps else ""

    # Status display
    status_code = (d.get("status_code") or "").strip()
    status_map = {"A": "Active/Authorized", "N": "Not Authorized"}
    status_display = status_map.get(status_code, status_code)

    result = {
        "mc_number": mc_number,
        "dot_number": d.get("dot_number") or "",
        "legal_name": d.get("legal_name") or "",
        "dba_name": d.get("dba_name") or "",
        "status": status_display,
        "status_code": status_code,
        "email": d.get("email_address") or "",
        "phone": d.get("phy_phone") or "",
        "power_units": d.get("nbr_power_unit") or "",
        "drivers": d.get("driver_total") or "",
        "physical_address": physical_address,
        "mailing_address": mailing_address,
        "mcs150_date": d.get("mcs150_date") or "",
        "mcs150_mileage": d.get("mcs150_mileage") or "",
        "mcs150_mileage_year": d.get("mcs150_mileage_year") or "",
        "operation_classification": classdef_list,
        "carrier_operation": [carrier_op_display] if carrier_op_display else [],
        "carrier_operation_code": (d.get("carrier_operation") or "").strip(),
        "cargo_carried": cargo_list,
        "duns_number": d.get("dun_bradstreet_no") or "",
        "safety_rating": d.get("safety_rating") or "",
        "safety_rating_date": d.get("safety_rating_date") or "",
        "hm_ind": (d.get("hm_ind") or "").strip(),
        "company_rep": company_rep,
        "add_date": d.get("add_date") or "",
        "phy_state": (d.get("phy_state") or "").strip(),
        "phy_city": (d.get("phy_city") or "").strip(),
        "total_cdl": d.get("total_cdl") or "",
        "owntruck": d.get("owntruck") or "",
        "owntract": d.get("owntract") or "",
        "owntrail": d.get("owntrail") or "",
        "bipd_file": (d.get("bipd_file") or "").strip(),
        "cargo_file": (d.get("cargo_file") or "").strip(),
        "bond_file": (d.get("bond_file") or "").strip(),
        "insurance_history_filings": insurance_filings,
    }
    return result


# ── Carrier query functions (Census schema) ─────────────────────────────────

# Columns to SELECT for carrier list queries — only what we need
_CARRIER_SELECT_COLS = """
    dot_number, legal_name, dba_name, status_code,
    email_address, phy_phone, nbr_power_unit, driver_total,
    phy_street, phy_city, phy_state, phy_zip,
    mai_street, mai_city, mai_state, mai_zip,
    mcs150_date, mcs150_mileage, mcs150_mileage_year,
    classdef, carrier_operation, cargo, dockets, temp,
    dun_bradstreet_no, safety_rating, safety_rating_date,
    hm_ind, company_rep_1, company_rep_2, add_date,
    total_cdl, owntruck, owntract, owntrail,
    bipd_file, cargo_file, bond_file,
    insurance
"""

# Reverse mapping from display cargo name to Census JSONB key
_CARGO_NAME_TO_KEY = {
    "General Freight": "genfreight", "Household Goods": "household",
    "Metal: Sheets, Coils, Rolls": "metalsheet", "Motor Vehicles": "motorveh",
    "Drive/Tow Away": "drivetow", "Logs, Poles, Beams, Lumber": "logpole",
    "Building Materials": "bldgmat", "Mobile Homes": "mobilehome",
    "Machinery, Large Objects": "machlrg", "Fresh Produce": "produce",
    "Liquids/Gases": "liqgas", "Intermodal Cont.": "intermodal",
    "Passengers": "passengers", "Oilfield Equipment": "oilfield",
    "Livestock": "livestock", "Grain, Feed, Hay": "grainfeed",
    "Coal/Coke": "coalcoke", "Meat": "meat",
    "Garbage/Refuse": "garbage", "US Mail": "usmail",
    "Chemicals": "chem", "Commodities Dry Bulk": "drybulk",
    "Refrigerated Food": "coldfood", "Beverages": "beverages",
    "Paper Products": "paperprod", "Utilities": "utility",
    "Agricultural/Farm Supplies": "farmsupp", "Construction": "construct",
    "Water Well": "waterwell", "Other": "cargoothr",
}

# Map carrier operation display names to Census single-char codes
_CARRIER_OP_TO_CODE = {
    "Interstate": "A",
    "Intrastate Only (HM)": "B",
    "Intrastate Only (Non-HM)": "C",
}


async def fetch_carriers(filters: dict) -> dict:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # MC/MX Number — search dockets JSONB text and temp bigint column
    if filters.get("mc_number"):
        mc_val = filters["mc_number"].strip()
        conditions.append(f"(temp::text ILIKE ${idx} OR dockets::text ILIKE ${idx})")
        params.append(f"%{mc_val}%")
        idx += 1

    # USDOT Number
    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    # Legal Name
    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    # DUNS Number
    if filters.get("duns_number"):
        conditions.append(f"dun_bradstreet_no ILIKE ${idx}")
        params.append(f"%{filters['duns_number']}%")
        idx += 1

    # Active/Authorized status
    active = filters.get("active")
    if active == "true":
        conditions.append("status_code = 'A'")
    elif active == "false":
        conditions.append("status_code != 'A'")

    # Years in Business (from add_date in YYYYMMDD format)
    if filters.get("years_in_business_min"):
        yrs = int(filters["years_in_business_min"])
        conditions.append(
            f"add_date IS NOT NULL AND add_date != '' AND LENGTH(add_date) = 8 "
            f"AND TO_DATE(add_date, 'YYYYMMDD') <= (CURRENT_DATE - MAKE_INTERVAL(years => ${idx}))"
        )
        params.append(yrs)
        idx += 1
    if filters.get("years_in_business_max"):
        yrs = int(filters["years_in_business_max"])
        conditions.append(
            f"add_date IS NOT NULL AND add_date != '' AND LENGTH(add_date) = 8 "
            f"AND TO_DATE(add_date, 'YYYYMMDD') >= (CURRENT_DATE - MAKE_INTERVAL(years => ${idx}))"
        )
        params.append(yrs)
        idx += 1

    # State filter — use phy_state column directly (exact match, indexed)
    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|") if s.strip()]
        if len(states) == 1:
            conditions.append(f"phy_state = ${idx}")
            params.append(states[0])
            idx += 1
        elif states:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(states)))
            conditions.append(f"phy_state IN ({placeholders})")
            for s in states:
                params.append(s)
                idx += 1

    # Has Email
    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    # Has Company Rep
    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("(company_rep_1 IS NOT NULL AND company_rep_1 != '')")
    elif has_company_rep == "false":
        conditions.append("(company_rep_1 IS NULL OR company_rep_1 = '')")

    # Phone filter
    if filters.get("phone"):
        conditions.append(f"phy_phone ILIKE ${idx}")
        params.append(f"%{filters['phone']}%")
        idx += 1

    # Email filter (specific email search)
    if filters.get("email"):
        conditions.append(f"email_address ILIKE ${idx}")
        params.append(f"%{filters['email']}%")
        idx += 1

    # Operation Classification (classdef is semicolon-separated string)
    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        or_clauses = []
        for cls in classifications:
            or_clauses.append(f"classdef ILIKE ${idx}")
            params.append(f"%{cls.strip()}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # Carrier Operation (single char: A=Interstate, B=Intra HM, C=Intra Non-HM)
    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        codes = []
        for op in ops:
            op_stripped = op.strip()
            code = _CARRIER_OP_TO_CODE.get(op_stripped, op_stripped)
            codes.append(code)
        if len(codes) == 1:
            conditions.append(f"carrier_operation = ${idx}")
            params.append(codes[0])
            idx += 1
        elif codes:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(codes)))
            conditions.append(f"carrier_operation IN ({placeholders})")
            for c in codes:
                params.append(c)
                idx += 1

    # Cargo filter (cargo is JSONB object with keys like "genfreight": "X")
    if filters.get("cargo"):
        cargo_names = filters["cargo"]
        if isinstance(cargo_names, str):
            cargo_names = cargo_names.split(",")
        cargo_keys = []
        for name in cargo_names:
            key = _CARGO_NAME_TO_KEY.get(name.strip(), name.strip().lower())
            cargo_keys.append(key)
        # Use JSONB ?| operator: cargo has any of these keys
        conditions.append(f"cargo ?| ${idx}::text[]")
        params.append(cargo_keys)
        idx += 1

    # Hazmat indicator
    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("hm_ind = 'Y'")
    elif hazmat == "false":
        conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    # Power Units
    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(nbr_power_unit, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(nbr_power_unit, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    # Drivers
    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(driver_total, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(driver_total, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    # MCS-150 Form Date range
    if filters.get("mcs150_date_from"):
        conditions.append(
            f"mcs150_date IS NOT NULL AND mcs150_date != '' AND LENGTH(mcs150_date) >= 8 "
            f"AND mcs150_date >= ${idx}"
        )
        params.append(filters["mcs150_date_from"])
        idx += 1
    if filters.get("mcs150_date_to"):
        conditions.append(
            f"mcs150_date IS NOT NULL AND mcs150_date != '' AND LENGTH(mcs150_date) >= 8 "
            f"AND mcs150_date <= ${idx}"
        )
        params.append(filters["mcs150_date_to"])
        idx += 1

    # Mileage/VMT range
    if filters.get("mileage_min"):
        conditions.append(f"NULLIF(REPLACE(mcs150_mileage, ',', ''), '')::bigint >= ${idx}")
        params.append(int(filters["mileage_min"]))
        idx += 1
    if filters.get("mileage_max"):
        conditions.append(f"NULLIF(REPLACE(mcs150_mileage, ',', ''), '')::bigint <= ${idx}")
        params.append(int(filters["mileage_max"]))
        idx += 1

    # ── Insurance filters (embedded JSONB) ──────────────────────────────────

    # BIPD / Cargo / Bond / Trust Fund on file
    bipd_on_file = filters.get("bipd_on_file")
    if bipd_on_file == "1":
        conditions.append("bipd_file = 'Y'")
    elif bipd_on_file == "0":
        conditions.append("(bipd_file IS NULL OR bipd_file != 'Y')")

    cargo_on_file = filters.get("cargo_on_file")
    if cargo_on_file == "1":
        conditions.append("cargo_file = 'Y'")
    elif cargo_on_file == "0":
        conditions.append("(cargo_file IS NULL OR cargo_file != 'Y')")

    bond_on_file = filters.get("bond_on_file")
    if bond_on_file == "1":
        conditions.append("bond_file = 'Y'")
    elif bond_on_file == "0":
        conditions.append("(bond_file IS NULL OR bond_file != 'Y')")

    trust_fund_on_file = filters.get("trust_fund_on_file")
    if trust_fund_on_file == "1":
        conditions.append(
            "insurance IS NOT NULL AND insurance::text ILIKE '%TRUST FUND%'"
        )
    elif trust_fund_on_file == "0":
        conditions.append(
            "(insurance IS NULL OR insurance::text NOT ILIKE '%TRUST FUND%')"
        )

    # Insurance Required types
    _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
    if filters.get("insurance_required"):
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_clauses = []
        for itype in ins_types:
            pattern = _INS_TYPE_PATTERN.get(itype, itype)
            or_clauses.append(
                f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
                f"WHERE elem->>'ins_type_desc' LIKE ${idx} "
                f"AND (elem->>'cancl_effective_date' IS NULL OR elem->>'cancl_effective_date' = ''))"
            )
            params.append(pattern)
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # BIPD coverage amount range
    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE NULLIF(REPLACE(elem->>'max_cov_amount', ',', ''), '')::numeric >= ${idx})"
        )
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE NULLIF(REPLACE(elem->>'max_cov_amount', ',', ''), '')::numeric <= ${idx})"
        )
        params.append(compare_max)
        idx += 1

    # Insurance effective date range
    if filters.get("ins_effective_date_from"):
        parts = filters["ins_effective_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'effective_date' IS NOT NULL AND elem->>'effective_date' LIKE '%/%/%' "
            f"AND TO_DATE(elem->>'effective_date', 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_effective_date_to"):
        parts = filters["ins_effective_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'effective_date' IS NOT NULL AND elem->>'effective_date' LIKE '%/%/%' "
            f"AND TO_DATE(elem->>'effective_date', 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # Insurance cancellation date range
    if filters.get("ins_cancellation_date_from"):
        parts = filters["ins_cancellation_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'cancl_effective_date' IS NOT NULL AND elem->>'cancl_effective_date' != '' "
            f"AND elem->>'cancl_effective_date' LIKE '%/%/%' "
            f"AND TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        parts = filters["ins_cancellation_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'cancl_effective_date' IS NOT NULL AND elem->>'cancl_effective_date' != '' "
            f"AND elem->>'cancl_effective_date' LIKE '%/%/%' "
            f"AND TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # Insurance Company filter
    _INSURANCE_COMPANY_PATTERNS: dict[str, list[str]] = {
        "GREAT WEST CASUALTY": ["GREAT WEST%"],
        "UNITED FINANCIAL CASUALTY": ["UNITED FINANCIAL%"],
        "GEICO MARINE": ["GEICO MARINE%"],
        "NORTHLAND INSURANCE": ["NORTHLAND%"],
        "ARTISAN & TRUCKERS": ["ARTISAN%", "TRUCKERS CASUALTY%"],
        "CANAL INSURANCE": ["CANAL INS%"],
        "PROGRESSIVE": ["PROGRESSIVE%"],
        "BERKSHIRE HATHAWAY": ["BERKSHIRE%"],
        "OLD REPUBLIC": ["OLD REPUBLIC%"],
        "SENTRY": ["SENTRY%"],
        "TRAVELERS": ["TRAVELERS%"],
    }
    if filters.get("insurance_company"):
        companies = filters["insurance_company"]
        if isinstance(companies, str):
            companies = companies.split(",")
        or_clauses = []
        for company in companies:
            company_upper = company.strip().upper()
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company_upper, [f"{company_upper}%"])
            for pattern in patterns:
                or_clauses.append(
                    f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
                    f"WHERE UPPER(elem->>'name_company') LIKE ${idx} "
                    f"AND (elem->>'cancl_effective_date' IS NULL OR elem->>'cancl_effective_date' = '' "
                    f"OR TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') >= CURRENT_DATE))"
                )
                params.append(pattern)
                idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # Renewal Policy Monthly filter
    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'effective_date' IS NOT NULL AND elem->>'effective_date' LIKE '%/%/%' "
            f"AND (elem->>'cancl_effective_date' IS NULL OR elem->>'cancl_effective_date' = '' "
            f"OR TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") BETWEEN CURRENT_DATE AND (DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
            f")"
        )
        params.append(months)
        idx += 1

    # Renewal Policy Date range filter
    if filters.get("renewal_date_from"):
        parts = filters["renewal_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'effective_date' IS NOT NULL AND elem->>'effective_date' LIKE '%/%/%' "
            f"AND (elem->>'cancl_effective_date' IS NULL OR elem->>'cancl_effective_date' = '' "
            f"OR TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") >= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("renewal_date_to"):
        parts = filters["renewal_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(insurance, '[]'::jsonb)) elem "
            f"WHERE elem->>'effective_date' IS NOT NULL AND elem->>'effective_date' LIKE '%/%/%' "
            f"AND (elem->>'cancl_effective_date' IS NULL OR elem->>'cancl_effective_date' = '' "
            f"OR TO_DATE(elem->>'cancl_effective_date', 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(elem->>'effective_date', 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") <= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_to_db_fmt)
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 500))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    # No JOINs needed — insurance is embedded in carriers table
    query = f"""
        SELECT {_CARRIER_SELECT_COLS}
        FROM carriers
        WHERE {where}
        ORDER BY dot_number ASC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt FROM carriers
        WHERE {where}
    """

    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        filtered_count = count_row["cnt"] if count_row else 0
        return {
            "data": [_carrier_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}


async def delete_carrier(dot_number: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE dot_number = $1", dot_number
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier DOT {dot_number}: {e}")
        return False


async def get_carrier_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM carriers")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting carrier count: {e}")
        return 0


async def get_carrier_dashboard_stats() -> dict:
    pool = get_pool()
    try:
        row = await pool.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status_code = 'A') AS active_carriers,
                COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                COUNT(*) FILTER (WHERE safety_rating IS NOT NULL AND safety_rating != '') AS with_safety_rating,
                COUNT(*) FILTER (WHERE insurance IS NOT NULL AND insurance != '[]'::jsonb AND insurance != 'null'::jsonb) AS with_insurance,
                COUNT(*) FILTER (WHERE hm_ind = 'Y') AS with_hazmat,
                COUNT(*) FILTER (WHERE status_code != 'A' OR status_code IS NULL) AS not_authorized
            FROM carriers
        """)
        if not row:
            return {
                "total": 0, "active_carriers": 0,
                "with_email": 0, "email_rate": "0",
                "with_safety_rating": 0, "with_insurance": 0,
                "with_hazmat": 0,
                "not_authorized": 0, "other": 0,
            }
        total = row["total"]
        active = row["active_carriers"]
        not_auth = row["not_authorized"]
        with_email = row["with_email"]
        email_rate = f"{(with_email / total * 100):.1f}" if total > 0 else "0"
        return {
            "total": total,
            "active_carriers": active,
            "with_email": with_email,
            "email_rate": email_rate,
            "with_safety_rating": row["with_safety_rating"],
            "with_insurance": row["with_insurance"],
            "with_hazmat": row["with_hazmat"],
            "not_authorized": not_auth,
            "other": total - active - not_auth,
        }
    except Exception as e:
        print(f"[DB] Error getting dashboard stats: {e}")
        return {
            "total": 0, "active_carriers": 0,
            "with_email": 0, "email_rate": "0",
            "with_safety_rating": 0, "with_insurance": 0,
            "with_hazmat": 0,
            "not_authorized": 0, "other": 0,
        }


async def get_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            f"""
            SELECT {_CARRIER_SELECT_COLS}
            FROM carriers
            WHERE temp >= $1::bigint AND temp <= $2::bigint
            ORDER BY temp ASC
            """,
            int(start),
            int(end),
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []


async def fetch_insurance_history(identifier: str) -> list[dict]:
    """Fetch insurance from embedded JSONB in carriers table.
    Accepts MC number or DOT number as identifier.
    """
    pool = get_pool()
    try:
        # Try to find by DOT number first, then by MC (temp or dockets)
        row = None
        if identifier.isdigit():
            row = await pool.fetchrow(
                "SELECT insurance FROM carriers WHERE dot_number = $1 LIMIT 1",
                identifier,
            )
            if not row:
                row = await pool.fetchrow(
                    "SELECT insurance FROM carriers WHERE temp = $1 LIMIT 1",
                    int(identifier),
                )
        else:
            row = await pool.fetchrow(
                "SELECT insurance FROM carriers WHERE dockets::text ILIKE $1 LIMIT 1",
                f"%{identifier}%",
            )

        if not row:
            return []
        return _format_embedded_insurance(row["insurance"])
    except Exception as e:
        print(f"[DB] Error fetching insurance history for {identifier}: {e}")
        return []


# ── User functions ───────────────────────────────────────────────────────────

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
        if row:
            return _user_row_to_dict(row)
        return None
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
        if row:
            return _user_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return None


async def update_user(user_id: str, user_data: dict) -> bool:
    pool = get_pool()
    _ALLOWED_COLUMNS = {
        "name", "role", "plan", "daily_limit",
        "records_extracted_today", "last_active",
        "ip_address", "is_online", "is_blocked",
    }
    columns = {k: v for k, v in user_data.items() if k in _ALLOWED_COLUMNS}
    if not columns:
        return False
    set_clauses = []
    values = []
    for idx, (col, val) in enumerate(columns.items(), start=1):
        set_clauses.append(f"{col} = ${idx}")
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
        result = await pool.execute(
            "DELETE FROM users WHERE user_id = $1", user_id
        )
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
        if row and row["password_hash"]:
            return row["password_hash"]
        return None
    except Exception as e:
        print(f"[DB] Error fetching password hash: {e}")
        return None


# ── Blocked IP functions ─────────────────────────────────────────────────────

async def fetch_blocked_ips() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT * FROM blocked_ips ORDER BY blocked_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching blocked IPs: {e}")
        return []


async def block_ip(ip_address: str, reason: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            """
            INSERT INTO blocked_ips (ip_address, reason)
            VALUES ($1, $2)
            ON CONFLICT (ip_address) DO NOTHING
            """,
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
        result = await pool.execute(
            "DELETE FROM blocked_ips WHERE ip_address = $1", ip_address
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error unblocking IP {ip_address}: {e}")
        return False


async def is_ip_blocked(ip_address: str) -> bool:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT ip_address FROM blocked_ips WHERE ip_address = $1",
            ip_address,
        )
        return row is not None
    except Exception as e:
        print(f"[DB] Error checking IP block status: {e}")
        return False


# ── New Ventures functions ───────────────────────────────────────────────────

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
    col_list = ", ".join(cols)
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))

    update_cols = [c for c in cols if c not in ("dot_number", "add_date")]
    on_conflict_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    on_conflict_set += ", updated_at = NOW()"

    insert_sql = f"""
        INSERT INTO new_ventures ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (dot_number, add_date) DO UPDATE SET {on_conflict_set}
    """

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
                        row_args = []
                        for col in _NV_COLUMNS:
                            val = entry.get(col)
                            row_args.append(val.strip() if isinstance(val, str) else val)
                        row_args.append(_to_jsonb(entry.get("raw_data")))
                        row_args.append(scrape_date)
                        args.append(tuple(row_args))
                    await conn.executemany(insert_sql, args)
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving new venture entries: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_new_ventures(filters: dict) -> list[dict]:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("docket_number"):
        conditions.append(f"docket_number ILIKE ${idx}")
        params.append(f"%{filters['docket_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("company_name"):
        conditions.append(f"(name ILIKE ${idx} OR name_dba ILIKE ${idx})")
        params.append(f"%{filters['company_name']}%")
        idx += 1

    if filters.get("date_from"):
        conditions.append(f"add_date >= ${idx}")
        params.append(filters["date_from"])
        idx += 1
    if filters.get("date_to"):
        conditions.append(f"add_date <= ${idx}")
        params.append(filters["date_to"])
        idx += 1

    active = filters.get("active")
    if active == "active":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "inactive":
        conditions.append(f"(operating_status ILIKE ${idx} OR operating_status IS NULL OR operating_status = '')")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "authorization_pending":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%PENDING%")
        idx += 1
    elif active == "not_authorized":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "true":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "false":
        conditions.append(f"operating_status NOT ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1

    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|") if s.strip()]
        if len(states) == 1:
            conditions.append(f"phy_st = ${idx}")
            params.append(states[0])
            idx += 1
        elif states:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(states)))
            conditions.append(f"phy_st IN ({placeholders})")
            for s in states:
                params.append(s)
                idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    if filters.get("carrier_operation"):
        conditions.append(f"carrier_operation ILIKE ${idx}")
        params.append(f"%{filters['carrier_operation']}%")
        idx += 1

    if filters.get("hazmat"):
        if filters["hazmat"] == "true":
            conditions.append("hm_ind = 'Y'")
        elif filters["hazmat"] == "false":
            conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(total_pwr, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(total_pwr, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    if filters.get("bipd_on_file"):
        if filters["bipd_on_file"] == "true":
            conditions.append("bipd_file = 'Y'")
        elif filters["bipd_on_file"] == "false":
            conditions.append("(bipd_file IS NULL OR bipd_file != 'Y')")
    if filters.get("cargo_on_file"):
        if filters["cargo_on_file"] == "true":
            conditions.append("cargo_file = 'Y'")
        elif filters["cargo_on_file"] == "false":
            conditions.append("(cargo_file IS NULL OR cargo_file != 'Y')")
    if filters.get("bond_on_file"):
        if filters["bond_on_file"] == "true":
            conditions.append("bond_file = 'Y'")
        elif filters["bond_on_file"] == "false":
            conditions.append("(bond_file IS NULL OR bond_file != 'Y')")

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 10000))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    query = f"""
        SELECT * FROM new_ventures
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt FROM new_ventures
        WHERE {where}
    """

    dates_query = "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"

    total_query = "SELECT COUNT(*) as cnt FROM new_ventures"

    try:
        rows = await pool.fetch(query, *params)
        count_row = await pool.fetchrow(count_query, *params)
        filtered_count = count_row["cnt"] if count_row else 0
        date_rows = await pool.fetch(dates_query)
        available_dates = [r["add_date"] for r in date_rows]
        total_row = await pool.fetchrow(total_query)
        total_count = total_row["cnt"] if total_row else 0
        return {
            "data": [_new_venture_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
            "total_count": total_count,
            "available_dates": available_dates,
        }
    except Exception as e:
        print(f"[DB] Error fetching new ventures: {e}")
        return {"data": [], "filtered_count": 0, "total_count": 0, "available_dates": []}


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
            "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"
        )
        return [row["add_date"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new venture dates: {e}")
        return []


async def fetch_new_venture_by_id(record_id: str) -> dict | None:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM new_ventures WHERE id = $1", record_id)
        if row:
            return _new_venture_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching new venture {record_id}: {e}")
        return None


async def delete_new_venture(record_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM new_ventures WHERE id = $1", record_id
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting new venture {record_id}: {e}")
        return False
