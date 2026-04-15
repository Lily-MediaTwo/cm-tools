import os
import time
from typing import Dict, List, Tuple, Any

import pandas as pd
from datetime import timezone
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build



# =========================
# CONFIG
# =========================
SERVICE_ACCOUNT_FILE = r'C:\Users\LilyDurbin\Documents\DataStudioReport\service_account.json'
DELEGATED_USER = os.getenv("DELEGATED_USER", "lily.johnson@mediatwo.net")
CM360_PROFILE_ID = os.getenv("CM360_PROFILE_ID", "")
ADVERTISER_ID = os.getenv("CM360_ADVERTISER_ID", "")
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_ID", "")
MASTER_TAB = "Creative_Master"
OPTIONS_TAB = "Creative_Field_Options"
CHANGELOG_TAB = "Creative_Change_Log"

SCOPES = [
    "https://www.googleapis.com/auth/dfatrafficking",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/dfareporting"
]
DISCOVERY_URL = "https://dfareporting.googleapis.com/$discovery/rest?version=v5"


# =========================
# AUTH
# =========================
def get_credentials() -> Credentials:
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )

    delegated_creds = creds.with_subject(DELEGATED_USER)
    delegated_creds.refresh(Request())
    return delegated_creds


def get_cm360_service(creds: Credentials):
    return build("dfareporting", "v5", credentials=creds, discoveryServiceUrl=DISCOVERY_URL ,cache_discovery=False)


def get_gspread_client(creds: Credentials) -> gspread.Client:
    return gspread.authorize(creds)


# =========================
# HELPERS
# =========================
def paged_list(request_builder):
    """Yield items across nextPageToken pagination."""
    page_token = None
    while True:
        response = request_builder(page_token).execute()
        yield response
        page_token = response.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.1)


def safe_sheet_title(name: str) -> str:
    bad = ["/", "\\", "?", "*", "[", "]"]
    for char in bad:
        name = name.replace(char, "_")
    return name[:100]


# =========================
# CM360 PULLS
# =========================
def get_creative_fields(service, profile_id: str, advertiser_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    def build_request(page_token=None):
        return service.creativeFields().list(
            profileId=profile_id,
            advertiserIds=[advertiser_id],
            pageToken=page_token,
            maxResults=200,
            sortField="NAME",
            sortOrder="ASCENDING",
        )

    for page in paged_list(build_request):
        items.extend(page.get("creativeFields", []))

    return items


def get_creative_field_values(service, profile_id: str, creative_field_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    def build_request(page_token=None):
        return service.creativeFieldValues().list(
            profileId=profile_id,
            creativeFieldId=creative_field_id,
            pageToken=page_token,
            maxResults=200,
            sortField="VALUE",
            sortOrder="ASCENDING",
        )

    for page in paged_list(build_request):
        items.extend(page.get("creativeFieldValues", []))

    return items


def get_active_creatives(service, profile_id: str, advertiser_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    def build_request(page_token=None):
        return service.creatives().list(
            profileId=profile_id,
            advertiserId=advertiser_id,
            archived=False,
            pageToken=page_token,
            maxResults=200,
            sortField="ID",
            sortOrder="ASCENDING",
        )

    for page in paged_list(build_request):
        items.extend(page.get("creatives", []))

    return items


# =========================
# TRANSFORMS
# =========================
def build_lookup_tables(
    creative_fields: List[Dict[str, Any]],
    creative_field_values_by_field: Dict[str, List[Dict[str, Any]]],
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, Dict[str, str]]]:
    """
    Returns:
      - field_id_to_name
      - field_name_to_id
      - value_id_to_value_by_field_id
    """
    field_id_to_name: Dict[str, str] = {}
    field_name_to_id: Dict[str, str] = {}
    value_id_to_value_by_field_id: Dict[str, Dict[str, str]] = {}

    for field in creative_fields:
        field_id = str(field["id"])
        field_name = field["name"].strip()
        field_id_to_name[field_id] = field_name
        field_name_to_id[field_name] = field_id

        value_map: Dict[str, str] = {}
        for value in creative_field_values_by_field.get(field_id, []):
            value_map[str(value["id"])] = value.get("value", "")
        value_id_to_value_by_field_id[field_id] = value_map

    return field_id_to_name, field_name_to_id, value_id_to_value_by_field_id


def flatten_creatives(
    creatives: List[Dict[str, Any]],
    field_id_to_name: Dict[str, str],
    value_id_to_value_by_field_id: Dict[str, Dict[str, str]],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    field_names = sorted(field_id_to_name.values())

    for creative in creatives:
        raw_last_modified = creative.get("lastModifiedInfo", {}).get("time", "")
        parsed_last_modified = ""
        if raw_last_modified not in (None, ""):
            try:
                parsed_last_modified = pd.to_datetime(
                    int(raw_last_modified),
                    unit="ms",
                    utc=True,
                ).tz_convert("America/New_York").strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                parsed_last_modified = str(raw_last_modified)

        row: Dict[str, Any] = {
            "Creative ID": str(creative.get("id", "")),
            "Creative Name": creative.get("name", ""),
            "Type": creative.get("type", ""),
            "Active": not creative.get("archived", False),
            "Last Modified": parsed_last_modified,
            "Last Modified Raw": raw_last_modified,
            "Needs Update": "",
            "Update Status": "",
            "Last Synced": pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d %H:%M:%S"),
        }

        for field_name in field_names:
            row[field_name] = ""

        assignments = creative.get("creativeFieldAssignments", []) or []
        for assignment in assignments:
            field_id = str(assignment.get("creativeFieldId", ""))
            value_id = str(assignment.get("creativeFieldValueId", ""))
            field_name = field_id_to_name.get(field_id)
            if not field_name:
                continue
            row[field_name] = value_id_to_value_by_field_id.get(field_id, {}).get(value_id, "")

        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["Last Modified Sort"] = pd.to_numeric(df["Last Modified Raw"], errors="coerce")
        df = df.sort_values(by="Last Modified Sort", ascending=False, na_position="last").reset_index(drop=True)

        fixed_cols = [
            "Creative ID",
            "Creative Name",
            "Type",
            "Active",
            "Last Modified",
            "Needs Update",
            "Update Status",
            "Last Synced",
        ]
        other_cols = [c for c in df.columns if c not in fixed_cols + ["Last Modified Raw", "Last Modified Sort"]]
        df = df[fixed_cols + other_cols]

    return df


def build_options_tab_df(
    creative_fields: List[Dict[str, Any]],
    creative_field_values_by_field: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    """
    Wide format for easier sheet references:
    Row 1: creative field names as headers
    Rows below: allowed values for each field
    """
    max_len = 0
    data: Dict[str, List[str]] = {}

    for field in sorted(creative_fields, key=lambda x: x["name"].lower()):
        field_id = str(field["id"])
        field_name = field["name"].strip()
        values = [v.get("value", "") for v in creative_field_values_by_field.get(field_id, [])]
        data[field_name] = values
        max_len = max(max_len, len(values))

    for field_name, values in data.items():
        if len(values) < max_len:
            data[field_name] = values + [""] * (max_len - len(values))

    return pd.DataFrame(data)


# =========================
# SHEETS WRITE
# =========================
def ensure_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 50):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=safe_sheet_title(title), rows=rows, cols=cols)


def clear_and_write_df(worksheet, df: pd.DataFrame):
    worksheet.clear()
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    worksheet.update("A1", values)


def format_tabs(master_ws, options_ws):
    master_ws.freeze(rows=1)
    master_ws.freeze(cols=2)
    options_ws.freeze(rows=1)


# =========================
# CONTROL + PUSH MODE
# =========================
def ensure_control_tab(spreadsheet):
    ws = ensure_worksheet(spreadsheet, "Control", rows=50, cols=10)
    existing = ws.get("A1:B20")
    if not existing:
        values = [
            ["Setting", "Value"],
            ["Run Mode", "SYNC"],
            ["Push Scope", "FLAGGED_ONLY"],
            ["Ready To Push", "NO"],
            ["Last Run Type", ""],
            ["Last Run Timestamp", ""],
            ["Last Run Result", ""],
            ["Last Run Message", ""],
        ]
        ws.update("A1", values)
        ws.freeze(rows=1)
    return ws


def read_control_settings(control_ws) -> Dict[str, str]:
    values = control_ws.get("A2:B20")
    settings: Dict[str, str] = {}
    for row in values:
        if len(row) >= 2 and row[0]:
            settings[str(row[0]).strip()] = str(row[1]).strip()
    return settings


def write_control_status(control_ws, run_type: str, result: str, message: str):
    ts = pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d %H:%M:%S")
    updates = {
        "B5": run_type,
        "B6": ts,
        "B7": result,
        "B8": message,
    }
    for cell, value in updates.items():
        control_ws.update(cell, [[value]])


def ensure_changelog_tab(spreadsheet):
    ws = ensure_worksheet(spreadsheet, CHANGELOG_TAB, rows=1000, cols=20)
    if not ws.get("A1:J2"):
        ws.update("A1", [[
            "Timestamp",
            "Creative ID",
            "Creative Name",
            "Field Name",
            "Old Value",
            "New Value",
            "Action",
            "Result",
            "Error Message",
            "Run User",
        ]])
        ws.freeze(rows=1)
    return ws


def append_changelog_rows(changelog_ws, rows: List[List[str]]):
    if rows:
        changelog_ws.append_rows(rows, value_input_option="USER_ENTERED")


def read_sheet_as_df(worksheet) -> pd.DataFrame:
    values = worksheet.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    data = values[1:]
    return pd.DataFrame(data, columns=header)


def get_editable_field_names(master_df: pd.DataFrame) -> List[str]:
    system_cols = {
        "Creative ID",
        "Creative Name",
        "Type",
        "Active",
        "Last Modified",
        "Needs Update",
        "Update Status",
        "Last Synced",
    }
    return [c for c in master_df.columns if c not in system_cols]


def build_value_lookup_maps(
    creative_fields: List[Dict[str, Any]],
    creative_field_values_by_field: Dict[str, List[Dict[str, Any]]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    field_name_to_id: Dict[str, str] = {}
    value_to_id_by_field_id: Dict[str, Dict[str, str]] = {}

    for field in creative_fields:
        field_id = str(field["id"])
        field_name = str(field["name"]).strip()
        field_name_to_id[field_name] = field_id
        value_to_id_by_field_id[field_id] = {
            str(v.get("value", "")).strip(): str(v["id"])
            for v in creative_field_values_by_field.get(field_id, [])
        }

    return field_name_to_id, value_to_id_by_field_id


def build_current_assignment_map(
    creative: Dict[str, Any],
    field_id_to_name: Dict[str, str],
    value_id_to_value_by_field_id: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    current: Dict[str, str] = {}
    for assignment in creative.get("creativeFieldAssignments", []) or []:
        field_id = str(assignment.get("creativeFieldId", ""))
        value_id = str(assignment.get("creativeFieldValueId", ""))
        field_name = field_id_to_name.get(field_id)
        if field_name:
            current[field_name] = value_id_to_value_by_field_id.get(field_id, {}).get(value_id, "")
    return current


def build_assignments_for_row(
    row: pd.Series,
    editable_fields: List[str],
    field_name_to_id: Dict[str, str],
    value_to_id_by_field_id: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    assignments: List[Dict[str, str]] = []

    for field_name in editable_fields:
        field_id = field_name_to_id.get(field_name)
        if not field_id:
            continue

        raw_value = str(row.get(field_name, "")).strip()
        if not raw_value:
            continue

        value_id = value_to_id_by_field_id.get(field_id, {}).get(raw_value)
        if not value_id:
            raise ValueError(f"Invalid value '{raw_value}' for creative field '{field_name}'")

        assignments.append({
            "creativeFieldId": field_id,
            "creativeFieldValueId": value_id,
        })

    return assignments


def diff_row_vs_current(
    row: pd.Series,
    current_values: Dict[str, str],
    editable_fields: List[str],
) -> List[Tuple[str, str, str, str]]:
    diffs: List[Tuple[str, str, str, str]] = []
    for field_name in editable_fields:
        old_value = str(current_values.get(field_name, "")).strip()
        new_value = str(row.get(field_name, "")).strip()
        if old_value != new_value:
            action = "REMOVE" if new_value == "" else ("ADD" if old_value == "" else "UPDATE")
            diffs.append((field_name, old_value, new_value, action))
    return diffs


def patch_creative_assignments(service, profile_id: str, creative_id: str, assignments: List[Dict[str, str]]):
    body = {"creativeFieldAssignments": assignments}
    return service.creatives().patch(
        profileId=profile_id,
        id=creative_id,
        body=body,
    ).execute()


def run_push_mode(
    service,
    spreadsheet,
    control_ws,
    creative_fields: List[Dict[str, Any]],
    creative_field_values_by_field: Dict[str, List[Dict[str, Any]]],
    creatives: List[Dict[str, Any]],
    field_id_to_name: Dict[str, str],
    value_id_to_value_by_field_id: Dict[str, Dict[str, str]],
):
    master_ws = spreadsheet.worksheet(MASTER_TAB)
    changelog_ws = ensure_changelog_tab(spreadsheet)
    master_df = read_sheet_as_df(master_ws)

    if master_df.empty:
        raise ValueError("Creative_Master is empty. Run SYNC first.")

    editable_fields = get_editable_field_names(master_df)
    field_name_to_id, value_to_id_by_field_id = build_value_lookup_maps(
        creative_fields,
        creative_field_values_by_field,
    )
    creative_map = {str(c["id"]): c for c in creatives}

    flagged = master_df[master_df["Needs Update"].astype(str).str.strip().str.upper() == "Y"].copy()
    if flagged.empty:
        write_control_status(control_ws, "PUSH", "SUCCESS", "No flagged rows found.")
        return

    changelog_rows: List[List[str]] = []
    timestamp = pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d %H:%M:%S")

    for idx, row in flagged.iterrows():
        creative_id = str(row.get("Creative ID", "")).strip()
        creative_name = str(row.get("Creative Name", "")).strip()

        try:
            creative = creative_map.get(creative_id)
            if not creative:
                raise ValueError(f"Creative ID {creative_id} not found in CM360 active creative pull")

            current_values = build_current_assignment_map(
                creative,
                field_id_to_name,
                value_id_to_value_by_field_id,
            )
            diffs = diff_row_vs_current(row, current_values, editable_fields)

            if not diffs:
                master_df.at[idx, "Update Status"] = "No changes"
                master_df.at[idx, "Last Synced"] = timestamp
                master_df.at[idx, "Needs Update"] = ""
                continue

            assignments = build_assignments_for_row(
                row,
                editable_fields,
                field_name_to_id,
                value_to_id_by_field_id,
            )
            patch_creative_assignments(service, CM360_PROFILE_ID, creative_id, assignments)

            master_df.at[idx, "Update Status"] = "Updated"
            master_df.at[idx, "Last Synced"] = timestamp
            master_df.at[idx, "Needs Update"] = ""

            for field_name, old_value, new_value, action in diffs:
                changelog_rows.append([
                    timestamp,
                    creative_id,
                    creative_name,
                    field_name,
                    old_value,
                    new_value,
                    action,
                    "SUCCESS",
                    "",
                    DELEGATED_USER,
                ])

        except Exception as exc:
            master_df.at[idx, "Update Status"] = f"Error: {str(exc)[:200]}"
            master_df.at[idx, "Last Synced"] = timestamp
            for field_name, old_value, new_value, action in diff_row_vs_current(
                row,
                build_current_assignment_map(
                    creative_map.get(creative_id, {}),
                    field_id_to_name,
                    value_id_to_value_by_field_id,
                ) if creative_id in creative_map else {},
                editable_fields,
            ):
                changelog_rows.append([
                    timestamp,
                    creative_id,
                    creative_name,
                    field_name,
                    old_value,
                    new_value,
                    action,
                    "ERROR",
                    str(exc)[:500],
                    DELEGATED_USER,
                ])

    clear_and_write_df(master_ws, master_df)
    append_changelog_rows(changelog_ws, changelog_rows)
    control_ws.update("B3", [["FLAGGED_ONLY"]])
    control_ws.update("B4", [["NO"]])
    write_control_status(control_ws, "PUSH", "SUCCESS", f"Processed {len(flagged)} flagged row(s).")


# =========================
# MAIN
# =========================
def main():
    if not DELEGATED_USER:
        raise ValueError("Missing DELEGATED_USER")
    if not CM360_PROFILE_ID:
        raise ValueError("Missing CM360_PROFILE_ID")
    if not ADVERTISER_ID:
        raise ValueError("Missing CM360_ADVERTISER_ID")
    if not SPREADSHEET_ID:
        raise ValueError("Missing GOOGLE_SHEETS_ID")

    creds = get_credentials()
    cm_service = get_cm360_service(creds)
    gs_client = get_gspread_client(creds)
    spreadsheet = gs_client.open_by_key(SPREADSHEET_ID)
    control_ws = ensure_control_tab(spreadsheet)
    ensure_changelog_tab(spreadsheet)

    settings = read_control_settings(control_ws)
    run_mode = settings.get("Run Mode", "SYNC").strip().upper()
    ready_to_push = settings.get("Ready To Push", "NO").strip().upper()

    advertiser_id = settings.get("Advertiser ID", "").strip() or ADVERTISER_ID

    print("Pulling creative fields...")
    creative_fields = get_creative_fields(cm_service, CM360_PROFILE_ID, advertiser_id)

    print("Pulling creative field values...")
    creative_field_values_by_field: Dict[str, List[Dict[str, Any]]] = {}
    for field in creative_fields:
        field_id = str(field["id"])
        creative_field_values_by_field[field_id] = get_creative_field_values(
            cm_service,
            CM360_PROFILE_ID,
            field_id,
        )

    print("Pulling active creatives...")
    creatives = get_active_creatives(cm_service, CM360_PROFILE_ID, advertiser_id)

    field_id_to_name, _, value_id_to_value_by_field_id = build_lookup_tables(
        creative_fields,
        creative_field_values_by_field,
    )

    if run_mode == "PUSH":
        if ready_to_push != "YES":
            write_control_status(control_ws, "PUSH", "ERROR", "Ready To Push must be YES before PUSH mode can run.")
            raise ValueError("Ready To Push must be YES before PUSH mode can run.")

        run_push_mode(
            cm_service,
            spreadsheet,
            control_ws,
            creative_fields,
            creative_field_values_by_field,
            creatives,
            field_id_to_name,
            value_id_to_value_by_field_id,
        )
        print("Push completed.")
        return

    master_df = flatten_creatives(
        creatives,
        field_id_to_name,
        value_id_to_value_by_field_id,
    )
    options_df = build_options_tab_df(
        creative_fields,
        creative_field_values_by_field,
    )

    print("Writing to Google Sheets...")
    master_ws = ensure_worksheet(spreadsheet, MASTER_TAB, rows=max(len(master_df) + 10, 1000), cols=max(len(master_df.columns) + 5, 20))
    options_ws = ensure_worksheet(spreadsheet, OPTIONS_TAB, rows=max(len(options_df) + 10, 1000), cols=max(len(options_df.columns) + 5, 20))

    clear_and_write_df(master_ws, master_df)
    clear_and_write_df(options_ws, options_df)
    format_tabs(master_ws, options_ws)
    write_control_status(control_ws, "SYNC", "SUCCESS", f"Wrote {len(master_df)} creatives and {len(options_df.columns)} creative fields.")

    print(f"Done. Wrote {len(master_df):,} creatives and {len(options_df.columns):,} creative fields.")

if __name__ == "__main__":
    main()


