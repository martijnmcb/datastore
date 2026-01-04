import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from decimal import Decimal
from extensions import db
from models import ConnectionProfile, RGRit

# Column definitions for safe casting in the remote SELECT
ALL_COLUMNS = [
    "rittype",
    "ritnummer",
    "schema",
    "status",
    "weekdag",
    "aankomst",
    "vertrek",
    "voornaam",
    "voorletters",
    "voorvoegsels",
    "achternaam",
    "straat",
    "huisnummer",
    "postcode",
    "woonplaats",
    "telefoonnummer",
    "faxnummer",
    "mobiel",
    "emailadres",
    "geslacht",
    "geboortedatum",
    "co_bijzonderheden",
    "huisnummer_toev",
    "land",
    "afdeling",
    "kamernummer",
    "cor_straat",
    "cor_huisnummer",
    "cor_huisnummer_toev",
    "cor_postcode",
    "cor_woonplaats",
    "cor_land",
    "cor_afdeling",
    "cor_kamernummer",
    "co_klantnummer",
    "emailadres2",
    "emailadres3",
    "klantnummer2",
    "tp_id",
    "tp_byzonderheden",
    "pasnummer",
    "ind_rolstoel",
    "rolstoel_type",
    "ind_begeleiding_sociaal",
    "ind_begeleiding_medisch",
    "ind_beperking_lichamelijk",
    "ind_beperking_verstandelijk",
    "ind_hulphond",
    "ind_voorin_in_taxi",
    "ind_kamer_tot_kamer_vervoer",
    "ind_gezinstaxi",
    "ind_personenauto",
    "ind_lage_instap",
    "ind_voldoende_beenruimte",
    "ind_rollator",
    "ind_beperking_visueel",
    "ind_beperking_auditief",
    "ind_autisme",
    "ind_individueel_vervoer",
    "beschikkingnummer",
    "vervoer_type",
    "vervoer_type_omschrijving",
    "rolstoel_type_omschrijving",
    "aantal_gezinstaxi",
    "aantal_begeleiding_sociaal",
    "aantal_begeleiding_medisch",
    "ind_scootmobiel",
    "ind_strippen",
    "aantal_strippen",
    "ind_voldoende_zithoogte",
    "ind_afwijkend_tarief",
    "ind_zithulp",
    "zithulp_type",
    "financiering_type",
    "ind_belservice",
    "ind_epilepsie",
    "ind_geen_warme_overdracht",
    "extra_instap_heen",
    "extra_instap_terug",
    "extra_uitstap_heen",
    "extra_uitstap_terug",
    "ind_kleinschalig",
    "max_reistijd",
    "ind_extra1",
    "ind_extra2",
    "ind_extra3",
    "ind_extra4",
    "ind_extra5",
    "ind_busbegeleider_nodig",
    "ind_is_busbegeleider",
    "ind_inclusief_begeleider",
    "ind_alleen_begeleider",
    "ind_invloed_op_factuur",
    "ind_client_mag_rit_wijzigen",
    "ind_ontheffing_mondkapje",
    "ind_ontheffing_gordelplicht",
    "ind_bus",
    "ind_mag_vraagafhankelijk",
    "budget_standaard",
    "budget_huidig",
    "ind_alleen_zitten",
    "ind_diabetes",
    "ind_deur_deur",
    "ind_anderstalig",
    "ind_opvouwbare_rolstoel",
    "ind_kinderstoel",
    "ind_stoelverhoger",
    "ind_gordelkapje",
    "ind_meerpuntsgordel",
    "ind_maxicosi",
    "ind_begeleiding_niet_verplicht",
    "ind_afasie",
    "ind_vaste_zitplaats",
    "ind_gordelverlenging",
    "ind_lifo",
    "ind_filo",
    "ind_fifo",
    "aantal_kleinschalig",
    "minimaal_scootmobiel_reisafstand",
    "minimale_reisafstand",
    "ind_vervroegde_belservice",
    "ind_opstappunt_verplicht",
    "ind_bagage",
    "type_bagage",
    "ind_niet_combineren",
    "niet_combineren_met",
    "locatie_van",
    "straat_van",
    "huisnummer_van",
    "huisnummer_toev_van",
    "postcode_van",
    "plaats_van",
    "locatie_naar",
    "straat_naar",
    "huisnummer_naar",
    "huisnummer_toev_naar",
    "postcode_naar",
    "plaats_naar",
    "weekdag_id",
    "owner_id",
    "carrier_id",
    "vervoerder",
    "opdrachtgever",
    "effective_date",
    "ritdatum",
    "routenummer",
    "vervoerder_routenummer",
    "instap",
    "uitstap",
    "afstand",
    "duur",
    "afstand2",
    "duur2",
    "zoneafstand1",
    "zoneafstand2",
    "perceel_id",
    "perceel_omschrijving",
    "gemeld_op",
    "afwezig_van",
    "afwezig_totmet",
    "attendance_reason",
    "tekst",
    "instelling_gemeld_op",
    "instelling_afwezig_van",
    "instelling_afwezig_totmet",
    "instelling_reden",
    "instelling_tekst",
    "vervangt_plannedtransport_id",
    "vervangt_datum",
    "realisatie_route",
    "instapgerealiseerd",
    "instaplatitude",
    "instaplongitude",
    "uitstapgerealiseerd",
    "uitstaplatitude",
    "uitstaplongitude",
    "loosmeldinggerealiseerd",
    "loosmeldinglatitude",
    "loosmeldinglongitude",
]

NUMERIC_COLS = {
    "tp_id",
    "ind_rolstoel",
    "rolstoel_type",
    "ind_begeleiding_sociaal",
    "ind_begeleiding_medisch",
    "ind_beperking_lichamelijk",
    "ind_beperking_verstandelijk",
    "ind_hulphond",
    "ind_voorin_in_taxi",
    "ind_kamer_tot_kamer_vervoer",
    "ind_gezinstaxi",
    "ind_personenauto",
    "ind_lage_instap",
    "ind_voldoende_beenruimte",
    "ind_rollator",
    "ind_beperking_visueel",
    "ind_beperking_auditief",
    "ind_autisme",
    "ind_individueel_vervoer",
    "vervoer_type",
    "aantal_gezinstaxi",
    "aantal_begeleiding_sociaal",
    "aantal_begeleiding_medisch",
    "ind_scootmobiel",
    "ind_strippen",
    "aantal_strippen",
    "ind_voldoende_zithoogte",
    "ind_afwijkend_tarief",
    "ind_zithulp",
    "zithulp_type",
    "financiering_type",
    "ind_belservice",
    "ind_epilepsie",
    "ind_geen_warme_overdracht",
    "extra_instap_heen",
    "extra_instap_terug",
    "extra_uitstap_heen",
    "extra_uitstap_terug",
    "ind_kleinschalig",
    "max_reistijd",
    "ind_extra1",
    "ind_extra2",
    "ind_extra3",
    "ind_extra4",
    "ind_extra5",
    "ind_busbegeleider_nodig",
    "ind_is_busbegeleider",
    "ind_inclusief_begeleider",
    "ind_alleen_begeleider",
    "ind_invloed_op_factuur",
    "ind_client_mag_rit_wijzigen",
    "ind_ontheffing_mondkapje",
    "ind_ontheffing_gordelplicht",
    "ind_bus",
    "ind_mag_vraagafhankelijk",
    "ind_alleen_zitten",
    "ind_diabetes",
    "ind_deur_deur",
    "ind_anderstalig",
    "ind_opvouwbare_rolstoel",
    "ind_kinderstoel",
    "ind_stoelverhoger",
    "ind_gordelkapje",
    "ind_meerpuntsgordel",
    "ind_maxicosi",
    "ind_begeleiding_niet_verplicht",
    "ind_afasie",
    "ind_vaste_zitplaats",
    "ind_gordelverlenging",
    "ind_lifo",
    "ind_filo",
    "ind_fifo",
    "aantal_kleinschalig",
    "minimaal_scootmobiel_reisafstand",
    "minimale_reisafstand",
    "ind_vervroegde_belservice",
    "ind_opstappunt_verplicht",
    "ind_bagage",
    "weekdag_id",
    "owner_id",
    "carrier_id",
    "afstand",
    "afstand2",
    "zoneafstand1",
    "zoneafstand2",
    "perceel_id",
    "vervangt_plannedtransport_id",
    "instaplatitude",
    "instaplongitude",
    "uitstaplatitude",
    "uitstaplongitude",
    "loosmeldinglatitude",
    "loosmeldinglongitude",
}

DATETIME_COLS = {
    "geboortedatum",
    "effective_date",
    "ritdatum",
    "gemeld_op",
    "afwezig_van",
    "afwezig_totmet",
    "instelling_gemeld_op",
    "instelling_afwezig_van",
    "instelling_afwezig_totmet",
    "vervangt_datum",
}

TIME_COLS = {
    "instap",
    "uitstap",
    "duur",
    "duur2",
    "instapgerealiseerd",
    "uitstapgerealiseerd",
    "loosmeldinggerealiseerd",
}

DECIMAL_COLS = {
    "afstand",
    "afstand2",
    "instaplatitude",
    "instaplongitude",
    "uitstaplatitude",
    "uitstaplongitude",
    "loosmeldinglatitude",
    "loosmeldinglongitude",
}


def _build_select(columns, min_ritdatum=None, last_date=None, last_ritnummer=None, limit=None):
    rit_expr = "TRY_CONVERT(bigint, [ritnummer])"
    date_expr = "TRY_CONVERT(date, [ritdatum])"
    select_parts = []
    for col in columns:
        if col in DATETIME_COLS:
            select_parts.append(f"TRY_CONVERT(datetime, [{col}]) AS [{col}]")
        elif col in TIME_COLS:
            select_parts.append(f"TRY_CONVERT(time, [{col}]) AS [{col}]")
        elif col in DECIMAL_COLS:
            select_parts.append(f"TRY_CONVERT(decimal(38, 10), [{col}]) AS [{col}]")
        elif col in NUMERIC_COLS:
            select_parts.append(f"TRY_CONVERT(decimal(38, 10), [{col}]) AS [{col}]")
        elif col == "ritnummer":
            select_parts.append(f"{rit_expr} AS [ritnummer]")
        else:
            select_parts.append(f"[{col}]")

    filters = [f"{rit_expr} IS NOT NULL"]
    if min_ritdatum:
        filters.append(f"{date_expr} >= :min_ritdatum")
    if last_date is not None:
        filters.append(
            f"(({date_expr} > :last_date) OR "
            f"({date_expr} = :last_date AND {rit_expr} > :last_ritnummer))"
        )

    top_clause = f"TOP {limit} " if limit else ""
    select_sql = (
        f"SELECT {top_clause}\n       " + ",\n       ".join(select_parts) + "\n"
        "FROM rpt.RGRitten\n"
        "WHERE " + " AND ".join(filters) + "\n"
        f"ORDER BY {date_expr}, {rit_expr}"
    )
    return select_sql


def sync_rgritten(
    profile_name: str = "Historie",
    chunk_size: int = 1000,
    min_ritdatum: str | None = None,
):
    """
    Append-only sync from SQL Server view rpt.RGRitten into local table rgritten.
    Uses ritnummer as the incremental cursor.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")

    profile = (
        db.session.query(ConnectionProfile)
        .filter(ConnectionProfile.name == profile_name)
        .first()
    )
    if not profile:
        raise ValueError(f"ConnectionProfile '{profile_name}' not found")

    last_row = (
        db.session.query(RGRit.ritdatum, RGRit.ritnummer)
        .order_by(RGRit.ritdatum.desc(), RGRit.ritnummer.desc())
        .first()
    )
    last_date = last_row.ritdatum.date().isoformat() if last_row and last_row.ritdatum else None
    last_ritnummer = last_row.ritnummer if last_row and last_row.ritnummer else 0

    select_sql = _build_select(
        ALL_COLUMNS,
        min_ritdatum=min_ritdatum,
        last_date=last_date,
        last_ritnummer=last_ritnummer,
        limit=None,
    )

    engine = sa.create_engine(profile.build_uri())
    stmt = sa.text(select_sql)

    inserted = 0
    max_ritnummer = last_ritnummer
    max_date = last_date

    try:
        with engine.connect() as remote:
            result = (
                remote.execution_options(stream_results=True).execute(
                    stmt,
                    {
                        "last_ritnummer": last_ritnummer,
                        "last_date": last_date,
                        "min_ritdatum": min_ritdatum,
                    },
                )
            )
            columns = result.keys()
            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break
                payload = []
                for row in rows:
                    mapping = dict(zip(columns, row))
                    # SQLite driver doesn't accept Decimal directly; cast numerics to float
                    for key, val in mapping.items():
                        if isinstance(val, Decimal):
                            mapping[key] = float(val)
                    if mapping.get("ritdatum"):
                        dval = (
                            mapping["ritdatum"].date().isoformat()
                            if hasattr(mapping["ritdatum"], "date")
                            else mapping["ritdatum"]
                        )
                        if dval:
                            max_date = dval if max_date is None else max(max_date, dval)
                    if mapping.get("ritnummer") is not None:
                        max_ritnummer = max(max_ritnummer, mapping["ritnummer"])
                    payload.append(mapping)
                if payload:
                    db.session.bulk_insert_mappings(RGRit, payload)
                    db.session.commit()
                    inserted += len(payload)
    except SQLAlchemyError:
        db.session.rollback()
        raise

    return {
        "profile": profile_name,
        "inserted": inserted,
        "from_ritdatum": last_date,
        "from_ritnummer": last_ritnummer,
        "through_ritdatum": max_date,
        "through_ritnummer": max_ritnummer,
    }


def locate_offending_columns(profile_name: str = "Historie", block_size: int = 8):
    """
    Quickly find columns that cause 'varchar to float' errors by probing in coarse blocks,
    then drilling down only inside failing blocks. Returns a list of offending columns.
    """
    profile = (
        db.session.query(ConnectionProfile)
        .filter(ConnectionProfile.name == profile_name)
        .first()
    )
    if not profile:
        raise ValueError(f"ConnectionProfile '{profile_name}' not found")

    engine = sa.create_engine(profile.build_uri())

    def probe(cols, top_only=True):
        sql = _build_select(cols, limit=1 if top_only else 10)
        try:
            with engine.connect() as remote:
                remote.execute(sa.text(sql)).fetchmany(1)
            return True
        except SQLAlchemyError:
            return False

    bad = []

    cols = list(ALL_COLUMNS)
    # Quick check: if all columns are fine, exit early.
    if probe(cols):
        return bad

    # Coarse pass: slice into blocks and probe each block.
    blocks = [cols[i : i + block_size] for i in range(0, len(cols), block_size)]
    suspect_blocks = []
    for block in blocks:
        if not probe(block):
            suspect_blocks.append(block)

    # Drill down: check each column inside failing blocks.
    for block in suspect_blocks:
        for col in block:
            if not probe([col], top_only=True):
                bad.append(col)

    return bad


def find_conversion_issues(
    profile_name: str = "Historie",
    cursor: int = 0,
    min_ritdatum: str | None = None,
):
    """
    Detect rows that fail numeric/date conversion by probing suspect columns with TRY_CONVERT.
    Returns a list of (column, ritnummer) where data is not convertible.
    """
    profile = (
        db.session.query(ConnectionProfile)
        .filter(ConnectionProfile.name == profile_name)
        .first()
    )
    if not profile:
        raise ValueError(f"ConnectionProfile '{profile_name}' not found")

    engine = sa.create_engine(profile.build_uri())
    problems = []
    rit_expr = "TRY_CONVERT(bigint, ritnummer)"
    with engine.connect() as remote:
        for col in sorted(NUMERIC_COLS.union(DECIMAL_COLS)):
            sql = sa.text(
                f"""
                SELECT TOP 5 ritnummer, {col}
                FROM rpt.RGRitten
                WHERE {rit_expr} IS NOT NULL
                  AND {rit_expr} > :cursor
                  {"" if not min_ritdatum else "AND TRY_CONVERT(date, ritdatum) >= :min_ritdatum"}
                  AND {col} IS NOT NULL
                  AND TRY_CONVERT(float, {col}) IS NULL
                ORDER BY {rit_expr}
                """
            )
            params = {"cursor": cursor}
            if min_ritdatum:
                params["min_ritdatum"] = min_ritdatum
            rows = remote.execute(sql, params).fetchall()
            for row in rows:
                problems.append({"column": col, "ritnummer": row.ritnummer, "value": row[col]})
    return problems
