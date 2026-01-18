from datetime import datetime
from passlib.hash import pbkdf2_sha256
from flask_login import UserMixin
from extensions import db

# Tabel associatie User <-> Role (many-to-many)
user_roles = db.Table(
    "user_roles",
    db.Column("user_id", db.Integer, db.ForeignKey("users.id")),
    db.Column("role_id", db.Integer, db.ForeignKey("roles.id")),
)

class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(120), nullable=False)
    last_name  = db.Column(db.String(120), nullable=False)
    email      = db.Column(db.String(255), unique=True, index=True)
    phone      = db.Column(db.String(50))
    username   = db.Column(db.String(120), unique=True, index=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_active  = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    roles = db.relationship("Role", secondary=user_roles, back_populates="users")
    project_links = db.relationship(
        "UserProject",
        cascade="all, delete-orphan",
        back_populates="user",
    )

    def set_password(self, raw):
        if raw is None:
            raise ValueError("Wachtwoord ontbreekt")
        try:
            raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("Ongeldig wachtwoord") from exc
        self.password_hash = pbkdf2_sha256.hash(raw)

    def check_password(self, raw):
        if raw is None:
            return False
        try:
            raw.encode("utf-8")
        except UnicodeEncodeError:
            return False
        try:
            return pbkdf2_sha256.verify(raw, self.password_hash)
        except ValueError:
            return False

    @property
    def project_names(self):
        return [link.project for link in self.project_links]

class Role(db.Model):
    __tablename__ = "roles"
    id   = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)  # Beheerder, Gebruiker, Lezer
    users = db.relationship("User", secondary=user_roles, back_populates="roles")


class UserProject(db.Model):
    __tablename__ = "user_projects"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    project = db.Column(db.String(120), nullable=False)

    user = db.relationship("User", back_populates="project_links")

    __table_args__ = (
        db.UniqueConstraint("user_id", "project", name="uq_user_project_membership"),
    )

# Opslag van SQL Server connectieparameters (via UI te beheren)
class ConnectionSetting(db.Model):
    __tablename__ = "connection_settings"
    id       = db.Column(db.Integer, primary_key=True)
    host     = db.Column(db.String(255), default="127.0.0.1")
    port     = db.Column(db.Integer, default=1433)
    database = db.Column(db.String(255), nullable=False, default="master")
    username = db.Column(db.String(255), nullable=False, default="sa")
    password = db.Column(db.String(255), nullable=False, default="")
    odbc_driver = db.Column(db.String(255), nullable=False, default="ODBC Driver 18 for SQL Server")
    trust_server_cert = db.Column(db.Boolean, default=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def build_uri(self):
        trust = "yes" if self.trust_server_cert else "no"
        # SQLAlchemy URI via pyodbc
        return (f"mssql+pyodbc://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
                f"?driver={self.odbc_driver.replace(' ', '+')}"
                f"&TrustServerCertificate={trust}")

class ConnectionProfile(db.Model):
    __tablename__ = "connection_profiles"
    id       = db.Column(db.Integer, primary_key=True)
    name     = db.Column(db.String(120), nullable=False, default="Default")
    project  = db.Column(db.String(120), nullable=False, default="Algemeen")
    host     = db.Column(db.String(255), default="127.0.0.1")
    port     = db.Column(db.Integer, default=1433)
    database = db.Column(db.String(255), nullable=False, default="master")
    username = db.Column(db.String(255), nullable=False, default="sa")
    password = db.Column(db.String(255), nullable=False, default="")
    odbc_driver = db.Column(db.String(255), nullable=False, default="ODBC Driver 17 for SQL Server")
    trust_server_cert = db.Column(db.Boolean, default=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def build_uri(self):
        trust = "yes" if self.trust_server_cert else "no"
        return (
            f"mssql+pyodbc://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?driver={self.odbc_driver.replace(' ', '+')}"
            f"&TrustServerCertificate={trust}"
        )


class RGRit(db.Model):
    __tablename__ = "rgritten"

    id = db.Column(db.Integer, primary_key=True)
    ingested_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    rittype = db.Column(db.String(255), nullable=False)
    ritnummer = db.Column(db.Integer, nullable=False, index=True)
    schema = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(255), nullable=False)
    weekdag = db.Column(db.String(255), nullable=True)
    aankomst = db.Column(db.String(255), nullable=True)
    vertrek = db.Column(db.String(255), nullable=True)
    voornaam = db.Column(db.String(255), nullable=True)
    voorletters = db.Column(db.String(255), nullable=True)
    voorvoegsels = db.Column(db.String(255), nullable=True)
    achternaam = db.Column(db.String(255), nullable=True)
    straat = db.Column(db.String(255), nullable=True)
    huisnummer = db.Column(db.String(255), nullable=True)
    postcode = db.Column(db.String(255), nullable=True)
    woonplaats = db.Column(db.String(255), nullable=True)
    telefoonnummer = db.Column(db.String(255), nullable=True)
    faxnummer = db.Column(db.String(255), nullable=True)
    mobiel = db.Column(db.String(255), nullable=True)
    emailadres = db.Column(db.String(255), nullable=True)
    geslacht = db.Column(db.String(255), nullable=True)
    geboortedatum = db.Column(db.DateTime, nullable=True)
    co_bijzonderheden = db.Column(db.String(255), nullable=True)
    huisnummer_toev = db.Column(db.String(255), nullable=True)
    land = db.Column(db.String(255), nullable=True)
    afdeling = db.Column(db.String(255), nullable=True)
    kamernummer = db.Column(db.String(255), nullable=True)
    cor_straat = db.Column(db.String(255), nullable=True)
    cor_huisnummer = db.Column(db.String(255), nullable=True)
    cor_huisnummer_toev = db.Column(db.String(255), nullable=True)
    cor_postcode = db.Column(db.String(255), nullable=True)
    cor_woonplaats = db.Column(db.String(255), nullable=True)
    cor_land = db.Column(db.String(255), nullable=True)
    cor_afdeling = db.Column(db.String(255), nullable=True)
    cor_kamernummer = db.Column(db.String(255), nullable=True)
    co_klantnummer = db.Column(db.String(255), nullable=True)
    emailadres2 = db.Column(db.String(255), nullable=True)
    emailadres3 = db.Column(db.String(255), nullable=True)
    klantnummer2 = db.Column(db.String(255), nullable=True)
    tp_id = db.Column(db.Integer, nullable=True)
    tp_byzonderheden = db.Column(db.String(255), nullable=True)
    pasnummer = db.Column(db.String(255), nullable=True)
    ind_rolstoel = db.Column(db.Integer, nullable=True)
    rolstoel_type = db.Column(db.Integer, nullable=True)
    ind_begeleiding_sociaal = db.Column(db.Integer, nullable=True)
    ind_begeleiding_medisch = db.Column(db.Integer, nullable=True)
    ind_beperking_lichamelijk = db.Column(db.Integer, nullable=True)
    ind_beperking_verstandelijk = db.Column(db.Integer, nullable=True)
    ind_hulphond = db.Column(db.Integer, nullable=True)
    ind_voorin_in_taxi = db.Column(db.Integer, nullable=True)
    ind_kamer_tot_kamer_vervoer = db.Column(db.Integer, nullable=True)
    ind_gezinstaxi = db.Column(db.Integer, nullable=True)
    ind_personenauto = db.Column(db.Integer, nullable=True)
    ind_lage_instap = db.Column(db.Integer, nullable=True)
    ind_voldoende_beenruimte = db.Column(db.Integer, nullable=True)
    ind_rollator = db.Column(db.Integer, nullable=True)
    ind_beperking_visueel = db.Column(db.Integer, nullable=True)
    ind_beperking_auditief = db.Column(db.Integer, nullable=True)
    ind_autisme = db.Column(db.Integer, nullable=True)
    ind_individueel_vervoer = db.Column(db.Integer, nullable=True)
    beschikkingnummer = db.Column(db.String(255), nullable=True)
    vervoer_type = db.Column(db.Integer, nullable=True)
    vervoer_type_omschrijving = db.Column(db.String(255), nullable=True)
    rolstoel_type_omschrijving = db.Column(db.String(255), nullable=True)
    aantal_gezinstaxi = db.Column(db.Integer, nullable=True)
    aantal_begeleiding_sociaal = db.Column(db.Integer, nullable=True)
    aantal_begeleiding_medisch = db.Column(db.Integer, nullable=True)
    ind_scootmobiel = db.Column(db.Integer, nullable=True)
    ind_strippen = db.Column(db.Integer, nullable=True)
    aantal_strippen = db.Column(db.Integer, nullable=True)
    ind_voldoende_zithoogte = db.Column(db.Integer, nullable=True)
    ind_afwijkend_tarief = db.Column(db.Integer, nullable=True)
    ind_zithulp = db.Column(db.Integer, nullable=True)
    zithulp_type = db.Column(db.Integer, nullable=True)
    financiering_type = db.Column(db.Integer, nullable=True)
    ind_belservice = db.Column(db.Integer, nullable=True)
    ind_epilepsie = db.Column(db.Integer, nullable=True)
    ind_geen_warme_overdracht = db.Column(db.Integer, nullable=True)
    extra_instap_heen = db.Column(db.Integer, nullable=True)
    extra_instap_terug = db.Column(db.Integer, nullable=True)
    extra_uitstap_heen = db.Column(db.Integer, nullable=True)
    extra_uitstap_terug = db.Column(db.Integer, nullable=True)
    ind_kleinschalig = db.Column(db.Integer, nullable=True)
    max_reistijd = db.Column(db.Integer, nullable=True)
    ind_extra1 = db.Column(db.Integer, nullable=True)
    ind_extra2 = db.Column(db.Integer, nullable=True)
    ind_extra3 = db.Column(db.Integer, nullable=True)
    ind_extra4 = db.Column(db.Integer, nullable=True)
    ind_extra5 = db.Column(db.Integer, nullable=True)
    ind_busbegeleider_nodig = db.Column(db.Integer, nullable=True)
    ind_is_busbegeleider = db.Column(db.Integer, nullable=True)
    ind_inclusief_begeleider = db.Column(db.Integer, nullable=True)
    ind_alleen_begeleider = db.Column(db.Integer, nullable=True)
    ind_invloed_op_factuur = db.Column(db.Integer, nullable=True)
    ind_client_mag_rit_wijzigen = db.Column(db.Integer, nullable=True)
    ind_ontheffing_mondkapje = db.Column(db.Integer, nullable=True)
    ind_ontheffing_gordelplicht = db.Column(db.Integer, nullable=True)
    ind_bus = db.Column(db.Integer, nullable=True)
    ind_mag_vraagafhankelijk = db.Column(db.Integer, nullable=True)
    budget_standaard = db.Column(db.String(255), nullable=True)
    budget_huidig = db.Column(db.String(255), nullable=True)
    ind_alleen_zitten = db.Column(db.Integer, nullable=True)
    ind_diabetes = db.Column(db.Integer, nullable=True)
    ind_deur_deur = db.Column(db.Integer, nullable=True)
    ind_anderstalig = db.Column(db.Integer, nullable=True)
    ind_opvouwbare_rolstoel = db.Column(db.Integer, nullable=True)
    ind_kinderstoel = db.Column(db.Integer, nullable=True)
    ind_stoelverhoger = db.Column(db.Integer, nullable=True)
    ind_gordelkapje = db.Column(db.Integer, nullable=True)
    ind_meerpuntsgordel = db.Column(db.Integer, nullable=True)
    ind_maxicosi = db.Column(db.Integer, nullable=True)
    ind_begeleiding_niet_verplicht = db.Column(db.Integer, nullable=True)
    ind_afasie = db.Column(db.Integer, nullable=True)
    ind_vaste_zitplaats = db.Column(db.Integer, nullable=True)
    ind_gordelverlenging = db.Column(db.Integer, nullable=True)
    ind_lifo = db.Column(db.Integer, nullable=True)
    ind_filo = db.Column(db.Integer, nullable=True)
    ind_fifo = db.Column(db.Integer, nullable=True)
    aantal_kleinschalig = db.Column(db.Integer, nullable=True)
    minimaal_scootmobiel_reisafstand = db.Column(db.Integer, nullable=True)
    minimale_reisafstand = db.Column(db.Integer, nullable=True)
    ind_vervroegde_belservice = db.Column(db.Integer, nullable=True)
    ind_opstappunt_verplicht = db.Column(db.Integer, nullable=True)
    ind_bagage = db.Column(db.Integer, nullable=True)
    type_bagage = db.Column(db.String(255), nullable=True)
    ind_niet_combineren = db.Column(db.Integer, nullable=True)
    niet_combineren_met = db.Column(db.String(255), nullable=True)
    locatie_van = db.Column(db.String(255), nullable=True)
    straat_van = db.Column(db.String(255), nullable=True)
    huisnummer_van = db.Column(db.String(255), nullable=True)
    huisnummer_toev_van = db.Column(db.String(255), nullable=True)
    postcode_van = db.Column(db.String(255), nullable=True)
    plaats_van = db.Column(db.String(255), nullable=True)
    locatie_naar = db.Column(db.String(255), nullable=True)
    straat_naar = db.Column(db.String(255), nullable=True)
    huisnummer_naar = db.Column(db.String(255), nullable=True)
    huisnummer_toev_naar = db.Column(db.String(255), nullable=True)
    postcode_naar = db.Column(db.String(255), nullable=True)
    plaats_naar = db.Column(db.String(255), nullable=True)
    weekdag_id = db.Column(db.Integer, nullable=True)
    owner_id = db.Column(db.Integer, nullable=False, index=True)
    carrier_id = db.Column(db.Integer, nullable=True)
    vervoerder = db.Column(db.String(255), nullable=False)
    opdrachtgever = db.Column(db.String(255), nullable=True)
    effective_date = db.Column(db.DateTime, nullable=True)
    ritdatum = db.Column(db.DateTime, nullable=True)
    routenummer = db.Column(db.String(255), nullable=True)
    vervoerder_routenummer = db.Column(db.String(255), nullable=True)
    instap = db.Column(db.Time, nullable=True)
    uitstap = db.Column(db.Time, nullable=True)
    afstand = db.Column(db.Numeric(18, 4), nullable=True)
    duur = db.Column(db.Time, nullable=True)
    afstand2 = db.Column(db.Numeric(18, 4), nullable=True)
    duur2 = db.Column(db.Time, nullable=True)
    zoneafstand1 = db.Column(db.Integer, nullable=True)
    zoneafstand2 = db.Column(db.Integer, nullable=True)
    perceel_id = db.Column(db.Integer, nullable=True)
    perceel_omschrijving = db.Column(db.String(255), nullable=True)
    gemeld_op = db.Column(db.DateTime, nullable=True)
    afwezig_van = db.Column(db.DateTime, nullable=True)
    afwezig_totmet = db.Column(db.DateTime, nullable=True)
    attendance_reason = db.Column(db.String(255), nullable=True)
    tekst = db.Column(db.String(255), nullable=True)
    instelling_gemeld_op = db.Column(db.DateTime, nullable=True)
    instelling_afwezig_van = db.Column(db.DateTime, nullable=True)
    instelling_afwezig_totmet = db.Column(db.DateTime, nullable=True)
    instelling_reden = db.Column(db.String(255), nullable=True)
    instelling_tekst = db.Column(db.String(255), nullable=True)
    vervangt_plannedtransport_id = db.Column(db.Integer, nullable=True)
    vervangt_datum = db.Column(db.DateTime, nullable=True)
    realisatie_route = db.Column(db.String(255), nullable=True)
    instapgerealiseerd = db.Column(db.Time, nullable=True)
    instaplatitude = db.Column(db.Numeric(18, 10), nullable=True)
    instaplongitude = db.Column(db.Numeric(18, 10), nullable=True)
    uitstapgerealiseerd = db.Column(db.Time, nullable=True)
    uitstaplatitude = db.Column(db.Numeric(18, 10), nullable=True)
    uitstaplongitude = db.Column(db.Numeric(18, 10), nullable=True)
    loosmeldinggerealiseerd = db.Column(db.Time, nullable=True)
    loosmeldinglatitude = db.Column(db.Numeric(18, 10), nullable=True)
    loosmeldinglongitude = db.Column(db.Numeric(18, 10), nullable=True)


class DataRefreshConfig(db.Model):
    __tablename__ = "data_refresh_config"

    id = db.Column(db.Integer, primary_key=True)
    enabled = db.Column(db.Boolean, nullable=False, default=False)
    run_time = db.Column(db.String(10), nullable=False, default="02:00")  # HH:MM
    profile_id = db.Column(db.Integer, db.ForeignKey("connection_profiles.id"), nullable=True)
    chunk_size = db.Column(db.Integer, nullable=False, default=1000)
    min_ritdatum = db.Column(db.String(20), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class ReportTemplate(db.Model):
    __tablename__ = "report_templates"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    dataset = db.Column(db.String(120), nullable=False, default="rgritten")
    row_limit = db.Column(db.Integer, nullable=False, default=1000)
    include_fields = db.Column(db.JSON, nullable=False, default=list)
    filter_fields = db.Column(db.JSON, nullable=False, default=list)
    sort_fields = db.Column(db.JSON, nullable=False, default=list)  # list of {"field":..., "dir": "asc|desc"}
    group_fields = db.Column(db.JSON, nullable=False, default=list)
    pivot_enabled = db.Column(db.Boolean, nullable=False, default=False)
    pivot_row_fields = db.Column(db.JSON, nullable=False, default=list)
    pivot_col_field = db.Column(db.String(255), nullable=True)
    pivot_values = db.Column(db.JSON, nullable=False, default=list)  # list of {"field":..., "agg": ...}
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
