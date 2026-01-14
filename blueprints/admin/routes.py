from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    PasswordField,
    BooleanField,
    SelectField,
    SelectMultipleField,
    IntegerField,
    HiddenField,
)
from wtforms.validators import DataRequired, Email, Optional, Length
from flask_login import login_required
from extensions import db
from models import User, Role, ConnectionSetting, ConnectionProfile, UserProject, DataRefreshConfig
from role_required import role_required
import sqlalchemy as sa

bp = Blueprint("admin", __name__, url_prefix="/beheer")

def get_project_choices():
    names = (
        db.session.query(ConnectionProfile.project)
        .distinct()
        .order_by(ConnectionProfile.project)
        .all()
    )
    choices = [(name[0], name[0]) for name in names if name[0]]
    if not choices:
        choices = [("Algemeen", "Algemeen")]
    return choices

# ----- Forms -----
class UserForm(FlaskForm):
    first_name = StringField("Voornaam", validators=[DataRequired()])
    last_name  = StringField("Achternaam", validators=[DataRequired()])
    email      = StringField("Email adres", validators=[Optional(), Email()])
    phone      = StringField("Telefoonnummer", validators=[Optional()])
    username   = StringField("Gebruikersnaam", validators=[DataRequired()])
    password   = PasswordField(
        "Wachtwoord",
        validators=[Optional(), Length(min=8, max=128, message="Minimaal 8 en maximaal 128 tekens")],
    )  # verplicht bij create
    role       = SelectField("Rol", choices=[("Beheerder","Beheerder"),("Gebruiker","Gebruiker"),("Lezer","Lezer")], validators=[DataRequired()])
    projects   = SelectMultipleField("Projecten", choices=[], validators=[Optional()])
    is_active  = BooleanField("Actief")

class ConnForm(FlaskForm):
    host = StringField("IP/Host", validators=[DataRequired()])
    port = IntegerField("Poort", validators=[DataRequired()])
    database = StringField("Database", validators=[DataRequired()])
    username = StringField("DB User", validators=[DataRequired()])
    password = PasswordField("DB Pass", validators=[Optional()])
    odbc_driver = StringField("ODBC Driver", default="ODBC Driver 17 for SQL Server", validators=[DataRequired()])
    trust_server_cert = BooleanField("Trust Server Certificate")

class ConnProfileForm(FlaskForm):
    id = HiddenField("id")
    name = StringField("Naam", validators=[DataRequired()])
    project = StringField("Project", validators=[DataRequired()])
    host = StringField("IP/Host", validators=[DataRequired()])
    port = IntegerField("Poort", validators=[DataRequired()])
    database = StringField("Database", validators=[DataRequired()])
    username = StringField("DB User", validators=[DataRequired()])
    password = PasswordField("DB Pass", validators=[Optional(), Length(min=0)])
    odbc_driver = StringField("ODBC Driver", default="ODBC Driver 17 for SQL Server", validators=[DataRequired()])
    trust_server_cert = BooleanField("Trust Server Certificate")


class RefreshConfigForm(FlaskForm):
    enabled = BooleanField("Scheduler aan")
    run_time = StringField("Dagelijks tijdstip (HH:MM)", default="02:00", validators=[DataRequired()])
    profile_id = SelectField("Profiel", choices=[], validators=[Optional()])
    chunk_size = IntegerField("Chunk grootte", default=1000, validators=[DataRequired()])
    min_ritdatum = StringField("Minimale ritdatum (YYYY-MM-DD)", validators=[Optional()])

# ----- Views -----
@bp.route("/")
@login_required
@role_required("Beheerder")
def dashboard():
    return render_template("admin_dashboard.html")

@bp.route("/users", methods=["GET", "POST"])
@login_required
@role_required("Beheerder")
def users():
    form = UserForm()
    form.projects.choices = get_project_choices()
    users = db.session.query(User).all()
    if request.method == "POST" and form.validate_on_submit():
        # Unieke username check
        if db.session.query(User.id).filter_by(username=form.username.data).first():
            flash("Gebruikersnaam bestaat al", "warning")
            return render_template("admin_users.html", form=form, users=users)
        # Unieke email (indien ingevuld)
        if form.email.data and db.session.query(User.id).filter_by(email=form.email.data).first():
            flash("E-mailadres is al in gebruik", "warning")
            return render_template("admin_users.html", form=form, users=users)
        u = User(
            first_name=form.first_name.data,
            last_name=form.last_name.data,
            email=form.email.data,
            phone=form.phone.data,
            username=form.username.data,
            is_active=form.is_active.data,
        )
        # rol koppelen
        role = db.session.query(Role).filter_by(name=form.role.data).first()
        if not role:
            role = Role(name=form.role.data)
            db.session.add(role)
        u.roles.append(role)
        # projecten koppelen (meerdere mogelijk)
        selected_projects = [p for p in form.projects.data if p]
        for project_name in selected_projects:
            u.project_links.append(UserProject(project=project_name))
        # wachtwoord
        if not form.password.data:
            flash("Wachtwoord is verplicht bij aanmaken (min. 8 tekens)", "danger")
            return render_template("admin_users.html", form=form, users=users)
        try:
            u.set_password(form.password.data)
        except ValueError as exc:
            flash(str(exc), "danger")
            return render_template("admin_users.html", form=form, users=users)
        db.session.add(u)
        db.session.commit()
        flash("Gebruiker aangemaakt", "success")
        return redirect(url_for("admin.users"))
    return render_template("admin_users.html", form=form, users=users)

@bp.route("/users/<int:user_id>/toggle", methods=["POST"])
@login_required
@role_required("Beheerder")
def toggle_user(user_id):
    u = db.session.get(User, user_id)
    if not u:
        flash("Niet gevonden", "warning")
        return redirect(url_for("admin.users"))
    u.is_active = not u.is_active
    db.session.commit()
    flash("Status gewijzigd", "success")
    return redirect(url_for("admin.users"))

@bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
@role_required("Beheerder")
def edit_user(user_id):
    u = db.session.get(User, user_id)
    if not u:
        flash("Niet gevonden", "warning")
        return redirect(url_for("admin.users"))

    form = UserForm(obj=u)
    form.projects.choices = get_project_choices()
    # Ensure the current role is pre-selected when editing
    if request.method == "GET":
        current_role = next(iter(u.roles), None)
        if current_role:
            form.role.data = current_role.name
        form.projects.data = u.project_names
    if form.validate_on_submit():
        # Unieke username behalve voor jezelf
        exists_username = (
            db.session.query(User.id)
            .filter(User.username == form.username.data, User.id != u.id)
            .first()
        )
        if exists_username:
            flash("Gebruikersnaam bestaat al", "warning")
            users = db.session.query(User).all()
            return render_template("admin_users.html", form=form, users=users, edit_id=u.id)
        # Unieke email behalve voor jezelf (indien ingevuld)
        if form.email.data:
            exists_email = (
                db.session.query(User.id)
                .filter(User.email == form.email.data, User.id != u.id)
                .first()
            )
            if exists_email:
                flash("E-mailadres is al in gebruik", "warning")
                users = db.session.query(User).all()
                return render_template("admin_users.html", form=form, users=users, edit_id=u.id)
        # Velden bijwerken
        u.first_name = form.first_name.data
        u.last_name = form.last_name.data
        u.email = form.email.data
        u.phone = form.phone.data
        u.username = form.username.data
        u.is_active = form.is_active.data
        # Rol bijwerken (enkelvoudig)
        u.roles.clear()
        role = db.session.query(Role).filter_by(name=form.role.data).first()
        if not role:
            role = Role(name=form.role.data)
            db.session.add(role)
        u.roles.append(role)
        # Projecten bijwerken (meervoudig)
        u.project_links.clear()
        selected_projects = [p for p in form.projects.data if p]
        for project_name in selected_projects:
            u.project_links.append(UserProject(project=project_name))
        # Wachtwoord alleen wijzigen indien opgegeven
        if form.password.data:
            try:
                u.set_password(form.password.data)
            except ValueError as exc:
                flash(str(exc), "danger")
                users = db.session.query(User).all()
                return render_template(
                    "admin_users.html", form=form, users=users, edit_id=u.id
                )
        db.session.commit()
        flash("Gebruiker bijgewerkt", "success")
        return redirect(url_for("admin.users"))

    users = db.session.query(User).all()
    return render_template("admin_users.html", form=form, users=users, edit_id=u.id)


@bp.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@role_required("Beheerder")
def delete_user(user_id):
    u = db.session.get(User, user_id)
    if not u:
        flash("Niet gevonden", "warning")
        return redirect(url_for("admin.users"))
    db.session.delete(u)
    db.session.commit()
    flash("Gebruiker verwijderd", "success")
    return redirect(url_for("admin.users"))

@bp.route("/connection", methods=["GET", "POST"])
@login_required
@role_required("Beheerder")
def connection():
    # Load all profiles and distinct projects
    profiles = db.session.query(ConnectionProfile).order_by(ConnectionProfile.project, ConnectionProfile.name).all()
    projects = sorted({p.project for p in profiles})

    # Determine selected project and profile via query args
    sel_project = request.args.get("project") or (projects[0] if projects else None)
    filtered = [p for p in profiles if (sel_project is None or p.project == sel_project)]
    sel_id = request.args.get("id", type=int) or (filtered[0].id if filtered else None)

    # Create initial profile if none exist yet
    if not profiles:
        cp = ConnectionProfile(name="Default", project="Algemeen")
        db.session.add(cp)
        db.session.commit()
        return redirect(url_for("admin.connection", project=cp.project, id=cp.id))

    # Handle actions
    action = request.form.get("action") if request.method == "POST" else None

    # Create new profile
    if action == "new":
        cp = ConnectionProfile(name="Nieuw profiel", project=sel_project or "Algemeen")
        db.session.add(cp)
        db.session.commit()
        return redirect(url_for("admin.connection", project=cp.project, id=cp.id))

    # Delete profile
    if action == "delete" and sel_id:
        cp = db.session.get(ConnectionProfile, sel_id)
        if cp:
            db.session.delete(cp)
            db.session.commit()
            remaining = db.session.query(ConnectionProfile).filter_by(project=cp.project).order_by(ConnectionProfile.name).all()
            if remaining:
                return redirect(url_for("admin.connection", project=remaining[0].project, id=remaining[0].id))
            else:
                cp2 = ConnectionProfile(name="Default", project="Algemeen")
                db.session.add(cp2)
                db.session.commit()
                return redirect(url_for("admin.connection", project=cp2.project, id=cp2.id))
        flash("Profiel niet gevonden", "warning")
        return redirect(url_for("admin.connection"))

    # Save/update current profile from form
    if action == "save" and sel_id:
        cp = db.session.get(ConnectionProfile, sel_id)
        if not cp:
            flash("Profiel niet gevonden", "warning")
            return redirect(url_for("admin.connection"))
        form = ConnProfileForm()
        if form.validate_on_submit():
            old_pwd = cp.password
            cp.name = form.name.data
            cp.project = form.project.data
            cp.host = form.host.data
            cp.port = int(form.port.data or 1433)
            cp.database = form.database.data
            cp.username = form.username.data
            if form.password.data:
                cp.password = form.password.data
            else:
                cp.password = old_pwd
            cp.odbc_driver = form.odbc_driver.data
            cp.trust_server_cert = bool(form.trust_server_cert.data)
            db.session.commit()
            flash("Profiel opgeslagen", "success")
            return redirect(url_for("admin.connection", project=cp.project, id=cp.id))

    # Render edit form for selected profile
    cp = db.session.get(ConnectionProfile, sel_id) if sel_id else None
    if not cp and profiles:
        cp = profiles[0]
    form = ConnProfileForm(obj=cp)

    # Build dropdown data
    project_choices = [(p, p) for p in projects] if projects else [("Algemeen", "Algemeen")]
    profile_choices = [(str(p.id), f"{p.name}") for p in filtered] if filtered else ([(str(cp.id), cp.name)] if cp else [])

    return render_template(
        "admin_connection.html",
        form=form,
        projects=project_choices,
        profiles=profile_choices,
        sel_project=sel_project or (cp.project if cp else None),
        sel_id=(cp.id if cp else None),
        multi_profiles=True,
    )


@bp.route("/refresh", methods=["GET", "POST"])
@login_required
@role_required("Beheerder")
def refresh_config():
    cfg = db.session.get(DataRefreshConfig, 1)
    if not cfg:
        cfg = DataRefreshConfig(id=1)
        db.session.add(cfg)
        db.session.commit()

    profiles = db.session.query(ConnectionProfile).order_by(ConnectionProfile.project, ConnectionProfile.name).all()
    form = RefreshConfigForm()
    form.profile_id.choices = [("", "- kies profiel -")] + [(str(p.id), f"{p.project} / {p.name}") for p in profiles]

    if request.method == "POST" and form.validate_on_submit():
        cfg.enabled = bool(form.enabled.data)
        cfg.run_time = (form.run_time.data or "02:00").strip()
        cfg.profile_id = int(form.profile_id.data) if form.profile_id.data else None
        cfg.chunk_size = form.chunk_size.data or 1000
        cfg.min_ritdatum = (form.min_ritdatum.data or "").strip() or None
        db.session.commit()
        flash("Data refresh instellingen opgeslagen", "success")
        return redirect(url_for("admin.refresh_config"))

    if request.method == "GET":
        form.enabled.data = cfg.enabled
        form.run_time.data = cfg.run_time or "02:00"
        form.profile_id.data = str(cfg.profile_id) if cfg.profile_id else ""
        form.chunk_size.data = cfg.chunk_size or 1000
        form.min_ritdatum.data = cfg.min_ritdatum or ""

    return render_template(
        "admin_refresh.html",
        form=form,
        profiles=profiles,
    )

@bp.route("/connection/test", methods=["POST"])
@login_required
@role_required("Beheerder")
def connection_test():
    sel_id = request.form.get("id") or request.form.get("profile_id") or request.args.get("id")
    if not sel_id:
        flash("Geen profiel gekozen", "warning")
        return redirect(url_for("admin.connection"))
    cp = db.session.get(ConnectionProfile, int(sel_id))
    if not cp:
        flash("Profiel niet gevonden", "warning")
        return redirect(url_for("admin.connection"))
    uri = cp.build_uri()
    try:
        engine = sa.create_engine(uri)
        with engine.connect() as con:
            version = con.execute(sa.text("SELECT @@VERSION")).scalar()
        flash(f"OK – {version}", "success")
    except Exception as e:
        flash(f"FOUT – {e}", "danger")
    return redirect(url_for("admin.connection", project=cp.project, id=cp.id))
