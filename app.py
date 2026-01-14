import os
import threading
import time
from datetime import datetime, timedelta
import click
from flask import Flask, redirect, url_for, request, abort
from config import Config
from extensions import db, login_manager, migrate
from models import User, Role, DataRefreshConfig, ConnectionProfile
from blueprints.auth.routes import bp as auth_bp
from blueprints.main.routes import bp as main_bp
from blueprints.admin.routes import bp as admin_bp
from blueprints.reports import bp as reports_bp


def _seconds_until(timestr: str) -> float:
    """Return seconds until next occurrence of HH:MM (local time)."""
    now = datetime.now()
    try:
        hh, mm = [int(x) for x in timestr.split(":")[:2]]
    except Exception:
        return 3600.0
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _data_refresh_loop(app: Flask):
    while True:
        try:
            with app.app_context():
                cfg = db.session.get(DataRefreshConfig, 1)
                if not cfg:
                    cfg = DataRefreshConfig(id=1)
                    db.session.add(cfg)
                    db.session.commit()
                if not cfg.enabled:
                    time.sleep(300)
                    continue

                schedule = cfg.run_time or "02:00"
                wait_s = _seconds_until(schedule)
                time.sleep(wait_s)

                # re-fetch in case it was changed while sleeping
                cfg = db.session.get(DataRefreshConfig, 1)
                if not cfg or not cfg.enabled:
                    continue
                profile = db.session.get(ConnectionProfile, cfg.profile_id) if cfg.profile_id else None
                if not profile:
                    app.logger.warning("Data refresh: geen profiel geselecteerd, sla over")
                    time.sleep(300)
                    continue

                from rgritten_sync import sync_rgritten

                stats = sync_rgritten(
                    profile_name=profile.name,
                    chunk_size=max(1, cfg.chunk_size or 1000),
                    min_ritdatum=cfg.min_ritdatum,
                )
                app.logger.info(
                    "Data refresh ok (%s %s): +%s (through %s/%s)",
                    profile.project,
                    profile.name,
                    stats.get("inserted"),
                    stats.get("through_ritdatum"),
                    stats.get("through_ritnummer"),
                )
        except Exception:
            app.logger.exception("Data refresh failed")
            time.sleep(60)


def _start_data_refresh(app: Flask):
    # Avoid double-start in dev reloader
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    if app.config.get("_data_refresh_started"):
        return
    app.config["_data_refresh_started"] = True
    t = threading.Thread(target=_data_refresh_loop, args=(app,), daemon=True)
    t.start()
    app.logger.info(
        "Data refresh scheduler started: profile=%s time=%s chunk=%s",
        "db-config",
        "db-config",
        "db-config",
    )


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    login_manager.init_app(app)
    migrate.init_app(app, db)
    login_manager.login_view = "auth.login"

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(reports_bp)

    _start_data_refresh(app)

    @app.route("/init")
    def init():
        # Guard: only allow in debug or with INIT_TOKEN
        if not app.debug:
            token = request.args.get("token")
            if not token or token != os.getenv("INIT_TOKEN"):
                abort(403)
        # Ensure tables exist before seeding default data (useful for fresh SQLite setups)
        db.create_all()
        # maak basisrollen
        for r in ["Beheerder", "Gebruiker", "Lezer"]:
            if not db.session.query(Role).filter_by(name=r).first():
                db.session.add(Role(name=r))
        db.session.commit()
        # maak admin user als die nog niet bestaat
        if not db.session.query(User).filter_by(username="admin").first():
            u = User(
                first_name="Admin",
                last_name="User",
                username="admin",
                email="admin@example.com",
                is_active=True,
            )
            seed_password = os.getenv("ADMIN_SEED_PASSWORD", "admin123")
            try:
                u.set_password(seed_password)
            except ValueError as exc:
                return str(exc), 500
            admin_role = db.session.query(Role).filter_by(name="Beheerder").first()
            u.roles.append(admin_role)
            db.session.add(u)
            db.session.commit()
        return redirect(url_for("auth.login"))

    @app.cli.command("sync-rgritten")
    @click.option("--profile", default="Historie", show_default=True)
    @click.option("--chunk-size", default=1000, show_default=True, type=int)
    @click.option("--min-ritdatum", default=None, help="Filter ritdatum >= YYYY-MM-DD")
    def sync_rgritten_cli(profile, chunk_size, min_ritdatum):
        """Append-only sync from remote rpt.RGRitten into local SQLite."""
        from rgritten_sync import sync_rgritten

        stats = sync_rgritten(
            profile_name=profile,
            chunk_size=chunk_size,
            min_ritdatum=min_ritdatum,
        )
        click.echo(
            f"Synced {stats['inserted']} rows "
            f"(ritnummer {stats['from_ritnummer']} -> {stats['through_ritnummer']})"
        )

    @app.cli.command("diagnose-rgritten")
    @click.option("--profile", default="Historie", show_default=True)
    @click.option("--cursor", default=0, show_default=True, type=int)
    @click.option("--min-ritdatum", default=None, help="Filter ritdatum >= YYYY-MM-DD")
    def diagnose_rgritten_cli(profile, cursor, min_ritdatum):
        """Find rows/columns in rpt.RGRitten that fail numeric conversion."""
        from rgritten_sync import find_conversion_issues

        problems = find_conversion_issues(
            profile_name=profile,
            cursor=cursor,
            min_ritdatum=min_ritdatum,
        )
        if not problems:
            click.echo("No conversion issues detected.")
            return
        for p in problems:
            click.echo(
                f"Column {p['column']} has non-convertible value "
                f"'{p['value']}' at ritnummer {p['ritnummer']}"
            )

    @app.cli.command("debug-rgritten-cols")
    @click.option("--profile", default="Historie", show_default=True)
    def debug_rgritten_cols(profile):
        """Binary-search which columns cause SQL conversion errors in rpt.RGRitten."""
        from rgritten_sync import locate_offending_columns

        bad = locate_offending_columns(profile_name=profile)
        if not bad:
            click.echo("No offending columns detected.")
            return
        click.echo("Columns failing selection:")
        for col in bad:
            click.echo(f"- {col}")

    return app

if __name__ == "__main__":
    app = create_app()
    debug = os.getenv("FLASK_DEBUG") == "1"
    app.run(host="0.0.0.0", port=5000, debug=debug)
