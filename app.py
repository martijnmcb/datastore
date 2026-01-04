import os
import click
from flask import Flask, redirect, url_for, request, abort
from config import Config
from extensions import db, login_manager, migrate
from models import User, Role
from blueprints.auth.routes import bp as auth_bp
from blueprints.main.routes import bp as main_bp
from blueprints.admin.routes import bp as admin_bp
from blueprints.reports import bp as reports_bp

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
