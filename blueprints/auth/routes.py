from flask import Blueprint, render_template, redirect, request, url_for, flash
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField
from wtforms.validators import DataRequired, Length
from flask_login import login_user, logout_user, login_required
from extensions import db, login_manager
from models import User

bp = Blueprint("auth", __name__, url_prefix="/auth")

class LoginForm(FlaskForm):
    username = StringField("Gebruikersnaam", validators=[DataRequired()])
    password = PasswordField(
        "Wachtwoord",
        validators=[DataRequired(), Length(max=128, message="Wachtwoord mag maximaal 128 tekens bevatten")],
    )

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

@bp.route("/login", methods=["GET", "POST"])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = db.session.query(User).filter_by(username=form.username.data).first()
        if user and user.is_active and user.check_password(form.password.data):
            login_user(user)
            return redirect(url_for("main.home"))
        flash("Onjuiste inlog of account inactief", "danger")
    return render_template("login.html", form=form)

@bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
