from flask import Blueprint, render_template
from flask_login import login_required

bp = Blueprint("main", __name__)

@bp.route("/")
@login_required
def home():
    # Empty home view; no SQL or profile logic
    return render_template("main_home.html")
