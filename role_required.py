from functools import wraps
from flask import abort
from flask_login import current_user

def role_required(*role_names):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
            names = {r.name for r in current_user.roles}
            if not any(r in names for r in role_names):
                abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return deco