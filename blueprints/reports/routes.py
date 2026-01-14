from io import StringIO, BytesIO
from datetime import datetime, date, time
from flask import render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required
from sqlalchemy import asc, desc, func
from extensions import db
from models import RGRit, ReportTemplate
from . import bp

DEFAULT_REPORT_ROW_LIMIT = 1000


def _dataset_fields(dataset):
    if dataset == "rgritten":
        cols = [c.key for c in RGRit.__table__.columns]
        base = [c for c in cols if c not in ("id", "ingested_at")]
        base.append("reistijd_calc")
        return base
    return []


def _field_kind(dataset, field):
    if dataset != "rgritten":
        return "text"
    col = getattr(RGRit, field, None)
    if not col:
        if field == "reistijd_calc":
            return "text"
        return "text"
    from sqlalchemy import Date, DateTime, Time

    if isinstance(col.type, (Date, DateTime)):
        return "date"
    if isinstance(col.type, Time):
        return "time"
    return "text"


def _parse_date_value(val, as_end=False):
    if not val:
        return None
    try:
        if "T" in val or " " in val:
            return datetime.fromisoformat(val)
        d = date.fromisoformat(val)
        return datetime.combine(d, time.max if as_end else time.min)
    except ValueError:
        return None


def _parse_time_value(val):
    if not val:
        return None
    try:
        return time.fromisoformat(val)
    except ValueError:
        return None


def _calc_value(field, row):
    if field == "reistijd_calc":
        a = getattr(row, "instapgerealiseerd", None)
        b = getattr(row, "uitstapgerealiseerd", None)
        if a and b:
            delta = (datetime.combine(date.today(), b) - datetime.combine(date.today(), a))
            if delta.total_seconds() < 0:
                return None
            total = int(delta.total_seconds())
            h = total // 3600
            m = (total % 3600) // 60
            s = total % 60
            return f"{h:02d}:{m:02d}:{s:02d}"
        return None
    return None


def _format_value(field, row):
    val = getattr(row, field, None)
    if val is None:
        return ""
    if field == "ritdatum" and hasattr(val, "date"):
        return val.date().isoformat()
    return val


def _parse_form(fields, dataset):
    include_fields = [f for f in fields if request.form.get(f"include_{f}") == "1"]
    filter_fields = []
    for f in fields:
        if request.form.get(f"filter_{f}") == "1":
            op = request.form.get(f"filter_op_{f}") or "="
            kind = _field_kind(dataset, f)
            if kind == "date" and op == "between":
                v1 = (request.form.get(f"filter_val_start_{f}") or "").strip()
                v2 = (request.form.get(f"filter_val_end_{f}") or "").strip()
                val = f"{v1},{v2}"
            else:
                val = (request.form.get(f"filter_val_{f}") or "").strip()
            filter_fields.append({"field": f, "op": op, "value": val})
    group_fields = [f for f in fields if request.form.get(f"group_{f}") == "1"]
    sort_fields = []
    for f in fields:
        dirv = request.form.get(f"sort_{f}")
        if dirv in ("asc", "desc"):
            sort_fields.append({"field": f, "dir": dirv})
    return include_fields, filter_fields, group_fields, sort_fields


@bp.route("/")
@login_required
def list_reports():
    templates = ReportTemplate.query.order_by(ReportTemplate.created_at.desc()).all()
    return render_template("reports_list.html", templates=templates)


@bp.route("/new", methods=["GET", "POST"])
@login_required
def new_report():
    dataset = request.form.get("dataset") or "rgritten"
    fields = _dataset_fields(dataset)
    field_kinds = {f: _field_kind(dataset, f) for f in fields}

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        row_limit = request.form.get("row_limit", type=int)
        if not row_limit or row_limit < 1:
            row_limit = DEFAULT_REPORT_ROW_LIMIT
        if not name:
            flash("Geef een rapportnaam op", "warning")
            return render_template(
                "reports_form.html",
                dataset=dataset,
                fields=fields,
                field_kinds=field_kinds,
                name=name,
                row_limit=row_limit,
                include_set=set(),
                filter_set=set(),
                filter_map={},
                group_set=set(),
                sort_map={},
                mode="new",
            )

        include_fields, filter_fields, group_fields, sort_fields = _parse_form(fields, dataset)
        if not include_fields:
            flash("Kies minimaal één veld om op te nemen", "warning")
            return render_template(
                "reports_form.html",
                dataset=dataset,
                fields=fields,
                field_kinds=field_kinds,
                name=name,
                row_limit=row_limit,
                include_set=set(include_fields),
                filter_set={f["field"] for f in filter_fields},
                filter_map={f["field"]: f for f in filter_fields},
                group_set=set(group_fields),
                sort_map={s["field"]: s["dir"] for s in sort_fields},
                mode="new",
            )

        tmpl = ReportTemplate(
            name=name,
            dataset=dataset,
            row_limit=row_limit,
            include_fields=include_fields,
            filter_fields=filter_fields,
            group_fields=group_fields,
            sort_fields=sort_fields,
        )
        db.session.add(tmpl)
        db.session.commit()
        flash("Rapporttemplate opgeslagen", "success")
        return redirect(url_for("reports.list_reports"))

    return render_template(
        "reports_form.html",
        dataset=dataset,
        fields=fields,
        field_kinds=field_kinds,
        name="",
        row_limit=DEFAULT_REPORT_ROW_LIMIT,
        include_set=set(),
        filter_set=set(),
        filter_map={},
        group_set=set(),
        sort_map={},
        mode="new",
    )


@bp.route("/<int:template_id>/edit", methods=["GET", "POST"])
@login_required
def edit_report(template_id):
    tmpl = db.session.get(ReportTemplate, template_id)
    if not tmpl:
        flash("Template niet gevonden", "warning")
        return redirect(url_for("reports.list_reports"))
    dataset = tmpl.dataset
    fields = _dataset_fields(dataset)
    field_kinds = {f: _field_kind(dataset, f) for f in fields}
    current_limit = tmpl.row_limit or DEFAULT_REPORT_ROW_LIMIT

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        row_limit = request.form.get("row_limit", type=int)
        if not row_limit or row_limit < 1:
            row_limit = DEFAULT_REPORT_ROW_LIMIT
        include_fields, filter_fields, group_fields, sort_fields = _parse_form(fields, dataset)
        if not name:
            flash("Geef een rapportnaam op", "warning")
        elif not include_fields:
            flash("Kies minimaal één veld om op te nemen", "warning")
        else:
            tmpl.name = name
            tmpl.row_limit = row_limit
            tmpl.include_fields = include_fields
            tmpl.filter_fields = filter_fields
            tmpl.group_fields = group_fields
            tmpl.sort_fields = sort_fields
            db.session.commit()
            flash("Template bijgewerkt", "success")
            return redirect(url_for("reports.list_reports"))
    else:
        row_limit = current_limit

    return render_template(
        "reports_form.html",
        dataset=dataset,
        fields=fields,
        field_kinds=field_kinds,
        name=tmpl.name,
        row_limit=row_limit,
        include_set=set(tmpl.include_fields or []),
        filter_set={f["field"] for f in tmpl.filter_fields or [] if isinstance(f, dict)},
        filter_map={f["field"]: f for f in tmpl.filter_fields or [] if isinstance(f, dict)},
        group_set=set(tmpl.group_fields or []),
        sort_map={s.get("field"): s.get("dir") for s in (tmpl.sort_fields or [])},
        mode="edit",
        template_id=template_id,
    )


def _apply_filters(query, dataset, template):
    if dataset != "rgritten":
        return query
    # Apply saved filters
    for fdef in template.filter_fields or []:
        if isinstance(fdef, str):
            # legacy: ignore without value
            continue
        field = fdef.get("field")
        op = (fdef.get("op") or "=").lower()
        val = fdef.get("value")
        col = getattr(RGRit, field, None)
        if col is None:
            continue
        kind = _field_kind(dataset, field)
        if kind == "date":
            if op == "is_null":
                query = query.filter(col.is_(None))
            elif op == "not_null":
                query = query.filter(col.isnot(None))
            else:
                # treat val as date-only or datetime; apply equality/range depending on op
                if op in ("=", "!=", "<", "<=", ">", ">="):
                    if not val:
                        continue
                    dt = _parse_date_value(val, as_end=(op in ("<=", "<")))
                    if dt:
                        if op == "=":
                            query = query.filter(func.date(col) == dt.date())
                        elif op == "!=":
                            query = query.filter(func.date(col) != dt.date())
                        elif op == "<":
                            query = query.filter(col < dt)
                        elif op == "<=":
                            query = query.filter(col <= dt)
                        elif op == ">":
                            query = query.filter(col > dt)
                        elif op == ">=":
                            query = query.filter(col >= dt)
                elif op == "between":
                    parts = (val or "").split(",")
                    if len(parts) == 2:
                        start = _parse_date_value(parts[0], as_end=False)
                        end = _parse_date_value(parts[1], as_end=True)
                        if start:
                            query = query.filter(col >= start)
                        if end:
                            query = query.filter(col <= end)
        elif kind == "time":
            if op == "is_null":
                query = query.filter(col.is_(None))
            elif op == "not_null":
                query = query.filter(col.isnot(None))
            elif op == "between":
                parts = (val or "").split(",")
                if len(parts) == 2:
                    t1 = _parse_time_value(parts[0])
                    t2 = _parse_time_value(parts[1])
                    if t1:
                        query = query.filter(col >= t1)
                    if t2:
                        query = query.filter(col <= t2)
            else:
                tval = _parse_time_value(val)
                if not tval:
                    continue
                if op == "!=":
                    query = query.filter(col != tval)
                elif op == ">":
                    query = query.filter(col > tval)
                elif op == ">=":
                    query = query.filter(col >= tval)
                elif op == "<":
                    query = query.filter(col < tval)
                elif op == "<=":
                    query = query.filter(col <= tval)
                else:
                    query = query.filter(col == tval)
        else:
            if op == "is_null":
                query = query.filter(col.is_(None))
            elif op == "not_null":
                query = query.filter(col.isnot(None))
            elif op == "like":
                if val:
                    query = query.filter(col.like(f"%{val}%"))
            elif op == "not_like":
                if val:
                    query = query.filter(~col.like(f"%{val}%"))
            elif op == "!=":
                if val:
                    query = query.filter(col != val)
            elif op == ">":
                if val:
                    query = query.filter(col > val)
            elif op == ">=":
                if val:
                    query = query.filter(col >= val)
            elif op == "<":
                if val:
                    query = query.filter(col < val)
            elif op == "<=":
                if val:
                    query = query.filter(col <= val)
            else:  # "=" default
                if val:
                    query = query.filter(col == val)

    # Apply runtime (ad-hoc) filters from request args
    for fdef in template.filter_fields or []:
        field = fdef.get("field") if isinstance(fdef, dict) else fdef
        col = getattr(RGRit, field, None)
        if col is None:
            continue
        kind = _field_kind(dataset, field)
        if kind == "date":
            raw_start = request.args.get(f"rt_from_{field}")
            raw_end = request.args.get(f"rt_to_{field}")
            # If input is date-only, compare on DATE(col)
            if (raw_start and len(raw_start) == 10) or (raw_end and len(raw_end) == 10):
                try:
                    if raw_start:
                        ds = date.fromisoformat(raw_start)
                        query = query.filter(func.date(col) >= ds)
                    if raw_end:
                        de = date.fromisoformat(raw_end)
                        query = query.filter(func.date(col) <= de)
                except ValueError:
                    pass
            else:
                if raw_start:
                    start = _parse_date_value(raw_start, as_end=False)
                    if start:
                        query = query.filter(col >= start)
                if raw_end:
                    end = _parse_date_value(raw_end, as_end=True)
                    if end:
                        query = query.filter(col <= end)
        elif kind == "time":
            op = (request.args.get(f"rt_op_{field}") or "=").lower()
            if op in ("is_null", "not_null", "between", "=", "!=", ">", ">=", "<", "<="):
                val = request.args.get(f"rt_val_{field}")
                if op == "is_null":
                    query = query.filter(col.is_(None))
                elif op == "not_null":
                    query = query.filter(col.isnot(None))
                elif op == "between":
                    parts = (val or "").split(",")
                    if len(parts) == 2:
                        t1 = _parse_time_value(parts[0])
                        t2 = _parse_time_value(parts[1])
                        if t1:
                            query = query.filter(col >= t1)
                        if t2:
                            query = query.filter(col <= t2)
                else:
                    tval = _parse_time_value(val)
                    if tval:
                        if op == "!=":
                            query = query.filter(col != tval)
                        elif op == ">":
                            query = query.filter(col > tval)
                        elif op == ">=":
                            query = query.filter(col >= tval)
                        elif op == "<":
                            query = query.filter(col < tval)
                        elif op == "<=":
                            query = query.filter(col <= tval)
                        else:
                            query = query.filter(col == tval)
        else:
            op = (request.args.get(f"rt_op_{field}") or "=").lower()
            val = request.args.get(f"rt_val_{field}")
            if (not val or val == "") and op not in ("is_null", "not_null"):
                continue
            if op == "is_null":
                query = query.filter(col.is_(None))
            elif op == "not_null":
                query = query.filter(col.isnot(None))
            elif op == "like":
                query = query.filter(col.like(f"%{val}%"))
            elif op == "not_like":
                query = query.filter(~col.like(f"%{val}%"))
            elif op == "!=":
                query = query.filter(col != val)
            elif op == ">":
                query = query.filter(col > val)
            elif op == ">=":
                query = query.filter(col >= val)
            elif op == "<":
                query = query.filter(col < val)
            elif op == "<=":
                query = query.filter(col <= val)
            else:  # "=" or unspecified
                if val is not None:
                    query = query.filter(col == val)
    return query


def _apply_sort(query, dataset, template):
    if dataset != "rgritten":
        return query
    orders = []
    for item in template.sort_fields or []:
        col = getattr(RGRit, item.get("field", ""), None)
        if col is None:
            continue
        if item.get("dir") == "desc":
            orders.append(desc(col))
        else:
            orders.append(asc(col))
    if template.group_fields:
        for gf in template.group_fields:
            col = getattr(RGRit, gf, None)
            if col is not None:
                orders.insert(0, asc(col))
    if orders:
        query = query.order_by(*orders)
    return query


@bp.route("/<int:template_id>/run")
@login_required
def run_report(template_id):
    tmpl = db.session.get(ReportTemplate, template_id)
    if not tmpl:
        flash("Template niet gevonden", "warning")
        return redirect(url_for("reports.list_reports"))
    if tmpl.dataset != "rgritten":
        flash("Onbekende dataset", "warning")
        return redirect(url_for("reports.list_reports"))

    fields = tmpl.include_fields or []
    query = db.session.query(RGRit)
    query = _apply_filters(query, tmpl.dataset, tmpl)
    query = _apply_sort(query, tmpl.dataset, tmpl)

    base_limit = tmpl.row_limit or DEFAULT_REPORT_ROW_LIMIT
    limit_arg = request.args.get("limit", type=int)
    row_limit = base_limit if not limit_arg or limit_arg < 1 else limit_arg

    fmt = request.args.get("format")
    rows = query.limit(row_limit).all()

    if fmt == "csv":
        buf = StringIO()
        header = fields
        buf.write(",".join(header) + "\n")
        for r in rows:
            values = []
            for f in header:
                if f == "reistijd_calc":
                    v = _calc_value(f, r)
                else:
                    v = _format_value(f, r)
                values.append('' if v is None else str(v))
            buf.write(",".join(values) + "\n")
        buf.seek(0)
        return send_file(
            BytesIO(buf.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{tmpl.name}.csv",
        )
    if fmt == "xlsx":
        try:
            import openpyxl
        except ImportError:
            flash("openpyxl niet geïnstalleerd voor xlsx export", "warning")
            return redirect(url_for("reports.run_report", template_id=template_id))
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(fields)
        for r in rows:
            rowvals = []
            for f in fields:
                if f == "reistijd_calc":
                    rowvals.append(_calc_value(f, r))
                else:
                    rowvals.append(_format_value(f, r))
            ws.append(rowvals)
        out = BytesIO()
        wb.save(out)
        out.seek(0)
        return send_file(
            out,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"{tmpl.name}.xlsx",
        )

    # default HTML view
    filter_meta = {}
    for fdef in (tmpl.filter_fields or []):
        fname = fdef["field"] if isinstance(fdef, dict) else fdef
        filter_meta[fname] = _field_kind(tmpl.dataset, fname)

    return render_template(
        "reports_run.html",
        template=tmpl,
        fields=fields,
        rows=[
            {
                "cols": {f: (_calc_value("reistijd_calc", r) if f == "reistijd_calc" else _format_value(f, r)) for f in fields},
            }
            for r in rows
        ],
        filter_meta=filter_meta,
        row_limit=row_limit,
    )
