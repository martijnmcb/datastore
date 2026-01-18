from io import StringIO, BytesIO
from datetime import datetime, date, time
from flask import render_template, request, redirect, url_for, flash, send_file, Response, stream_with_context
from flask_login import login_required
from sqlalchemy import asc, desc, func
from extensions import db
from models import RGRit, ReportTemplate
from . import bp

DEFAULT_REPORT_ROW_LIMIT = 1000
PIVOT_AGGS = ("count", "sum", "avg", "min", "max")


def _dataset_fields(dataset):
    if dataset == "rgritten":
        cols = [c.key for c in RGRit.__table__.columns]
        base = [c for c in cols if c not in ("id", "ingested_at")]
        base.append("reistijd_calc")
        base.append("locatie")
        return base
    return []


def _field_kind(dataset, field):
    if dataset != "rgritten":
        return "text"
    col = getattr(RGRit, field, None)
    if not col:
        if field == "reistijd_calc":
            return "text"
        if field == "locatie":
            return "text"
        return "text"
    from sqlalchemy import Date, DateTime, Float, Integer, Numeric, Time

    if isinstance(col.type, (Date, DateTime)):
        return "date"
    if isinstance(col.type, Time):
        return "time"
    if isinstance(col.type, (Integer, Numeric, Float)):
        return "number"
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
    if field == "locatie":
        aankomst = getattr(row, "aankomst", None)
        if aankomst is not None and str(aankomst).strip() != "":
            return getattr(row, "locatie_naar", None)
        return getattr(row, "locatie_van", None)
    return None


def _format_value(field, row):
    val = getattr(row, field, None)
    if val is None:
        return ""
    if field == "ritdatum" and hasattr(val, "date"):
        return val.date().isoformat()
    return val


def _report_value(field, row):
    if field in ("reistijd_calc", "locatie"):
        return _calc_value(field, row)
    return _format_value(field, row)


def _format_scalar(dataset, field, value):
    if value is None:
        return ""
    kind = _field_kind(dataset, field)
    if kind == "date" and hasattr(value, "date"):
        return value.date().isoformat()
    if kind == "time" and isinstance(value, time):
        return value.isoformat()
    return value


def _build_pivot_table(dataset, query, row_fields, col_field, value_defs, row_limit):
    if dataset != "rgritten":
        return [], []
    if not row_fields or not col_field or not value_defs:
        return [], []

    pivot_fields = row_fields + [col_field]
    row_cols = []
    for f in pivot_fields:
        col = getattr(RGRit, f, None)
        if col is None:
            return [], []
        row_cols.append(col)

    cleaned_values = []
    value_cols = []
    for vdef in value_defs:
        field = vdef.get("field")
        agg = vdef.get("agg")
        col = getattr(RGRit, field, None)
        if col is None:
            continue
        if agg in ("sum", "avg", "min", "max") and _field_kind(dataset, field) != "number":
            continue
        cleaned_values.append({"field": field, "agg": agg, "label": (vdef.get("label") or "").strip()})
        value_cols.append(col)

    if not cleaned_values:
        return [], []

    base_query = query.with_entities(*(row_cols + value_cols))
    if row_limit:
        base_query = base_query.limit(row_limit)
    subq = base_query.subquery()

    row_sel_cols = [subq.c[f] for f in row_fields]
    col_sel = subq.c[col_field]
    select_cols = row_sel_cols + [col_sel]
    agg_defs = []
    for vdef in cleaned_values:
        field = vdef["field"]
        agg = vdef["agg"]
        label = (vdef.get("label") or "").strip()
        col = subq.c[field]
        if agg == "count":
            expr = func.count(col)
        elif agg == "sum":
            expr = func.sum(col)
        elif agg == "avg":
            expr = func.avg(col)
        elif agg == "min":
            expr = func.min(col)
        else:
            expr = func.max(col)
        agg_label = f"{agg}_{field}"
        agg_defs.append({"label": label, "field": field, "agg": agg, "agg_label": agg_label})
        select_cols.append(expr.label(agg_label))

    grouped = (
        db.session.query(*select_cols)
        .group_by(*(row_sel_cols + [col_sel]))
        .all()
    )

    col_values = []
    row_map = {}
    row_len = len(row_fields)
    for row in grouped:
        row_key = tuple(row[:row_len])
        col_val = row[row_len]
        agg_vals = list(row[row_len + 1 :])
        row_map.setdefault(row_key, {})[col_val] = agg_vals
        if col_val not in col_values:
            col_values.append(col_val)

    try:
        col_values = sorted(col_values)
    except TypeError:
        col_values = sorted(col_values, key=lambda v: "" if v is None else str(v))

    headers = list(row_fields)
    for col_val in col_values:
        col_label = _format_scalar(dataset, col_field, col_val)
        for adef in agg_defs:
            value_label = adef["label"] or f"{adef['agg']}({adef['field']})"
            headers.append(f"{col_label} {value_label}")

    def row_sort_key(key):
        return ["" if v is None else str(_format_scalar(dataset, f, v)) for f, v in zip(row_fields, key)]

    rows = []
    for row_key in sorted(row_map.keys(), key=row_sort_key):
        row_vals = [_format_scalar(dataset, f, v) for f, v in zip(row_fields, row_key)]
        for col_val in col_values:
            agg_vals = row_map.get(row_key, {}).get(col_val)
            if not agg_vals:
                row_vals.extend([""] * len(agg_defs))
            else:
                row_vals.extend(agg_vals)
        rows.append(row_vals)

    return headers, rows


def _parse_form(fields, dataset):
    field_pos = {f: i for i, f in enumerate(fields)}
    include_fields = []
    include_order_map = {}
    for f in fields:
        if request.form.get(f"include_{f}") == "1":
            include_fields.append(f)
            order_val = request.form.get(f"order_{f}", type=int)
            if order_val and order_val > 0:
                include_order_map[f] = order_val
    if include_order_map:
        include_fields.sort(key=lambda f: (include_order_map.get(f, 10**9), field_pos[f]))
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
    pivot_enabled = request.form.get("pivot_enabled") == "1"
    pivot_fields = [f for f in fields if f not in ("reistijd_calc", "locatie")]
    pivot_row_fields = [f for f in request.form.getlist("pivot_row_fields") if f in pivot_fields]
    pivot_col_field = request.form.get("pivot_col_field") or ""
    if pivot_col_field not in pivot_fields:
        pivot_col_field = ""
    pivot_values = []
    for idx in range(1, 4):
        field = request.form.get(f"pivot_value_field_{idx}") or ""
        agg = request.form.get(f"pivot_value_agg_{idx}") or ""
        label = (request.form.get(f"pivot_value_label_{idx}") or "").strip()
        if field in pivot_fields and agg in PIVOT_AGGS:
            pivot_values.append({"field": field, "agg": agg, "label": label})
    return (
        include_fields,
        include_order_map,
        filter_fields,
        group_fields,
        sort_fields,
        pivot_enabled,
        pivot_row_fields,
        pivot_col_field,
        pivot_values,
    )


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
    pivot_fields = [f for f in fields if f not in ("reistijd_calc", "locatie")]

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
                pivot_fields=pivot_fields,
                field_kinds=field_kinds,
                name=name,
                row_limit=row_limit,
                include_set=set(),
                include_order_map={},
                filter_set=set(),
                filter_map={},
                group_set=set(),
                sort_map={},
                pivot_enabled=False,
                pivot_row_fields=[],
                pivot_col_field="",
                pivot_values=[],
                pivot_aggs=PIVOT_AGGS,
                mode="new",
            )

        (
            include_fields,
            include_order_map,
            filter_fields,
            group_fields,
            sort_fields,
            pivot_enabled,
            pivot_row_fields,
            pivot_col_field,
            pivot_values,
        ) = _parse_form(fields, dataset)
        pivot_valid = bool(pivot_row_fields and pivot_col_field and pivot_values)
        if pivot_enabled and not pivot_valid:
            flash("Pivot vereist rijen, een kolom en minimaal één waarde", "warning")
            return render_template(
                "reports_form.html",
                dataset=dataset,
                fields=fields,
                pivot_fields=pivot_fields,
                field_kinds=field_kinds,
                name=name,
                row_limit=row_limit,
                include_set=set(include_fields),
                include_order_map=include_order_map,
                filter_set={f["field"] for f in filter_fields},
                filter_map={f["field"]: f for f in filter_fields},
                group_set=set(group_fields),
                sort_map={s["field"]: s["dir"] for s in sort_fields},
                pivot_enabled=pivot_enabled,
                pivot_row_fields=pivot_row_fields,
                pivot_col_field=pivot_col_field,
                pivot_values=pivot_values,
                pivot_aggs=PIVOT_AGGS,
                mode="new",
            )
        if not include_fields and not pivot_enabled:
            flash("Kies minimaal één veld om op te nemen", "warning")
            return render_template(
                "reports_form.html",
                dataset=dataset,
                fields=fields,
                pivot_fields=pivot_fields,
                field_kinds=field_kinds,
                name=name,
                row_limit=row_limit,
                include_set=set(include_fields),
                include_order_map=include_order_map,
                filter_set={f["field"] for f in filter_fields},
                filter_map={f["field"]: f for f in filter_fields},
                group_set=set(group_fields),
                sort_map={s["field"]: s["dir"] for s in sort_fields},
                pivot_enabled=pivot_enabled,
                pivot_row_fields=pivot_row_fields,
                pivot_col_field=pivot_col_field,
                pivot_values=pivot_values,
                pivot_aggs=PIVOT_AGGS,
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
            pivot_enabled=pivot_enabled,
            pivot_row_fields=pivot_row_fields,
            pivot_col_field=pivot_col_field or None,
            pivot_values=pivot_values,
        )
        db.session.add(tmpl)
        db.session.commit()
        flash("Rapporttemplate opgeslagen", "success")
        return redirect(url_for("reports.list_reports"))

    return render_template(
        "reports_form.html",
        dataset=dataset,
        fields=fields,
        pivot_fields=pivot_fields,
        field_kinds=field_kinds,
        name="",
        row_limit=DEFAULT_REPORT_ROW_LIMIT,
        include_set=set(),
        include_order_map={},
        filter_set=set(),
        filter_map={},
        group_set=set(),
        sort_map={},
        pivot_enabled=False,
        pivot_row_fields=[],
        pivot_col_field="",
        pivot_values=[],
        pivot_aggs=PIVOT_AGGS,
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
    pivot_fields = [f for f in fields if f not in ("reistijd_calc", "locatie")]
    current_limit = tmpl.row_limit or DEFAULT_REPORT_ROW_LIMIT

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        row_limit = request.form.get("row_limit", type=int)
        if not row_limit or row_limit < 1:
            row_limit = DEFAULT_REPORT_ROW_LIMIT
        (
            include_fields,
            include_order_map,
            filter_fields,
            group_fields,
            sort_fields,
            pivot_enabled,
            pivot_row_fields,
            pivot_col_field,
            pivot_values,
        ) = _parse_form(fields, dataset)
        if not name:
            flash("Geef een rapportnaam op", "warning")
        elif pivot_enabled and not (pivot_row_fields and pivot_col_field and pivot_values):
            flash("Pivot vereist rijen, een kolom en minimaal één waarde", "warning")
        elif not include_fields and not pivot_enabled:
            flash("Kies minimaal één veld om op te nemen", "warning")
        else:
            tmpl.name = name
            tmpl.row_limit = row_limit
            tmpl.include_fields = include_fields
            tmpl.filter_fields = filter_fields
            tmpl.group_fields = group_fields
            tmpl.sort_fields = sort_fields
            tmpl.pivot_enabled = pivot_enabled
            tmpl.pivot_row_fields = pivot_row_fields
            tmpl.pivot_col_field = pivot_col_field or None
            tmpl.pivot_values = pivot_values
            db.session.commit()
            flash("Template bijgewerkt", "success")
            return redirect(url_for("reports.list_reports"))
    else:
        row_limit = current_limit

    return render_template(
        "reports_form.html",
        dataset=dataset,
        fields=fields,
        pivot_fields=pivot_fields,
        field_kinds=field_kinds,
        name=tmpl.name,
        row_limit=row_limit,
        include_set=set(tmpl.include_fields or []),
        include_order_map={f: i + 1 for i, f in enumerate(tmpl.include_fields or [])},
        filter_set={f["field"] for f in tmpl.filter_fields or [] if isinstance(f, dict)},
        filter_map={f["field"]: f for f in tmpl.filter_fields or [] if isinstance(f, dict)},
        group_set=set(tmpl.group_fields or []),
        sort_map={s.get("field"): s.get("dir") for s in (tmpl.sort_fields or [])},
        pivot_enabled=bool(tmpl.pivot_enabled),
        pivot_row_fields=tmpl.pivot_row_fields or [],
        pivot_col_field=tmpl.pivot_col_field or "",
        pivot_values=tmpl.pivot_values or [],
        pivot_aggs=PIVOT_AGGS,
        mode="edit",
        template_id=template_id,
    )


@bp.route("/<int:template_id>/duplicate", methods=["POST"])
@login_required
def duplicate_report(template_id):
    tmpl = db.session.get(ReportTemplate, template_id)
    if not tmpl:
        flash("Template niet gevonden", "warning")
        return redirect(url_for("reports.list_reports"))
    base_name = (tmpl.name or "Rapport").strip()
    suffix = " (kopie)"
    new_name = (base_name + suffix)[:255]
    copy_tmpl = ReportTemplate(
        name=new_name,
        dataset=tmpl.dataset,
        row_limit=tmpl.row_limit,
        include_fields=list(tmpl.include_fields or []),
        filter_fields=list(tmpl.filter_fields or []),
        group_fields=list(tmpl.group_fields or []),
        sort_fields=list(tmpl.sort_fields or []),
        pivot_enabled=bool(tmpl.pivot_enabled),
        pivot_row_fields=list(tmpl.pivot_row_fields or []),
        pivot_col_field=tmpl.pivot_col_field,
        pivot_values=list(tmpl.pivot_values or []),
    )
    db.session.add(copy_tmpl)
    db.session.commit()
    flash("Template gekopieerd", "success")
    return redirect(url_for("reports.edit_report", template_id=copy_tmpl.id))


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

    dataset_fields = _dataset_fields(tmpl.dataset)
    fields = tmpl.include_fields or []
    query = db.session.query(RGRit)
    query = _apply_filters(query, tmpl.dataset, tmpl)
    query = _apply_sort(query, tmpl.dataset, tmpl)

    base_limit = tmpl.row_limit or DEFAULT_REPORT_ROW_LIMIT
    limit_arg = request.args.get("limit", type=int)
    row_limit = base_limit if not limit_arg or limit_arg < 1 else limit_arg

    fmt = request.args.get("format")
    pivot_fields = [f for f in dataset_fields if f not in ("reistijd_calc", "locatie")]
    pivot_enabled = bool(tmpl.pivot_enabled)
    pivot_row_fields = [f for f in (tmpl.pivot_row_fields or []) if f in pivot_fields]
    pivot_col_field = tmpl.pivot_col_field if tmpl.pivot_col_field in pivot_fields else ""
    pivot_values = [
        v
        for v in (tmpl.pivot_values or [])
        if v.get("field") in pivot_fields and v.get("agg") in PIVOT_AGGS
    ]
    pivot_headers = []
    pivot_rows = []
    if pivot_enabled and pivot_row_fields and pivot_col_field and pivot_values:
        pivot_headers, pivot_rows = _build_pivot_table(
            tmpl.dataset,
            query,
            pivot_row_fields,
            pivot_col_field,
            pivot_values,
            row_limit,
        )
        if not pivot_headers:
            pivot_enabled = False
    else:
        pivot_enabled = False

    if fmt == "csv":
        def generate():
            if pivot_enabled:
                header = pivot_headers
                yield ",".join(header) + "\n"
                for prow in pivot_rows:
                    values = ["" if v is None else str(v) for v in prow]
                    yield ",".join(values) + "\n"
            else:
                header = fields
                yield ",".join(header) + "\n"
                for r in query.limit(row_limit).yield_per(1000):
                    values = []
                    for f in header:
                        v = _report_value(f, r)
                        values.append("" if v is None else str(v))
                    yield ",".join(values) + "\n"

        headers = {"Content-Disposition": f'attachment; filename="{tmpl.name}.csv"'}
        return Response(stream_with_context(generate()), mimetype="text/csv", headers=headers)
    if fmt == "xlsx":
        try:
            import openpyxl
        except ImportError:
            flash("openpyxl niet geïnstalleerd voor xlsx export", "warning")
            return redirect(url_for("reports.run_report", template_id=template_id))
        wb = openpyxl.Workbook(write_only=True)
        ws = wb.create_sheet()
        if pivot_enabled:
            ws.append(pivot_headers)
            for prow in pivot_rows:
                ws.append(prow)
        else:
            ws.append(fields)
            for r in query.limit(row_limit).yield_per(1000):
                rowvals = []
                for f in fields:
                    rowvals.append(_report_value(f, r))
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
    if fmt == "pdf":
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
        except ImportError:
            flash("reportlab niet geïnstalleerd voor pdf export", "warning")
            return redirect(url_for("reports.run_report", template_id=template_id))
        data = []
        if pivot_enabled:
            data = [pivot_headers] + pivot_rows
        else:
            data = [fields]
            for r in query.limit(row_limit).all():
                data.append(
                    [
                        _report_value(f, r)
                        for f in fields
                    ]
                )
        if data:
            styles = getSampleStyleSheet()
            header_style = styles["Normal"]
            header_style.fontSize = 7
            header_style.leading = 8
            data[0] = [Paragraph(str(h), header_style) for h in data[0]]
        out = BytesIO()
        doc = SimpleDocTemplate(out, pagesize=landscape(A4))
        table = Table(data, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f1f1f1")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#999999")),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("WORDWRAP", (0, 0), (-1, 0), True),
                ]
            )
        )
        table_width, _ = table.wrap(doc.width, doc.height)
        if table_width > doc.width:
            col_count = len(data[0]) if data else 1
            col_width = doc.width / max(1, col_count)
            table = Table(data, repeatRows=1, colWidths=[col_width] * col_count)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f1f1f1")),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#999999")),
                        ("FONTSIZE", (0, 0), (-1, -1), 7),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("WORDWRAP", (0, 0), (-1, 0), True),
                    ]
                )
            )
        doc.build([table, Spacer(1, 6)])
        out.seek(0)
        return send_file(
            out,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"{tmpl.name}.pdf",
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
        pivot_enabled=pivot_enabled,
        pivot_headers=pivot_headers,
        pivot_rows=pivot_rows,
        rows=[
            {
                "cols": {f: _report_value(f, r) for f in fields},
            }
            for r in query.limit(row_limit).all()
        ],
        filter_meta=filter_meta,
        row_limit=row_limit,
    )
