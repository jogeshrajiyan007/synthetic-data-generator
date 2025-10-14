import json
import re
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import timedelta
import inspect

# -----------------------
# Configuration / Seeding
# -----------------------
np.random.seed(42)
random.seed(42)

# -----------------------
# Helpers
# -----------------------
def load_generators(path="generators.json"):
    with open(path) as f:
        return json.load(f)["generators"]

def load_schema(path="schema.json"):
    with open(path) as f:
        return json.load(f)

def enforce_uniqueness(values, regenerate_fn):
    seen, result = set(), []
    for v in values:
        while v in seen:
            v = regenerate_fn()
        seen.add(v)
        result.append(v)
    return result

def sanitize_name(name):
    if not name:
        return ""
    match = re.match(r"[A-Za-z0-9]+", name)
    return match.group(0) if match else ""

# -----------------------
# Core generator
# -----------------------
def apply_generator(gen_name, generators, n, overrides=None, row_context=None, ctx=None):
    if gen_name not in generators:
        raise KeyError(f"Generator '{gen_name}' not found")
    gen_config = dict(generators[gen_name])
    if overrides:
        for k, v in overrides.items():
            if k == "params":
                gen_config["params"] = {**gen_config.get("params", {}), **v}
            else:
                gen_config[k] = v

    gtype = gen_config.get("type")
    params = gen_config.get("params", {}) or {}
    unique = gen_config.get("unique", False)

    # --- Faker-based ---
    if gtype == "faker":
        method_name = gen_config.get("method")
        fake = Faker()

        # Handle date offsets
        start_col = params.get("start_col")
        if start_col and row_context:
            min_days = int(params.get("min_days", 0))
            max_days = int(params.get("max_days", 365))
            return [
                pd.to_datetime(d) + timedelta(days=random.randint(min_days, max_days))
                for d in row_context[start_col]
            ]

        # Email and gender contextual
        if gen_name == "email" and row_context:
            emails = []
            for i in range(n):
                first = row_context.get("first_name", [None]*n)[i]
                last = row_context.get("last_name", [None]*n)[i]
                full = row_context.get("name", [None]*n)[i]
                if not first and full:
                    parts = full.split(" ", 1)
                    first, last = parts[0], parts[1] if len(parts)>1 else ""
                if not first:
                    first = fake.first_name()
                if not last:
                    last = fake.last_name()
                first_word = sanitize_name(first)
                last_word = sanitize_name(last)
                email_user = f"{first_word.lower()}.{last_word.lower()}"
                emails.append(f"{email_user}@{fake.free_email_domain()}")
            return emails

        if gen_name == "gender" and row_context:
            genders = []
            for i in range(n):
                first = row_context.get("first_name", [None]*n)[i]
                full = row_context.get("name", [None]*n)[i]
                _ = first or (full.split(" ")[0] if full else "")
                genders.append(random.choice(["Male","Female"]))
            return genders

        # Normal faker call
        method = getattr(fake, method_name)
        valid_params = {}
        if params:
            sig = inspect.signature(method)
            for k in params:
                if k in sig.parameters:
                    valid_params[k] = params[k]
        values = [method(**valid_params) if valid_params else method() for _ in range(n)]
        if unique:
            values = enforce_uniqueness(values, lambda: method(**valid_params) if valid_params else method())
        return values

    # --- Numeric / category / sequence / derived ---
    elif gtype in ["int_uniform","random_int"]:
        low, high = int(params.get("min",0)), int(params.get("max",9999))
        if unique and (high-low+1)<n:
            raise ValueError(f"Cannot generate {n} unique ints in range [{low},{high}]")
        if unique:
            return np.random.choice(np.arange(low,high+1), size=n, replace=False).tolist()
        return np.random.randint(low, high+1, n).tolist()

    elif gtype in ["float_uniform","random_float","float_exponential"]:
        dist = params.get("distribution","uniform")
        dec = int(params.get("decimals",2))
        if dist=="exponential" or gen_name=="float_exponential":
            scale = float(params.get("scale",1.0))
            vals = np.random.exponential(scale,n)
        else:
            low, high = float(params.get("min",0.0)), float(params.get("max",1.0))
            vals = np.random.uniform(low,high,n)
        return np.round(vals,dec).tolist()

    elif gtype in ["category","category_generic"]:
        vals = params.get("values") or gen_config.get("values")
        if not vals:
            raise ValueError(f"Category generator '{gen_name}' missing 'values'")
        weights = params.get("weights")
        if weights:
            weights = np.array(weights,dtype=float)
            weights /= weights.sum()
        return np.random.choice(vals,size=n,p=weights).tolist()

    elif gtype=="sequence":
        start, step = int(params.get("start",1)), int(params.get("step",1))
        return [start+i*step for i in range(n)]

    elif gtype=="derived":
        expr = gen_config.get("expression") or params.get("expression")
        if not expr or row_context is None:
            raise ValueError("Derived generator requires expression and row_context")
        out = []
        for i in range(n):
            local_vars = {k: row_context[k][i] for k in row_context}
            v = eval(expr, {"np": np, "random": random}, local_vars)
            out.append(v)
        dec = params.get("decimals")
        if dec:
            out = np.round(out,int(dec)).tolist()
        else:
            out = np.round(out,2).tolist()
        return out

    else:
        raise ValueError(f"Unsupported generator type: {gtype}")

# -----------------------
# FK helpers
# -----------------------
def gen_fk(n, spec, ctx):
    ref_table, ref_col = spec["ref"].split(".")
    ref_values = ctx["tables"][ref_table][ref_col].tolist()
    return np.random.choice(ref_values,n).tolist()

def gen_fk_value(col_spec, data, ctx, n):
    ref_table, ref_column, via_col = col_spec["ref_table"], col_spec["ref_column"], col_spec["via"]
    ref_df = ctx["tables"][ref_table]
    out = []
    for i in range(n):
        key = data[via_col][i]
        matches = ref_df.loc[ref_df[via_col]==key, ref_column].values
        out.append(matches[0] if len(matches)>0 else None)
    return out

# -----------------------
# Composite groups
# -----------------------
def generate_composite_group(group_name, group_spec, n, generators):
    params = group_spec.get("params",{}) or {}
    locales = params.get("locales", ["en_US","en_GB","en_IN","de_DE","fr_FR"])
    faker_pool = [Faker(l) for l in locales]

    records = []
    for _ in range(n):
        f = random.choice(faker_pool)
        rec = {}
        if group_name=="person_profile":
            gender_choice = random.choice(["Male","Female"])
            try:
                first = f.first_name_male() if gender_choice=="Male" else f.first_name_female()
            except:
                first = f.first_name()
            last = f.last_name()
            rec.update({
                "first_name": first,
                "last_name": last,
                "email": f"{sanitize_name(first).lower()}.{sanitize_name(last).lower()}@{f.free_email_domain()}",
                "gender": gender_choice,
                "phone_number": f.phone_number(),
                "date_of_birth": f.date_of_birth(minimum_age=params.get("min_age",18), maximum_age=params.get("max_age",80)),
                "ssn": f.ssn() if hasattr(f,"ssn") else None,
                "job": f.job() if hasattr(f,"job") else None,
                "company": f.company() if hasattr(f,"company") else None
            })
        elif group_name=="address_profile":
            rec.update({
                "street_address": f.street_address(),
                "secondary_address": f.secondary_address() if hasattr(f,"secondary_address") else f.building_number(),
                "city": f.city(),
                "state": f.state() if hasattr(f,"state") else None,
                "country": f.country() if hasattr(f,"country") else None,
                "post_code": f.postcode() if hasattr(f,"postcode") else f.zipcode(),
                "latitude": f.latitude() if hasattr(f,"latitude") else None,
                "longitude": f.longitude() if hasattr(f,"longitude") else None
            })
        else:
            raise ValueError(f"Unknown composite group '{group_name}'")
        records.append(rec)
    return records

# -----------------------
# Table generation
# -----------------------
def generate_table(name, spec, generators, ctx):
    n = int(spec.get("rows",100))
    data = {}

    # Composite groups
    composite_groups = {}
    for col,col_spec in spec["columns"].items():
        group = col_spec.get("composite_group")
        if group:
            composite_groups.setdefault(group,[]).append((col,col_spec))

    for group_name,cols in composite_groups.items():
        group_spec = generators[group_name]
        records = generate_composite_group(group_name,group_spec,n,generators)
        for col,col_spec in cols:
            field_name = col_spec.get("field")
            data[col] = [rec.get(field_name) for rec in records]

    # Normal columns
    for col,col_spec in spec["columns"].items():
        if col in data: continue
        if "generator" in col_spec and generators[col_spec["generator"]]["type"]!="derived":
            data[col] = apply_generator(col_spec["generator"],generators,n,overrides=col_spec,row_context=data,ctx=ctx)
        elif col_spec.get("type")=="fk":
            data[col] = gen_fk(n,col_spec,ctx)
        elif col_spec.get("type")=="fk_value":
            continue
        else:
            data[col] = [None]*n

    for col,col_spec in spec["columns"].items():
        if col_spec.get("type")=="fk_value":
            data[col] = gen_fk_value(col_spec,data,ctx,n)

    for col,col_spec in spec["columns"].items():
        if "generator" in col_spec and generators[col_spec["generator"]]["type"]=="derived":
            data[col] = apply_generator(col_spec["generator"],generators,n,overrides=col_spec,row_context=data,ctx=ctx)

    df = pd.DataFrame(data)
    ctx["tables"][name] = df
    return df

# -----------------------
# Entrypoint
# -----------------------
def generate_all(schema,generators):
    ctx = {"tables":{}}
    for tname,tspec in schema["tables"].items():
        print(f"Generating {tname} ({tspec.get('rows',0)} rows)...")
        generate_table(tname,tspec,generators,ctx)
    return ctx["tables"]

if __name__=="__main__":
    generators = load_generators("generators.json")
    schema = load_schema("schema.json")
    tables = generate_all(schema,generators)
    for name,df in tables.items():
        df.to_csv(f"{name}.csv",index=False)
        print(f"[✔] {name}.csv generated ({len(df)} rows)")
