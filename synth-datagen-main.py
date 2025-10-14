import json
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import timedelta

fake = Faker()
np.random.seed(42)
random.seed(42)

def load_generators(path="generators.json"):
    with open(path) as f:
        return json.load(f)["generators"]

def load_schema(path="schema.json"):
    with open(path) as f:
        return json.load(f)

def apply_generator(gen_name, generators, n, overrides=None, row_context=None,ctx=None):
    gen_config = generators[gen_name].copy()
    if overrides:
        gen_config.update(overrides)

        if "params" in overrides:
            gen_config["params"] = {**gen_config.get("params",{}), **overrides["params"]}

    gtype = gen_config["type"]
    params = gen_config.get("params",{})
    unique = gen_config.get("unique", False)

    def enforce_uniqueness(values, regenerate_fn):
        seen = set()
        result = []
        for v in values:
            while v in seen:
                v = regenerate_fn()
            seen.add(v)
            result.append(v)
        return result

    if gtype == "faker":
        fk_table = params.get("ref_table")
        fk_column = params.get("ref_column")
        via_col = params.get("via")
        min_days = params.get("min_days",0)
        max_days = params.get("max_days",365)

        if fk_table and fk_column and via_col:
            ref_df = ctx["tables"][fk_table]
            out = []

            for i in range(n):
                fk_value = row_context[via_col][i]
                ref_val = ref_df.loc[ref_df[via_col] == fk_value, fk_column].values[0]
                delta = pd.Timedelta(days=random.randint(min_days,max_days))
                out.append(pd.to_datetime(ref_val) + delta)

            return out

        start_col = params.get("start_col")
        if start_col and row_context:
            start_dates = row_context[start_col]
            out = []
            for d in start_dates:
                delta = timedelta(days=random.randint(min_days, max_days))
                out.append(pd.to_datetime(d) + delta)

            return out
            
        method = getattr(fake, gen_config["method"])
        values = [method(**params) if params else method() for _ in range(n)]

        if unique:
            values = enforce_uniqueness(values, lambda:method(**params) if params else method())
        return values

    elif gtype in ["int_uniform","random_int"]:
        low = params.get("min",0)
        high = params.get("max",9999)
        if unique and (high - low + 1) < n:
            raise ValueError(f"Cannot generate {n} unique ints in range [{low}, {high}]")

        if unique:
            values = np.random.choice(np.arange(low,high +1),size=n,replace=False).tolist()
        else:
            values = np.random.randint(low,high + 1, n).tolist()
            
        return values

    elif gtype in ["float_uniform","random_float","float_exponential"]:
        low = params.get("min",0.0)
        high = params.get("max",1.0)
        dist = params.get("distribution","uniform")
        dec = params.get("decimals",2)

        if dist == "exponential" or gen_name == "float_exponential":
            scale = params.get("scale",1.0)
            vals = np.random.exponential(scale,n)
        else:
            vals = np.random.uniform(low,high,n)
        return np.round(vals,dec).tolist()

    elif gtype in ["category","category_generic"]:
        vals = params.get("values")
        if not vals:
            raise ValueError(f"Category generator '{gen_name}' missing 'values'")
        weights = params.get("weights")
        return np.random.choice(vals, size=n, p=weights if weights else None).tolist()

    elif gtype == "sequence":
        start = params.get("start",1)
        step = params.get("step", 1)
        return list(range(start, start + n * step,step))

    elif gtype == "derived":
        if row_context is None:
            raise ValueError("Derived generator requires row context")
        expr = schema["tables"]["claims"]["columns"]["amount_paid"].get("expression")
        
        values = []
        for i in range(n):
            local_vars= {k: row_context[k][i] for k in row_context}
            values.append(eval(expr, {"np":np,"random":random}, local_vars))
        return np.round(values, 2).tolist()

    else:
        raise ValueError(f"Unsupported generator type: {gen_name}")

def gen_fk(n, spec, ctx):
    ref_table, ref_col = spec["ref"].split(".")
    ref_values = ctx["tables"][ref_table][ref_col].tolist()
    return np.random.choice(ref_values, n).tolist()

def gen_fk_value(col_spec, data, ctx, n):
    ref_table = col_spec["ref_table"]
    ref_column = col_spec["ref_column"]
    via_col = col_spec["via"]
    ref_df = ctx["tables"][ref_table]

    out = []

    for i in range(n):
        key = data[via_col][i]
        val = ref_df.loc[ref_df[via_col] == key, ref_column].values[0]
        out.append(val)
    return out

def generate_table(name, spec, generators, ctx):
    n = spec.get("rows",100)
    data = {}

    

    for col, col_spec in spec["columns"].items():
        if "generator" in col_spec and generators[col_spec["generator"]]["type"] != "derived":
            data[col] = apply_generator(col_spec["generator"],generators,n,overrides=col_spec,row_context=data,ctx=ctx)
        elif col_spec.get("type") == "fk":
            data[col] = gen_fk(n,col_spec,ctx)
        elif col_spec.get("type") == "fk_value":
            continue
        else:
            data[col] = [None] * n

    for col, col_spec in spec["columns"].items():
        if col_spec.get("type") == "fk_value":
            data[col] = gen_fk_value(col_spec, data, ctx, n)

    for col, col_spec in spec["columns"].items():
        if "generator" in col_spec and generators[col_spec["generator"]]["type"] == "derived":
            data[col] = apply_generator(col_spec["generator"], generators, n, overrides=col_spec, row_context=data,ctx=ctx)

    df = pd.DataFrame(data)
    ctx["tables"][name] = df
    return df

def generate_all(schema, generators):
    ctx = {"tables":{}}
    for tname, tspec in schema["tables"].items():
        print(f"Generating {tname} ({tspec.get('rows',0)} rows)...")
        generate_table(tname, tspec, generators, ctx)
    return ctx["tables"]

if __name__ == "__main__":
    generators = load_generators()
    schema = load_schema()
    tables = generate_all(schema, generators)

    for name, df in tables.items():
        df.to_csv(f"{name}.csv", index=False)
        print(f"[✔] {name}.csv generated ({len(df)} rows)")
