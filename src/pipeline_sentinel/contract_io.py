"""Strict, versioned YAML contracts. Validation never connects to a database."""
from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy

import yaml

TYPES = {"integer", "float", "string", "boolean", "datetime"}
RULE_FIELDS = {
    "not_null": {"max_null_ratio"}, "range": {"min", "max"},
    "uniqueness": set(), "freshness": {"max_lag_minutes"},
    "allowed_values": {"values"}, "referential_integrity": {"reference_values"},
    "row_count": {"min_rows", "max_rows"}, "column_compare": {"other", "operator"},
}
COMPARE_OPERATORS = {"<", "<=", ">", ">=", "==", "!="}
_NUMERIC = {"integer", "float"}


class ContractError(ValueError):
    pass


def validate_contract(value: dict) -> dict:
    if not isinstance(value, dict):
        raise ContractError("contract: bir nesne olmalı")
    unknown = set(value) - {"version", "expected_schema", "rules"}
    if unknown:
        raise ContractError(f"contract: bilinmeyen alanlar {sorted(unknown)}")
    if type(value.get("version", 1)) is not int or value.get("version", 1) < 1:
        raise ContractError("version: pozitif tam sayı olmalı")
    schema = value.get("expected_schema")
    if not isinstance(schema, dict) or not schema:
        raise ContractError("expected_schema: en az bir kolon gerekli")
    for name, dtype in schema.items():
        if not isinstance(name, str) or not name or not isinstance(dtype, str) or dtype not in TYPES:
            raise ContractError(f"expected_schema.{name}: geçersiz kolon/tip")
    rules = value.get("rules", [])
    if not isinstance(rules, list):
        raise ContractError("rules: liste olmalı")
    for i, rule in enumerate(rules):
        path = f"rules[{i}]"
        if not isinstance(rule, dict) or not isinstance(rule.get("type"), str) or rule.get("type") not in RULE_FIELDS:
            raise ContractError(f"{path}.type: desteklenmeyen kural")
        kind = rule["type"]
        if kind == "row_count":
            if "column" in rule:
                raise ContractError(f"{path}: row_count kuralı kolon almaz")
        elif not isinstance(rule.get("column"), str) or rule.get("column") not in schema:
            raise ContractError(f"{path}.column: şemada bulunamadı")
        if set(rule) - {"type", "column"} - RULE_FIELDS[kind]:
            raise ContractError(f"{path}: bilinmeyen kural alanı")
        required = {"range": {"min", "max"}, "allowed_values": {"values"},
                    "referential_integrity": {"reference_values"},
                    "column_compare": {"other", "operator"}}.get(kind, set())
        if required - set(rule):
            raise ContractError(f"{path}: zorunlu alan eksik {sorted(required - set(rule))}")
        if kind == "row_count":
            if not {"min_rows", "max_rows"} & set(rule):
                raise ContractError(f"{path}: min_rows veya max_rows gerekli")
            for key in ("min_rows", "max_rows"):
                if key in rule and (type(rule[key]) is not int or rule[key] < 0):
                    raise ContractError(f"{path}.{key}: negatif olmayan tam sayı olmalı")
            if rule.get("min_rows", 0) > rule.get("max_rows", float("inf")):
                raise ContractError(f"{path}: min_rows <= max_rows gerekli")
        if kind == "column_compare":
            other = rule["other"]
            if other not in schema or other == rule["column"] or rule["operator"] not in COMPARE_OPERATORS:
                raise ContractError(f"{path}: other şemada farklı bir kolon, operator ise {sorted(COMPARE_OPERATORS)} olmalı")
            left, right = schema[rule["column"]], schema[other]
            if not (left in _NUMERIC and right in _NUMERIC or left == right == "datetime"):
                raise ContractError(f"{path}: yalnızca sayısal-sayısal veya datetime-datetime karşılaştırılır")
        for key in ("min", "max", "max_null_ratio", "max_lag_minutes"):
            if key in rule and (type(rule[key]) not in (float, int) or not math.isfinite(rule[key])):
                raise ContractError(f"{path}.{key}: sonlu sayı olmalı")
        if kind == "range" and (schema[rule["column"]] not in _NUMERIC or rule["min"] > rule["max"]):
            raise ContractError(f"{path}: sayısal kolon ve min <= max gerekli")
        if not 0 <= rule.get("max_null_ratio", 0) <= 1 or rule.get("max_lag_minutes", 1) <= 0:
            raise ContractError(f"{path}: geçersiz eşik")
        if kind == "freshness" and schema[rule["column"]] != "datetime":
            raise ContractError(f"{path}: datetime kolon gerekli")
        for key in ("values", "reference_values"):
            if key in rule and (not isinstance(rule[key], list) or any(type(v) not in (str, int, float, bool) or (type(v) is float and not math.isfinite(v)) for v in rule[key])):
                raise ContractError(f"{path}.{key}: skaler değer listesi gerekli")
    result = deepcopy(value)
    result.setdefault("version", 1)
    result.setdefault("rules", [])
    return result


def load_contract(content: str) -> dict:
    if len(content) > 200_000:
        raise ContractError("contract: en fazla 200 KB")
    try:
        # Reject duplicate mapping keys instead of silently replacing a rule.
        class UniqueLoader(yaml.SafeLoader):
            pass

        def mapping(loader, node):
            result = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node)
                if not isinstance(key, str) or key in result:
                    raise ContractError(f"satır {key_node.start_mark.line + 1}: yinelenen/geçersiz alan")
                result[key] = loader.construct_object(value_node)
            return result

        UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
        return validate_contract(yaml.load(content, Loader=UniqueLoader))
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        raise ContractError(f"YAML satır {mark.line + 1 if mark else '?'}: geçersiz sözdizimi") from None
    except (RecursionError, TypeError):
        raise ContractError("contract: geçersiz veya döngüsel YAML") from None


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False).encode()).hexdigest()


def at_time(contract: dict, timestamp) -> dict:
    result = deepcopy(contract)
    for rule in result.get("rules", []):
        if rule["type"] == "freshness":
            rule["as_of"] = timestamp
    return result
