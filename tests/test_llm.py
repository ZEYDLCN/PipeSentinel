"""llm.py — ağ çağrısı yapmayan saf testler: env-var kontrolü ve LLM
çıktısının §11.3 kanıt zorunluluğuna karşı doğrulanması/fallback mantığı.

Gerçek Anthropic API çağrısı burada test edilmez (ağ gerektirir, kredi
harcar); yalnızca `_validate_and_merge`'in güvenlik davranışı test edilir.
"""

import pytest

from pipeline_sentinel.llm import _validate_and_merge, is_llm_available
from pipeline_sentinel.rca import IncidentReport


@pytest.fixture
def fallback() -> IncidentReport:
    return IncidentReport(
        dataset="orders",
        run_id="run1",
        summary="deterministik özet",
        severity="critical",
        root_causes=[],
        affected_assets=["payments.amount", "finance_dashboard"],
        recommended_actions=[{"type": "sql_patch", "description": "x", "requires_approval": True}],
        uncertainties=[],
    )


def test_is_llm_available_false_without_env(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert is_llm_available() is False


def test_is_llm_available_true_with_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    assert is_llm_available() is True


def test_validate_rejects_non_dict(fallback):
    assert _validate_and_merge("not a dict", fallback, known_evidence_ids=set()) is None
    assert _validate_and_merge(["a", "list"], fallback, known_evidence_ids=set()) is None
    assert _validate_and_merge(None, fallback, known_evidence_ids=set()) is None


def test_validate_rejects_missing_summary(fallback):
    raw = {"root_causes": []}
    assert _validate_and_merge(raw, fallback, known_evidence_ids=set()) is None


def test_validate_rejects_empty_summary(fallback):
    raw = {"summary": "   ", "root_causes": []}
    assert _validate_and_merge(raw, fallback, known_evidence_ids=set()) is None


def test_validate_strips_hallucinated_evidence_and_flags_uncertain(fallback):
    raw = {
        "summary": "LLM özeti",
        "root_causes": [
            {
                "hypothesis": "hayali bir iddia",
                "confidence": 0.9,
                "evidence_ids": ["does-not-exist"],
                "counter_evidence": [],
            }
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1", "real-2"})
    # Tek hipotez de hayali kanıtlıysa -> hiçbir doğrulanabilir hipotez yok -> None (fallback)
    assert result is None


def test_validate_keeps_valid_hypothesis_and_drops_hallucinated_one(fallback):
    raw = {
        "summary": "LLM özeti",
        "root_causes": [
            {"hypothesis": "gerçek kanıtlı", "confidence": 0.9, "evidence_ids": ["real-1"], "counter_evidence": []},
            {"hypothesis": "hayali kanıtlı", "confidence": 0.8, "evidence_ids": ["fake-id"], "counter_evidence": []},
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1", "real-2"})
    assert result is not None
    assert len(result.root_causes) == 1
    assert result.root_causes[0].hypothesis == "gerçek kanıtlı"
    assert any("[doğrulanmamış çıkarım]" in u and "hayali kanıtlı" in u for u in result.uncertainties)


def test_validate_preserves_deterministic_fields(fallback):
    """severity / affected_assets / recommended_actions LLM tarafından
    değiştirilemez — her zaman fallback'ten (deterministik katman) gelir."""
    raw = {
        "summary": "LLM özeti",
        "severity": "low",  # LLM bunu değiştirmeye çalışsa da yok sayılmalı
        "affected_assets": ["hayali_dashboard"],
        "recommended_actions": [{"type": "delete_everything"}],
        "root_causes": [
            {"hypothesis": "gerçek", "confidence": 0.7, "evidence_ids": ["real-1"], "counter_evidence": []},
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1"})
    assert result.severity == fallback.severity == "critical"
    assert result.affected_assets == fallback.affected_assets
    assert result.recommended_actions == fallback.recommended_actions
    assert result.llm_used is True


def test_validate_clamps_confidence_to_valid_range(fallback):
    raw = {
        "summary": "s",
        "root_causes": [
            {"hypothesis": "h", "confidence": 5.0, "evidence_ids": ["real-1"], "counter_evidence": []},
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1"})
    assert result.root_causes[0].confidence == 1.0


def test_validate_rejects_non_numeric_confidence(fallback):
    raw = {
        "summary": "s",
        "root_causes": [
            {"hypothesis": "h", "confidence": "high", "evidence_ids": ["real-1"], "counter_evidence": []},
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1"})
    assert result is None  # only malformed hypothesis -> nothing validated -> None


def test_validate_caps_at_max_hypotheses(fallback):
    raw = {
        "summary": "s",
        "root_causes": [
            {"hypothesis": f"h{i}", "confidence": 0.1 * i, "evidence_ids": ["real-1"], "counter_evidence": []}
            for i in range(1, 6)
        ],
    }
    result = _validate_and_merge(raw, fallback, known_evidence_ids={"real-1"})
    assert len(result.root_causes) == 3
