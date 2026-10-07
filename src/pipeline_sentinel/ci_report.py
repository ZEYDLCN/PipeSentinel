"""PR/CI için okunabilir Markdown özeti (GitHub step summary, PR yorumu)."""
from __future__ import annotations


def _cell(value, limit=80) -> str:
    """Markdown tablo hücresi: CSV içeriği tablo yapısını veya işaretlemeyi bozamaz."""
    text = "—" if value is None else str(value)
    for old, new in (("\\", "\\\\"), ("|", "\\|"), ("`", "'"), ("<", "&lt;"), (">", "&gt;"), ("\r", " "), ("\n", " ")):
        text = text.replace(old, new)
    return text[:limit] + ("…" if len(text) > limit else "")


def _table(headings, rows) -> list[str]:
    return ["| " + " | ".join(headings) + " |", "|" + "---|" * len(headings),
            *("| " + " | ".join(_cell(v) for v in row) + " |" for row in rows), ""]


def render_markdown(report: dict, title: str = "Veri kalite kapısı") -> str:
    diff, violations = report["diff"], report["violations"]
    lines = [f"## {title}: {'✅ Geçti' if report['passed'] else '❌ Başarısız'}", "",
             *_table(["Ölçü", "Değer"], [
                 ["Eklenen satır", diff["added_rows"]], ["Silinen satır", diff["removed_rows"]],
                 ["Değişen satır", diff["changed_rows"]], ["Değişen hücre", diff["changed_cells"]],
                 ["Değişim oranı", f"%{diff['change_ratio'] * 100:.2f}"],
                 ["Şema değişti", "evet" if diff["schema_changed"] else "hayır"]])]
    if diff.get("changed_by_column"):
        lines += ["### Kolon bazında değişim", *_table(["Kolon", "Değişen hücre"], sorted(
            diff["changed_by_column"].items(), key=lambda item: -item[1]))]
    totals = [(c, t["before"], t["after"], t["after"] - t["before"]) for c, t in diff["numeric_totals"].items()
              if t["before"] != t["after"]]
    if totals:
        lines += ["### Sayısal toplamlar", *_table(["Kolon", "Önce", "Sonra", "Fark"], [
            (c, f"{b:,.4g}", f"{a:,.4g}", f"{d:+,.4g}") for c, b, a, d in totals])]
    if violations:
        lines += ["### Sözleşme ihlalleri", *_table(["Kural", "Kolon", "Açıklama"], [
            (v["rule_type"], v["column"], v["message"]) for v in violations[:20]])]
        if len(violations) > 20:
            lines += [f"… ve {len(violations) - 20} ihlal daha (tam liste JSON raporunda).", ""]
    samples = diff.get("samples", {})
    if samples.get("changed"):
        lines += ["### Örnek değişen satırlar", *_table(["Anahtar", "Değişiklik (önce → sonra)"], [
            (s["key"], "; ".join(f"{c}: {a} → {b}" for c, (a, b) in s["changes"].items())) for s in samples["changed"]])]
    for label, key in (("Eklenen anahtarlar (ilk 5)", "added_keys"), ("Silinen anahtarlar (ilk 5)", "removed_keys")):
        if samples.get(key):
            lines += [f"**{label}:** " + ", ".join(_cell(k) for k in samples[key]), ""]
    return "\n".join(lines).rstrip() + "\n"
