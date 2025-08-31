
from .schemas import VarianceItem, ConfigModel

SYSTEM_PROMPT = (
    "You are an Oaktree Group financial analyst. "
    "Write budget-vs-actual variance explanations for investors. "
    "Always begin with the variance percentage and SAR amount. "
    "Explain using only the provided drivers and vendor names; do not speculate. "
    "Conclude with mitigation or reassurance. "
    "Tone: professional, concise, and non-alarmist. "
    "Length: 2–4 sentences. "
)

def build_user_prompt(v: VarianceItem, cfg: ConfigModel) -> str:
    drivers = "; ".join(v.drivers) if v.drivers else "None provided"
    vendors = "; ".join(v.vendors) if v.vendors else "N/A"
    links = "; ".join(v.evidence_links) if v.evidence_links else "N/A"
    rule = "Do not speculate. If a cause is not provided in drivers, state 'cause pending analyst review'." if cfg.enforce_no_speculation else ""
    return (
        f"Project: {v.project_id}\n"
        f"Period: {v.period}\n"
        f"Category: {v.category}\n"
        f"Budget (SAR): {v.budget_sar:.2f}\n"
        f"Actual (SAR): {v.actual_sar:.2f}\n"
        f"Variance (SAR): {v.variance_sar:.2f}\n"
        f"Variance %: {v.variance_pct:.2f}\n"
        f"Drivers: {drivers}\n"
        f"Vendors: {vendors}\n"
        f"Evidence links: {links}\n"
        f"{rule}\n"
        "Write an English paragraph that starts with the variance percentage and amount, "
        "explains cause(s) strictly from Drivers, and closes with mitigation/reassurance."
    )

def build_arabic_instruction() -> str:
    return (
        "ثم قدّم نفس الشرح باللغة العربية بأسلوب مهني وموجز، "
        "وبنفس القواعد: البدء بنسبة وقيمة التفاوت، ثم السبب من Drivers فقط، ثم التطمين/الإجراء التالي."
    )
