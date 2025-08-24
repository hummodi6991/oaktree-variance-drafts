
import os
from typing import Tuple
from .schemas import VarianceItem, ConfigModel
from .prompt_contract import SYSTEM_PROMPT, build_user_prompt, build_arabic_instruction

def generate_draft(v: VarianceItem, cfg: ConfigModel) -> Tuple[str, str]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    user_prompt = build_user_prompt(v, cfg)
    ar_instr = build_arabic_instruction() if cfg.bilingual else ""

    if not api_key:
        en = (
            f"{v.variance_pct:.2f}% (SAR {abs(v.variance_sar):,.0f}) variance in {v.category}. "
            f"{'Drivers: ' + '; '.join(v.drivers) + '. ' if v.drivers else 'Cause pending analyst review. '}"
            f"{'Vendors: ' + '; '.join(v.vendors) + '. ' if v.vendors else ''}"
            "Impact remains contained within management oversight; corrective actions are in progress if required."
        )
        ar = (
            f"تفاوت بنسبة {v.variance_pct:.2f}% (بقيمة {abs(v.variance_sar):,.0f} ريال) في فئة {v.category}. "
            f"{'الأسباب: ' + '؛ '.join(v.drivers) + '. ' if v.drivers else 'السبب قيد المراجعة من قبل المحلل. '}"
            f"{'الموردون: ' + '؛ '.join(v.vendors) + '. ' if v.vendors else ''}"
            "يبقى الأثر ضمن نطاق المتابعة الإدارية، وسيتم اتخاذ الإجراءات التصحيحية عند الحاجة."
        ) if cfg.bilingual else ""
        return en, ar

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt + ("\n\n" + ar_instr if ar_instr else "")},
        ]
        resp = client.chat.completions.create(model=os.getenv("OPENAI_MODEL","gpt-5.1-mini"), messages=messages)
        text = resp.choices[0].message.content
        return text.strip(), ""  # simple case; refine splitting later if needed
    except Exception:
        en = (
            f"{v.variance_pct:.2f}% (SAR {abs(v.variance_sar):,.0f}) variance in {v.category}. "
            f"{'Drivers: ' + '; '.join(v.drivers) + '. ' if v.drivers else 'Cause pending analyst review. '}"
            f"{'Vendors: ' + '; '.join(v.vendors) + '. ' if v.vendors else ''}"
            "Impact remains contained within management oversight; corrective actions are in progress if required."
        )
        ar = (
            f"تفاوت بنسبة {v.variance_pct:.2f}% (بقيمة {abs(v.variance_sar):,.0f} ريال) في فئة {v.category}. "
            f"{'الأسباب: ' + '؛ '.join(v.drivers) + '. ' if v.drivers else 'السبب قيد المراجعة من قبل المحلل. '}"
            f"{'الموردون: ' + '؛ '.join(v.vendors) + '. ' if v.vendors else ''}"
            "يبقى الأثر ضمن نطاق المتابعة الإدارية، وسيتم اتخاذ الإجراءات التصحيحية عند الحاجة."
        ) if cfg.bilingual else ""
        return en, ar
