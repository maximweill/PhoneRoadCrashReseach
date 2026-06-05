import re
from typing import Dict, Any,Protocol

import re
from typing import Dict, Any, Protocol

def parse_convention(name: str) -> Dict[str, Any]:
    pattern = re.compile(
        r"^(?P<speed>[\d.]+)mps_"
        r"(?P<config>.+?)_"
        r"REPEAT(?P<repeat>-?\d+)_"
        r"(?P<rest>.+)$"
    )

    m = pattern.match(name)
    if not m:
        raise ValueError(f"Unrecognized format: {name}")

    speed = float(m.group("speed"))
    config = m.group("config")
    repeat = int(m.group("repeat"))
    rest = m.group("rest")

    settings = {
        "target_speed_mps": speed,
        "config": config,
        "repeat": repeat,
        "phone_id": "",  # default fallback
    }

    # --- extract convention suffix ---
    is_psd = rest.endswith("_psd")
    core = rest[:-4] if is_psd else rest  # strip "_psd" safely

    # --- phone_id extraction via regex (robust) ---
    phone_match = re.search(r"Headform_Transformed_(.+)", core)
    framed_match = re.search(r"(.+)_framed$", core)

    if is_psd:
        if phone_match:
            settings["convention"] = "ref_psd"
            settings["phone_id"] = phone_match.group(1) or ""
        elif framed_match:
            settings["convention"] = "framed_psd"
            settings["phone_id"] = framed_match.group(1) or ""
        else:
            settings["convention"] = "default_psd"
            settings["phone_id"] = core or ""
    else:
        if phone_match:
            settings["convention"] = "ref"
            settings["phone_id"] = phone_match.group(1) or ""
        elif framed_match:
            settings["convention"] = "framed"
            settings["phone_id"] = framed_match.group(1) or ""
        else:
            settings["convention"] = "default"
            settings["phone_id"] = core or ""

    return settings


class NameConvention(Protocol):
    def __call__(
        self,
        phone_id: str,
        config: str,
        target_speed_mps: float,
        repeat: int,
    ) -> str:
        ...

def default_convention(
    phone_id: str,
    config: str,
    target_speed_mps: float,
    repeat: int,
) -> str:
    return (
        f"{target_speed_mps}mps_"
        f"{config}_"
        f"REPEAT{repeat}_"
        f"{phone_id}"
    )

def ref_convention(
    phone_id: str,
    config: str,
    target_speed_mps: float,
    repeat: int,
) -> str:
    return (
        f"{target_speed_mps}mps_"
        f"{config}_"
        f"REPEAT{repeat}_"
        f"Headform_Transformed_"
        f"{phone_id}"
    )


def framed_convention(
    phone_id: str,
    config: str,
    target_speed_mps: float,
    repeat: int,
) -> str:
    return default_convention(phone_id,config,target_speed_mps,repeat)+"_framed"

def framed_psd_convention(
    phone_id: str,
    config: str,
    target_speed_mps: float,
    repeat: int,
) -> str:
    return framed_convention(phone_id,config,target_speed_mps,repeat) + "_psd"

def ref_psd_convention(
    phone_id: str,
    config: str,
    target_speed_mps: float,
    repeat: int,
) -> str:
    return ref_convention(phone_id,config,target_speed_mps,repeat) + "_psd"




