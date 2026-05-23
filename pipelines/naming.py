import re
from typing import Dict, Any,Protocol

def parse_convention(name: str) -> Dict[str, Any]:
    pattern = re.compile(
        r"^(?P<speed>[\d.]+)mps_"
        r"(?P<config>.+?)_"
        r"REPEAT(?P<repeat>\d+)_"
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
    }

    # Infer convention + phone_id
    if rest.startswith("Headform_Transformed_"):
        settings["convention"] = "ref"
        settings["phone_id"] = rest.replace("Headform_Transformed_", "", 1)

    elif rest.endswith("_framed"):
        settings["convention"] = "framed"
        settings["phone_id"] = rest[:-7]  # remove "_framed"

    else:
        settings["convention"] = "default"
        settings["phone_id"] = rest

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
    return (
        f"{target_speed_mps}mps_"
        f"{config}_"
        f"REPEAT{repeat}_"
        f"{phone_id}_framed"
    )


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