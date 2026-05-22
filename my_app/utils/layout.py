from shiny import ui


EMPTY_CARD_CSS = """
.empty-card {
    border: 1px dashed var(--bs-border-color);
    border-radius: 0.375rem;
    background: var(--bs-tertiary-bg);
}
"""


def empty_card(title: str, height: str = "260px"):
    return ui.card(
        ui.card_header(title),
        ui.div(class_="empty-card", style=f"min-height: {height};"),
    )
