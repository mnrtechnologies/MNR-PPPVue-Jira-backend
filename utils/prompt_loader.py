from pathlib import Path

def load_prompt(prompt_name: str) -> str:
    prompt_path = Path(__file__).parent.parent / "src" / "prompts" / f"{prompt_name}.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()
