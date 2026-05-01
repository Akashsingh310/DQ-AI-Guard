"""
AI root-cause analysis supporting Google Gemini and xAI Grok.
Uses strict prompt engineering to force a structured JSON response.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """\
You are a senior data quality engineer with expertise in data pipelines, \
data governance, and root-cause analysis.

TASK
Given a set of FAILED data quality checks and a representative sample of the \
raw dataset, produce a precise, evidence-based diagnosis.

CRITICAL RULES — VIOLATION WILL CAUSE SYSTEM FAILURE
1. You MUST respond with RAW JSON ONLY. Do NOT wrap in ```json``` fences.
2. Do NOT include ANY text before or after the JSON object.
3. The FIRST character of your response must be '{' and the LAST must be '}'.
4. Ground every claim in the provided evidence. Do not invent columns, values, \
   or patterns that are not visible in the supplied data.
5. Severity definitions:
     "low"    — cosmetic issue; minimal downstream impact.
     "medium" — business logic impact; may cause incorrect aggregations \
                or downstream failures in non-critical paths.
     "high"   — pipeline-breaking, compliance risk, or data loss potential.
6. recommended_fix must be a concrete, actionable engineering step. \
   Generic advice such as "clean your data" is not acceptable.
7. If multiple checks fail for the same column, consolidate into one issue \
   entry referencing the primary check_name.
8. data_health_score is an integer 0–100 reflecting overall data fitness. \
   Deduct points proportionally to severity and failure rate.

OUTPUT SCHEMA — return exactly this structure, NO MARKDOWN:
{
  "analysis_summary": "<single sentence>",
  "issues": [
    {
      "check_name": "<exact check_name string from input>",
      "issue_summary": "<description>",
      "root_cause": "<most probable upstream cause>",
      "severity": "low|medium|high",
      "recommended_fix": "<specific, actionable step>",
      "affected_column": "<column name or null>",
      "example_bad_values": ["<up to 3 representative bad values>"]
    }
  ],
  "overall_severity": "low|medium|high",
  "data_health_score": <integer 0-100>
}
"""

_USER_PROMPT_TEMPLATE = """\
FAILED VALIDATION CHECKS
{failures_json}

DATASET SAMPLE (first {sample_size} rows)
{sample_json}

Analyse the failures above. For each failed check identify root cause, \
severity, and a concrete recommended fix. Return ONLY the JSON object defined \
in the system prompt."""


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _serialise_sample(df: pd.DataFrame, n: int) -> list[dict[str, Any]]:
    """Extract first n rows as JSON-safe dicts, normalising NaN/NaT."""
    raw = df.head(n).to_dict(orient="records")

    def _coerce(v: Any) -> Any:
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    return [{k: _coerce(v) for k, v in row.items()} for row in raw]


def _parse_ai_response(raw_text: str) -> dict[str, Any]:
    """
    Parse AI response into JSON, handling common formatting issues.
    
    Handles:
    - Markdown code fences (```json ... ```)
    - Leading/trailing whitespace
    - Text before/after JSON
    """
    text = raw_text.strip()
    
    # Strategy 1: Try direct JSON parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Strategy 2: Extract JSON from markdown code fences
    # Pattern: ```json\n{...}\n``` or ```\n{...}\n```
    import re
    
    # Try to find JSON inside ```json ... ``` blocks
    pattern = r'```(?:json)?\s*\n(.*?)\n```'
    matches = re.findall(pattern, text, re.DOTALL)
    
    if matches:
        for match in matches:
            try:
                return json.loads(match.strip())
            except json.JSONDecodeError:
                continue
    
    # Strategy 3: Find anything that looks like a JSON object
    # Look for { ... } pairs
    brace_pattern = r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}'
    matches = re.findall(brace_pattern, text, re.DOTALL)
    
    if matches:
        # Try the longest match first (likely the complete JSON)
        for match in sorted(matches, key=len, reverse=True):
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
    
    # Strategy 4: Try removing markdown artifacts
    # Remove lines that start with ```
    lines = text.splitlines()
    cleaned_lines = [line for line in lines if not line.strip().startswith('```')]
    cleaned_text = '\n'.join(cleaned_lines).strip()
    
    try:
        return json.loads(cleaned_text)
    except json.JSONDecodeError:
        pass
    
    # If all strategies fail, raise with helpful error
    raise ValueError(
        f"Unable to parse AI response as JSON after multiple attempts.\n"
        f"First 500 chars of response:\n{raw_text[:500]}\n"
        f"Last 200 chars of response:\n{raw_text[-200:]}"
    )


# ---------------------------------------------------------------------------
# Provider-specific backends
# ---------------------------------------------------------------------------
def _call_gemini(
    user_prompt: str,
    api_key: str,
    model_name: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """Send prompt to Google Gemini using the new google-genai package."""
    from google import genai

    client = genai.Client(api_key=api_key)
    
    response = client.models.generate_content(
        model=model_name,
        contents=user_prompt,
        config={
            "system_instruction": _SYSTEM_PROMPT,
            "max_output_tokens": max_tokens,
            "temperature": temperature,
        },
    )
    
    if not response.text:
        raise RuntimeError(
            f"Gemini returned empty response. "
            f"Finish reason: {response.candidates[0].finish_reason if response.candidates else 'Unknown'}"
        )
    
    return response.text


def _call_grok(
    user_prompt: str,
    api_key: str,
    model_name: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """Send prompt to xAI Grok via OpenAI-compatible endpoint."""
    from openai import OpenAI

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Model discovery helper
# ---------------------------------------------------------------------------
def list_available_gemini_models(api_key: str) -> list[str]:
    """List all Gemini models that support text generation."""
    from google import genai
    
    client = genai.Client(api_key=api_key)
    available = []
    
    for model in client.models.list():
        if "generateContent" in model.supported_actions:
            available.append(model.name)
    
    return sorted(available)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def analyze_failures(
    failures: list[dict[str, Any]],
    df: pd.DataFrame,
    ai_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Perform root-cause analysis using the configured LLM provider.
    """
    if not failures:
        logger.info("No failures to analyse.")
        return {
            "analysis_summary": "All validation checks passed. No issues detected.",
            "issues": [],
            "overall_severity": "low",
            "data_health_score": 100,
        }

    provider = ai_config.get("provider", "gemini").lower()
    if provider not in ("gemini", "grok"):
        raise ValueError(f"Unsupported AI provider '{provider}'. Use 'gemini' or 'grok'.")

    model_dict = ai_config.get("model", {})
    model_name = model_dict.get(provider)
    if not model_name:
        raise KeyError(f"No model name configured for provider '{provider}' in ai.model section.")

    api_key_env_var = "GEMINI_API_KEY" if provider == "gemini" else "XAI_API_KEY"
    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise EnvironmentError(
            f"{api_key_env_var} environment variable not set. "
            f"Export it before running."
        )

    max_tokens = int(ai_config.get("max_tokens", 1500))
    temperature = float(ai_config.get("temperature", 0.1))
    sample_size = int(ai_config.get("sample_rows", 10))
    retry_attempts = max(1, int(ai_config.get("retry_attempts", 2)))
    retry_delay = float(ai_config.get("retry_delay_seconds", 3))

    sample_rows = _serialise_sample(df, sample_size)
    user_prompt = _USER_PROMPT_TEMPLATE.format(
        failures_json=json.dumps(failures, indent=2),
        sample_size=len(sample_rows),
        sample_json=json.dumps(sample_rows, indent=2),
    )

    logger.info(
        "Sending %d failure(s) to AI (%s, model=%s).",
        len(failures), provider, model_name,
    )

    last_error = ""
    for attempt in range(1, retry_attempts + 1):
        try:
            if provider == "gemini":
                raw_text = _call_gemini(user_prompt, api_key, model_name, max_tokens, temperature)
            else:
                raw_text = _call_grok(user_prompt, api_key, model_name, max_tokens, temperature)

            logger.info("AI raw response received (%d chars). Parsing.", len(raw_text))
            analysis = _parse_ai_response(raw_text)

            logger.info(
                "AI analysis complete. Issues: %d, Severity: %s, Health Score: %s.",
                len(analysis.get("issues", [])),
                analysis.get("overall_severity"),
                analysis.get("data_health_score"),
            )
            return analysis

        except Exception as exc:
            last_error = str(exc)
            logger.warning("AI call failed (attempt %d/%d): %s", attempt, retry_attempts, exc)

            # Extract retry delay from API error if present
            sleep_secs = retry_delay * attempt
            if "retry_delay" in last_error.lower() or "retry" in last_error.lower():
                match = re.search(r'seconds[:\s]*(\d+)', last_error, re.IGNORECASE)
                if match:
                    sleep_secs = int(match.group(1))
                    logger.info("Respecting API retry delay: %d seconds.", sleep_secs)

        if attempt < retry_attempts:
            logger.info("Retrying in %.1f seconds.", sleep_secs)
            time.sleep(sleep_secs)

    logger.error("AI analysis failed after %d attempts. Last error: %s", retry_attempts, last_error)
    return {
        "error": True,
        "reason": "Max retries exceeded",
        "detail": last_error,
        "analysis_summary": "AI analysis could not be completed.",
        "issues": [],
        "overall_severity": "unknown",
        "data_health_score": None,
    }