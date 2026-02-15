"""
Gemini AI Service for Silver Transformation Code Generation.

Uses Google's Gemini API to generate PySpark transformation code
from natural language prompts, with schema context and conversation history.
Supports conversational flow with clarifying questions.
"""

import os
import re
import json
import logging
import requests
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
)

# ---------------------------------------------------------------------------
# System prompt — instructs Gemini how to behave
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a PySpark data transformation expert working inside an autonomous data pipeline system.

Your job is to help users create Silver-layer transformations by generating PySpark code.

## CONTEXT
- The user has a Bronze-layer dataset with a known schema (provided below).
- You will generate a PySpark `transform(df, spark)` function that takes a Bronze DataFrame and returns a transformed Silver DataFrame.
- The code runs inside an Airflow DAG with PySpark 3.5.1 on Spark 3.5.1.

## RULES
1. **Ask clarifying questions** if the user's request is ambiguous, incomplete, or could be interpreted multiple ways. Prefix your response with `[CLARIFICATION]` when asking questions. DO NOT generate code when clarifying.
2. When you DO generate code, wrap it in a ```python code fence. The code must define exactly one function: `def transform(df, spark):` that returns a DataFrame.
3. Include necessary imports (from pyspark.sql.functions, pyspark.sql.types, etc.) INSIDE the function or at the top of the code block.
4. Add brief inline comments explaining each transformation step.
5. Do NOT use `spark.read` or any I/O — the input `df` is already loaded, just transform it.
6. Keep the code idiomatic PySpark — prefer DataFrame API over SQL strings.
7. If the user asks for enrichment via external API, use `requests` inside a UDF (the Airflow workers have `requests` installed).
8. If the user iterates ("add more columns", "also filter out…"), build on your previous code — don't start from scratch.
9. After generating code, briefly explain what the transformation does in 2-3 sentences.
10. If the user says the dry-run failed, help debug based on the error message.

## OUTPUT FORMAT
When generating code:
```python
def transform(df, spark):
    \"\"\"Brief description of what this transformation does.\"\"\"
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    
    # ... transformation logic ...
    
    return result_df
```

When clarifying (no code):
[CLARIFICATION]
Your clarifying questions here...
"""


def _build_schema_context(input_schema: List[Dict], sample_rows: List[Dict]) -> str:
    """Build a schema description string for the AI prompt."""
    lines = ["## INPUT SCHEMA (Bronze layer)"]
    lines.append("| Column | Type | Nullable | Sample Values |")
    lines.append("|--------|------|----------|---------------|")
    for field in input_schema:
        name = field.get("name", "?")
        dtype = field.get("detected_type", field.get("type", "string"))
        nullable = "Yes" if field.get("nullable", True) else "No"
        samples = field.get("sample_values", [])
        sample_str = ", ".join(str(s) for s in samples[:3]) if samples else "—"
        lines.append(f"| {name} | {dtype} | {nullable} | {sample_str} |")

    if sample_rows:
        lines.append("")
        lines.append(f"## SAMPLE DATA ({len(sample_rows)} rows)")
        lines.append("```json")
        lines.append(json.dumps(sample_rows[:5], indent=2, default=str))
        lines.append("```")

    return "\n".join(lines)


def _build_messages(
    conversation_history: List[Dict],
    input_schema: List[Dict],
    sample_rows: List[Dict],
    user_prompt: str,
) -> List[Dict]:
    """
    Build the full message list for Gemini API.
    Gemini uses 'user' and 'model' roles (not 'assistant').
    """
    messages = []

    # System context as a user message at the start (Gemini doesn't have a separate system role)
    schema_context = _build_schema_context(input_schema, sample_rows)
    system_content = SYSTEM_PROMPT + "\n\n" + schema_context

    messages.append({"role": "user", "parts": [{"text": system_content}]})
    messages.append({"role": "model", "parts": [{"text": "Understood. I have the schema and sample data. I'm ready to help you create Silver-layer transformations. What transformation would you like to perform?"}]})

    # Add conversation history
    for msg in conversation_history:
        role = "model" if msg["role"] == "assistant" else "user"
        messages.append({"role": role, "parts": [{"text": msg["content"]}]})

    # Add current user prompt
    messages.append({"role": "user", "parts": [{"text": user_prompt}]})

    return messages


def _extract_code_block(text: str) -> Optional[str]:
    """Extract the first Python code block from the response."""
    pattern = r"```python\s*\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def _is_clarification(text: str) -> bool:
    """Check if the response is a clarification request (no code generated)."""
    return text.strip().startswith("[CLARIFICATION]") or (
        "[CLARIFICATION]" in text[:200] and "```python" not in text
    )


def generate_transformation(
    user_prompt: str,
    input_schema: List[Dict],
    sample_rows: List[Dict],
    conversation_history: List[Dict] = None,
) -> Dict:
    """
    Send a prompt to Gemini and get back either:
    - A clarification question (no code)
    - Generated PySpark code with explanation

    Returns:
        {
            "type": "clarification" | "code",
            "content": str,          # Full response text
            "code": str | None,      # Extracted code block (if type=code)
            "error": str | None,     # Error message if API call failed
        }
    """
    if not GEMINI_API_KEY:
        return {
            "type": "error",
            "content": "Gemini API key not configured. Set GEMINI_API_KEY environment variable.",
            "code": None,
            "error": "missing_api_key",
        }

    conversation_history = conversation_history or []
    messages = _build_messages(conversation_history, input_schema, sample_rows, user_prompt)

    payload = {
        "contents": messages,
        "generationConfig": {
            "temperature": 0.3,
            "topP": 0.95,
            "topK": 40,
            "maxOutputTokens": 4096,
        },
    }

    try:
        resp = requests.post(
            f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        # Extract text from Gemini response
        candidates = data.get("candidates", [])
        if not candidates:
            return {
                "type": "error",
                "content": "Gemini returned no candidates.",
                "code": None,
                "error": "no_candidates",
            }

        parts = candidates[0].get("content", {}).get("parts", [])
        response_text = "\n".join(p.get("text", "") for p in parts).strip()

        if not response_text:
            return {
                "type": "error",
                "content": "Gemini returned empty response.",
                "code": None,
                "error": "empty_response",
            }

        # Determine if this is a clarification or code
        if _is_clarification(response_text):
            return {
                "type": "clarification",
                "content": response_text.replace("[CLARIFICATION]", "").strip(),
                "code": None,
                "error": None,
            }

        # Try to extract code
        code_block = _extract_code_block(response_text)
        if code_block:
            # Remove the code fence from the explanation text
            explanation = re.sub(r"```python\s*\n.*?```", "", response_text, flags=re.DOTALL).strip()
            return {
                "type": "code",
                "content": response_text,
                "code": code_block,
                "error": None,
            }

        # No code fence found — treat as conversational response
        return {
            "type": "clarification",
            "content": response_text,
            "code": None,
            "error": None,
        }

    except requests.exceptions.Timeout:
        logger.error("Gemini API timeout")
        return {
            "type": "error",
            "content": "Gemini API request timed out. Please try again.",
            "code": None,
            "error": "timeout",
        }
    except requests.exceptions.HTTPError as e:
        error_body = e.response.text if e.response else str(e)
        logger.error("Gemini API HTTP error: %s — %s", e.response.status_code if e.response else "?", error_body)
        return {
            "type": "error",
            "content": f"Gemini API error ({e.response.status_code if e.response else '?'}): {error_body[:500]}",
            "code": None,
            "error": "api_error",
        }
    except Exception as e:
        logger.error("Gemini AI service error: %s", e, exc_info=True)
        return {
            "type": "error",
            "content": f"AI service error: {str(e)}",
            "code": None,
            "error": "unknown",
        }


def validate_transform_code(code: str) -> Tuple[bool, str]:
    """
    Basic static validation of the generated transform function.
    Checks for syntax errors and required function signature.
    """
    # Check for the required function signature
    if "def transform(" not in code:
        return False, "Code must define a `def transform(df, spark):` function."

    # Check for forbidden operations
    forbidden = ["spark.read", "df.write", "os.system", "subprocess", "eval(", "exec("]
    for f in forbidden:
        if f in code:
            return False, f"Code contains forbidden operation: `{f}`. Only transformations are allowed."

    # Syntax check
    try:
        compile(code, "<transform>", "exec")
    except SyntaxError as e:
        return False, f"Syntax error at line {e.lineno}: {e.msg}"

    return True, "Code validation passed."
