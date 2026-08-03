"""RAG example: semantic retrieval with the native vector API, generation via OpenRouter."""

from __future__ import annotations

import json
import os
import urllib.request

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import search, vector
from setup import Product, engine_from_env, setup_database

MODEL = os.environ.get("RAG_MODEL", "anthropic/claude-3-haiku")

# Stand-in for a real embedding model: the demo products embed into a tiny
# 3-dimensional space (footwear, audio, home decor).
QUERY = "What running shoes do you have?"
QUERY_EMBEDDING = [1.0, 0.0, 0.0]


def retrieve(session: Session, query_embedding: list[float], limit: int = 3):
    stmt = (
        select(Product.description, Product.category, Product.rating)
        .where(search.all(Product.id))
        .order_by(vector.l2_distance(Product.embedding, query_embedding))
        .limit(limit)
    )
    return session.execute(stmt).all()


def generate(query: str, context: str) -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return "(Set OPENROUTER_API_KEY to enable generation.)"

    prompt = (
        "You are a helpful product assistant. Answer the customer's question based only on "
        f"the product information provided below.\n\nProduct Catalog:\n{context}\n\n"
        f"Customer Question: {query}\n\n"
        "Provide a helpful, concise answer. If the products don't match what the customer "
        "is looking for, say so."
    )
    request = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps({"model": MODEL, "messages": [{"role": "user", "content": prompt}]}).encode(),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = json.load(response)
        return body["choices"][0]["message"]["content"]
    except (OSError, KeyError, IndexError, ValueError) as exc:
        return f"(OpenRouter error: {exc}. Check your API key.)"


def main() -> None:
    engine = engine_from_env()
    setup_database(engine)

    with Session(engine) as session:
        rows = retrieve(session, QUERY_EMBEDDING)

    context = "\n".join(f"- {row.description} | Category: {row.category} | Rating: {row.rating}/5" for row in rows)
    print(f"Question: {QUERY}")
    print("Retrieved products:")
    print(context)
    print("Answer:")
    print(generate(QUERY, context))


if __name__ == "__main__":
    main()
