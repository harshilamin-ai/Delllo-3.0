# scripts/reembed_chunks.py
import asyncio
from app.db.postgres import get_db
from app.services.retrieval import embed_text
from sqlalchemy import text

async def reembed_all():
    async for db in get_db():
        # Fetch all chunks missing embeddings
        result = await db.execute(text("""
            SELECT chunk_id, text 
            FROM document_chunks 
            WHERE embedding IS NULL
            ORDER BY chunk_id
        """))
        chunks = result.mappings().all()
        print(f"Found {len(chunks)} chunks to re-embed")

        for i, chunk in enumerate(chunks):
            vector = await embed_text(chunk["text"])
            if vector:
                await db.execute(
                    text("UPDATE document_chunks SET embedding = :vec WHERE chunk_id = :id"),
                    {"vec": vector, "id": chunk["chunk_id"]}
                )
                print(f"[{i+1}/{len(chunks)}] ✅ {chunk['chunk_id']}")
            else:
                print(f"[{i+1}/{len(chunks)}] ❌ embedding failed for {chunk['chunk_id']}")

        await db.commit()
        print("Done.")

asyncio.run(reembed_all())