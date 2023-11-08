from fire import Fire
from app.schema import Document
from app.db.session import SessionLocal
from fastapi.encoders import jsonable_encoder
from app.api import crud
import asyncio
from app.schema import (
    SecDocumentMetadata,
    DocumentMetadataMap,
    DocumentMetadataKeysEnum,
    SecDocumentTypeEnum,
    Document,
)


async def upsert_single_document(doc_url: str):
    """
    Upserts a single SEC document into the database using its URL.
    """
    if not doc_url or not doc_url.startswith('http'):
        print("DOC_URL must be an http(s) based url value")
        return
    metadata_map = {}
    
    sec_doc_metadata = SecDocumentMetadata(
        company_name="Sample Name 2",
        company_ticker="Sample ticket 2",
        doc_type="10-Q",
        year=2021,
        # quarter=filing.quarter,
        accession_number="0000950170-22-000796",
        cik="0001318605",
        period_of_report_date="2021-12-31T00:00:00",
        filed_as_of_date="2022-02-07T00:00:00",
        date_as_of_change="2022-02-04T00:00:00",
    )
    metadata_map: DocumentMetadataMap = {
        DocumentMetadataKeysEnum.SEC_DOCUMENT: jsonable_encoder(
            sec_doc_metadata.dict(exclude_none=True)
        )
    }
    doc = Document(url=doc_url, metadata_map=metadata_map)

    async with SessionLocal() as db:
        document = await crud.upsert_document_by_url(db, doc)
        print(f"Upserted document. Database ID:\n{document.id}")


def main_upsert_single_document(doc_url: str):
    """
    Script to upsert a single document by URL. metada_map parameter will be empty dict ({})
    This script is useful when trying to use your own PDF files.
    """
    asyncio.run(upsert_single_document(doc_url))

if __name__ == "__main__":
    Fire(main_upsert_single_document)
