"""Basic NLP utilities for summarization and entity extraction."""

from __future__ import annotations

from typing import List, Tuple

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.text_rank import TextRankSummarizer

import spacy
from spacy.cli import download as spacy_download

from .db import Entity, News, SessionLocal


def summarize_text(text: str, sentences: int = 3) -> str:
    """Return a short summary of *text* using TextRank."""

    if not text:
        return ""
    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = TextRankSummarizer()
    summary = summarizer(parser.document, sentences)
    return " ".join(str(s) for s in summary)


_nlp = None


def load_spacy():
    """Load spaCy's small English model, downloading if needed."""

    global _nlp
    if _nlp is not None:
        return _nlp
    model = "en_core_web_sm"
    try:
        _nlp = spacy.load(model)
    except OSError:  # pragma: no cover - model download
        spacy_download(model)
        _nlp = spacy.load(model)
    return _nlp


def extract_entities(text: str) -> List[Tuple[str, str, float]]:
    """Extract named entities from text."""

    nlp = load_spacy()
    doc = nlp(text)
    entities: List[Tuple[str, str, float]] = []
    for ent in doc.ents:
        if ent.label_ in {"ORG", "GPE", "PRODUCT"}:
            score = float(ent._.score) if ent.has_extension("score") else 1.0
            entities.append((ent.label_, ent.text, score))
    return entities


def enrich_news(limit: int = 200) -> None:
    """Generate summaries and extract entities for recent news."""

    session: Session = SessionLocal()
    with session.begin():
        stmt = (
            select(News)
            .where((News.summary_ai == None) | (News.summary_ai == ""))
            .where(~exists(select(Entity.id).where(Entity.news_id == News.id)))
            .order_by(News.published_at.desc())
            .limit(limit)
        )
        items = session.scalars(stmt).all()
        for item in items:
            item.summary_ai = item.summary if item.summary else summarize_text(item.title)
            ents = extract_entities(f"{item.title}. {item.summary or ''}")
            for etype, value, score in ents:
                session.add(Entity(news_id=item.id, type=etype, value=value, score=score))
