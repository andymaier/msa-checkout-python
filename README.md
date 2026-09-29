# msa-checkout (Python)

Python-Portierung des Java/Spring-Boot-Service `checkout` (predic8-MSA-Shop).
Flask (REST) + kafka-python (Event-Anbindung), In-Memory-Preisspeicher.

## Architektur
- REST `POST /checkouts` (Body = Basket): prueft Verfuegbarkeit, vergibt eine
  uuid, setzt je Item den Preis aus dem Preisspeicher (fehlt -> 400), sendet
  eine `Operation("basket","upsert", basket)` auf das Topic `shop` und
  antwortet `202 Accepted` mit `{ "uuid": ... }`.
- Kafka-Listener auf `shop`: pflegt aus `bo="article"`-Events den Preisspeicher
  (`upsert`/`delete`).
- Fehler: kein Preis -> `400`; Eventbus nicht erreichbar -> `503`.

## Branches
- `main`     – Skelett; **API (`app/api.py`) und Kafka-Listener
  (`app/events.py`) sind als TODO offen** (Uebungsstand).
- `solution` – fertige Loesung.

## Start
    python3 -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python -m app.main   # Kafka muss laufen (Default localhost:9092)
