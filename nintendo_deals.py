#!/usr/bin/env python3
"""
Surveille l'eShop Nintendo français (jeux Switch / Switch 2) et envoie sur
Discord, via un webhook, chaque nouvelle promotion détectée.

Fonctionnement :
  1. Le catalogue des jeux (identifiants "nsuid", titres, liens) est récupéré
     sur le moteur de recherche de Nintendo, puis conservé quelques heures.
  2. À chaque passage, les prix réels de tous les jeux sont lus sur l'API de
     prix de Nintendo (par paquets de 50).
  3. Les jeux en promo sont comparés à la liste mémorisée au passage précédent
     (data/state.json). Les nouvelles promos, ou les baisses supplémentaires,
     partent sur Discord.

Aucune bibliothèque à installer : uniquement la bibliothèque standard Python.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# ----------------------------------------------------------------------------
# Réglages (modifiables dans le fichier scan.yml, pas besoin de toucher ici)
# ----------------------------------------------------------------------------
COUNTRY = "FR"
LANG = "fr"
CATALOG_URL = "https://search.nintendo-europe.com/fr/select"
PRICE_URL = "https://api.ec.nintendo.com/v1/price"

WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
MIN_DISCOUNT = float(os.getenv("MIN_DISCOUNT_PERCENT") or 30)
MIN_REGULAR_PRICE = float(os.getenv("MIN_REGULAR_PRICE") or 0)
MAX_ALERTS = int(os.getenv("MAX_ALERTS_PER_RUN") or 30)
TEST_DISCORD = os.getenv("TEST_DISCORD", "").strip().lower() == "true"

CATALOG_MAX_AGE_SECONDS = 7 * 3600
BATCH_SIZE = 50
WORKERS = 4

DATA_DIR = Path("data")
CATALOG_FILE = DATA_DIR / "catalog.json"
STATE_FILE = DATA_DIR / "state.json"

PARIS = ZoneInfo("Europe/Paris")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ----------------------------------------------------------------------------
# Outils réseau
# ----------------------------------------------------------------------------
def get_json(url, params=None, retries=3, timeout=30):
    """Télécharge une page JSON, avec quelques nouvelles tentatives."""
    if params:
        url = url + "?" + urllib.parse.urlencode(
            params, quote_via=urllib.parse.quote, safe=",*"
        )
    last_error = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            last_error = err
            if err.code not in (429, 500, 502, 503, 504):
                break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
            last_error = err
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Échec de la requête vers {url[:120]} : {last_error}")


def post_discord(payload):
    """Envoie un message sur le webhook Discord (gère la limite de débit)."""
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(3):
        req = urllib.request.Request(
            WEBHOOK,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": HEADERS["User-Agent"],
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30):
                return
        except urllib.error.HTTPError as err:
            if err.code == 429:
                try:
                    wait = float(json.loads(err.read().decode()).get("retry_after", 2))
                except Exception:
                    wait = 2.0
                time.sleep(min(wait, 30) + 0.5)
                continue
            raise RuntimeError(
                f"Discord a refusé le message (code {err.code}). "
                "Vérifie l'adresse du webhook dans les Secrets GitHub."
            )
    raise RuntimeError("Discord limite trop l'envoi, réessai au prochain passage.")


# ----------------------------------------------------------------------------
# Catalogue des jeux
# ----------------------------------------------------------------------------
def fetch_catalog():
    """Liste tous les jeux Switch / Switch 2 connus du moteur de recherche."""
    games = {}
    start, rows, total = 0, 500, None
    while total is None or start < total:
        data = get_json(
            CATALOG_URL,
            {
                "q": "*",
                "fq": "type:GAME AND system_type:nintendoswitch*",
                "sort": "sorting_title asc",
                "wt": "json",
                "start": start,
                "rows": rows,
            },
        )
        response = data.get("response", {})
        total = int(response.get("numFound", 0))
        docs = response.get("docs", [])
        if not docs:
            break
        for doc in docs:
            image = doc.get("image_url") or ""
            if image.startswith("//"):
                image = "https:" + image
            systems = doc.get("system_names_txt") or []
            info = {
                "t": doc.get("title") or "Jeu sans titre",
                "u": doc.get("url") or "",
                "i": image,
                "s": ", ".join(systems) if isinstance(systems, list) else str(systems),
            }
            for nsuid in doc.get("nsuid_txt") or []:
                games[str(nsuid)] = info
        start += rows
    if len(games) < 100:
        raise RuntimeError(
            f"Catalogue suspect ({len(games)} jeux seulement) : on l'ignore."
        )
    return games


def load_catalog():
    """Charge le catalogue conservé, ou le retélécharge s'il est trop ancien."""
    cached = None
    if CATALOG_FILE.exists():
        try:
            cached = json.loads(CATALOG_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            cached = None
    if cached and time.time() - cached.get("fetched_at", 0) < CATALOG_MAX_AGE_SECONDS:
        return cached["games"]
    try:
        games = fetch_catalog()
    except RuntimeError as err:
        if cached:
            print(f"Catalogue non rafraîchi ({err}), on garde l'ancien.")
            return cached["games"]
        raise
    DATA_DIR.mkdir(exist_ok=True)
    CATALOG_FILE.write_text(
        json.dumps({"fetched_at": time.time(), "games": games}, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Catalogue mis à jour : {len(games)} jeux.")
    return games


# ----------------------------------------------------------------------------
# Prix
# ----------------------------------------------------------------------------
def to_float(value):
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def fetch_prices(batch):
    data = get_json(
        PRICE_URL, {"country": COUNTRY, "lang": LANG, "ids": ",".join(batch)}
    )
    prices = data.get("prices")
    if prices is None:
        raise RuntimeError("Réponse de prix inattendue.")
    return prices


def scan_prices(nsuids):
    """
    Renvoie (promos, ids_en_echec).
    promos : {nsuid: {"reg": prix normal, "sale": prix promo, "end": date de fin}}
    """
    batches = [nsuids[i:i + BATCH_SIZE] for i in range(0, len(nsuids), BATCH_SIZE)]
    now = datetime.now(timezone.utc)
    promos, failed = {}, set()

    def work(batch):
        try:
            return batch, fetch_prices(batch), None
        except Exception as err:  # noqa: BLE001
            return batch, None, err

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for batch, prices, err in pool.map(work, batches):
            if err is not None:
                failed.update(batch)
                continue
            for entry in prices:
                regular = to_float((entry.get("regular_price") or {}).get("raw_value"))
                discount = entry.get("discount_price") or {}
                sale = to_float(discount.get("raw_value"))
                if regular is None or sale is None or sale >= regular:
                    continue
                start = parse_date(discount.get("start_datetime"))
                end = parse_date(discount.get("end_datetime"))
                if start and start > now:
                    continue
                if end and end < now:
                    continue
                promos[str(entry.get("title_id"))] = {
                    "reg": regular,
                    "sale": sale,
                    "end": discount.get("end_datetime"),
                }
    return promos, failed


# ----------------------------------------------------------------------------
# Mémoire entre deux passages
# ----------------------------------------------------------------------------
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"initialized": False, "onsale": {}}


def save_state(state):
    DATA_DIR.mkdir(exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )


# ----------------------------------------------------------------------------
# Discord
# ----------------------------------------------------------------------------
def euros(amount):
    return f"{amount:.2f}".replace(".", ",") + " €"


def game_link(info, nsuid):
    path = (info or {}).get("u") or ""
    if path.startswith("http"):
        link = path
    elif path.startswith("/"):
        if path.startswith("/fr/"):
            path = "/fr-fr/" + path[4:]
        link = "https://www.nintendo.com" + path
    else:
        title = urllib.parse.quote((info or {}).get("t", ""))
        return f"https://www.nintendo.com/fr-fr/Search/Search-299117.html?q={title}"
    return link + ("&" if "?" in link else "?") + "nsuid=" + nsuid


def build_embed(nsuid, promo, info):
    percent = round((1 - promo["sale"] / promo["reg"]) * 100)
    info = info or {}
    fields = [
        {"name": "Prix normal", "value": euros(promo["reg"]), "inline": True},
        {"name": "Prix promo", "value": f"**{euros(promo['sale'])}**", "inline": True},
        {"name": "Remise", "value": f"**-{percent} %**", "inline": True},
    ]
    end = parse_date(promo.get("end"))
    if end:
        fields.append(
            {
                "name": "Fin de l'offre",
                "value": end.astimezone(PARIS).strftime("%d/%m/%Y à %H:%M"),
                "inline": True,
            }
        )
    if info.get("s"):
        fields.append({"name": "Plateforme", "value": info["s"], "inline": True})
    embed = {
        "title": info.get("t", f"Jeu {nsuid}"),
        "url": game_link(info, nsuid),
        "color": 0xE60012,
        "fields": fields,
        "footer": {"text": f"nsuid {nsuid}"},
    }
    if info.get("i"):
        embed["thumbnail"] = {"url": info["i"]}
    return embed


def send_alerts(alerts, extra_text=None):
    """Envoie les alertes par groupes de 10 (limite de Discord)."""
    for i in range(0, len(alerts), 10):
        group = alerts[i:i + 10]
        payload = {"embeds": group}
        if i == 0:
            payload["content"] = extra_text or "🔥 **Nouvelle promo eShop France**"
        post_discord(payload)
        time.sleep(1.2)


# ----------------------------------------------------------------------------
# Programme principal
# ----------------------------------------------------------------------------
def main():
    if not WEBHOOK:
        sys.exit(
            "Le secret DISCORD_WEBHOOK_URL est vide. "
            "Ajoute l'adresse du webhook dans Settings > Secrets and variables > Actions."
        )

    if TEST_DISCORD:
        post_discord(
            {
                "content": "✅ Test réussi : le bot peut écrire dans ce salon. "
                "Il préviendra ici dès qu'une promo apparaît sur l'eShop France."
            }
        )
        print("Message de test envoyé.")
        return

    catalog = load_catalog()
    nsuids = sorted(catalog.keys())
    state = load_state()
    previous = state.get("onsale", {})

    promos, failed = scan_prices(nsuids)
    total_batches = -(-len(nsuids) // BATCH_SIZE)
    failed_batches = -(-len(failed) // BATCH_SIZE) if failed else 0
    print(
        f"{len(nsuids)} jeux vérifiés, {len(promos)} en promo, "
        f"{failed_batches}/{total_batches} paquets en échec."
    )
    if failed and failed_batches == total_batches:
        sys.exit("Aucun prix n'a pu être lu : Nintendo bloque peut-être la requête.")

    # Premier passage : on mémorise l'existant sans rien envoyer.
    if not state.get("initialized"):
        new_state = {
            "initialized": True,
            "onsale": {k: {"p": v["sale"], "e": v["end"]} for k, v in promos.items()},
        }
        save_state(new_state)
        post_discord(
            {
                "content": (
                    f"✅ Surveillance démarrée : {len(promos)} promos déjà en cours "
                    "(non listées). Je préviens ici dès qu'une nouvelle apparaît."
                )
            }
        )
        print("Initialisation terminée.")
        return

    # Détection des nouveautés.
    candidates = []
    for nsuid, promo in promos.items():
        before = previous.get(nsuid)
        is_new = before is None or promo["sale"] < before["p"] - 0.005
        percent = (1 - promo["sale"] / promo["reg"]) * 100
        passes = percent >= MIN_DISCOUNT and promo["reg"] >= MIN_REGULAR_PRICE
        if is_new and passes:
            candidates.append((percent, nsuid, promo))
    candidates.sort(key=lambda item: -item[0])
    to_send = candidates[:MAX_ALERTS]
    postponed = {nsuid for _, nsuid, _ in candidates[MAX_ALERTS:]}

    # Nouvel état : promos actuelles, sauf celles repoussées au prochain passage.
    new_onsale = {
        k: {"p": v["sale"], "e": v["end"]}
        for k, v in promos.items()
        if k not in postponed
    }
    # Les paquets en échec gardent leur ancien état (on ne sait pas).
    for nsuid in failed:
        if nsuid in previous:
            new_onsale[nsuid] = previous[nsuid]

    if to_send:
        embeds = [build_embed(n, p, catalog.get(n)) for _, n, p in to_send]
        extra = None
        if postponed:
            extra = (
                f"🔥 **Nouvelles promos eShop France** "
                f"({len(postponed)} autres arrivent au prochain passage)"
            )
        send_alerts(embeds, extra)
        print(f"{len(to_send)} alertes envoyées.")
    else:
        print("Rien de nouveau.")

    save_state({"initialized": True, "onsale": new_onsale})


if __name__ == "__main__":
    main()
