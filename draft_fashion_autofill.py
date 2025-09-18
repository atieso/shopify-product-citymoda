import os, sys, html, time, json, csv, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

VERSION = "2025-09-19-v6"
load_dotenv()

# ========= CONFIG =========
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "city-tre-srl.myshopify.com")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
SHOPIFY_ADMIN_TOKEN  = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

# Fonti immagini (usa Google e/o Bing; va bene anche solo una)
BING_IMAGE_KEY       = os.getenv("BING_IMAGE_KEY", "")
GOOGLE_CSE_KEY       = os.getenv("GOOGLE_CSE_KEY", "")
GOOGLE_CSE_CX        = os.getenv("GOOGLE_CSE_CX", "")

# Descrizioni (se NON usi Shopify Magic)
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
USE_SHOPIFY_MAGIC_ONLY = os.getenv("USE_SHOPIFY_MAGIC_ONLY", "false").lower() == "true"

# Quante immagini caricare per prodotto (hard cap = 5)
MAX_IMAGES_PER_PRODUCT = min(int(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")), 5)
MAX_PRODUCTS           = int(os.getenv("MAX_PRODUCTS", "25"))

# Preferenze selezione immagini
SAFE_DOMAINS_HINTS   = ["cdn", "images", "media", "static", "assets", "content", "img", "cloudfront", "akamaized"]
WHITE_BG_KEYWORDS    = ["white", "bianco", "packshot", "studio", "product", "plain"]
# opzionale: priorità a domini brand (csv): es. "guess.com,guess.eu,calvinklein.it"
BRAND_DOMAINS_WHITELIST = [d.strip().lower() for d in os.getenv("BRAND_DOMAINS_WHITELIST", "").split(",") if d.strip()]

DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ========= UTILS =========
def safe_get(d, *path, default=None):
    cur = d or {}
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return default
    return cur if cur is not None else default

def safe_strip(v):
    try:
        return str(v or "").strip()
    except Exception:
        return ""

def product_id_from_gid(gid: str) -> int:
    return int(str(gid).split("/")[-1])

def first_barcode(variants):
    edges = safe_get(variants, "edges", default=[]) or []
    for edge in edges:
        node = edge.get("node") or {}
        bc = safe_strip(node.get("barcode"))
        if bc:
            return bc
    return ""

def domain(url):
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def score_image_url(u, vendor=""):
    """Heuristics: priorità a CDN/brand, packshot, white background hints; penalizza marketplace/social."""
    u = u or ""
    d = domain(u)
    score = 0
    # 1) brand whitelist
    if any(wh in d for wh in BRAND_DOMAINS_WHITELIST if wh):
        score -= 5
    # 2) vendor nel dominio
    if vendor and vendor.lower().replace(" ", "") in d.replace("-", "").replace(" ", ""):
        score -= 3
    # 3) CDN/hint puliti
    if any(h in d for h in SAFE_DOMAINS_HINTS):
        score -= 2
    # 4) parole chiave da packshot/white bg
    if any(k in u.lower() for k in WHITE_BG_KEYWORDS):
        score -= 2
    # 5) penalizza marketplace/social rumorosi
    if any(bad in d for bad in ["ebay.", "aliexpress.", "pinterest.", "facebook.", "tumblr.", "wordpress.", "blogspot."]):
        score += 3
    return score

# ========= SHOPIFY HELPERS =========
def shopify_graphql(query: str, variables=None):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    r = requests.post(url, json={"query": query, "variables": variables or {}}, headers=h, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]

def fetch_draft_products(limit=50, cursor=None):
    q = """
    query($first:Int!, $after:String){
      products(first:$first, after:$after, query:"status:draft") {
        pageInfo { hasNextPage endCursor }
        edges {
          cursor
          node {
            id
            title
            vendor
            productType
            handle
            bodyHtml
            images(first:1){ edges{ node{ id } } }
            variants(first:10){ edges{ node{ id sku barcode title } } }
          }
        }
      }
    }
    """
    return shopify_graphql(q, {"first": limit, "after": cursor})

def add_image(product_id_num: int, image_src: str, alt_text: str=""):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}/images.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    payload = {"image": {"src": image_src, "alt": (alt_text or "")[:255]}}
    r = requests.post(url, json=payload, headers=h, timeout=30)
    r.raise_for_status()
    return safe_get(r.json(), "image", "id")

def update_description(product_id_num: int, body_html: str):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    r = requests.put(url, json={"product": {"id": product_id_num, "body_html": body_html}}, headers=h, timeout=30)
    r.raise_for_status()
    return True

def set_product_metafield_gql(product_gid: str, namespace: str, key: str, value: str, mtype: str="single_line_text_field"):
    """Crea/aggiorna un metafield sul prodotto via GraphQL metafieldsSet."""
    mutation = """
    mutation SetMF($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id key namespace }
        userErrors { field message code }
      }
    }
    """
    vars_ = {
      "metafields": [{
        "ownerId": product_gid,
        "namespace": namespace,
        "key": key,
        "type": mtype,
        "value": value
      }]
    }
    data = shopify_graphql(mutation, vars_)
    errs = (data.get("metafieldsSet") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")
    return True

# ========= IMAGE SEARCH =========
def bing_image_search(query: str):
    if not BING_IMAGE_KEY: return None
    url = "https://api.bing.microsoft.com/v7.0/images/search"
    h = {"Ocp-Apim-Subscription-Key": BING_IMAGE_KEY}
    params = {
        "q": query,
        "safeSearch": "Moderate",
        "count": 30,
        "imageType": "Photo",
        "imageContent": "Product",
        "license": "Any"
    }
    try:
        r = requests.get(url, headers=h, params=params, timeout=20)
        r.raise_for_status()
        data = r.json().get("value", [])
        return [x.get("contentUrl") for x in data if x.get("contentUrl")]
    except Exception as e:
        print(f"[Bing ERROR] {e}")
        return None

def google_cse_image_search(query: str):
    if not (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
        return None
    base = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_CSE_KEY,
        "cx": GOOGLE_CSE_CX,
        "q": query,
        "searchType": "image",
        "num": 10,
        "safe": "active",
        "imgType": "photo",
        "imgDominantColor": "white"  # privilegia sfondo bianco
    }
    try:
        r = requests.get(base, params=params, timeout=20)
        if r.status_code >= 400:
            try:
                print(f"[Google CSE ERROR {r.status_code}] {r.json()}")
            except Exception:
                print(f"[Google CSE ERROR {r.status_code}] {r.text}")
            return None
        items = r.json().get("items", [])
        return [x.get("link") for x in items if x.get("link")]
    except Exception as e:
        print(f"[Google CSE EXCEPTION] {e}")
        return None

def collect_candidate_images(queries, vendor=""):
    """Raccoglie più URL, li ordina per qualità euristica e rimuove duplicati."""
    urls = []
    seen = set()
    for q in queries:
        ulist = None
        # Google prima (se disponibile), poi Bing
        if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
            ulist = google_cse_image_search(q)
        if (not ulist) and BING_IMAGE_KEY:
            ulist = bing_image_search(q)
        if not ulist:
            continue
        for u in ulist:
            if not u or u in seen:
                continue
            seen.add(u)
            urls.append(u)

    # Scoring & sort
    urls.sort(key=lambda u: score_image_url(u, vendor))
    return urls

def pick_top_images(urls, vendor="", max_n=3):
    """Seleziona fino a max_n immagini preferendo packshot/white e domini 'puliti'."""
    if not urls:
        return []
    # già ordinate da collect_candidate_images
    top = []
    seen_domains = set()
    for u in urls:
        d = domain(u)
        # evita 2 immagini dallo stesso dominio se possibile
        if d in seen_domains and len(top) < max_n - 1:
            continue
        top.append(u)
        seen_domains.add(d)
        if len(top) >= max_n:
            break
    return top

# ========= DESCRIPTION (paragrafo + bullet) =========
def gen_description_html(title: str, vendor: str, ptype: str, ean: str) -> str:
    """Ritorna HTML con <p> iniziale + <ul> di bullet."""
    # Se hai OPENAI_API_KEY, prova un testo un po' più ricco
    prompt = f"""
Scrivi una descrizione per un prodotto moda.
Dati:
- Titolo: {title}
- Brand: {vendor or "N/D"}
- Categoria: {ptype or "Abbigliamento"}
- EAN: {ean or "N/D"}
Formato:
1) Un paragrafo iniziale in italiano (max 120-150 parole), tono professionale e chiaro.
2) Un elenco puntato (3-5 punti) con dettagli pratici (materiali, vestibilità, cura, occasioni d'uso, fit).
Output SOLO in HTML semplice usando <p> e <ul><li>.
Niente claim esagerati o linguaggio promozionale eccessivo.
""".strip()

    if OPENAI_API_KEY:
        try:
            resp = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                data=json.dumps({"model": "gpt-4o-mini", "input": prompt}),
                timeout=45
            )
            resp.raise_for_status()
            txt = resp.json().get("output_text", "").strip()
            if txt:
                return txt
        except Exception as e:
            print(f"[WARN] OpenAI non disponibile: {e}")

    # Fallback semplice, garantendo paragrafo + bullet
    title_h = html.escape(title or "Prodotto moda")
    vendor_h = html.escape(vendor or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    p = (
        f"<p>{title_h}{(' di ' + vendor_h) if vendor_h else ''}: un capo {ptype_h.lower()} essenziale, "
        f"pensato per un utilizzo quotidiano con attenzione a comfort e durata. "
        f"Linee pulite e dettagli curati lo rendono facile da abbinare nelle diverse occasioni, "
        f"dal lavoro al tempo libero.</p>"
    )
    ul = "<ul>" + "".join([
        "<li>Materiali selezionati per comfort e resistenza</li>",
        "<li>Vestibilità equilibrata, facile da indossare</li>",
        "<li>Dettagli curati e finiture pulite</li>",
        "<li>Indicazioni di cura semplici</li>",
        "<li>Adatto a molteplici occasioni</li>",
    ]) + "</ul>"
    return p + ul

# ========= MAIN =========
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    if DEBUG:
        print(f"[DEBUG] Store={SHOPIFY_STORE_DOMAIN} | APIv={SHOPIFY_API_VERSION} | MagicOnly={USE_SHOPIFY_MAGIC_ONLY} | MaxProducts={MAX_PRODUCTS} | MaxImages={MAX_IMAGES_PER_PRODUCT}")
        fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[DEBUG] Fonte immagini: {fonte}")

    processed = 0
    cursor = None
    results = []  # per report CSV

    while processed < MAX_PRODUCTS:
        try:
            data = fetch_draft_products(limit=50, cursor=cursor)
        except Exception as e:
            print(f"[ERROR fetch_draft_products] {e}")
            break

        products = safe_get(data, "products", default={})
        edges = products.get("edges", []) or []
        page_info = products.get("pageInfo", {}) or {}
        cursor = page_info.get("endCursor")
        has_next = bool(page_info.get("hasNextPage"))

        if not edges:
            print("Nessun prodotto in Bozza trovato.")
            break

        for e in edges:
            if processed >= MAX_PRODUCTS:
                break

            n = e.get("node") or {}
            notes = []
            desc_updated = False
            uploaded = 0

            try:
                # --- stato contenuti (robusto a None) ---
                img_edges = safe_get(n, "images", "edges", default=[]) or []
                has_img   = len(img_edges) > 0
                body_html = safe_strip(n.get("bodyHtml"))
                has_desc  = bool(body_html)
                if has_img or has_desc:
                    continue

                # --- campi principali ---
                title  = safe_strip(n.get("title"))
                vendor = safe_strip(n.get("vendor"))
                ptype  = safe_strip(n.get("productType"))
                ean    = first_barcode(n.get("variants"))
                pid_num = product_id_from_gid(n["id"])

                if not title:
                    notes.append("skip: titolo mancante")
                    results.append({
                        "product_id": pid_num, "title": "", "vendor": vendor, "ean": ean,
                        "images_uploaded": uploaded, "description_updated": desc_updated, "notes": "; ".join(notes)
                    })
                    continue

                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'}")

                # --- DESCRIZIONE ---
                if USE_SHOPIFY_MAGIC_ONLY:
                    try:
                        set_product_metafield_gql(n["id"], "ai_flags", "needs_description", "true")
                        notes.append("flag Magic impostato")
                        print("  - Flag impostato: ai_flags.needs_description=true (usa Shopify Magic dall’Admin)")
                    except Exception as ex:
                        notes.append(f"flag Magic errore: {ex}")
                        print(f"  - ERRORE flag Magic: {ex}")
                else:
                    try:
                        desc_html = gen_description_html(title, vendor, ptype, ean)
                        update_description(pid_num, desc_html)
                        desc_updated = True
                        print("  - Descrizione aggiornata ✅")
                    except Exception as ex:
                        notes.append(f"descrizione errore: {ex}")
                        print(f"  - ERRORE descrizione: {ex}")

                # --- IMMAGINI (fino a MAX_IMAGES_PER_PRODUCT, cap 5) ---
                img_urls = []
                if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                    base = " ".join([x for x in [vendor, title, "product"] if x]).strip()
                    queries = []
                    if ean:
                        queries.append(f"{base} {ean}")
                    queries += [
                        base,
                        f"{vendor} {title}".strip(),
                        f"{vendor} {title} packshot".strip(),
                        f"{vendor} {title} white background".strip(),
                        f"{vendor} {title} lookbook".strip(),
                        f"{vendor} {title} site:{(vendor or '').lower()}.com".strip(),
                    ]

                    candidates = collect_candidate_images(queries, vendor=vendor)
                    img_urls = pick_top_images(candidates, vendor=vendor, max_n=MAX_IMAGES_PER_PRODUCT)

                if img_urls:
                    for u in img_urls:
                        try:
                            img_id = add_image(pid_num, u, alt_text=f"{vendor} {title}".strip())
                            uploaded += 1
                            print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                        except Exception as ex:
                            notes.append(f"img errore: {ex}")
                            print(f"  - ERRORE immagine: {ex}")
                    if uploaded == 0:
                        notes.append("nessuna immagine caricata (tutte fallite)")
                        print("  - Nessuna immagine caricata (tutte fallite).")
                else:
                    notes.append("nessuna immagine trovata")
                    print("  - Nessuna immagine trovata per la query.")

                processed += 1

            except Exception as ex:
                notes.append(f"errore prodotto: {ex}")
                print(f"[ERROR prodotto] {ex}")
                traceback.print_exc()

            # append result row
            results.append({
                "product_id": pid_num if 'pid_num' in locals() else "",
                "title": title if 'title' in locals() else "",
                "vendor": vendor if 'vendor' in locals() else "",
                "ean": ean if 'ean' in locals() else "",
                "images_uploaded": uploaded,
