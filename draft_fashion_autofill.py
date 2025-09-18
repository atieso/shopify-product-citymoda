import os, sys, html, time, json, traceback, requests
from dotenv import load_dotenv

VERSION = "2025-09-19-v3"
load_dotenv()

# ========= CONFIG =========
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "city-tre-srl.myshopify.com")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
SHOPIFY_ADMIN_TOKEN  = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

# Scegli UNA fonte immagini (Bing OPPURE Google CSE)
BING_IMAGE_KEY       = os.getenv("BING_IMAGE_KEY", "")            # Azure Cognitive Services (facoltativo)
GOOGLE_CSE_KEY       = os.getenv("GOOGLE_CSE_KEY", "")            # Google API Key (facoltativo)
GOOGLE_CSE_CX        = os.getenv("GOOGLE_CSE_CX", "")             # Custom Search Engine ID (facoltativo)

# Descrizioni (opzionali, se NON usi Shopify Magic)
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")            # lascia vuoto per non usare AI esterna
USE_SHOPIFY_MAGIC_ONLY = os.getenv("USE_SHOPIFY_MAGIC_ONLY", "true").lower() == "true"

MAX_PRODUCTS         = int(os.getenv("MAX_PRODUCTS", "25"))       # quanti prodotti processare per run
SAFE_DOMAINS_HINTS   = ["cdn", "images", "media", "static", "assets", "content", "img"]  # euristica
DEBUG                = os.getenv("DEBUG", "false").lower() == "true"

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

def create_product_metafield(product_id_num: int, namespace: str, key: str, value: str, mtype: str="single_line_text_field"):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/metafields.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    payload = {"metafield": {
        "namespace": namespace,
        "key": key,
        "value": value,
        "type": mtype,
        "owner_resource": "product",
        "owner_id": product_id_num
    }}
    r = requests.post(url, json=payload, headers=h, timeout=20)
    r.raise_for_status()
    return safe_get(r.json(), "metafield", "id")

# ========= IMAGE SEARCH =========
def bing_image_search(query: str):
    if not BING_IMAGE_KEY: return None
    url = "https://api.bing.microsoft.com/v7.0/images/search"
    h = {"Ocp-Apim-Subscription-Key": BING_IMAGE_KEY}
    params = {
        "q": query,
        "safeSearch": "Moderate",
        "count": 12,
        "imageType": "Photo",
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
        "safe": "active"
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

def pick_best_image(urls):
    if not urls: return None
    urls_sorted = sorted(urls, key=lambda u: 0 if any(h in (u or "").lower() for h in SAFE_DOMAINS_HINTS) else 1)
    return urls_sorted[0]

# ========= DESCRIPTION (solo se NON usi Magic) =========
def gen_description(title: str, vendor: str, ptype: str, ean: str):
    prompt = f"""
Scrivi una descrizione breve e pulita per un prodotto moda.
Dati:
- Titolo: {title}
- Brand: {vendor or "N/D"}
- Categoria: {ptype or "Abbigliamento"}
- EAN: {ean or "N/D"}
Stile: professionale, italiano, max 120-150 parole.
Chiudi con 3-5 bullet (materiali, vestibilità, cura, occasioni d’uso, fit).
Output in HTML semplice (<p>, <ul><li>), senza claim esagerati.
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

    title_h = html.escape(title or "Prodotto moda")
    vendor_h = html.escape(vendor or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    base = f"<p>{title_h}{(' di ' + vendor_h) if vendor_h else ''}: essenziale {ptype_h} per il guardaroba quotidiano. " \
           f"Design curato e materiali selezionati per comfort e durata, adatto a molteplici occasioni.</p>"
    bullets = "<ul>" + "".join([
        "<li>Materiali di qualità</li>",
        "<li>Vestibilità equilibrata</li>",
        "<li>Dettagli curati</li>",
        "<li>Facile da abbinare</li>",
        "<li>Istruzioni di cura semplici</li>",
    ]) + "</ul>"
    return base + bullets

# ========= MAIN =========
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    if DEBUG:
        print(f"[DEBUG] Store={SHOPIFY_STORE_DOMAIN} | APIv={SHOPIFY_API_VERSION} | MagicOnly={USE_SHOPIFY_MAGIC_ONLY} | Max={MAX_PRODUCTS}")
        fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[DEBUG] Fonte immagini: {fonte}")

    processed = 0
    cursor = None
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
            try:
                # --- stato contenuti (robusto a None) ---
                img_edges = safe_get(n, "images", "edges", default=[]) or []
                has_img   = len(img_edges) > 0

                body_html = safe_strip(n.get("bodyHtml"))
                has_desc  = bool(body_html)
                if has_img or has_desc:
                    continue

                # --- campi principali ---
                title = safe_strip(n.get("title"))
                vendor = safe_strip(n.get("vendor"))
                ptype  = safe_strip(n.get("productType"))
                ean    = first_barcode(n.get("variants"))

                if not title:
                    print("[SKIP] Prodotto senza titolo.")
                    continue

                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'}")
                pid_num = product_id_from_gid(n["id"])

                # --- DESCRIZIONE ---
                if USE_SHOPIFY_MAGIC_ONLY:
                    try:
                        create_product_metafield(pid_num, "ai", "needs_description", "true")
                        print("  - Flag impostato: ai.needs_description=true (usa Shopify Magic dall’Admin)")
                    except Exception as ex:
                        print(f"  - ERRORE flag Magic: {ex}")
                else:
                    desc_html = gen_description(title, vendor, ptype, ean)
                    try:
                        update_description(pid_num, desc_html)
                        print("  - Descrizione aggiornata ✅")
                    except Exception as ex:
                        print(f"  - ERRORE descrizione: {ex}")

                # --- IMMAGINE ---
                img_url = None
                if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                    base = " ".join([x for x in [vendor, title, "product"] if x]).strip()
                    queries = []
                    if ean:
                        queries.append(f"{base} {ean}")
                    queries += [
                        base,
                        f"{vendor} {title}".strip(),
                        f"{vendor} {title} lookbook".strip(),
                        f"{vendor} {title} site:{(vendor or '').lower()}.com".strip(),
                    ]

                    urls = None
                    for q in queries:
                        if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
                            urls = google_cse_image_search(q)
                        if (not urls) and BING_IMAGE_KEY:
                            urls = bing_image_search(q)
                        if urls:
                            img_url = pick_best_image(urls)
                            if img_url:
                                break

                if img_url:
                    try:
                        image_id = add_image(pid_num, img_url, alt_text=f"{vendor} {title}".strip())
                        print(f"  - Immagine aggiunta id={image_id} ✅")
                    except Exception as ex:
                        print(f"  - ERRORE immagine: {ex}")
                else:
                    print("  - Nessuna immagine trovata per la query.")

                processed += 1
                time.sleep(0.8)  # rate limit “gentile”
            except Exception as ex:
                print(f"[ERROR prodotto] {ex}")
                traceback.print_exc()
                continue

        if not has_next:
            break

    print(f"Fatto. Prodotti processati: {processed}")

# ========= ENTRYPOINT =========
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)  # cron resta verde
    except Exception as e:
        print("=== UNCAUGHT ERROR ===")
        print(repr(e))
        traceback.print_exc()
        sys.exit(0)  # cron resta verde anche su errori imprevisti
