import os, sys, html, time, json, requests
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

# ========= CONFIG =========
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "city-tre-srl.myshopify.com")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
SHOPIFY_ADMIN_TOKEN  = os.getenv("SHOPIFY_ADMIN_TOKEN", "PASTE_ADMIN_TOKEN")

# Scegli UNA delle due fonti immagini: BING *oppure* GOOGLE CSE
BING_IMAGE_KEY       = os.getenv("BING_IMAGE_KEY", "")          # Azure Cognitive Services
GOOGLE_CSE_KEY       = os.getenv("GOOGLE_CSE_KEY", "")          # Google API Key
GOOGLE_CSE_CX        = os.getenv("GOOGLE_CSE_CX", "")           # Custom Search Engine ID

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")          # opzionale per descrizioni migliori

MAX_PRODUCTS         = int(os.getenv("MAX_PRODUCTS", "25"))     # quanti prodotti per run
SAFE_DOMAINS_HINTS   = ["cdn", "images", "media", "static"]     # euristiche: preferisci CDN ufficiali

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

def fetch_draft_products(limit=25, cursor=None):
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
    return r.json().get("image", {}).get("id")

def update_description(product_id_num: int, body_html: str):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id_num}.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    r = requests.put(url, json={"product": {"id": product_id_num, "body_html": body_html}}, headers=h, timeout=30)
    r.raise_for_status()
    return True

# ========= IMAGE SEARCH (choose one) =========
def bing_image_search(query: str):
    if not BING_IMAGE_KEY: return None
    url = "https://api.bing.microsoft.com/v7.0/images/search"
    h = {"Ocp-Apim-Subscription-Key": BING_IMAGE_KEY}
    params = {
        "q": query,
        "safeSearch": "Moderate",
        "count": 10,
        "imageType": "Photo",
        "license": "Any"
    }
    r = requests.get(url, headers=h, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("value", [])
    return [x.get("contentUrl") for x in data if x.get("contentUrl")]

def google_cse_image_search(query: str):
    if not (GOOGLE_CSE_KEY and GOOGLE_CSE_CX): return None
    base = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_CSE_KEY,
        "cx": GOOGLE_CSE_CX,
        "q": query,
        "searchType": "image",
        "num": 10,
        "safe": "active"
    }
    r = requests.get(base, params=params, timeout=20)
    r.raise_for_status()
    items = r.json().get("items", [])
    return [x.get("link") for x in items if x.get("link")]

def pick_best_image(urls):
    if not urls: return None
    # piccola euristica: preferisci CDN / domini "puliti"
    urls_sorted = sorted(urls, key=lambda u: 0 if any(h in u for h in SAFE_DOMAINS_HINTS) else 1)
    return urls_sorted[0]

# ========= DESCRIPTION =========
def gen_description(title: str, vendor: str, ptype: str, ean: str):
    prompt = f"""
Scrivi una descrizione breve e pulita per un prodotto moda.
Dati:
- Titolo: {title}
- Brand: {vendor or "N/D"}
- Categoria: {ptype or "Abbigliamento"}
- EAN: {ean or "N/D"}
Stile: professionale, italiano, massimo 120-150 parole.
Chiudi con 3-5 bullet sui punti chiave (materiali, vestibilità, cura, occasioni d’uso, fit).
Output in HTML semplice (<p>, <ul><li>), niente claim esagerati.
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
            if txt: return txt
        except Exception as e:
            print(f"[WARN] OpenAI non disponibile: {e}")

    # fallback senza AI
    title_h = html.escape(title)
    vendor_h = html.escape(vendor or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    base = f"<p>{title_h} di {vendor_h}: essenziale {ptype_h} per il guardaroba quotidiano. " \
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
def product_id_from_gid(gid: str) -> int:
    return int(gid.split("/")[-1])

def first_barcode(variants):
    for edge in variants.get("edges", []):
        bc = edge["node"].get("barcode") or ""
        if bc.strip():
            return bc.strip()
    return ""

def main():
    processed = 0
    cursor = None
    while processed < MAX_PRODUCTS:
        data = fetch_draft_products(limit=50, cursor=cursor)
        edges = data["products"]["edges"]
        cursor = data["products"]["pageInfo"]["endCursor"]
        has_next = data["products"]["pageInfo"]["hasNextPage"]

        if not edges:
            print("Nessun prodotto in Bozza trovato.")
            break

        for e in edges:
            if processed >= MAX_PRODUCTS:
                break
            n = e["node"]
            # filtra SOLO prodotti senza immagini e senza descrizione
            has_img = len(n["images"]["edges"]) > 0
            has_desc = bool(n.get("bodyHtml", "").strip())
            if has_img or has_desc:
                continue

            title = n["title"].strip()
            vendor = (n.get("vendor") or "").strip()
            ptype  = (n.get("productType") or "").strip()
            ean    = first_barcode(n["variants"])

            # se manca anche l'EAN, salta (puoi togliere questo vincolo se vuoi)
            if not ean:
                print(f"[SKIP] {title} (no EAN)")
                continue

            print(f"[PROCESS] {title} | brand={vendor} | ean={ean}")

            # 1) descrizione
            desc_html = gen_description(title, vendor, ptype, ean)

            # 2) immagine: query semplice brand + title (+ ean)
            query = " ".join([x for x in [vendor, title, ean, "product"] if x]).strip()

            urls = None
            if BING_IMAGE_KEY:
                urls = bing_image_search(query)
            elif GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
                urls = google_cse_image_search(query)
            img_url = pick_best_image(urls) if urls else None

            pid_num = product_id_from_gid(n["id"])

            # carica prima la descrizione (così almeno qualcosa aggiorna)
            try:
                update_description(pid_num, desc_html)
                print("  - Descrizione aggiornata ✅")
            except Exception as ex:
                print(f"  - ERRORE descrizione: {ex}")

            # poi prova immagine (se trovata)
            if img_url:
                try:
                    image_id = add_image(pid_num, img_url, alt_text=f"{vendor} {title}".strip())
                    print(f"  - Immagine aggiunta id={image_id} ✅")
                except Exception as ex:
                    print(f"  - ERRORE immagine: {ex}")
            else:
                print("  - Nessuna immagine trovata per la query.")

            processed += 1
            time.sleep(0.8)  # piccolo rate limit “gentile”

        if not has_next:
            break

    print(f"Fatto. Prodotti processati: {processed}")

if __name__ == "__main__":
    main()
