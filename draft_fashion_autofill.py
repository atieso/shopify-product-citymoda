import os, sys, html, time, json, csv, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

VERSION = "2025-09-19-v7g"
load_dotenv()

# ========= CONFIG =========
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "city-tre-srl.myshopify.com")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
SHOPIFY_ADMIN_TOKEN  = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

# Fonti immagini
BING_IMAGE_KEY       = os.getenv("BING_IMAGE_KEY", "")
GOOGLE_CSE_KEY       = os.getenv("GOOGLE_CSE_KEY", "")
GOOGLE_CSE_CX        = os.getenv("GOOGLE_CSE_CX", "")

# Descrizioni
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
USE_SHOPIFY_MAGIC_ONLY = os.getenv("USE_SHOPIFY_MAGIC_ONLY", "false").lower() == "true"

# Immagini (cap 5)
MAX_IMAGES_PER_PRODUCT = min(int(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")), 5)
MAX_PRODUCTS           = int(os.getenv("MAX_PRODUCTS", "25"))

SAFE_DOMAINS_HINTS   = ["cdn", "images", "media", "static", "assets", "content", "img", "cloudfront", "akamaized"]
WHITE_BG_KEYWORDS    = ["white", "bianco", "packshot", "studio", "product", "plain"]
BRAND_DOMAINS_WHITELIST = [d.strip().lower() for d in os.getenv("BRAND_DOMAINS_WHITELIST", "").split(",") if d.strip()]

WHITE_BG_BORDER_PCT  = float(os.getenv("WHITE_BG_BORDER_PCT", "0.12"))
WHITE_BG_THRESHOLD   = int(os.getenv("WHITE_BG_THRESHOLD", "242"))
WHITE_BG_MIN_RATIO   = float(os.getenv("WHITE_BG_MIN_RATIO", "0.82"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC", "7"))
MAX_DOWNLOAD_BYTES   = int(os.getenv("MAX_DOWNLOAD_BYTES", "3500000"))

DEBUG = os.getenv("DEBUG", "false").lower() == "true"
ADMIN_URL_TEMPLATE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/products/{{pid}}"

# Filtro opzionale: solo questi ID (product o variant)
ALLOWED_IDS = [x.strip() for x in os.getenv("PRODUCT_IDS", "").split(",") if x.strip()]

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

def gid_from_product_id(pid_num: int) -> str:
    return f"gid://shopify/Product/{pid_num}"

def domain(u):
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def score_image_url(u, vendor=""):
    u = u or ""
    d = domain(u)
    score = 0
    if any(wh in d for wh in BRAND_DOMAINS_WHITELIST if wh): score -= 5
    if vendor and vendor.lower().replace(" ", "") in d.replace("-", "").replace(" ", ""): score -= 3
    if any(h in d for h in SAFE_DOMAINS_HINTS): score -= 2
    if any(k in u.lower() for k in WHITE_BG_KEYWORDS): score -= 2
    if any(bad in d for bad in ["ebay.", "aliexpress.", "pinterest.", "facebook.", "tumblr.", "wordpress.", "blogspot."]): score += 3
    return score

def first_barcode(variants):
    edges = safe_get(variants, "edges", default=[]) or []
    for edge in edges:
        node = edge.get("node") or {}
        bc = safe_strip(node.get("barcode"))
        if bc:
            return bc
    return ""

def product_header(title, vendor, ean):
    return f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'}"

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

def shopify_rest_get(path, params=None):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}{path}"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN}
    r = requests.get(url, headers=h, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

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
            status
            bodyHtml
            images(first:1){ edges{ node{ id } } }
            variants(first:10){ edges{ node{ id sku barcode title } } }
          }
        }
      }
    }
    """
    return shopify_graphql(q, {"first": limit, "after": cursor})

def fetch_products_by_ids_graphql(id_list):
    """GraphQL nodes(ids:[gid://shopify/Product/<id>])"""
    if not id_list: return []
    q = """
    query($ids:[ID!]!){
      nodes(ids:$ids){
        ... on Product {
          id title vendor productType handle status bodyHtml
          images(first:1){ edges{ node{ id } } }
          variants(first:10){ edges{ node{ id sku barcode title } } }
        }
      }
    }
    """
    gids = [gid_from_product_id(int(x)) for x in id_list]
    data = shopify_graphql(q, {"ids": gids})
    nodes = data.get("nodes") or []
    out = []
    for n in nodes:
        if n:
            out.append({"node": n})
    return out

def fetch_products_by_ids_rest(id_list):
    """REST /products.json?ids=... (comma-separated product IDs)"""
    try:
        ids_param = ",".join(str(int(x)) for x in id_list)
        js = shopify_rest_get(f"/products.json", params={"ids": ids_param, "limit": 250})
        prods = js.get("products", []) or []
        edges = []
        for p in prods:
            # mappa REST -> nodi compatibili
            node = {
                "id": gid_from_product_id(int(p["id"])),
                "title": p.get("title"),
                "vendor": p.get("vendor"),
                "productType": p.get("product_type"),
                "handle": p.get("handle"),
                "status": p.get("status"),
                "bodyHtml": p.get("body_html"),
                "images": {"edges": [{"node": {"id": im.get("id")}} for im in (p.get("images") or [])[:1]]},
                "variants": {"edges": [{"node": {
                    "id": v.get("id"),
                    "sku": v.get("sku"),
                    "barcode": v.get("barcode"),
                    "title": v.get("title")
                }} for v in (p.get("variants") or [])[:10]]}
            }
            edges.append({"node": node})
        return edges
    except Exception:
        return []

def resolve_product_ids_from_variant_ids(variant_ids):
    """Dato un elenco di variant IDs numerici, ritorna la lista di product IDs (numerici)."""
    resolved = []
    for vid in variant_ids:
        try:
            js = shopify_rest_get(f"/variants/{int(vid)}.json")
            v = js.get("variant", {})
            pid = v.get("product_id")
            if pid:
                resolved.append(str(pid))
        except Exception as e:
            print(f"[WARN] variant {vid} non risolvibile: {e}")
    return list(dict.fromkeys(resolved))  # unique, preserve order

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
    mutation = """
    mutation SetMF($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id key namespace }
        userErrors { field message code }
      }
    }
    """
    vars_ = {"metafields": [{"ownerId": product_gid, "namespace": namespace, "key": key, "type": mtype, "value": value}]}
    data = shopify_graphql(mutation, vars_)
    errs = (data.get("metafieldsSet") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")
    return True

# ========= IMAGE SEARCH/FILTER =========
def bing_image_search(query: str, count=50, pages=2):
    if not BING_IMAGE_KEY: return []
    url = "https://api.bing.microsoft.com/v7.0/images/search"
    h = {"Ocp-Apim-Subscription-Key": BING_IMAGE_KEY}
    out = []
    for p in range(pages):
        params = {
            "q": query, "safeSearch": "Moderate",
            "count": count, "offset": p * count,
            "imageType": "Photo", "imageContent": "Product", "license": "Any"
        }
        try:
            r = requests.get(url, headers=h, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("value", [])
            out += [x.get("contentUrl") for x in data if x.get("contentUrl")]
        except Exception as e:
            print(f"[Bing ERROR] {e}")
            break
    return out

def google_cse_image_search(query: str, per_page=10, pages=3):
    if not (GOOGLE_CSE_KEY and GOOGLE_CSE_CX): return []
    base = "https://www.googleapis.com/customsearch/v1"
    out = []
    for i in range(pages):
        start = 1 + i * per_page
        params = {
            "key": GOOGLE_CSE_KEY, "cx": GOOGLE_CSE_CX, "q": query,
            "searchType": "image", "num": per_page, "start": start,
            "safe": "active", "imgType": "photo", "imgDominantColor": "white"
        }
        try:
            r = requests.get(base, params=params, timeout=20)
            if r.status_code >= 400:
                try: print(f"[Google CSE ERROR {r.status_code}] {r.json()}")
                except: print(f"[Google CSE ERROR {r.status_code}] {r.text}")
                break
            items = r.json().get("items", []) or []
            out += [x.get("link") for x in items if x.get("link")]
        except Exception as e:
            print(f"[Google CSE EXCEPTION] {e}")
            break
    return out

def _download_bytes(url: str):
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as r:
            r.raise_for_status()
            total = 0
            chunks = []
            for chunk in r.iter_content(8192):
                if chunk:
                    total += len(chunk)
                    if total > MAX_DOWNLOAD_BYTES:
                        return None
                    chunks.append(chunk)
            return b"".join(chunks)
    except Exception:
        return None

def _ahash(img, hash_size=8):
    from PIL import Image
    im = img.convert("L"). resize((hash_size, hash_size), Image.BILINEAR)
    pixels = list(im.getdata())
    avg = sum(pixels) / len(pixels)
    bits = "".join("1" if p > avg else "0" for p in pixels)
    return bits

def _is_white_bg(img):
    from PIL import Image
    im = img.convert("RGB")
    w, h = im.size
    if w < 120 or h < 120: return False
    bw = int(w * WHITE_BG_BORDER_PCT); bh = int(h * WHITE_BG_BORDER_PCT)
    px = im.load(); white = 0; total = 0; thr = WHITE_BG_THRESHOLD
    for y in list(range(0, bh)) + list(range(h - bh, h)):
        for x in range(w):
            r, g, b = px[x, y]
            if r >= thr and g >= thr and b >= thr: white += 1
            total += 1
    for y in range(bh, h - bh):
        for x in list(range(0, bw)) + list(range(w - bw, w)):
            r, g, b = px[x, y]
            if r >= thr and g >= thr and b >= thr: white += 1
            total += 1
    return (white / max(1, total)) >= WHITE_BG_MIN_RATIO

def collect_candidate_images(queries, vendor=""):
    urls, seen = [], set()
    for q in queries:
        g = google_cse_image_search(q, per_page=10, pages=3) if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else []
        b = bing_image_search(q, count=50, pages=2) if BING_IMAGE_KEY else []
        for u in (g + b):
            if not u or u in seen: continue
            seen.add(u); urls.append(u)
    urls.sort(key=lambda u: score_image_url(u, vendor))
    return urls

def filter_and_select_images(candidates, vendor="", want_n=5):
    selected, seen_hashes = [], set()
    for url in candidates:
        if len(selected) >= want_n: break
        data = _download_bytes(url)
        if not data: continue
        try:
            from PIL import Image
            from io import BytesIO
            img = Image.open(BytesIO(data))
            if not _is_white_bg(img): continue
            h = _ahash(img)
            if h in seen_hashes: continue
            seen_hashes.add(h)
            selected.append(url)
        except Exception:
            continue
    return selected

# ========= DESCRIPTION =========
def gen_description_html(title: str, vendor: str, ptype: str, ean: str) -> str:
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
            if txt: return txt
        except Exception as e:
            print(f"[WARN] OpenAI non disponibile: {e}")
    # fallback
    title_h = html.escape(title or "Prodotto moda")
    vendor_h = html.escape(vendor or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    p = (f"<p>{title_h}{(' di ' + vendor_h) if vendor_h else ''}: un capo {ptype_h.lower()} essenziale, "
         f"pensato per l'uso quotidiano con attenzione a comfort e durata. "
         f"Linee pulite e dettagli curati lo rendono facile da abbinare nelle diverse occasioni, "
         f"dal lavoro al tempo libero.</p>")
    ul = "<ul>" + "".join([
        "<li>Materiali selezionati per comfort e resistenza</li>",
        "<li>Vestibilità equilibrata e facile da indossare</li>",
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
        if ALLOWED_IDS:
            print(f"[DEBUG] PRODUCT_IDS ricevuti: {', '.join(ALLOWED_IDS)}")

    processed = 0
    scanned = 0
    skipped = 0
    results = []

    # --- Sorgente prodotti
    edges = []
    if ALLOWED_IDS:
        # 1) prova GraphQL nodes
        edges = fetch_products_by_ids_graphql(ALLOWED_IDS)
        if not edges:
            print("[INFO] GraphQL nodes non ha trovato prodotti; provo REST /products.json?ids=...")
            edges = fetch_products_by_ids_rest(ALLOWED_IDS)
        if not edges:
            print("[INFO] Nessun product trovato via /products.json; provo a risolvere come VARIANT IDs...")
            product_ids = resolve_product_ids_from_variant_ids(ALLOWED_IDS)
            if product_ids:
                print(f"[INFO] Variant IDs risolti in product IDs: {', '.join(product_ids)}")
                edges = fetch_products_by_ids_rest(product_ids)
            else:
                print("[WARN] Nessun ID risolto. Controlla che PRODUCT_IDS contenga ID prodotto o variante validi.")
    else:
        data = fetch_draft_products(limit=50, cursor=None)
        edges = safe_get(data, "products", "edges", default=[]) or []

    for e in edges:
        if (processed + skipped) >= MAX_PRODUCTS:
            break

        n = e.get("node") or {}
        scanned += 1
        notes = []
        desc_updated = False
        uploaded = 0

        try:
            pid_num = product_id_from_gid(n["id"])
            title  = safe_strip(n.get("title"))
            vendor = safe_strip(n.get("vendor"))
            ptype  = safe_strip(n.get("productType"))
            ean    = first_barcode(n.get("variants"))

            status = safe_strip(n.get("status"))
            if status and status.lower() != "draft":
                skipped += 1
                print(product_header(title, vendor, ean) + " - SKIP: status non DRAFT")
                results.append({
                    "product_id": pid_num, "title": title, "vendor": vendor, "ean": ean,
                    "images_uploaded": 0, "description_updated": False,
                    "notes": "skip: status non DRAFT"
                })
                continue

            img_edges = safe_get(n, "images", "edges", default=[]) or []
            has_img   = len(img_edges) > 0
            body_html = safe_strip(n.get("bodyHtml"))
            has_desc  = bool(body_html)

            if has_img or has_desc:
                why = []
                if has_img:  why.append("ha già immagini")
                if has_desc: why.append("ha già descrizione")
                print(product_header(title, vendor, ean) + " - SKIP: " + ", ".join(why))
                skipped += 1
                results.append({
                    "product_id": pid_num, "title": title, "vendor": vendor, "ean": ean,
                    "images_uploaded": 0, "description_updated": False,
                    "notes": "skip: " + ", ".join(why)
                })
                continue

            if not title:
                skipped += 1
                print("[PROCESS] SKIP: titolo mancante")
                results.append({
                    "product_id": pid_num, "title": "", "vendor": vendor, "ean": ean,
                    "images_uploaded": 0, "description_updated": False,
                    "notes": "skip: titolo mancante"
                })
                continue

            print(product_header(title, vendor, ean))

            # DESCRIZIONE
            if USE_SHOPIFY_MAGIC_ONLY:
                try:
                    set_product_metafield_gql(n["id"], "ai_flags", "needs_description", "true")
                    notes.append("flag Magic impostato")
                    print("  - Flag impostato: ai_flags.needs_description=true (usa Shopify Magic)")
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

            # IMMAGINI (fino a 5, white bg)
            img_urls = []
            if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                base = " ".join([x for x in [vendor, title, "product"] if x]).strip()
                queries = []
                if ean:
                    queries += [
                        f"{base} {ean}",
                        f"{vendor} {title} {ean} packshot",
                        f"{vendor} {title} {ean} white background",
                    ]
                queries += [
                    base,
                    f"{vendor} {title}".strip(),
                    f"{vendor} {title} packshot".strip(),
                    f"{vendor} {title} white background".strip(),
                    f"{vendor} {title} studio".strip(),
                    f"{vendor} {title} site:{(vendor or '').lower()}.com".strip(),
                ]
                candidates = collect_candidate_images(queries, vendor=vendor)
                img_urls = filter_and_select_images(candidates, vendor=vendor, want_n=MAX_IMAGES_PER_PRODUCT)

            if img_urls:
                for u in img_urls:
                    try:
                        img_id = add_image(pid_num, u, alt_text=f"{vendor} {title}".strip())
                        uploaded += 1
                        print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                    except Exception as ex:
                        notes.append(f"img errore: {ex}")
                        print(f"  - ERRORE immagine: {ex}")
            else:
                print("  - Nessuna immagine trovata per la query.")

            if uploaded > 0:
                print(f"  - Immagini caricate: {uploaded} ✅")
            else:
                print("  - Nessuna immagine trovata per la query.")
            if desc_updated or uploaded > 0:
                print(f"  Admin: {ADMIN_URL_TEMPLATE.format(pid=pid_num)}")

            if desc_updated or uploaded > 0:
                processed += 1
            else:
                skipped += 1

            results.append({
                "product_id": pid_num, "title": title, "vendor": vendor, "ean": ean,
                "images_uploaded": uploaded, "description_updated": desc_updated,
                "notes": "; ".join(notes)
            })

        except Exception as ex:
            skipped += 1
            notes.append(f"errore prodotto: {ex}")
            print(f"[ERROR prodotto] {ex}")
            traceback.print_exc()
            results.append({
                "product_id": product_id_from_gid(n.get("id","gid://shopify/Product/0")) if n else "",
                "title": safe_strip(n.get("title")) if n else "",
                "vendor": safe_strip(n.get("vendor")) if n else "",
                "ean": first_barcode(n.get("variants")) if n else "",
                "images_uploaded": 0, "description_updated": False,
                "notes": "; ".join(notes)
            })

    # REPORT CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"report_autofill_{ts}.csv"
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["product_id","title","vendor","ean","images_uploaded","description_updated","notes"])
            w.writeheader()
            for row in results:
                w.writerow(row)
        print(f"[REPORT] Salvato: {csv_path}")
        print("[REPORT HEAD]")
        with open(csv_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                print(line.rstrip())
                if i >= 10: break
    except Exception as e:
        print(f"[REPORT ERROR] {e}")

    print(f"[SUMMARY] Scanned: {scanned} | Updated: {processed} | Skipped: {skipped}")

# ========= ENTRYPOINT =========
if __name__ == "__main__":
    try:
        print(f"[INFO] Using store: {SHOPIFY_STORE_DOMAIN}")
        fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[INFO] Fonte immagini: {fonte} | Max img/prodotto: {MAX_IMAGES_PER_PRODUCT}")
        if ALLOWED_IDS:
            print(f"[INFO] Processando questi IDs (product o variant): {', '.join(ALLOWED_IDS)}")
        main()
        sys.exit(0)
    except Exception as e:
        print("=== UNCAUGHT ERROR ===")
        print(repr(e))
        traceback.print_exc()
        sys.exit(0)
