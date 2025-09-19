import os, sys, html, json, csv, re, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

VERSION = "2025-09-19-v7i"
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

# Immagini
MAX_IMAGES_PER_PRODUCT = min(int(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")), 5)
MAX_PRODUCTS           = int(os.getenv("MAX_PRODUCTS", "25"))

SAFE_DOMAINS_HINTS   = ["cdn", "images", "media", "static", "assets", "content", "img", "cloudfront", "akamaized"]
WHITE_BG_KEYWORDS    = ["white", "bianco", "packshot", "studio", "product", "plain"]
BRAND_DOMAINS_WHITELIST = [d.strip().lower() for d in os.getenv("BRAND_DOMAINS_WHITELIST", "").split(",") if d.strip()]

WHITE_BG_BORDER_PCT  = float(os.getenv("WHITE_BG_BORDER_PCT", "0.12"))
WHITE_BG_THRESHOLD   = int(os.getenv("WHITE_BG_THRESHOLD", "242"))
WHITE_BG_MIN_RATIO   = float(os.getenv("WHITE_BG_MIN_RATIO", "0.82"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC", "8"))
MAX_DOWNLOAD_BYTES   = int(os.getenv("MAX_DOWNLOAD_BYTES", "3500000"))
STRICT_EAN_IMAGE_MATCH = os.getenv("STRICT_EAN_IMAGE_MATCH", "true").lower() == "true"
CONTEXT_FETCH_MAX    = int(os.getenv("CONTEXT_FETCH_MAX", "200000"))  # 200 KB

DEBUG = os.getenv("DEBUG", "false").lower() == "true"
ADMIN_URL_TEMPLATE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/products/{{pid}}"

# Filtri: ID, SKU, EAN
ALLOWED_IDS  = [x.strip() for x in os.getenv("PRODUCT_IDS", "").split(",") if x.strip()]
ALLOWED_SKUS = [x.strip() for x in os.getenv("PRODUCT_SKUS", "").split(",") if x.strip()]
ALLOWED_EANS = [x.strip() for x in os.getenv("PRODUCT_EANS", "").split(",") if x.strip()]

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
    try: return str(v or "").strip()
    except: return ""

def product_id_from_gid(gid: str) -> int:
    return int(str(gid).split("/")[-1])

def gid_from_product_id(pid_num: int) -> str:
    return f"gid://shopify/Product/{pid_num}"

def domain(u):
    try: return urlparse(u).netloc.lower()
    except: return ""

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
        if bc: return bc
    return ""

def variant_selected_options(variants):
    # ritorna dizionario con possibili 'Color', 'Colore', 'Size', 'Taglia', etc.
    edges = safe_get(variants, "edges", default=[]) or []
    out = {}
    for edge in edges:
        node = edge.get("node") or {}
        so = node.get("selectedOptions") or []
        for opt in so:
            name = safe_strip(opt.get("name")).lower()
            val  = safe_strip(opt.get("value"))
            if not name or not val: continue
            if name in ["color", "colour", "colore"]:
                out["color"] = val
            elif name in ["size", "taglia", "misura"]:
                out["size"] = val
    return out

# ========= SHOPIFY HELPERS =========
def shopify_graphql(query: str, variables=None):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN, "Content-Type": "application/json"}
    r = requests.post(url, json={"query": query, "variables": variables or {}}, headers=h, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "errors" in j: raise RuntimeError(j["errors"])
    return j["data"]

def shopify_rest_get(path, params=None):
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}{path}"
    h = {"X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN}
    r = requests.get(url, headers=h, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_products_by_variants_query(terms, kind="sku"):
    if not terms: return []
    out = {}
    q = """
    query($first:Int!, $query:String!){
      productVariants(first:$first, query:$query){
        edges{
          node{
            id sku barcode
            selectedOptions{ name value }
            product{
              id title vendor productType handle status bodyHtml tags
              images(first:1){ edges{ node{ id } } }
              variants(first:20){
                edges{ node{ id sku barcode title selectedOptions{ name value } } }
              }
            }
          }
        }
      }
    }"""
    for term in terms:
        query = f"{kind}:{term}"
        data = shopify_graphql(q, {"first": 20, "query": query})
        edges = safe_get(data, "productVariants", "edges", default=[]) or []
        for e in edges:
            prod = safe_get(e, "node", "product", default=None)
            if prod:
                out[prod["id"]] = {"node": prod}
    return list(out.values())

def fetch_products_by_product_ids(id_list):
    if not id_list: return []
    q = """
    query($ids:[ID!]!){
      nodes(ids:$ids){
        ... on Product {
          id title vendor productType handle status bodyHtml tags
          images(first:1){ edges{ node{ id } } }
          variants(first:20){
            edges{ node{ id sku barcode title selectedOptions{ name value } } }
          }
        }
      }
    }"""
    gids = [gid_from_product_id(int(x)) for x in id_list]
    nodes = shopify_graphql(q, {"ids": gids}).get("nodes") or []
    return [{"node": n} for n in nodes if n]

# ========= IMAGE SEARCH (con pagina di contesto) =========
def bing_image_search(query: str, count=50, pages=2):
    if not BING_IMAGE_KEY: return []
    url = "https://api.bing.microsoft.com/v7.0/images/search"
    h = {"Ocp-Apim-Subscription-Key": BING_IMAGE_KEY}
    out = []
    for p in range(pages):
        params = {
            "q": query, "safeSearch": "Moderate",
            "count": count, "offset": p * count,
            "imageType": "Photo", "imageContent": "Product",
            "license": "Any"
        }
        try:
            r = requests.get(url, headers=h, params=params, timeout=20)
            r.raise_for_status()
            for item in r.json().get("value", []):
                content = item.get("contentUrl")
                context = item.get("hostPageUrl")
                if content:
                    out.append({"content": content, "context": context})
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
            for item in r.json().get("items", []) or []:
                content = item.get("link")
                context = item.get("image", {}).get("contextLink")
                if content:
                    out.append({"content": content, "context": context})
        except Exception as e:
            print(f"[Google CSE EXCEPTION] {e}")
            break
    return out

def _http_get_text(url: str, limit_bytes=200000):
    try:
        with requests.get(url, timeout=10, stream=True) as r:
            r.raise_for_status()
            chunks = []
            total = 0
            for ch in r.iter_content(8192, decode_unicode=True):
                if not ch: continue
                if isinstance(ch, bytes):
                    ch = ch.decode(errors="ignore")
                total += len(ch)
                if total > limit_bytes:
                    chunks.append(ch[: max(0, limit_bytes - (total - len(ch)))])
                    break
                chunks.append(ch)
            return "".join(chunks)
    except Exception:
        return ""

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
    im = img.convert("L").resize((hash_size, hash_size), Image.BILINEAR)
    px = list(im.getdata()); avg = sum(px)/len(px)
    return "".join("1" if p > avg else "0" for p in px)

def _is_white_bg(img):
    from PIL import Image
    im = img.convert("RGB"); w, h = im.size
    if w < 120 or h < 120: return False
    bw = int(w * WHITE_BG_BORDER_PCT); bh = int(h * WHITE_BG_BORDER_PCT)
    px = im.load(); white = 0; total = 0; thr = WHITE_BG_THRESHOLD
    # top/bottom
    for y in list(range(0, bh)) + list(range(h - bh, h)):
        for x in range(w):
            r, g, b = px[x, y]
            if r >= thr and g >= thr and b >= thr: white += 1
            total += 1
    # left/right
    for y in range(bh, h - bh):
        for x in list(range(0, bw)) + list(range(w - bw, w)):
            r, g, b = px[x, y]
            if r >= thr and g >= thr and b >= thr: white += 1
            total += 1
    return (white / max(1, total)) >= WHITE_BG_MIN_RATIO

def _context_has_ean(text: str, ean: str):
    e = re.escape(ean)
    return re.search(rf"(^|[^0-9]){e}([^0-9]|$)", text) is not None

def collect_candidate_images(queries, vendor="", ean=""):
    items = []
    seen = set()
    for q in queries:
        g = google_cse_image_search(q, per_page=10, pages=3) if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else []
        b = bing_image_search(q, count=50, pages=2) if BING_IMAGE_KEY else []
        for it in (g + b):
            content = it.get("content"); context = it.get("context")
            if not content or content in seen: continue
            seen.add(content)
            items.append({"content": content, "context": context})
    # ordina per qualità dominio/keyword
    items.sort(key=lambda it: score_image_url(it["content"], vendor))
    # se STRICT, filtra per EAN
    if ean and STRICT_EAN_IMAGE_MATCH:
        filtered = []
        for it in items:
            c_url = it["content"].lower()
            # se l'EAN è nel nome file/URL, basta
            if ean in c_url:
                filtered.append(it); continue
            # se abbiamo pagina contesto, scarica e verifica presenza EAN
            ctx = it.get("context")
            if ctx:
                txt = _http_get_text(ctx, limit_bytes=CONTEXT_FETCH_MAX)
                if txt and _context_has_ean(txt.lower(), ean.lower()):
                    filtered.append(it)
        items = filtered
    return items

def filter_and_select_images(candidates, vendor="", want_n=5):
    selected = []
    seen_hashes = set()
    for it in candidates:
        if len(selected) >= want_n: break
        url = it["content"]
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

# ========= DESCRIPTION (personalizzata) =========
MATERIALS = ["cotone","pelle","nylon","denim","lana","cachemire","viscosa","poliestere","seta","lin(o)?","gomma","poliuretano","elastan","acrilico","microfibra"]
FIT_WORDS = ["slim","regular","relaxed","oversize","skinny","tapered","straight"]
GENDER_WORDS = {"uomo":["uomo","men","maschile"], "donna":["donna","women","femminile"], "unisex":["unisex"]}

def guess_from_text(text, words):
    t = text.lower()
    for w in words:
        if re.search(rf"\b{w}\b", t):
            return w
    return ""

def extract_materials(text):
    t = text.lower()
    found = []
    for m in MATERIALS:
        if re.search(rf"\b{m}\b", t):
            found.append(re.sub(r"\(o\)\?$","o",m))
    return list(dict.fromkeys(found))[:3]

def extract_gender(text):
    t = text.lower()
    for k, arr in GENDER_WORDS.items():
        for w in arr:
            if re.search(rf"\b{w}\b", t):
                return k
    return ""

def build_unique_description(title, vendor, ptype, tags, selopts, ean):
    # base
    title_h = html.escape(title or "Prodotto moda")
    vendor_h = html.escape(vendor or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    color = selopts.get("color","")
    size  = selopts.get("size","")
    color_h = html.escape(color) if color else ""
    size_h  = html.escape(size) if size else ""
    ean_h   = html.escape(ean or "N/D")

    # features da titolo/tags
    textpool = " ".join([title or "", ptype or "", " ".join(tags or [])])
    mats = extract_materials(textpool)
    fit = guess_from_text(textpool, FIT_WORDS)
    gender = extract_gender(textpool)

    # paragrafo
    par = f"<p>{title_h}"
    if vendor_h: par += f" di {vendor_h}"
    par += f": {ptype_h.lower()} "
    if color_h: par += f"colore {color_h} "
    par += "pensato per un uso quotidiano, con attenzione a comfort e durata"
    if fit: par += f", in vestibilità {fit}"
    par += "."
    if mats: par += f" Realizzato con materiali come {', '.join(mats)}."
    if gender: par += f" Linea {gender}."
    par += f" Codice articolo: {ean_h}."
    par += "</p>"

    # bullet punti specifici
    bullets = []
    if mats: bullets.append(f"Materiali: {', '.join(mats)}")
    if fit:  bullets.append(f"Vestibilità: {fit}")
    if color_h: bullets.append(f"Colore: {color_h}")
    if size_h: bullets.append(f"Taglia/varianti disponibili")
    bullets.append("Finiture pulite e dettagli curati")
    bullets.append("Cura: seguire le indicazioni in etichetta")

    ul = "<ul>" + "".join(f"<li>{html.escape(b)}</li>" for b in bullets[:5]) + "</ul>"
    return par + ul

def gen_description_html(title: str, vendor: str, ptype: str, ean: str, tags=None, selopts=None) -> str:
    tags = tags or []
    selopts = selopts or {}
    # se disponibile OpenAI, chiedo una descrizione vincolata ai dati
    if OPENAI_API_KEY:
        prompt = {
            "istruzioni": "Scrivi una descrizione unica in italiano per un prodotto moda.",
            "vincoli": {
                "paragrafo": "120-150 parole, tono chiaro e professionale. Cita brand, categoria, colore (se presente), materiali (se noti), vestibilità (se nota) e il codice EAN come codice articolo.",
                "bullet": "3-5 punti pratici (materiali, vestibilità, colore/taglia, cura, uso). Output solo HTML con <p> e <ul><li>."
            },
            "dati": {
                "titolo": title, "brand": vendor, "categoria": ptype,
                "ean": ean, "tags": tags, "selectedOptions": selopts
            }
        }
        try:
            r = requests.post("https://api.openai.com/v1/responses",
                              headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                              data=json.dumps({"model":"gpt-4o-mini","input": json.dumps(prompt, ensure_ascii=False)}),
                              timeout=45)
            r.raise_for_status()
            txt = r.json().get("output_text","").strip()
            if txt: return txt
        except Exception as e:
            print(f"[WARN] OpenAI non disponibile: {e}")
    # fallback artigianale ma personalizzato
    return build_unique_description(title, vendor, ptype, tags, selopts, ean)

# ========= MAIN =========
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    if DEBUG:
        print(f"[DEBUG] Store={SHOPIFY_STORE_DOMAIN} | APIv={SHOPIFY_API_VERSION} | MagicOnly={USE_SHOPIFY_MAGIC_ONLY} | MaxProducts={MAX_PRODUCTS} | MaxImages={MAX_IMAGES_PER_PRODUCT}")
        fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[DEBUG] Fonte immagini: {fonte}")
        if ALLOWED_SKUS: print(f"[DEBUG] PRODUCT_SKUS: {', '.join(ALLOWED_SKUS)}")
        if ALLOWED_EANS: print(f"[DEBUG] PRODUCT_EANS: {', '.join(ALLOWED_EANS)}")
        if ALLOWED_IDS:  print(f"[DEBUG] PRODUCT_IDS: {', '.join(ALLOWED_IDS)}")
        print(f"[DEBUG] STRICT_EAN_IMAGE_MATCH={STRICT_EAN_IMAGE_MATCH}")

    processed = scanned = skipped = 0
    results = []

    # ---- Selezione prodotti ----
    edges = []
    if ALLOWED_SKUS:
        edges += fetch_products_by_variants_query(ALLOWED_SKUS, kind="sku")
    if ALLOWED_EANS:
        edges += fetch_products_by_variants_query(ALLOWED_EANS, kind="barcode")
    if ALLOWED_IDS:
        # interpreta (se 8-14 cifre) come EAN addizionale
        as_ean = [x for x in ALLOWED_IDS if x.isdigit() and 8 <= len(x) <= 14]
        pure_ids = [x for x in ALLOWED_IDS if not (x.isdigit() and 8 <= len(x) <= 14)]
        if as_ean:
            edges += fetch_products_by_variants_query(as_ean, kind="barcode")
        if pure_ids:
            edges += fetch_products_by_product_ids(pure_ids)

    # dedup per product.id
    uniq = {}
    for e in edges:
        n = e.get("node"); 
        if n: uniq[n["id"]] = e
    edges = list(uniq.values())

    if not edges:
        print("[INFO] Nessun prodotto trovato dai filtri. Usa PRODUCT_SKUS o PRODUCT_EANS nel .env.")
        report_and_exit(results, scanned, processed, skipped)
        return

    for e in edges:
        if (processed + skipped) >= MAX_PRODUCTS: break

        n = e.get("node") or {}
        scanned += 1
        notes = []; desc_updated = False; uploaded = 0

        try:
            pid_num = product_id_from_gid(n["id"])
            title  = safe_strip(n.get("title"))
            vendor = safe_strip(n.get("vendor"))
            ptype  = safe_strip(n.get("productType"))
            tags   = n.get("tags") or []
            ean    = first_barcode(n.get("variants"))
            selopts = variant_selected_options(n.get("variants"))

            status = safe_strip(n.get("status"))
            if status and status.lower() != "draft":
                skipped += 1
                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'} - SKIP: status non DRAFT")
                results.append(row(pid_num, title, vendor, ean, 0, False, "skip: status non DRAFT"))
                continue

            has_img = len(safe_get(n, "images", "edges", default=[]) or []) > 0
            has_desc = bool(safe_strip(n.get("bodyHtml")))
            if has_img or has_desc:
                why = []
                if has_img:  why.append("ha già immagini")
                if has_desc: why.append("ha già descrizione")
                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'} - SKIP: " + ", ".join(why))
                skipped += 1
                results.append(row(pid_num, title, vendor, ean, 0, False, "skip: " + ", ".join(why)))
                continue

            if not title:
                skipped += 1
                print("[PROCESS] SKIP: titolo mancante")
                results.append(row(pid_num, "", vendor, ean, 0, False, "skip: titolo mancante"))
                continue

            print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'}")

            # --- DESCRIZIONE personalizzata ---
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
                    desc_html = gen_description_html(title, vendor, ptype, ean, tags=tags, selopts=selopts)
                    update_description(pid_num, desc_html)
                    desc_updated = True
                    print("  - Descrizione aggiornata ✅")
                except Exception as ex:
                    notes.append(f"descrizione errore: {ex}")
                    print(f"  - ERRORE descrizione: {ex}")

            # --- IMMAGINI (strict EAN match + white bg) ---
            img_urls = []
            if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                base  = " ".join([x for x in [vendor, title, "product"] if x]).strip()
                qbase = [base, f"{vendor} {title} packshot", f"{vendor} {title} white background"]
                queries = []
                if ean:
                    queries += [f"\"{ean}\"", f"{vendor} {title} {ean}", f"{vendor} {ean}", f"{title} {ean}"]
                    queries += [f"site:{d} {ean}" for d in BRAND_DOMAINS_WHITELIST]
                queries += qbase

                cands = collect_candidate_images(queries, vendor=vendor, ean=ean)
                img_urls = filter_and_select_images(cands, vendor=vendor, want_n=MAX_IMAGES_PER_PRODUCT)

            if img_urls:
                for u in img_urls:
                    try:
                        img_id = add_image(pid_num, u, alt_text=f"{vendor} {title}".strip())
                        uploaded += 1
                        print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                    except Exception as ex:
                        notes.append(f"img errore: {ex}")
                        print(f"  - ERRORE immagine: {ex}")
                print(f"  - Immagini caricate: {uploaded} ✅")
            else:
                print("  - Nessuna immagine trovata coerente con EAN.")

            if desc_updated or uploaded > 0:
                print(f"  Admin: {ADMIN_URL_TEMPLATE.format(pid=pid_num)}")

            if desc_updated or uploaded > 0: processed += 1
            else:                             skipped += 1

            results.append(row(pid_num, title, vendor, ean, uploaded, desc_updated, "; ".join(notes)))

        except Exception as ex:
            skipped += 1
            print(f"[ERROR prodotto] {ex}")
            traceback.print_exc()
            results.append(row(pid_num if 'pid_num' in locals() else "", title if 'title' in locals() else "",
                               vendor if 'vendor' in locals() else "", ean if 'ean' in locals() else "",
                               0, False, f"errore prodotto: {ex}"))

    report_and_exit(results, scanned, processed, skipped)

def row(pid_num, title, vendor, ean, uploaded, desc_updated, notes):
    return {
        "product_id": pid_num, "title": title, "vendor": vendor, "ean": ean,
        "images_uploaded": uploaded, "description_updated": bool(desc_updated), "notes": notes
    }

def report_and_exit(results, scanned, processed, skipped):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"report_autofill_{ts}.csv"
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["product_id","title","vendor","ean","images_uploaded","description_updated","notes"])
            w.writeheader()
            for r in results: w.writerow(r)
        print(f"[REPORT] Salvato: {csv_path}")
        print("[REPORT HEAD]")
        with open(csv_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                print(line.rstrip())
                if i >= 10: break
    except Exception as e:
        print(f"[REPORT ERROR] {e}")
    print(f"[SUMMARY] Scanned: {scanned} | Updated: {processed} | Skipped: {skipped}")

# ========= METAFIELDS (se usi Shopify Magic) =========
def set_product_metafield_gql(product_gid: str, namespace: str, key: str, value: str, mtype: str="single_line_text_field"):
    mutation = """
    mutation SetMF($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id key namespace }
        userErrors { field message code }
      }
    }"""
    vars_ = {"metafields": [{"ownerId": product_gid, "namespace": namespace, "key": key, "type": mtype, "value": value}]}
    data = shopify_graphql(mutation, vars_)
    errs = (data.get("metafieldsSet") or {}).get("userErrors") or []
    if errs: raise RuntimeError(f"metafieldsSet errors: {errs}")
    return True

# ========= ENTRYPOINT =========
if __name__ == "__main__":
    try:
        print(f"[INFO] Using store: {SHOPIFY_STORE_DOMAIN}")
        fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[INFO] Fonte immagini: {fonte} | Max img/prodotto: {MAX_IMAGES_PER_PRODUCT}")
        if ALLOWED_SKUS: print(f"[INFO] Filtrando per SKU: {', '.join(ALLOWED_SKUS)}")
        if ALLOWED_EANS: print(f"[INFO] Filtrando per EAN: {', '.join(ALLOWED_EANS)}")
        if ALLOWED_IDS:  print(f"[INFO] Filtrando per IDs: {', '.join(ALLOWED_IDS)}")
        print(f"[INFO] STRICT_EAN_IMAGE_MATCH={STRICT_EAN_IMAGE_MATCH}")
        main(); sys.exit(0)
    except Exception as e:
        print("=== UNCAUGHT ERROR ==="); print(repr(e)); traceback.print_exc(); sys.exit(0)
