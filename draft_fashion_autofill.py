import os, sys, html, json, csv, re, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup

VERSION = "2025-09-19-v7n-skur"
load_dotenv()

# ========= CONFIG =========
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
SHOPIFY_ADMIN_TOKEN  = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

BING_IMAGE_KEY       = os.getenv("BING_IMAGE_KEY", "")
GOOGLE_CSE_KEY       = os.getenv("GOOGLE_CSE_KEY", "")
GOOGLE_CSE_CX        = os.getenv("GOOGLE_CSE_CX", "")

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
USE_SHOPIFY_MAGIC_ONLY = os.getenv("USE_SHOPIFY_MAGIC_ONLY", "false").lower() == "true"

MAX_IMAGES_PER_PRODUCT = min(int(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")), 5)
MAX_PRODUCTS           = int(os.getenv("MAX_PRODUCTS", "25"))

SAFE_DOMAINS_HINTS   = ["cdn","images","media","static","assets","content","img","cloudfront","akamaized"]
WHITE_BG_KEYWORDS    = ["white","bianco","packshot","studio","product","plain","ghost","sfondo-bianco"]
BRAND_DOMAINS_WHITELIST = [d.strip().lower() for d in os.getenv("BRAND_DOMAINS_WHITELIST","").split(",") if d.strip()]
DOMAINS_BLACKLIST    = ["ebay.","aliexpress.","pinterest.","facebook.","tumblr.","wordpress.","blogspot.","vk.","tiktok.","twitter.","x.com","instagram."]

WHITE_BG_BORDER_PCT  = float(os.getenv("WHITE_BG_BORDER_PCT","0.12"))
WHITE_BG_THRESHOLD   = int(os.getenv("WHITE_BG_THRESHOLD","242"))
WHITE_BG_MIN_RATIO   = float(os.getenv("WHITE_BG_MIN_RATIO","0.82"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC","8"))
MAX_DOWNLOAD_BYTES   = int(os.getenv("MAX_DOWNLOAD_BYTES","3500000"))

STRICT_CODE_IMAGE_MATCH = os.getenv("STRICT_CODE_IMAGE_MATCH","true").lower()=="true"
STRICT_CODE_DESC_ONLY   = os.getenv("STRICT_CODE_DESC_ONLY","true").lower()=="true"
CONTEXT_FETCH_MAX       = int(os.getenv("CONTEXT_FETCH_MAX","250000"))  # bytes

SUPPLIER_CODE_OFFSET    = int(os.getenv("SUPPLIER_CODE_OFFSET", "6"))
SUPPLIER_CODE_REGEX     = os.getenv("SUPPLIER_CODE_REGEX", "")  # opzionale

DEBUG = os.getenv("DEBUG","false").lower()=="true"
ADMIN_URL_TEMPLATE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/products/{{pid}}"

ALLOWED_IDS  = [x.strip() for x in os.getenv("PRODUCT_IDS","").split(",") if x.strip()]
ALLOWED_SKUS = [x.strip() for x in os.getenv("PRODUCT_SKUS","").split(",") if x.strip()]
ALLOWED_EANS = [x.strip() for x in os.getenv("PRODUCT_EANS","").split(",") if x.strip()]

# ========= UTILS =========
def safe_get(d,*path,default=None):
    cur=d or {}
    for k in path:
        if isinstance(cur,dict): cur=cur.get(k)
        else: return default
    return cur if cur is not None else default

def safe_strip(v):
    try: return str(v or "").strip()
    except: return ""

def product_id_from_gid(gid:str)->int:
    return int(str(gid).split("/")[-1])

def gid_from_product_id(pid:int)->str:
    return f"gid://shopify/Product/{pid}"

def domain(u):
    try: 
        return urlparse(u).netloc.lower()
    except: 
        return ""

def score_image_url(u,vendor=""):
    d=domain(u or ""); s=0
    if any(wh in d for wh in BRAND_DOMAINS_WHITELIST if wh): s-=6
    if vendor and vendor.lower().replace(" ","") in d.replace("-","").replace(" ",""): s-=3
    if any(h in d for h in SAFE_DOMAINS_HINTS): s-=2
    if any(k in (u or "").lower() for k in WHITE_BG_KEYWORDS): s-=1
    if any(b in d for b in DOMAINS_BLACKLIST): s+=6
    return s

def supplier_code_from_sku(sku: str) -> str:
    sku = safe_strip(sku)
    if not sku: return ""
    if SUPPLIER_CODE_REGEX:
        try:
            m = re.search(SUPPLIER_CODE_REGEX, sku)
            if m and m.groupdict().get("code"):
                return m.group("code").strip()
        except Exception:
            pass
    if len(sku) > SUPPLIER_CODE_OFFSET:
        return sku[SUPPLIER_CODE_OFFSET:]
    return sku

def sku_root(s: str) -> str:
    """Restituisce la radice prima di _ o - (es. ABC123_S -> ABC123)."""
    s = safe_strip(s)
    if not s:
        return ""
    for sep in ["_", "-"]:
        if sep in s:
            return s.split(sep, 1)[0]
    return s

def expand_sku_terms_for_selection(input_skus):
    """
    Da una lista di SKU utente produce termini per la selezione:
    - SKU originale
    - supplier code (tagliato)
    - radice(SKU)
    - radice(supplier code)
    Deduplicati e in ordine.
    """
    seen = set()
    out = []
    for s in input_skus:
        s = safe_strip(s)
        if not s:
            continue
        cand = [s]
        sc = supplier_code_from_sku(s)
        if sc and sc.lower() != s.lower():
            cand.append(sc)
        # radici senza taglia
        for x in list(cand):
            r = sku_root(x)
            if r and r.lower() not in [y.lower() for y in cand]:
                cand.append(r)
        for c in cand:
            cl = c.lower()
            if cl in seen: 
                continue
            seen.add(cl)
            out.append(c)
    return out

def first_barcode(variants):
    edges = safe_get(variants,"edges",default=[]) or []
    for e in edges:
        n=e.get("node") or {}
        bc=safe_strip(n.get("barcode"))
        if bc: return bc
    return ""

def collect_all_skus(variants):
    edges = safe_get(variants,"edges",default=[]) or []
    out=[]
    for e in edges:
        n=e.get("node") or {}
        sku=safe_strip(n.get("sku"))
        if sku: out.append(sku)
    return out

# ========= Shopify API =========
def shopify_graphql(q, vars_=None):
    url=f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    h={"X-Shopify-Access-Token":SHOPIFY_ADMIN_TOKEN,"Content-Type":"application/json"}
    r=requests.post(url, json={"query":q,"variables":vars_ or {}}, headers=h, timeout=30)
    r.raise_for_status()
    j=r.json()
    if "errors" in j: raise RuntimeError(j["errors"])
    return j["data"]

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

# ========= Ricerca robusta per varianti (SKU) =========
def fetch_products_by_variants_query_terms(terms, kind="sku"):
    """
    Prova più sintassi di query:
    - product_status:draft AND sku:"TERM"
    - sku:"TERM"
    - sku:TERM
    Raccoglie i Product (dedup per product.id).
    """
    if not terms: return []
    out={}
    q_tpl = """
    query($first:Int!, $query:String!){
      productVariants(first:$first, query:$query){
        edges{
          node{
            id sku barcode
            product{
              id title vendor productType handle status bodyHtml tags
              images(first:1){ edges{ node{ id } } }
              variants(first:30){ edges{ node{ id sku barcode title selectedOptions{ name value } } } }
            }
          }
        }
      }
    }"""
    tried = []
    for t in terms:
        candidates = [
            f'product_status:draft AND {kind}:"{t}"',
            f'{kind}:"{t}"',
            f'{kind}:{t}',
        ]
        for query_s in candidates:
            key=(t, query_s)
            if key in tried: continue
            tried.append(key)
            try:
                data=shopify_graphql(q_tpl, {"first": 30, "query": query_s})
                edges = safe_get(data,"productVariants","edges",default=[]) or []
                if DEBUG:
                    print(f"[DEBUG] search '{query_s}' -> {len(edges)} variants")
                for e in edges:
                    prod=safe_get(e,"node","product")
                    if prod: out[prod["id"]]={"node":prod}
                if edges: break
            except Exception as ex:
                if DEBUG:
                    print(f"[DEBUG] query error '{query_s}': {ex}")
                continue
    return list(out.values())

def fetch_products_by_product_ids(id_list):
    if not id_list: return []
    q="""
    query($ids:[ID!]!){
      nodes(ids:$ids){
        ... on Product {
          id title vendor productType handle status bodyHtml tags
          images(first:1){ edges{ node{ id } } }
          variants(first:30){ edges{ node{ id sku barcode title selectedOptions{ name value } } } }
        }
      }
    }"""
    gids=[f"gid://shopify/Product/{int(x)}" for x in id_list]
    nodes=shopify_graphql(q,{"ids":gids}).get("nodes") or []
    return [{"node":n} for n in nodes if n]

def fallback_scan_draft_products_and_filter(terms, limit_pages=5):
    """
    Fallback: scansiona i prodotti Draft e filtra localmente i variant con sku che:
    - è uguale a un termine,
    - inizia con un termine,
    - ha radice (prima di _ o -) uguale a un termine.
    """
    if not terms:
        return []
    # normalizza termini (lower + radici)
    terms_all = set(t.lower() for t in terms)
    for t in list(terms_all):
        rt = sku_root(t).lower()
        terms_all.add(rt)

    out={}
    q = """
    query($first:Int!, $after:String){
      products(first:$first, after:$after, query:"status:draft"){
        pageInfo{ hasNextPage endCursor }
        edges{
          node{
            id title vendor productType handle status bodyHtml tags
            images(first:1){ edges{ node{ id } } }
            variants(first:50){
              edges{ node{ id sku barcode title selectedOptions{ name value } } }
            }
          }
        }
      }
    }"""
    after=None
    pages=0
    while pages < limit_pages:
        data = shopify_graphql(q, {"first": 50, "after": after})
        edges = safe_get(data,"products","edges",default=[]) or []
        for e in edges:
            n = e.get("node") or {}
            var_edges = safe_get(n,"variants","edges",default=[]) or []
            hit=False
            for ve in var_edges:
                sku_val = safe_strip(safe_get(ve,"node","sku"))
                if not sku_val:
                    continue
                sv = sku_val.lower()
                sv_root = sku_root(sv).lower()
                for t in terms_all:
                    if sv == t or sv.startswith(t) or sv_root == t:
                        hit=True
                        break
                if hit:
                    break
            if hit:
                out[n["id"]] = {"node": n}
        page = safe_get(data,"products","pageInfo")
        if page and page.get("hasNextPage"):
            after = page.get("endCursor"); pages += 1
        else:
            break
    if DEBUG:
        print(f"[DEBUG] fallback scan matched products: {len(out)} (pages scanned: {pages+1})")
    return list(out.values())

# ========= Search (immagini + pagine) =========
def bing_image_search(qry,count=50,pages=2):
    if not BING_IMAGE_KEY: return []
    url="https://api.bing.microsoft.com/v7.0/images/search"
    h={"Ocp-Apim-Subscription-Key":BING_IMAGE_KEY}
    out=[]
    for p in range(pages):
        params={"q":qry,"safeSearch":"Strict","count":count,"offset":p*count,
                "imageType":"Photo","imageContent":"Product","license":"Any"}
        try:
            r=requests.get(url,headers=h,params=params,timeout=20); r.raise_for_status()
            for it in r.json().get("value",[]):
                if it.get("contentUrl"):
                    out.append({"content":it.get("contentUrl"),"context":it.get("hostPageUrl")})
        except Exception as e:
            print(f"[Bing ERROR] {e}"); break
    return out

def google_cse_image_search(qry, per_page=10, pages=3):
    if not (GOOGLE_CSE_KEY and GOOGLE_CSE_CX): return []
    base="https://www.googleapis.com/customsearch/v1"; out=[]
    for i in range(pages):
        start=1+i*per_page
        params={"key":GOOGLE_CSE_KEY,"cx":GOOGLE_CSE_CX,"q":qry,"searchType":"image","num":per_page,
                "start":start,"safe":"active","imgType":"photo","imgDominantColor":"white"}
        try:
            r=requests.get(base,params=params,timeout=20)
            if r.status_code>=400:
                try: print(f"[Google CSE ERROR {r.status_code}] {r.json()}")
                except: print(f"[Google CSE ERROR {r.status_code}] {r.text}")
                break
            for it in r.json().get("items",[]) or []:
                out.append({"content":it.get("link"),"context":safe_get(it,"image","contextLink")})
        except Exception as e:
            print(f"[Google CSE EXC] {e}"); break
    return out

def google_cse_web_search(qry, num=10):
    if not (GOOGLE_CSE_KEY and GOOGLE_CSE_CX): return []
    base="https://www.googleapis.com/customsearch/v1"
    try:
        r=requests.get(base, params={"key":GOOGLE_CSE_KEY,"cx":GOOGLE_CSE_CX,"q":qry,"num":num,"safe":"active"}, timeout=20)
        if r.status_code>=400: return []
        return r.json().get("items",[]) or []
    except Exception:
        return []

def _http_get_text(url, limit_bytes=250000):
    try:
        with requests.get(url, timeout=10, stream=True) as r:
            r.raise_for_status()
            total=0; chunks=[]
            for ch in r.iter_content(8192):
                if not ch: continue
                try: t = ch.decode("utf-8","ignore")
                except: t = ch.decode("latin-1","ignore")
                total += len(t)
                if total>limit_bytes:
                    chunks.append(t[:max(0,limit_bytes-(total-len(t)))]); break
                chunks.append(t)
            return "".join(chunks)
    except Exception:
        return ""

def _download_bytes(url):
    try:
        with requests.get(url,stream=True,timeout=DOWNLOAD_TIMEOUT_SEC) as r:
            r.raise_for_status()
            total=0; parts=[]
            for ch in r.iter_content(8192):
                if ch:
                    total+=len(ch)
                    if total>MAX_DOWNLOAD_BYTES: return None
                    parts.append(ch)
            return b"".join(parts)
    except Exception:
        return None

# ========= Image checks =========
def _ahash(img, hash_size=8):
    from PIL import Image
    im=img.convert("L").resize((hash_size,hash_size),Image.BILINEAR)
    px=list(im.getdata()); avg=sum(px)/len(px)
    return "".join("1" if p>avg else "0" for p in px)

def _is_white_bg(img):
    from PIL import Image
    im=img.convert("RGB"); w,h=im.size
    if w<600 or h<600: return False
    bw=int(w*WHITE_BG_BORDER_PCT); bh=int(h*WHITE_BG_BORDER_PCT)
    px=im.load(); white=0; tot=0; thr=WHITE_BG_THRESHOLD
    for y in list(range(0,bh))+list(range(h-bh,h)):
        for x in range(w):
            r,g,b=px[x,y]
            if r>=thr and g>=thr and b>=thr: white+=1
            tot+=1
    for y in range(bh,h-bh):
        for x in list(range(0,bw))+list(range(w-bw,w)):
            r,g,b=px[x,y]
            if r>=thr and g>=thr and b>=thr: white+=1
            tot+=1
    return (white/max(1,tot))>=WHITE_BG_MIN_RATIO

def _context_has_code(text, code):
    if not code: return False
    c=re.escape(code)
    return re.search(rf"(^|[^A-Za-z0-9]){c}([^A-Za-z0-9]|$)", text or "", re.I) is not None

def _extract_product_structured(text):
    try:
        soup=BeautifulSoup(text,"lxml")
        meta_title = (soup.title.string if soup.title else "") or ""
        og_title = soup.find("meta",{"property":"og:title"})
        og_title = og_title.get("content","") if og_title else ""
        candidates=[]
        for s in soup.find_all("script",{"type":"application/ld+json"}):
            try:
                data=json.loads(s.string or "{}")
                if isinstance(data, list):
                    for d in data: candidates.append(d)
                else:
                    candidates.append(data)
            except Exception:
                continue
        prod={}
        for d in candidates:
            if not isinstance(d, dict): continue
            t=d.get("@type")
            if t=="Product" or (isinstance(t,list) and "Product" in t):
                prod.update(d)
        info = {
            "title": prod.get("name") or og_title or meta_title,
            "brand":  (prod.get("brand",{}) or {}).get("name") if isinstance(prod.get("brand"),dict) else prod.get("brand"),
            "gtin13": prod.get("gtin13") or prod.get("gtin"),
            "mpn":    prod.get("mpn"),
            "sku":    prod.get("sku"),
            "color":  prod.get("color"),
            "material": prod.get("material"),
        }
        specs = {}
        for th in soup.find_all(["th","td"]):
            t = (th.get_text(" ", strip=True) or "").lower()
            if not t: continue
            if "composizione" in t or "material" in t: specs["material_hint"]=t
            if "colore" in t or "color" in t: specs["color_hint"]=t
        info["specs"]=specs
        return info
    except Exception:
        return {}

# ========= Candidati immagini & filtri =========
def collect_candidate_images(queries, vendor="", code=""):
    items=[]; seen=set()
    for q in queries:
        g=google_cse_image_search(q, per_page=10, pages=3) if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else []
        b=bing_image_search(q, count=50, pages=2) if BING_IMAGE_KEY else []
        for it in (g+b):
            c=it.get("content"); ctx=it.get("context")
            if not c or c in seen: continue
            if any(bad in domain(c) for bad in DOMAINS_BLACKLIST): continue
            seen.add(c); items.append({"content":c,"context":ctx})
    items.sort(key=lambda it: score_image_url(it["content"], vendor))
    if code and STRICT_CODE_IMAGE_MATCH:
        filtered=[]
        for it in items:
            url=(it["content"] or "").lower()
            if code.lower() in url:
                filtered.append(it); continue
            ctx=it.get("context")
            if ctx:
                txt=_http_get_text(ctx, limit_bytes=CONTEXT_FETCH_MAX)
                if not txt: continue
                if _context_has_code(txt, code):
                    filtered.append(it); continue
                info=_extract_product_structured(txt)
                if any((safe_strip(info.get(k)) or "").lower()==code.lower() for k in ["sku","mpn","gtin13","gtin"]):
                    filtered.append(it); continue
        items=filtered
    return items

def filter_and_select_images(candidates, vendor="", title="", want_n=5):
    from PIL import Image
    selected=[]; seen_hash=set()
    tokens = [t.lower() for t in re.findall(r"[a-z0-9]+", (title or "")) if len(t)>3][:4]
    for it in candidates:
        if len(selected)>=want_n: break
        url=it["content"]; ctx=(it.get("context") or "").lower()
        d=domain(url)
        penalty=0
        if BRAND_DOMAINS_WHITELIST and not any(w in d for w in BRAND_DOMAINS_WHITELIST): penalty+=1
        if tokens and ctx and not any(t in ctx for t in tokens): penalty+=1
        if penalty>=2: continue
        data=_download_bytes(url)
        if not data: continue
        try:
            from io import BytesIO
            img=Image.open(BytesIO(data))
            if not _is_white_bg(img): continue
            h=_ahash(img)
            if h in seen_hash: continue
            seen_hash.add(h)
            selected.append(url)
        except Exception:
            continue
    return selected

# ========= Descrizione =========
def build_unique_description_from_page(title, vendor, ptype, code, info):
    title_src = safe_strip(info.get("title")) or title
    brand_src = safe_strip(info.get("brand")) or vendor
    color     = safe_strip(info.get("color")) or safe_strip(safe_get(info,"specs","color_hint") or "")
    material  = safe_strip(info.get("material")) or safe_strip(safe_get(info,"specs","material_hint") or "")
    title_h = html.escape(title_src or title or "Prodotto moda")
    brand_h = html.escape(brand_src or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    color_h = html.escape(color) if color else ""
    code_h  = html.escape(code or "N/D")
    par = f"<p>{title_h}"
    if brand_h: par += f" di {brand_h}"
    par += f": {ptype_h.lower()} pensato per l’uso quotidiano, con attenzione a comfort e durata."
    if color_h: par += f" Colore: {color_h}."
    if material: par += f" Composizione/Materiali: {html.escape(material)}."
    par += f" Codice articolo: {code_h}.</p>"
    bullets = []
    if material: bullets.append(f"Composizione: {html.escape(material)}")
    if color_h: bullets.append(f"Colore: {color_h}")
    bullets += ["Vestibilità confortevole e finiture curate",
                "Cura: seguire le indicazioni in etichetta",
                "Adatto a diverse occasioni d’uso"]
    ul = "<ul>" + "".join(f"<li>{b}</li>" for b in bullets[:5]) + "</ul>"
    return par + ul

def gen_description_from_sources(title, vendor, ptype, code):
    queries=[f"\"{code}\"", f"{vendor} {code}", f"{title} {code}"]
    pages=[]
    for q in queries:
        items = google_cse_web_search(q, num=8)
        for it in items:
            link = it.get("link"); d=domain(link)
            if not link: continue
            if any(b in d for b in DOMAINS_BLACKLIST): continue
            txt=_http_get_text(link, limit_bytes=CONTEXT_FETCH_MAX)
            if not txt: continue
            info=_extract_product_structured(txt)
            ok = _context_has_code(txt, code) or any((safe_strip(info.get(k)) or "").lower()==code.lower() for k in ["sku","mpn","gtin13","gtin"])
            if ok:
                pages.append((link, info))
        if pages: break
    if not pages: 
        return "", None
    link, info = pages[0]
    desc_html = build_unique_description_from_page(title, vendor, ptype, code, info)
    return desc_html, link

# ========= Report =========
def row(pid, title, vendor, code, uploaded, desc_updated, notes, context_url="", image_urls=""):
    return {
        "product_id": pid, "title": title, "vendor": vendor, "code": code,
        "images_uploaded": uploaded, "description_updated": bool(desc_updated),
        "notes": notes, "context_url": context_url, "image_urls": image_urls
    }

def report_and_exit(results, scanned, processed, skipped):
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path=f"report_autofill_{ts}.csv"
    try:
        with open(csv_path,"w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=["product_id","title","vendor","code","images_uploaded","description_updated","notes","context_url","image_urls"])
            w.writeheader()
            for r in results: w.writerow(r)
        print(f"[REPORT] Salvato: {csv_path}")
        print("[REPORT HEAD]")
        with open(csv_path,"r",encoding="utf-8") as f:
            for i,line in enumerate(f):
                print(line.rstrip())
                if i>=10: break
    except Exception as e:
        print(f"[REPORT ERROR] {e}")
    print(f"[SUMMARY] Scanned: {scanned} | Updated: {processed} | Skipped: {skipped}")

# ========= MAIN =========
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    fonte="Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
    print(f"[INFO] Fonte immagini: {fonte} | Max img/prodotto: {MAX_IMAGES_PER_PRODUCT}")
    print(f"[INFO] STRICT_CODE_IMAGE_MATCH={STRICT_CODE_IMAGE_MATCH} | STRICT_CODE_DESC_ONLY={STRICT_CODE_DESC_ONLY}")
    print(f"[INFO] Filtrando per SKU: {', '.join(ALLOWED_SKUS) if ALLOWED_SKUS else '(none)'}")
    print(f"[INFO] SUPPLIER_CODE_OFFSET={SUPPLIER_CODE_OFFSET}" + (f" | SUPPLIER_CODE_REGEX={SUPPLIER_CODE_REGEX}" if SUPPLIER_CODE_REGEX else ""))

    processed=scanned=skipped=0
    results=[]

    # --- prepara termini SKU: originali + supplier-code + radici (senza taglia) ---
    sku_terms = expand_sku_terms_for_selection(ALLOWED_SKUS)
    if DEBUG and sku_terms:
        print(f"[DEBUG] SKU terms for selection (expanded): {', '.join(sku_terms)}")

    # 1) ricerca principale per varianti (sku)
    edges = fetch_products_by_variants_query_terms(sku_terms, kind="sku")

    # 2) opzionale: EAN veri (se impostati correttamente come numeri)
    valid_eans = [x for x in ALLOWED_EANS if x.isdigit()]
    if valid_eans:
        edges += fetch_products_by_variants_query_terms(valid_eans, kind="barcode")

    # 3) fallback: scan prodotti draft e filtra localmente
    if not edges:
        edges = fallback_scan_draft_products_and_filter(sku_terms, limit_pages=5)

    # dedup
    uniq={}
    for e in edges:
        n=e.get("node"); 
        if n: uniq[n["id"]]=e
    edges=list(uniq.values())

    if not edges:
        print("[INFO] Nessun prodotto trovato (controlla che gli SKU siano realmente nei variant.sku delle bozze).")
        report_and_exit(results, scanned, processed, skipped); return

    for e in edges:
        if (processed+skipped)>=MAX_PRODUCTS: break
        n=e.get("node") or {}
        scanned+=1
        notes=[]; desc_updated=False; uploaded=0
        used_context_url=""; uploaded_urls=[]

        try:
            pid=product_id_from_gid(n["id"])
            title=safe_strip(n.get("title"))
            vendor=safe_strip(n.get("vendor"))
            ptype=safe_strip(n.get("productType"))
            status=safe_strip(n.get("status"))
            variants=n.get("variants")

            # scegli il codice per la ricerca esterna: preferisci uno SKU variante che matcha i termini
            chosen_sku = ""
            for ve in (safe_get(n,"variants","edges",default=[]) or []):
                s = safe_strip(safe_get(ve,"node","sku"))
                if not s:
                    continue
                sl = s.lower()
                if any(sl == t.lower() or sl.startswith(t.lower()) or sku_root(sl) == t.lower() for t in sku_terms):
                    chosen_sku = s
                    break
            if not chosen_sku:
                v_edges = safe_get(n,"variants","edges",default=[]) or []
                if v_edges:
                    chosen_sku = safe_strip(safe_get(v_edges[0],"node","sku"))

            supplier_code = supplier_code_from_sku(chosen_sku) if chosen_sku else ""
            supplier_root = sku_root(supplier_code) if supplier_code else ""
            code_for_search = supplier_root or supplier_code or sku_root(chosen_sku) or chosen_sku or first_barcode(n.get("variants")) or ""

            code_msg = code_for_search
            if supplier_code and chosen_sku and supplier_code != chosen_sku:
                code_msg = f"{code_for_search} (from SKU {chosen_sku})"

            if status and status.lower()!="draft":
                skipped+=1
                print(f"[PROCESS] {title} | brand={vendor or '-'} | code={code_msg} - SKIP: status non DRAFT")
                results.append(row(pid,title,vendor,code_for_search,0,False,"skip: status non DRAFT")); continue

            has_img = len(safe_get(n,"images","edges",default=[]) or [])>0
            has_desc = bool(safe_strip(n.get("bodyHtml")))
            if has_img or has_desc:
                why=[]; 
                if has_img: why.append("ha già immagini")
                if has_desc: why.append("ha già descrizione")
                print(f"[PROCESS] {title} | brand={vendor or '-'} | code={code_msg} - SKIP: " + ", ".join(why))
                skipped+=1
                results.append(row(pid,title,vendor,code_for_search,0,False,"skip: "+", ".join(why))); continue

            if not title:
                skipped+=1
                print("[PROCESS] SKIP: titolo mancante")
                results.append(row(pid,"",vendor,code_for_search,0,False,"skip: titolo mancante")); continue

            print(f"[PROCESS] {title} | brand={vendor or '-'} | code={code_msg}")

            # --- DESCRIZIONE
            if USE_SHOPIFY_MAGIC_ONLY:
                try:
                    shopify_graphql("""
                    mutation($m:[MetafieldsSetInput!]!){
                      metafieldsSet(metafields:$m){ userErrors{ field message code } }
                    }""", {"m":[{"ownerId":n["id"],"namespace":"ai_flags","key":"needs_description","type":"single_line_text_field","value":"true"}]})
                    print("  - Flag Shopify Magic impostato")
                except Exception as ex:
                    notes.append(f"flag Magic errore: {ex}"); print(f"  - ERRORE flag Magic: {ex}")
            else:
                desc_html=""; context_url=None
                if code_for_search:
                    desc_html, context_url = gen_description_from_sources(title, vendor, ptype, code_for_search)
                if not desc_html:
                    if STRICT_CODE_DESC_ONLY:
                        notes.append("descrizione non aggiornata: nessuna sorgente affidabile per il codice")
                        print("  - Nessuna pagina affidabile con il codice: descrizione NON aggiornata")
                    else:
                        desc_html = f"<p>{html.escape(title)} di {html.escape(vendor)}: {html.escape(ptype or 'prodotto')} (Codice {html.escape(code_for_search or 'N/D')}).</p><ul><li>Dettagli essenziali</li><li>Materiali e cura in etichetta</li><li>Vestibilità confortevole</li></ul>"
                if desc_html:
                    update_description(pid, desc_html); desc_updated=True; print("  - Descrizione aggiornata ✅")
                    if context_url: 
                        used_context_url = context_url
                        print(f"    • Fonte descrizione: {context_url}")

            # --- IMMAGINI
            img_urls=[]
            if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                base=" ".join([x for x in [vendor,title,"product"] if x]).strip()
                q_img=[]
                if code_for_search:
                    q_img += [f"\"{code_for_search}\"", f"{vendor} {code_for_search}", f"{title} {code_for_search} packshot", f"{vendor} {title} {code_for_search} white background"]
                    q_img += [f"site:{d} {code_for_search}" for d in BRAND_DOMAINS_WHITELIST]
                q_img += [f"{base} packshot", f"{base} white background"]
                cands = collect_candidate_images(q_img, vendor=vendor, code=code_for_search)
                img_urls = filter_and_select_images(cands, vendor=vendor, title=title, want_n=MAX_IMAGES_PER_PRODUCT)

            if img_urls:
                for u in img_urls:
                    try:
                        img_id=add_image(pid, u, alt_text=f"{vendor} {title}".strip()); uploaded+=1
                        uploaded_urls.append(u)
                        print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                    except Exception as ex:
                        notes.append(f"img errore: {ex}"); print(f"  - ERRORE immagine: {ex}")
                print(f"  - Immagini caricate: {uploaded} ✅")
            else:
                print("  - Nessuna immagine coerente con il codice (nessun upload).")

            if desc_updated or uploaded>0:
                print(f"  Admin: {ADMIN_URL_TEMPLATE.format(pid=pid)}")

            if desc_updated or uploaded>0: processed+=1
            else: skipped+=1

            results.append(row(pid,title,vendor,code_for_search,uploaded,desc_updated,"; ".join(notes), used_context_url, " | ".join(uploaded_urls)))

        except Exception as ex:
            skipped+=1
            print(f"[ERROR prodotto] {ex}"); traceback.print_exc()
            results.append(row(pid if 'pid' in locals() else "", title if 'title' in locals() else "",
                               vendor if 'vendor' in locals() else "", code_for_search if 'code_for_search' in locals() else "",
                               0, False, f"errore prodotto: {ex}"))

    report_and_exit(results, scanned, processed, skipped)

# ========= ENTRY =========
if __name__ == "__main__":
    try:
        print(f"[INFO] Using store: {SHOPIFY_STORE_DOMAIN}")
        main(); sys.exit(0)
    except Exception as e:
        print("=== UNCAUGHT ERROR ==="); print(repr(e)); traceback.print_exc(); sys.exit(0)
