import os, sys, html, json, csv, re, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup

VERSION = "2025-09-22-magic+imgs-v3-strict"
load_dotenv()

# ========= CONFIG =========
STORE = os.getenv("SHOPIFY_STORE_DOMAIN", "")
APIV  = os.getenv("SHOPIFY_API_VERSION", "2025-01")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "")

BING_IMAGE_KEY = os.getenv("BING_IMAGE_KEY", "")
GOOGLE_CSE_KEY = os.getenv("GOOGLE_CSE_KEY", "")
GOOGLE_CSE_CX  = os.getenv("GOOGLE_CSE_CX", "")

MAX_IMAGES_PER_PRODUCT = min(int(os.getenv("MAX_IMAGES_PER_PRODUCT", "5")), 5)
MAX_PRODUCTS           = int(os.getenv("MAX_PRODUCTS", "25"))

# Domini brand/retailer affidabili
BRAND_DOMAINS_WHITELIST = [d.strip().lower() for d in os.getenv("BRAND_DOMAINS_WHITELIST","").split(",") if d.strip()]
TRUSTED_RETAILER_DOMAINS = [d.strip().lower() for d in os.getenv(
    "TRUSTED_RETAILER_DOMAINS",
    "zalando.,aboutyou.,farfetch.,yoox.,ssense.,endclothing.,footlocker.,jdsports.,luisaviaroma.,zappos.,asos.,cdn.shopify.com,shopifycdn.com"
).split(",") if d.strip()]

DOMAINS_BLACKLIST = ["ebay.","aliexpress.","pinterest.","facebook.","tumblr.","wordpress.","blogspot.","vk.","tiktok.","twitter.","x.com","instagram."]
SAFE_DOMAINS_HINTS= ["cdn","images","media","static","assets","content","img","cloudfront","akamaized"]
WHITE_BG_KEYWORDS = ["white","bianco","packshot","studio","product","plain","ghost","sfondo-bianco"]

WHITE_BG_BORDER_PCT  = float(os.getenv("WHITE_BG_BORDER_PCT","0.10"))
WHITE_BG_THRESHOLD   = int(os.getenv("WHITE_BG_THRESHOLD","245"))
WHITE_BG_MIN_RATIO   = float(os.getenv("WHITE_BG_MIN_RATIO","0.88"))
IMAGE_MIN_SIDE       = int(os.getenv("IMAGE_MIN_SIDE","800"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC","10"))
MAX_DOWNLOAD_BYTES   = int(os.getenv("MAX_DOWNLOAD_BYTES","3500000"))

STRICT_CODE_IMAGE_MATCH = os.getenv("STRICT_CODE_IMAGE_MATCH","true").lower()=="true"
STRICT_CODE_DESC_ONLY   = os.getenv("STRICT_CODE_DESC_ONLY","true").lower()=="true"
CONTEXT_FETCH_MAX       = int(os.getenv("CONTEXT_FETCH_MAX","300000"))

# === Strictness & soglie (consigliato: lasciare i default severi) ===
REQUIRE_BRAND_MATCH         = os.getenv("REQUIRE_BRAND_MATCH","true").lower()=="true"
REQUIRE_CODE_IN_URL_OR_CTX  = os.getenv("REQUIRE_CODE_IN_URL_OR_CTX","true").lower()=="true"
REQUIRE_TRUSTED_DOMAIN_IMG  = os.getenv("REQUIRE_TRUSTED_DOMAIN_IMG","true").lower()=="true"  # solo brand/retailer whitelist
DESC_CONFIDENCE_THRESHOLD   = float(os.getenv("DESC_CONFIDENCE_THRESHOLD","0.80"))
IMG_CONFIDENCE_THRESHOLD    = float(os.getenv("IMG_CONFIDENCE_THRESHOLD","0.85"))

# Escludi immagini lifestyle/editorial
REJECT_LIFESTYLE_HINTS      = os.getenv("REJECT_LIFESTYLE_HINTS","true").lower()=="true"
LIFESTYLE_HINT_WORDS        = [w.strip().lower() for w in os.getenv(
    "LIFESTYLE_HINT_WORDS",
    "lookbook,campaign,street,editorial,model,runway,backstage,outfit"
).split(",") if w.strip()]

# Descrizioni / “Shopify Magic”
WRITE_MAGIC_PROMPT_METAFIELD = os.getenv("WRITE_MAGIC_PROMPT_METAFIELD","true").lower()=="true"
MAGIC_PROMPT_NAMESPACE = os.getenv("MAGIC_PROMPT_NAMESPACE","custom")
MAGIC_PROMPT_KEY       = os.getenv("MAGIC_PROMPT_KEY","magic_prompt_it")

SUPPLIER_CODE_OFFSET    = int(os.getenv("SUPPLIER_CODE_OFFSET","6"))
SUPPLIER_CODE_REGEX     = os.getenv("SUPPLIER_CODE_REGEX","")

DEBUG = os.getenv("DEBUG","false").lower()=="true"
ADMIN_URL = f"https://{STORE}/admin/products/{{pid}}"

ALLOWED_SKUS = [x.strip() for x in os.getenv("PRODUCT_SKUS","").split(",") if x.strip()]
ALLOWED_EANS = [x.strip() for x in os.getenv("PRODUCT_EANS","").split(",") if x.strip()]
ALLOWED_IDS  = [x.strip() for x in os.getenv("PRODUCT_IDS","").split(",") if x.strip()]

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

def domain(u):
    try: return urlparse(u or "").netloc.lower()
    except: return ""

def product_id_from_gid(gid:str)->int:
    return int(str(gid).split("/")[-1])

def shopify_graphql(q, vars_=None):
    url=f"https://{STORE}/admin/api/{APIV}/graphql.json"
    h={"X-Shopify-Access-Token":TOKEN,"Content-Type":"application/json"}
    r=requests.post(url,json={"query":q,"variables":vars_ or {}},headers=h,timeout=30)
    r.raise_for_status()
    j=r.json()
    if "errors" in j: raise RuntimeError(j["errors"])
    return j["data"]

def add_image(product_id_num: int, image_src: str, alt_text: str=""):
    url = f"https://{STORE}/admin/api/{APIV}/products/{product_id_num}/images.json"
    h = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    payload = {"image": {"src": image_src, "alt": (alt_text or "")[:255]}}
    r = requests.post(url, json=payload, headers=h, timeout=30)
    r.raise_for_status()
    return safe_get(r.json(), "image", "id")

def update_description(product_id_num: int, body_html: str):
    url = f"https://{STORE}/admin/api/{APIV}/products/{product_id_num}.json"
    h = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    r = requests.put(url, json={"product": {"id": product_id_num, "body_html": body_html}}, headers=h, timeout=30)
    r.raise_for_status()
    return True

def create_or_update_metafield(product_id_num:int, namespace:str, key:str, value:str, value_type="single_line_text_field"):
    """Crea/aggiorna un metafield testo su prodotto."""
    url = f"https://{STORE}/admin/api/{APIV}/metafields.json"
    h = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}
    payload = {
        "metafield":{
            "namespace": namespace,
            "key": key,
            "value": value,
            "type": value_type,
            "owner_resource": "product",
            "owner_id": product_id_num
        }
    }
    r = requests.post(url, json=payload, headers=h, timeout=30)
    if r.status_code == 422:
        # Metafield esiste già -> update via GraphQL
        q = """
        query($id:ID!){
          product(id:$id){
            metafields(first:50, namespace: "%s"){
              edges{ node{ id key namespace } }
            }
          }
        }""" % namespace
        data = shopify_graphql(q, {"id": f"gid://shopify/Product/{product_id_num}"})
        for e in safe_get(data,"product","metafields","edges",default=[]) or []:
            n=e.get("node") or {}
            if n.get("key")==key:
                mid=n.get("id")
                mq = """
                mutation($id:ID!, $val:String!){
                  metafieldsSet(metafields:[{id:$id, value:$val, type:"%s"}]){ userErrors{ field message } }
                }""" % value_type
                shopify_graphql(mq, {"id": mid, "val": value})
                return True
    r.raise_for_status()
    return True

# === Helpers ===
def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _brand_like(s):
    return _norm(s)

def _bool_score(ok, w): 
    return w if ok else 0.0

# === SKU helpers ===
def sku_root(s: str) -> str:
    s = safe_strip(s)
    if not s: return ""
    for sep in ["_", "-"]:
        if sep in s:
            return s.split(sep,1)[0]
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
    return sku[SUPPLIER_CODE_OFFSET:] if len(sku)>SUPPLIER_CODE_OFFSET else sku

def expand_sku_terms_for_selection(input_skus):
    """SKU originale + radice + supplier + supplier_radice (dedup)."""
    seen=set(); out=[]
    for s in input_skus:
        if not s: continue
        cand=[s]
        root = sku_root(s); 
        if root.lower()!=s.lower(): cand.append(root)
        sup  = supplier_code_from_sku(s); 
        if sup.lower()!=s.lower(): cand.append(sup)
        sup_r= sku_root(sup); 
        if sup_r.lower() not in [x.lower() for x in cand]: cand.append(sup_r)
        for c in cand:
            cl=c.lower()
            if cl in seen: continue
            seen.add(cl); out.append(c)
    return out

# === Product variant fetch (SKU) ===
def fetch_products_by_variants_query_terms(terms, kind="sku"):
    """Query robuste per variants in draft; ritorna prodotti (dedup)."""
    if not terms: return []
    out={}
    q = """
    query($first:Int!, $query:String!){
      productVariants(first:$first, query:$query){
        edges{ node{
          id sku barcode
          product{
            id title vendor productType handle status bodyHtml tags
            images(first:1){ edges{ node{ id } } }
            variants(first:50){ edges{ node{ id sku barcode title selectedOptions{ name value } } } }
          }
        }}
      }
    }"""
    for t in terms:
        for qs in [f'product_status:draft AND {kind}:"{t}"', f'{kind}:"{t}"', f'{kind}:{t}']:
            try:
                data=shopify_graphql(q,{"first":50,"query":qs})
                edges=safe_get(data,"productVariants","edges",default=[]) or []
                if DEBUG: print(f"[DEBUG] search '{qs}' -> {len(edges)} variants")
                for e in edges:
                    prod=safe_get(e,"node","product")
                    if prod: out[prod["id"]]={"node":prod}
                if edges: break
            except Exception as ex:
                if DEBUG: print(f"[DEBUG] query error '{qs}': {ex}")
    return list(out.values())

def fallback_scan_draft_products_and_filter(terms, limit_pages=6):
    """Scansiona solo draft e filtra localmente per sku==, startswith, radice."""
    if not terms: return []
    terms_all=set(t.lower() for t in terms)
    for t in list(terms_all): terms_all.add(sku_root(t).lower())
    out={}
    q=f"""
    query($first:Int!, $after:String){{
      products(first:$first, after:$after, query:"status:draft"){{
        pageInfo{{ hasNextPage endCursor }}
        edges{{ node{{
          id title vendor productType handle status bodyHtml tags
          images(first:1){{ edges{{ node{{ id }} }} }}
          variants(first:100){{ edges{{ node{{ id sku barcode title selectedOptions{{ name value }} }} }} }}
        }} }}
      }}
    }}
    """
    after=None; pages=0
    while pages<limit_pages:
        data=shopify_graphql(q,{"first":50,"after":after})
        for e in safe_get(data,"products","edges",default=[]) or []:
            n=e.get("node") or {}; hit=False
            for ve in safe_get(n,"variants","edges",default=[]) or []:
                s = safe_strip(safe_get(ve,"node","sku")).lower()
                if not s: continue
                sroot = sku_root(s).lower()
                for t in terms_all:
                    if s==t or s.startswith(t) or sroot==t:
                        hit=True; break
                if hit: break
            if hit: out[n["id"]]={"node":n}
        pi=safe_get(data,"products","pageInfo") or {}
        if pi.get("hasNextPage"): after=pi.get("endCursor"); pages+=1
        else: break
    if DEBUG: print(f"[DEBUG] fallback matched products: {len(out)} (pages scanned: {pages+1})")
    return list(out.values())

# === Google/Bing search ===
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

def google_cse_web_search(qry, num=8):
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

# === Image checks & parsing ===
def _ahash(img, hash_size=8):
    from PIL import Image
    im=img.convert("L").resize((hash_size,hash_size),Image.BILINEAR)
    px=list(im.getdata()); avg=sum(px)/len(px)
    return "".join("1" if p>avg else "0" for p in px)

def _hamming(a,b):
    return sum(ch1!=ch2 for ch1,ch2 in zip(a,b))

def _is_white_bg(img):
    im=img.convert("RGB"); w,h=im.size
    if w<IMAGE_MIN_SIDE or h<IMAGE_MIN_SIDE: return False
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
    ratio = (white/max(1,tot))
    return ratio>=WHITE_BG_MIN_RATIO

def _context_has_code(text, code):
    if not code: return False
    c=re.escape(code)
    return re.search(rf"(^|[^A-Za-z0-9]){c}([^A-Za-z0-9]|$)", text or "", re.I) is not None

def _extract_product_structured(text):
    try:
        soup=BeautifulSoup(text,"lxml")
        meta_title = (soup.title.string if soup.title else "") or ""
        ogt = soup.find("meta",{"property":"og:title"})
        og_title = ogt.get("content","") if ogt else ""
        candidates=[]
        for s in soup.find_all("script",{"type":"application/ld+json"}):
            try:
                data=json.loads(s.string or "{}")
                candidates += data if isinstance(data,list) else [data]
            except Exception:
                continue
        prod={}
        for d in candidates:
            if not isinstance(d, dict): continue
            t=d.get("@type")
            types = [t] if isinstance(t,str) else (t or [])
            if "Product" in types:
                prod.update(d)
        info={"title": prod.get("name") or og_title or meta_title,
              "brand": (prod.get("brand",{}) or {}).get("name") if isinstance(prod.get("brand"),dict) else prod.get("brand"),
              "gtin13": prod.get("gtin13") or prod.get("gtin") or prod.get("gtin8") or prod.get("isbn"),
              "mpn": prod.get("mpn"), "sku": prod.get("sku"),
              "color": prod.get("color"), "material": prod.get("material")}
        specs={}
        for el in soup.find_all(["th","td","li","p","span","div"]):
            t=(el.get_text(" ",strip=True) or "").lower()
            if "composizione" in t or "material" in t: specs["material_hint"]=t
            if "colore" in t or "color" in t: specs["color_hint"]=t
            if "maniche" in t or "sleeve" in t: specs["sleeve_hint"]=t
        info["specs"]=specs
        return info
    except Exception:
        return {}

def _brand_domain_like(vendor, d):
    return _brand_like(vendor) in d.replace(".","")

# === Candidates & filters (ranking) ===
def score_image_url(u,vendor=""):
    d=domain(u or ""); s=0
    if any(wh in d for wh in BRAND_DOMAINS_WHITELIST if wh): s-=8
    if any(wh in d for wh in TRUSTED_RETAILER_DOMAINS if wh): s-=5
    if vendor and _brand_domain_like(vendor, d): s-=3
    if any(h in d for h in SAFE_DOMAINS_HINTS): s-=1
    if any(k in (u or "").lower() for k in WHITE_BG_KEYWORDS): s-=1
    if any(b in d for b in DOMAINS_BLACKLIST): s+=10
    return s

def _is_lifestyle_url_or_ctx(url, ctx):
    u=(url or "").lower(); c=(ctx or "").lower()
    if any(w in u for w in LIFESTYLE_HINT_WORDS): return True
    if any(w in c for w in LIFESTYLE_HINT_WORDS): return True
    return False

# === Confidence models ===
def _desc_confidence(vendor, code, info, page_domain, page_text):
    brand_page = _brand_like(safe_strip(info.get("brand")))
    brand_prod = _brand_like(vendor)
    brand_match = bool(brand_page) and brand_page==brand_prod
    brand_in_domain = bool(brand_prod) and (brand_prod in page_domain.replace(".",""))
    code_in_struct = any((safe_strip(info.get(k)) or "").lower()==(code or "").lower() for k in ["sku","mpn","gtin13","gtin"])
    code_in_text = _context_has_code(page_text, code)

    s = 0.0
    s += _bool_score(code_in_struct, 0.5)
    s += _bool_score(code_in_text,   0.3)
    s += _bool_score(brand_match,    0.3)
    s += _bool_score(brand_in_domain,0.2)

    if REQUIRE_BRAND_MATCH and not (brand_match or brand_in_domain):
        return 0.0
    if REQUIRE_CODE_IN_URL_OR_CTX and not (code_in_struct or code_in_text):
        return 0.0
    return min(s,1.0)

def _img_confidence(vendor, code, url, ctx, page_text, info):
    d=domain(ctx or url)
    brand_ok = _brand_like(safe_strip(info.get("brand"))) == _brand_like(vendor)
    brand_in_domain = _brand_domain_like(vendor, d)
    code_in_url = (code or "").lower() in (url or "").lower()
    code_in_struct = any((safe_strip(info.get(k)) or "").lower()==(code or "").lower() for k in ["sku","mpn","gtin13","gtin"])
    code_in_text = _context_has_code(page_text, code)

    s = 0.0
    s += _bool_score(code_in_url,        0.35)
    s += _bool_score(code_in_struct,     0.35)
    s += _bool_score(code_in_text,       0.20)
    s += _bool_score(brand_ok,           0.25)
    s += _bool_score(brand_in_domain,    0.20)
    if any(w in d for w in BRAND_DOMAINS_WHITELIST): s += 0.25
    elif any(w in d for w in TRUSTED_RETAILER_DOMAINS): s += 0.15

    if REQUIRE_TRUSTED_DOMAIN_IMG and not (any(w in d for w in BRAND_DOMAINS_WHITELIST) or any(w in d for w in TRUSTED_RETAILER_DOMAINS)):
        return 0.0
    if REQUIRE_BRAND_MATCH and not (brand_ok or brand_in_domain):
        return 0.0
    if REQUIRE_CODE_IN_URL_OR_CTX and not (code_in_url or code_in_struct or code_in_text):
        return 0.0
    return min(s, 1.0)

# === Search wrappers ===
def collect_candidate_images(queries, vendor="", code=""):
    items=[]; seen=set()
    for q in queries:
        g=google_cse_image_search(q, per_page=10, pages=3) if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else []
        b=bing_image_search(q, count=50, pages=2) if BING_IMAGE_KEY else []
        for it in (g+b):
            c=it.get("content"); ctx=it.get("context")
            if not c or c in seen: continue
            d=domain(c)
            if any(bad in d for bad in DOMAINS_BLACKLIST): continue
            seen.add(c); items.append({"content":c,"context":ctx})
    items.sort(key=lambda it: score_image_url(it["content"], vendor))
    return items

def filter_and_select_images(candidates, vendor="", title="", code="", want_n=5):
    from PIL import Image
    selected=[]; seen_hash=[]

    for it in candidates:
        if len(selected)>=want_n: break
        url=it["content"]; ctx=it.get("context") or url

        # lifestyle hints
        if REJECT_LIFESTYLE_HINTS and _is_lifestyle_url_or_ctx(url, ctx):
            continue

        # tenta HEAD per MIME (tollerante)
        try:
            r1=requests.head(url, timeout=8, allow_redirects=True)
            ct=r1.headers.get("Content-Type","").lower()
            if "image" not in ct and not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I):
                # alcuni server non impostano correttamente il MIME, quindi non scartiamo solo per questo
                pass
        except:
            pass

        data=_download_bytes(url)
        if not data: 
            continue

        page_txt = _http_get_text(ctx, limit_bytes=CONTEXT_FETCH_MAX) if ctx else ""
        info = _extract_product_structured(page_txt) if page_txt else {}

        conf = _img_confidence(vendor, code, url, ctx, page_txt, info)
        if conf < IMG_CONFIDENCE_THRESHOLD:
            continue

        try:
            from io import BytesIO
            img=Image.open(BytesIO(data))
            w,h=img.size
            if w<IMAGE_MIN_SIDE or h<IMAGE_MIN_SIDE: 
                continue
            if not _is_white_bg(img): 
                continue
            hcode=_ahash(img)
            if any(_hamming(hcode, prev)<=5 for prev in seen_hash):
                continue
            seen_hash.append(hcode)
            selected.append((url, conf))
        except Exception:
            continue

    selected.sort(key=lambda t: t[1], reverse=True)
    return [u for (u,_) in selected]

# === Descrizioni (Shopify Magic style) ===
def magic_prompt_for_sku(sku: str) -> str:
    sku = safe_strip(sku) or "N/D"
    return f"descrivi prodotto {sku} in italiano, con prima parte emozionale e seconda parte Bullet Point"

def build_magic_style_description(title, vendor, ptype, code, info):
    title_src = safe_strip(info.get("title")) or title
    brand_src = safe_strip(info.get("brand")) or vendor
    color     = safe_strip(info.get("color")) or safe_strip(safe_get(info,"specs","color_hint") or "")
    material  = safe_strip(info.get("material")) or safe_strip(safe_get(info,"specs","material_hint") or "")
    sleeve    = safe_strip(safe_get(info,"specs","sleeve_hint") or "")

    name_bits = []
    if ptype: name_bits.append(ptype)
    if title_src and (not ptype or title_src.lower() not in (ptype or "").lower()):
        name_bits.append(title_src)
    display_name = " ".join(name_bits) or (title or "Capo")

    emo = f"Indossa {html.escape(display_name)}"
    if brand_src: emo += f" di {html.escape(brand_src)}"
    emo += " e scopri un equilibrio perfetto tra stile e comfort quotidiano."
    if color:
        emo += f" La tonalità {html.escape(color)} aggiunge carattere al tuo look."
    if material:
        emo += f" La composizione selezionata garantisce una mano piacevole e durata nel tempo."
    if sleeve:
        emo += f" Dettagli come {html.escape(sleeve)} completano l’insieme."
    emo_par = f"<p>{emo}</p>"

    bullets=[]
    if color: bullets.append(f"Colore: {html.escape(color)}")
    if material: bullets.append(f"Composizione: {html.escape(material)}")
    bullets += [
        "Vestibilità confortevole e facile da abbinare",
        "Dettagli essenziali e finiture curate",
        "Ideale dal lavoro al tempo libero",
        "Cura: seguire le istruzioni in etichetta"
    ]
    if code: bullets.append(f"Codice articolo: {html.escape(code)}")
    ul="<ul>" + "".join(f"<li>{b}</li>" for b in bullets[:6]) + "</ul>"
    return emo_par + ul

def gen_description_from_sources_magic_format(title, vendor, ptype, code):
    queries=[f"\"{code}\"", f"{vendor} {code}", f"{title} {code}"]
    best=None
    for q in queries:
        items=google_cse_web_search(q, num=8)
        for it in items:
            link=it.get("link")
            d=domain(link)
            if not link: continue
            if any(b in d for b in DOMAINS_BLACKLIST): continue
            txt=_http_get_text(link, limit_bytes=CONTEXT_FETCH_MAX)
            if not txt: continue
            info=_extract_product_structured(txt)
            conf=_desc_confidence(vendor, code, info, d, txt)
            if conf>=DESC_CONFIDENCE_THRESHOLD:
                best=(link, info, conf)
                break
        if best: break

    if best:
        link, info, conf = best
        desc_html = build_magic_style_description(title, vendor, ptype, code, info)
        return desc_html, link, conf

    # fallback “sicuro”: testo generico in formato Magic, conf 0.0 (non verrà scritto se la soglia > 0)
    info={}
    desc_html = build_magic_style_description(title, vendor, ptype, code, info)
    return desc_html, None, 0.0

# === Report ===
def row(pid, title, vendor, code, uploaded, desc_updated, notes, context_url="", image_urls=""):
    return {"product_id": pid, "title": title, "vendor": vendor, "code": code,
            "images_uploaded": uploaded, "description_updated": bool(desc_updated),
            "notes": notes, "context_url": context_url, "image_urls": image_urls}

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

# === MAIN ===
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    fonte="Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
    print(f"[INFO] Fonte immagini: {fonte} | Max img/prodotto: {MAX_IMAGES_PER_PRODUCT}")
    print(f"[INFO] Filtrando per SKU (con taglia): {', '.join(ALLOWED_SKUS) if ALLOWED_SKUS else '(none)'}")

    if not ALLOWED_SKUS:
        print("[INFO] Nessun SKU in .env (PRODUCT_SKUS)."); report_and_exit([],0,0,0); return

    results=[]; scanned=processed=skipped=0

    # espandi termini per selezione in draft
    sku_terms = expand_sku_terms_for_selection(ALLOWED_SKUS)
    if DEBUG: print(f"[DEBUG] SKU terms (expanded): {', '.join(sku_terms)}")

    # 1) ricerca variants per SKU
    edges = fetch_products_by_variants_query_terms(sku_terms, kind="sku")

    # 2) fallback: scan solo draft e match locale
    if not edges:
        edges = fallback_scan_draft_products_and_filter(sku_terms, limit_pages=6)

    # dedup per product.id
    uniq={}
    for e in edges:
        n=e.get("node")
        if n: uniq[n["id"]]=e
    edges=list(uniq.values())

    if not edges:
        print("[INFO] Nessun prodotto trovato (assicurati che gli SKU indicati siano presenti nelle varianti **in bozza**).")
        report_and_exit(results, scanned, processed, skipped); return

    for e in edges:
        if (processed+skipped)>=MAX_PRODUCTS: break
        n=e.get("node") or {}
        scanned+=1
        try:
            pid=product_id_from_gid(n["id"])
            title=safe_strip(n.get("title"))
            vendor=safe_strip(n.get("vendor"))
            ptype=safe_strip(n.get("productType"))
            status=safe_strip(n.get("status")).lower()

            has_img = len(safe_get(n,"images","edges",default=[]) or [])>0
            has_desc = bool(safe_strip(n.get("bodyHtml")))

            if status!="draft":
                skipped+=1
                print(f"[PROCESS] {title} | brand={vendor or '-'} | SKIP: non DRAFT")
                results.append(row(pid,title,vendor,"",0,False,"skip: non draft")); continue
            if has_img or has_desc:
                why=[]
                if has_img: why.append("ha immagini")
                if has_desc: why.append("ha descrizione")
                skipped+=1
                print(f"[PROCESS] {title} | brand={vendor or '-'} | SKIP: {', '.join(why)}")
                results.append(row(pid,title,vendor,"",0,False,"skip: "+", ".join(why))); continue

            # scegli una variante che matcha i termini per derivare il codice web
            chosen_sku=""
            for ve in (safe_get(n,"variants","edges",default=[]) or []):
                s=safe_strip(safe_get(ve,"node","sku"))
                if not s: continue
                sl=s.lower(); sr=sku_root(sl)
                if any(sl==t.lower() or sl.startswith(t.lower()) or sr==t.lower() for t in sku_terms):
                    chosen_sku=s; break
            if not chosen_sku:
                v_edges=safe_get(n,"variants","edges",default=[]) or []
                if v_edges: chosen_sku=safe_strip(safe_get(v_edges[0],"node","sku"))

            # per il WEB: togli primi 6 e la taglia
            supplier = supplier_code_from_sku(chosen_sku) if chosen_sku else ""
            supplier_root = sku_root(supplier) if supplier else ""
            code_for_search = supplier_root or supplier
            code_msg = code_for_search or "(no-code)"

            print(f"[PROCESS] {title} | brand={vendor or '-'} | code={code_msg} (from SKU {chosen_sku or '-'})")

            # ----- METAFIELD prompt per Shopify Magic
            if WRITE_MAGIC_PROMPT_METAFIELD and chosen_sku:
                prompt = magic_prompt_for_sku(chosen_sku)
                try:
                    create_or_update_metafield(pid, MAGIC_PROMPT_NAMESPACE, MAGIC_PROMPT_KEY, prompt)
                    print(f"  - Metafield prompt Magic scritto: {MAGIC_PROMPT_NAMESPACE}.{MAGIC_PROMPT_KEY} ✅")
                except Exception as ex:
                    print(f"  - ERRORE metafield Magic: {ex}")

            # ----- DESCRIZIONE (intro emozionale + bullets) con soglia confidenza
            desc_updated=False; used_context_url=""; desc_conf=0.0
            desc_html=""; ctx=None
            if code_for_search:
                desc_html, ctx, desc_conf = gen_description_from_sources_magic_format(title, vendor, ptype, code_for_search)

            if desc_conf >= DESC_CONFIDENCE_THRESHOLD and desc_html:
                update_description(pid, desc_html); desc_updated=True; print(f"  - Descrizione aggiornata (conf={desc_conf:.2f}) ✅")
                if ctx: used_context_url=ctx; print(f"    • Fonte: {ctx}")
            else:
                print(f"  - Descrizione NON aggiornata (conf={desc_conf:.2f} < {DESC_CONFIDENCE_THRESHOLD})")

            # ----- IMMAGINI (selettore severo con soglia)
            uploaded=0; uploaded_urls=[]
            img_urls=[]
            if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                base=" ".join([x for x in [vendor,title,"product"] if x]).strip()
                q_img=[]
                if code_for_search:
                    q_img += [
                        f"\"{code_for_search}\" packshot",
                        f"{vendor} {code_for_search} packshot",
                        f"{title} {code_for_search} white background",
                        f"{vendor} {title} {code_for_search} white background",
                    ]
                    q_img += [f"site:{d} {code_for_search}" for d in (BRAND_DOMAINS_WHITELIST+TRUSTED_RETAILER_DOMAINS)]
                q_img += [f"{base} packshot", f"{base} white background"]
                cands = collect_candidate_images(q_img, vendor=vendor, code=code_for_search)
                img_urls = filter_and_select_images(cands, vendor=vendor, title=title, code=code_for_search, want_n=MAX_IMAGES_PER_PRODUCT)

            if img_urls:
                for u in img_urls:
                    try:
                        img_id=add_image(pid,u,alt_text=f"{vendor} {title}".strip()); uploaded+=1
                        uploaded_urls.append(u)
                        print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                    except Exception as ex:
                        print(f"  - ERRORE immagine: {ex}")
                print(f"  - Immagini caricate: {uploaded} ✅")
            else:
                print("  - Nessuna immagine con confidenza sufficiente (nessun upload).")

            if desc_updated or uploaded>0:
                print(f"  Admin: {ADMIN_URL.format(pid=pid)}")

            results.append(row(pid,title,vendor,code_for_search,uploaded,desc_updated,"",used_context_url," | ".join(uploaded_urls)))
            if desc_updated or uploaded>0: processed+=1
            else: skipped+=1

        except Exception as ex:
            skipped+=1
            print(f"[ERROR prodotto] {ex}"); traceback.print_exc()
            results.append(row(pid if 'pid' in locals() else "", title if 'title' in locals() else "",
                               vendor if 'vendor' in locals() else "", "", 0, False, f"errore prodotto: {ex}"))

    report_and_exit(results, scanned, processed, skipped)

if __name__ == "__main__":
    try:
        print(f"[INFO] Using store: {STORE}")
        main(); sys.exit(0)
    except Exception as e:
        print("=== UNCAUGHT ERROR ==="); print(repr(e)); traceback.print_exc(); sys.exit(0)
