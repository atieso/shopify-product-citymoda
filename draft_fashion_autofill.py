import os, sys, html, json, csv, re, traceback, requests
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

from bs4 import BeautifulSoup

VERSION = "2025-09-19-v7j"
load_dotenv()

# ====== CONFIG
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "city-tre-srl.myshopify.com")
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
DOMAINS_BLACKLIST = ["ebay.","aliexpress.","pinterest.","facebook.","tumblr.","wordpress.","blogspot.","vk.","tiktok.","twitter.","x.com","instagram.","lacentrale.","subito.","kijiji.","mercatinousato."]

WHITE_BG_BORDER_PCT  = float(os.getenv("WHITE_BG_BORDER_PCT","0.12"))
WHITE_BG_THRESHOLD   = int(os.getenv("WHITE_BG_THRESHOLD","242"))
WHITE_BG_MIN_RATIO   = float(os.getenv("WHITE_BG_MIN_RATIO","0.82"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC","8"))
MAX_DOWNLOAD_BYTES   = int(os.getenv("MAX_DOWNLOAD_BYTES","3500000"))
STRICT_EAN_IMAGE_MATCH = os.getenv("STRICT_EAN_IMAGE_MATCH","true").lower()=="true"
STRICT_EAN_DESC_ONLY   = os.getenv("STRICT_EAN_DESC_ONLY","true").lower()=="true"
CONTEXT_FETCH_MAX    = int(os.getenv("CONTEXT_FETCH_MAX","250000")) # 250KB

DEBUG = os.getenv("DEBUG","false").lower()=="true"
ADMIN_URL_TEMPLATE = f"https://{SHOPIFY_STORE_DOMAIN}/admin/products/{{pid}}"

ALLOWED_IDS  = [x.strip() for x in os.getenv("PRODUCT_IDS","").split(",") if x.strip()]
ALLOWED_SKUS = [x.strip() for x in os.getenv("PRODUCT_SKUS","").split(",") if x.strip()]
ALLOWED_EANS = [x.strip() for x in os.getenv("PRODUCT_EANS","").split(",") if x.strip()]

# ====== UTILS
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
    try: return urlparse(u).netloc.lower()
    except: return ""

def score_image_url(u,vendor=""):
    d=domain(u or "")
    s=0
    if any(wh in d for wh in BRAND_DOMAINS_WHITELIST if wh): s-=6
    if vendor and vendor.lower().replace(" ","") in d.replace("-","").replace(" ",""): s-=3
    if any(h in d for h in SAFE_DOMAINS_HINTS): s-=2
    if any(k in (u or "").lower() for k in WHITE_BG_KEYWORDS): s-=1
    if any(b in d for b in DOMAINS_BLACKLIST): s+=6
    return s

def first_barcode(variants):
    edges = safe_get(variants,"edges",default=[]) or []
    for e in edges:
        n=e.get("node") or {}
        bc=safe_strip(n.get("barcode"))
        if bc: return bc
    return ""

def variant_selected_options(variants):
    edges=safe_get(variants,"edges",default=[]) or []
    out={}
    for e in edges:
        n=e.get("node") or {}
        for opt in (n.get("selectedOptions") or []):
            name=safe_strip(opt.get("name")).lower()
            val =safe_strip(opt.get("value"))
            if not name or not val: continue
            if name in ["color","colour","colore"]: out["color"]=val
            if name in ["size","taglia","misura"]: out["size"]=val
    return out

# ====== Shopify
def shopify_graphql(q, vars_=None):
    url=f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    h={"X-Shopify-Access-Token":SHOPIFY_ADMIN_TOKEN,"Content-Type":"application/json"}
    r=requests.post(url, json={"query":q,"variables":vars_ or {}}, headers=h, timeout=30)
    r.raise_for_status()
    j=r.json()
    if "errors" in j: raise RuntimeError(j["errors"])
    return j["data"]

def add_image(pid:int, src:str, alt:str=""):
    url=f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{pid}/images.json"
    h={"X-Shopify-Access-Token":SHOPIFY_ADMIN_TOKEN,"Content-Type":"application/json"}
    r=requests.post(url, json={"image":{"src":src,"alt":alt[:255]}}, headers=h, timeout=30)
    r.raise_for_status(); return safe_get(r.json(),"image","id")

def update_description(pid:int, body_html:str):
    url=f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/products/{pid}.json"
    h={"X-Shopify-Access-Token":SHOPIFY_ADMIN_TOKEN,"Content-Type":"application/json"}
    r=requests.put(url, json={"product":{"id":pid,"body_html":body_html}}, headers=h, timeout=30)
    r.raise_for_status(); return True

def fetch_products_by_variants_query(terms, kind="sku"):
    if not terms: return []
    out={}
    q="""
    query($first:Int!, $query:String!){
      productVariants(first:$first, query:$query){
        edges{ node{
          id sku barcode selectedOptions{ name value }
          product{
            id title vendor productType handle status bodyHtml tags
            images(first:1){ edges{ node{ id } } }
            variants(first:20){ edges{ node{ id sku barcode title selectedOptions{ name value } } } }
          }
        }}
      }
    }"""
    for t in terms:
        data=shopify_graphql(q,{"first":20,"query":f"{kind}:{t}"})
        edges=safe_get(data,"productVariants","edges",default=[]) or []
        for e in edges:
            prod=safe_get(e,"node","product")
            if prod: out[prod["id"]]={"node":prod}
    return list(out.values())

def fetch_products_by_product_ids(id_list):
    if not id_list: return []
    q="""
    query($ids:[ID!]!){
      nodes(ids:$ids){
        ... on Product {
          id title vendor productType handle status bodyHtml tags
          images(first:1){ edges{ node{ id } } }
          variants(first:20){ edges{ node{ id sku barcode title selectedOptions{ name value } } } }
        }
      }
    }"""
    gids=[gid_from_product_id(int(x)) for x in id_list]
    nodes=shopify_graphql(q,{"ids":gids}).get("nodes") or []
    return [{"node":n} for n in nodes if n]

# ====== Search (con context)
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

def _http_get_text(url, limit_bytes=250000):
    try:
        with requests.get(url, timeout=10, stream=True) as r:
            r.raise_for_status()
            total=0; chunks=[]
            for ch in r.iter_content(8192):
                if not ch: continue
                try:
                    t = ch.decode("utf-8","ignore")
                except:
                    t = ch.decode("latin-1","ignore")
                total += len(t)
                if total>limit_bytes:
                    chunks.append(t[:max(0,limit_bytes-(total-len(t)))])
                    break
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

# ====== Image checks
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

def _context_has_ean(text, ean):
    e=re.escape(ean)
    return re.search(rf"(^|[^0-9]){e}([^0-9]|$)", text or "", re.I) is not None

def _extract_product_structured(text):
    # prova a leggere JSON-LD Product
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
            if (d.get("@type")=="Product") or ("Product" in (d.get("@type") or [])):
                prod.update(d)
        # normalizza
        info = {
            "title": prod.get("name") or og_title or meta_title,
            "brand":  (prod.get("brand",{}) or {}).get("name") if isinstance(prod.get("brand"),dict) else prod.get("brand"),
            "gtin13": prod.get("gtin13") or prod.get("gtin"),
            "mpn":    prod.get("mpn"),
            "sku":    prod.get("sku"),
            "color":  prod.get("color"),
            "material": prod.get("material"),
        }
        # estrai tabelline basiche
        specs = {}
        for th in soup.find_all(["th","td"]):
            t = (th.get_text(" ", strip=True) or "").lower()
            if not t: continue
            if "material" in t or "composizione" in t: specs["material_hint"]=t
            if "colore" in t or "color" in t: specs["color_hint"]=t
        info["specs"]=specs
        return info
    except Exception:
        return {}

def collect_candidate_images(queries, vendor="", ean=""):
    items=[]; seen=set()
    for q in queries:
        g=google_cse_image_search(q, per_page=10, pages=3) if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else []
        b=bing_image_search(q, count=50, pages=2) if BING_IMAGE_KEY else []
        for it in (g+b):
            c=it.get("content"); ctx=it.get("context")
            if not c or c in seen: continue
            # scarta domini blacklist subito
            if any(bad in domain(c) for bad in DOMAINS_BLACKLIST): continue
            seen.add(c); items.append({"content":c,"context":ctx})
    # ordina
    items.sort(key=lambda it: score_image_url(it["content"], vendor))
    # filtro EAN strict
    if ean and STRICT_EAN_IMAGE_MATCH:
        filtered=[]
        for it in items:
            url= (it["content"] or "").lower()
            d=domain(url)
            if any(b in d for b in DOMAINS_BLACKLIST): continue
            if ean in url:  # ean nel filename/url
                filtered.append(it); continue
            ctx=it.get("context")
            if ctx:
                txt=_http_get_text(ctx, limit_bytes=CONTEXT_FETCH_MAX)
                if not txt: continue
                if _context_has_ean(txt, ean):
                    filtered.append(it); continue
                # prova JSON-LD
                info=_extract_product_structured(txt)
                if (safe_strip(info.get("gtin13"))==ean) or (safe_strip(info.get("sku"))==ean) or (safe_strip(info.get("mpn"))==ean):
                    filtered.append(it); continue
        items=filtered
    return items

def filter_and_select_images(candidates, vendor="", title="", ean="", want_n=5):
    from PIL import Image
    selected=[]; seen_hash=set()
    title_tokens = [t.lower() for t in re.findall(r"[a-z0-9]+", title or "") if len(t)>3][:4]
    for it in candidates:
        if len(selected)>=want_n: break
        url=it["content"]; ctx=it.get("context") or ""
        d=domain(url)
        # preferisci se brand/whitelist o titolo compare nella context url
        penalty = 0
        if not any(w in d for w in BRAND_DOMAINS_WHITELIST): penalty += 1
        if title_tokens and ctx and not any(t in (ctx.lower()) for t in title_tokens): penalty += 1
        if penalty>=2: # troppo debole
            continue
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

# ====== Description (personalizzata da pagina EAN)
def build_unique_description_from_page(title, vendor, ptype, ean, page_text, info):
    # prendi titolo dal page info se credibile
    title_src = safe_strip(info.get("title")) or title
    brand_src = safe_strip(info.get("brand")) or vendor
    color = safe_strip(info.get("color")) or ""
    material = safe_strip(info.get("material")) or ""
    if not material:
        mh = safe_strip(safe_get(info,"specs","material_hint"))
        if mh: material = mh.replace("composizione","").replace("material","").strip(": ").title()

    title_h = html.escape(title_src or title or "Prodotto moda")
    brand_h = html.escape(brand_src or "")
    ptype_h = html.escape(ptype or "Abbigliamento")
    color_h = html.escape(color) if color else ""
    ean_h = html.escape(ean or "N/D")

    par = f"<p>{title_h}"
    if brand_h: par += f" di {brand_h}"
    par += f": {ptype_h.lower()} progettato per un uso quotidiano, con attenzione a comfort e durata."
    if color_h: par += f" Colore: {color_h}."
    if material: par += f" Materiali/Composizione: {html.escape(material)}."
    par += f" Codice articolo (EAN): {ean_h}.</p>"

    bullets = []
    if material: bullets.append(f"Composizione: {html.escape(material)}")
    if color_h: bullets.append(f"Colore: {color_h}")
    bullets.append("Vestibilità equilibrata e finiture pulite")
    bullets.append("Cura: seguire le indicazioni in etichetta")
    bullets.append("Adatto a diverse occasioni d'uso")
    ul = "<ul>" + "".join(f"<li>{b}</li>" for b in bullets) + "</ul>"
    return par + ul

def gen_description_from_sources(title, vendor, ptype, ean, queries):
    # cerca pagine dove l'EAN è certo
    pages = []
    for q in queries:
        # usa google per pagine web (non immagini)
        items=[]
        if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
            base="https://www.googleapis.com/customsearch/v1"
            try:
                r=requests.get(base, params={"key":GOOGLE_CSE_KEY,"cx":GOOGLE_CSE_CX,"q":q,"num":10,"safe":"active"}, timeout=20)
                if r.status_code<400:
                    items = r.json().get("items",[]) or []
            except: pass
        for it in items:
            link = it.get("link"); d=domain(link)
            if not link: continue
            if any(b in d for b in DOMAINS_BLACKLIST): continue
            if BRAND_DOMAINS_WHITELIST and not any(w in d for w in BRAND_DOMAINS_WHITELIST):
                # accetta anche retailer comuni credibili (*.it/*.com) ma evita forum/social
                if any(b in d for b in ["amazon.","zalando.","zara.","unavailable"]):
                    pass
            txt=_http_get_text(link, limit_bytes=CONTEXT_FETCH_MAX)
            if not txt: continue
            if not _context_has_ean(txt, ean):
                info=_extract_product_structured(txt)
                if not (safe_strip(info.get("gtin13"))==ean or safe_strip(info.get("gtin"))==ean or safe_strip(info.get("sku"))==ean or safe_strip(info.get("mpn"))==ean):
                    continue
            pages.append((link, txt, _extract_product_structured(txt)))
            if len(pages)>=2: break
        if len(pages)>=2: break
    if not pages:
        return ""  # nessuna pagina affidabile
    # usa la prima pagina valida per descrizione
    _, text, info = pages[0]
    return build_unique_description_from_page(title, vendor, ptype, ean, text, info)

# ====== MAIN
def main():
    print(f"[START] draft_fashion_autofill {VERSION}")
    fonte = "Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
    if DEBUG:
        print(f"[DEBUG] Fonte immagini: {fonte} | STRICT_EAN_IMAGE_MATCH={STRICT_EAN_IMAGE_MATCH} | STRICT_EAN_DESC_ONLY={STRICT_EAN_DESC_ONLY}")

    processed=scanned=skipped=0
    results=[]

    # seleziona prodotti (preferisci EAN/SKU)
    edges=[]
    if ALLOWED_SKUS: edges += fetch_products_by_variants_query(ALLOWED_SKUS, kind="sku")
    if ALLOWED_EANS: edges += fetch_products_by_variants_query(ALLOWED_EANS, kind="barcode")
    # se PRODUCT_IDS contiene EAN (8-14 cifre) trattali come barcode:
    if ALLOWED_IDS:
        as_ean=[x for x in ALLOWED_IDS if x.isdigit() and 8<=len(x)<=14]
        pure_ids=[x for x in ALLOWED_IDS if not (x.isdigit() and 8<=len(x)<=14)]
        if as_ean: edges += fetch_products_by_variants_query(as_ean, kind="barcode")
        if pure_ids: edges += fetch_products_by_product_ids(pure_ids)

    # dedup
    uniq={}
    for e in edges:
        n=e.get("node"); 
        if n: uniq[n["id"]]=e
    edges=list(uniq.values())
    if not edges:
        print("[INFO] Nessun prodotto trovato (controlla PRODUCT_EANS / PRODUCT_SKUS).")
        report_and_exit(results, scanned, processed, skipped); return

    for e in edges:
        if (processed+skipped)>=MAX_PRODUCTS: break
        n=e.get("node") or {}
        scanned+=1
        notes=[]; desc_updated=False; uploaded=0

        try:
            pid=product_id_from_gid(n["id"])
            title=safe_strip(n.get("title"))
            vendor=safe_strip(n.get("vendor"))
            ptype=safe_strip(n.get("productType"))
            tags=n.get("tags") or []
            ean=first_barcode(n.get("variants"))
            selopts=variant_selected_options(n.get("variants"))
            status=safe_strip(n.get("status"))

            if status and status.lower()!="draft":
                skipped+=1
                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'} - SKIP: status non DRAFT")
                results.append(row(pid,title,vendor,ean,0,False,"skip: status non DRAFT")); continue

            has_img = len(safe_get(n,"images","edges",default=[]) or [])>0
            has_desc = bool(safe_strip(n.get("bodyHtml")))
            if has_img or has_desc:
                why=[]; 
                if has_img: why.append("ha già immagini")
                if has_desc: why.append("ha già descrizione")
                print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'} - SKIP: " + ", ".join(why))
                skipped+=1
                results.append(row(pid,title,vendor,ean,0,False,"skip: "+", ".join(why))); continue

            if not title:
                skipped+=1
                print("[PROCESS] SKIP: titolo mancante")
                results.append(row(pid,"",vendor,ean,0,False,"skip: titolo mancante")); continue

            print(f"[PROCESS] {title} | brand={vendor or '-'} | ean={ean or '-'}")

            # --- DESCRIZIONE (EAN-driven)
            if USE_SHOPIFY_MAGIC_ONLY:
                try:
                    # opzionale: flag per Magic (namespace >=3 char)
                    shopify_graphql("""
                    mutation($m:[MetafieldsSetInput!]!){
                      metafieldsSet(metafields:$m){ userErrors{ field message code } }
                    }""", {"m":[{"ownerId":n["id"],"namespace":"ai_flags","key":"needs_description","type":"single_line_text_field","value":"true"}]})
                    print("  - Flag per Shopify Magic impostato")
                except Exception as ex:
                    notes.append(f"flag Magic errore: {ex}"); print(f"  - ERRORE flag Magic: {ex}")
            else:
                desc_html=""
                # Query per pagina affidabile (EAN nel testo o JSON-LD)
                if ean:
                    q_list=[f"\"{ean}\"", f"{vendor} {ean}", f"{title} {ean}"]
                    desc_html = gen_description_from_sources(title, vendor, ptype, ean, q_list)
                if not desc_html:
                    if STRICT_EAN_DESC_ONLY:
                        notes.append("descrizione non aggiornata: nessuna sorgente EAN affidabile")
                        print("  - Nessuna pagina affidabile con EAN: descrizione NON aggiornata")
                    else:
                        # fallback personalizzato leggero (ancora diverso per EAN)
                        desc_html = f"<p>{html.escape(title)} di {html.escape(vendor)}: {html.escape(ptype or 'prodotto')}. Codice EAN: {html.escape(ean or 'N/D')}.</p><ul><li>Dettagli essenziali</li><li>Materiali e cura in etichetta</li><li>Vestibilità confortevole</li></ul>"
                if desc_html:
                    update_description(pid, desc_html); desc_updated=True; print("  - Descrizione aggiornata ✅")

            # --- IMMAGINI (EAN strict + white bg + dedup + min 600px)
            img_urls=[]
            if BING_IMAGE_KEY or (GOOGLE_CSE_KEY and GOOGLE_CSE_CX):
                base=" ".join([x for x in [vendor,title,"product"] if x]).strip()
                q_img=[]
                if ean:
                    q_img += [f"\"{ean}\"", f"{vendor} {ean}", f"{title} {ean} packshot", f"{vendor} {title} {ean} white background"]
                    q_img += [f"site:{d} {ean}" for d in BRAND_DOMAINS_WHITELIST]
                q_img += [f"{base} packshot", f"{base} white background"]
                cands = collect_candidate_images(q_img, vendor=vendor, ean=ean)
                img_urls = filter_and_select_images(cands, vendor=vendor, title=title, ean=ean, want_n=MAX_IMAGES_PER_PRODUCT)

            if img_urls:
                for u in img_urls:
                    try:
                        img_id=add_image(pid, u, alt=f"{vendor} {title}".strip()); uploaded+=1
                        print(f"  - Immagine aggiunta (#{uploaded}) id={img_id} ✅")
                    except Exception as ex:
                        notes.append(f"img errore: {ex}"); print(f"  - ERRORE immagine: {ex}")
                print(f"  - Immagini caricate: {uploaded} ✅")
            else:
                print("  - Nessuna immagine con EAN confermato (nessun upload).")

            if desc_updated or uploaded>0:
                print(f"  Admin: {ADMIN_URL_TEMPLATE.format(pid=pid)}")

            if desc_updated or uploaded>0: processed+=1
            else: skipped+=1

            results.append(row(pid,title,vendor,ean,uploaded,desc_updated,"; ".join(notes)))

        except Exception as ex:
            skipped+=1
            print(f"[ERROR prodotto] {ex}"); traceback.print_exc()
            results.append(row(pid if 'pid' in locals() else "", title if 'title' in locals() else "",
                               vendor if 'vendor' in locals() else "", ean if 'ean' in locals() else "",
                               0, False, f"errore prodotto: {ex}"))

    report_and_exit(results, scanned, processed, skipped)

def row(pid, title, vendor, ean, uploaded, desc_updated, notes):
    return {"product_id": pid, "title": title, "vendor": vendor, "ean": ean,
            "images_uploaded": uploaded, "description_updated": bool(desc_updated), "notes": notes}

def report_and_exit(results, scanned, processed, skipped):
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path=f"report_autofill_{ts}.csv"
    try:
        with open(csv_path,"w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=["product_id","title","vendor","ean","images_uploaded","description_updated","notes"])
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

if __name__ == "__main__":
    try:
        print(f"[INFO] Using store: {SHOPIFY_STORE_DOMAIN}")
        fonte="Google" if (GOOGLE_CSE_KEY and GOOGLE_CSE_CX) else ("Bing" if BING_IMAGE_KEY else "Nessuna")
        print(f"[INFO] Fonte immagini: {fonte} | Max img/prodotto: {MAX_IMAGES_PER_PRODUCT}")
        print(f"[INFO] STRICT_EAN_IMAGE_MATCH={STRICT_EAN_IMAGE_MATCH} | STRICT_EAN_DESC_ONLY={STRICT_EAN_DESC_ONLY}")
        if ALLOWED_SKUS: print(f"[INFO] SKU: {', '.join(ALLOWED_SKUS)}")
        if ALLOWED_EANS: print(f"[INFO] EAN: {', '.join(ALLOWED_EANS)}")
        if ALLOWED_IDS:  print(f"[INFO] IDs: {', '.join(ALLOWED_IDS)}")
        main(); sys.exit(0)
    except Exception as e:
        print("=== UNCAUGHT ERROR ==="); print(repr(e)); traceback.print_exc(); sys.exit(0)
