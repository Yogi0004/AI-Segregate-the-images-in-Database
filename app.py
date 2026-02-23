"""
app.py — AI Image Segregator  (Auto-Bucket Edition)
JSON log format matches:
  results.moved_count      = images moved
  results.unchanged_count  = 0  (always — real count is in extra)
  extra.unchanged_count    = actual images that stayed
  extra.total_processed    = moved + unchanged (full scan count)
Run: streamlit run app.py
"""

import os, shutil, re, tempfile, math, urllib.parse, json, datetime
from pathlib import Path

import streamlit as st
import boto3
from botocore.client import Config
from PIL import Image as PILImage

st.set_page_config(page_title="AI Image Segregator", page_icon="🗂️",
                   layout="wide", initial_sidebar_state="expanded")

R2_ENDPOINT   = "https://93.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "d62481sdgsEDHnbda1361"
R2_SECRET_KEY = "b22d834fb76c5gserhurdztjzrtjkrztkfyyytc4f9de44f"
SUPPORTED     = {".jpg",".jpeg",".png",".gif",".webp",".bmp",".tiff",".tif"}

st.markdown("""<style>
.guide-box{background:#1a2332;border-left:4px solid #4a9eff;padding:.9rem 1.1rem;
           border-radius:4px;margin:.4rem 0 .8rem}
.guide-box h4{color:#4a9eff;margin:0 0 .4rem;font-size:.88rem}
.guide-box p{color:#ccd6e0;font-size:.81rem;margin:.15rem 0}
.kp{background:#0d1117;border:1px solid #30363d;padding:.2rem .5rem;border-radius:3px;
    font-family:monospace;color:#79c0ff;font-size:.78rem}
.ok-box{background:#0d2818;border:1px solid #238636;border-radius:4px;
        padding:.5rem .9rem;margin:.3rem 0;color:#3fb950;font-size:.82rem}
.err-box{background:#2d1116;border:1px solid #da3633;border-radius:4px;
         padding:.5rem .9rem;margin:.3rem 0;color:#f85149;font-size:.82rem}
div[data-testid="metric-container"]{background:#161b22;border:1px solid #30363d;padding:.7rem}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#58a6ff}
</style>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════════════
# JSON SESSION LOGGER
# Produces this exact structure for every event type:
#
#  {
#    "id":              "2026-02-19T18-02-40-706873_r2_folder_upload",
#    "timestamp":       "2026-02-19T18:02:40.706873",
#    "date":            "2026-02-19",
#    "time":            "18:02:40",
#    "event_type":      "r2_folder_upload",
#    "bucket":          "img",
#    "source_prefix":   "data/",
#    "output_prefix":   "data/abstract_pattern/",
#    "mode":            "",
#    "reference_image": "<uploaded>",
#    "results": {
#      "total_processed": 25,         ← images that were moved
#      "groups": { "abstract_pattern": ["file.jpg", …] },
#      "moved_count":     25,
#      "unchanged_count": 0           ← always 0 here; real count in extra
#    },
#    "extra": {
#      "is_placeholder":  false,
#      "dest_sub":        "abstract_pattern",
#      "moved":           25,
#      "unchanged_count": 78,         ← actual images that stayed
#      "total_processed": 103         ← moved + unchanged
#    }
#  }
# ════════════════════════════════════════════════════════════════════════════════
_SESSION_LOG_FILE = "segregator_session_log.json"


def _load_log() -> list:
    try:
        with open(_SESSION_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_log(entries: list) -> None:
    with open(_SESSION_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False, default=str)


def log_session_event(
    event_type: str,
    bucket: str = "",
    source_prefix: str = "",
    output_prefix: str = "",
    mode: str = "",
    summary: dict = None,
    reference_image: str = "",
    extra: dict = None,
) -> None:
    now = datetime.datetime.now()
    iso = now.isoformat()

    # ID format: 2026-02-19T18-02-40-706873_event_type
    entry_id = f"{iso}_{event_type}".replace(":", "-").replace(".", "-")

    # ── results block ─────────────────────────────────────────────────────────
    # total_processed & moved_count = images actually moved/grouped
    # unchanged_count = always 0 (real unchanged lives in extra)
    results: dict = {}
    if summary:
        moved_total = sum(len(v) for v in summary.values())
        results = {
            "total_processed": moved_total,
            "groups":          summary,
            "moved_count":     moved_total,
            "unchanged_count": 0,
        }

    # ── extra block: full picture ─────────────────────────────────────────────
    # Callers must pass:
    #   moved           = int (same as moved_count)
    #   unchanged_count = int (images that stayed)
    #   total_processed = moved + unchanged  (full scan size)
    # Plus any event-specific keys (is_placeholder, dest_sub, etc.)
    _extra = dict(extra) if extra else {}

    entry = {
        "id":              entry_id,
        "timestamp":       iso,
        "date":            now.strftime("%Y-%m-%d"),
        "time":            now.strftime("%H:%M:%S"),
        "event_type":      event_type,
        "bucket":          bucket,
        "source_prefix":   source_prefix,
        "output_prefix":   output_prefix,
        "mode":            mode,
        "reference_image": reference_image,
        "results":         results,
        "extra":           _extra,
    }

    # auto-saved to segregator_session_log.json on every call
    log = _load_log()
    log.append(entry)
    _save_log(log)


def _render_log_sidebar():
    st.divider()
    st.subheader("📋 Session Log")
    log = _load_log()
    if not log:
        st.caption("No events logged yet.")
        return

    st.caption(f"{len(log)} event(s) recorded")

    st.download_button(
        label="⬇️ Download Full Log (JSON)",
        data=json.dumps(log, indent=2, ensure_ascii=False, default=str),
        file_name=f"segregator_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json",
        use_container_width=True,
    )

    with st.expander("📄 Latest 5 events"):
        for ev in reversed(log[-5:]):
            r   = ev.get("results", {})
            ext = ev.get("extra", {})
            st.markdown(
                f"**{ev['date']} {ev['time']}** · `{ev['event_type']}`  \n"
                f"Bucket: `{ev.get('bucket','—')}` | "
                f"Src: `{ev.get('source_prefix','—')}` | "
                f"Moved: **{r.get('moved_count','—')}** | "
                f"Unchanged: **{ext.get('unchanged_count','—')}** | "
                f"Total scanned: **{ext.get('total_processed','—')}**"
            )
            st.divider()

    if st.button("🗑️ Clear Log", key="clear_log_btn"):
        _save_log([])
        st.success("Log cleared.")
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# R2 CLIENT
# ════════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner=False)
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4",
                      retries={"max_attempts": 5, "mode": "adaptive"}),
        region_name="auto",
    )

def fetch_buckets(s3) -> list:
    try:
        return sorted(b["Name"] for b in s3.list_buckets().get("Buckets", []))
    except Exception as e:
        st.sidebar.error(f"Cannot list buckets: {e}")
        return []

def scan_bucket(s3, bucket: str, prefix: str = "", max_keys: int = 0):
    def _fetch(pfx):
        keys, folders = [], set()
        try:
            pager = s3.get_paginator("list_objects_v2")
            for page in pager.paginate(Bucket=bucket, Prefix=pfx,
                                       PaginationConfig={"PageSize": 1000}):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    keys.append(k)
                    rel = k[len(pfx):]
                    if "/" in rel:
                        folders.add(pfx + rel.split("/")[0] + "/")
                if max_keys and len(keys) >= max_keys:
                    break
        except Exception as e:
            st.error(f"Scan error: {e}")
        return keys, folders

    pfx = prefix.strip().lstrip("/")
    all_keys, folders = _fetch(pfx)
    if not all_keys and pfx and not pfx.endswith("/"):
        all_keys, folders = _fetch(pfx + "/")

    img_keys = [k for k in all_keys if Path(k).suffix.lower() in SUPPORTED]
    return img_keys, all_keys, sorted(folders)

# ════════════════════════════════════════════════════════════════════════════════
# IMAGE CLASSIFICATION
# ════════════════════════════════════════════════════════════════════════════════
def _avg_hsv(img):
    px = list(img.convert("RGB").resize((60,60),PILImage.LANCZOS).convert("HSV").getdata())
    n  = len(px)
    return sum(p[0] for p in px)/n*360/255, sum(p[1] for p in px)/n/255, sum(p[2] for p in px)/n/255

def _edge(img):
    px = list(img.convert("L").resize((80,80),PILImage.LANCZOS).getdata())
    m  = sum(px)/len(px)
    return math.sqrt(sum((p-m)**2 for p in px)/len(px))

def _perceptual_hash(img, hash_size=8):
    img = img.convert("L").resize((hash_size + 1, hash_size), PILImage.LANCZOS)
    px = list(img.getdata())
    diff = [px[i] < px[i+1] for i in range(len(px)-1)]
    return sum(2**i for i, b in enumerate(diff) if b)

def _hamming_distance(h1, h2):
    return bin(h1 ^ h2).count('1')

PLACEHOLDER_TEXTS = [
    'NO IMAGE', 'COMING SOON', 'NO MASTER PLAN', 'NO FLOOR PLAN',
    'NOT AVAILABLE', 'IMAGE NOT FOUND', 'NO PHOTO', 'PHOTO NOT AVAILABLE',
    'IMAGE COMING SOON', 'NO PICTURE', 'PLACEHOLDER', 'N/A',
    'NO IMAGE FOUND', 'IMAGE UNAVAILABLE', 'NO MASTERPLAN', 'NO FLOORPLAN',
    'COMING SOON!', 'STAY TUNED', 'UPLOADING SOON', 'WILL BE UPDATED',
    'NOT YET AVAILABLE', 'IMAGE PENDING', 'NO IMAGE AVAILABLE',
]

def _detect_placeholder_text(img) -> bool:
    rgb = img.convert("RGB").resize((120, 120))
    px  = list(rgb.getdata())
    n   = len(px)

    avg_r = sum(p[0] for p in px) / n
    avg_g = sum(p[1] for p in px) / n
    avg_b = sum(p[2] for p in px) / n
    is_light_bg = avg_r > 190 and avg_g > 190 and avg_b > 190
    is_grey_bg  = (abs(avg_r - avg_g) < 25 and abs(avg_g - avg_b) < 25 and avg_r > 150)
    is_plain_bg = is_light_bg or is_grey_bg

    colour_counts: dict = {}
    for p in px:
        b = (p[0] // 20 * 20, p[1] // 20 * 20, p[2] // 20 * 20)
        colour_counts[b] = colour_counts.get(b, 0) + 1
    top_count      = max(colour_counts.values())
    dominant_ratio = top_count / n
    S1 = dominant_ratio > 0.50

    grey_px    = list(img.convert("L").resize((80, 80), PILImage.LANCZOS).getdata())
    mean_g     = sum(grey_px) / len(grey_px)
    edge_score = math.sqrt(sum((p - mean_g) ** 2 for p in grey_px) / len(grey_px))

    S2 = edge_score < 14
    S3 = is_plain_bg and (10 <= edge_score <= 40)

    bw_px       = list(img.convert("L").resize((100, 100), PILImage.LANCZOS).getdata())
    light_px    = sum(1 for v in bw_px if v > 220)
    dark_px     = sum(1 for v in bw_px if v < 60)
    total_bw    = len(bw_px)
    light_ratio = light_px / total_bw
    dark_ratio  = dark_px  / total_bw
    S4 = is_plain_bg and light_ratio > 0.55 and 0.01 <= dark_ratio <= 0.35

    if sum([S1, S2, S3, S4]) >= 2:
        return True

    try:
        import pytesseract
        text = pytesseract.image_to_string(img).upper()
        if any(ph in text for ph in PLACEHOLDER_TEXTS):
            return True
    except Exception:
        pass

    return False


def _pixel_similarity(img_a: PILImage.Image, img_b: PILImage.Image, size: int = 32) -> float:
    a = list(img_a.convert("L").resize((size, size), PILImage.LANCZOS).getdata())
    b = list(img_b.convert("L").resize((size, size), PILImage.LANCZOS).getdata())
    n = len(a)
    return 1.0 - (sum(abs(a[i] - b[i]) for i in range(n)) / (255 * n))


def classify_image(path: str) -> dict:
    try: img = PILImage.open(path).convert("RGB")
    except Exception as e: return {"category":"Unclassified","subcategory":"Unknown","tags":[],"description":str(e)}
    w,h=img.size; ratio=w/h if h else 1
    hue,sat,val=_avg_hsv(img); edge=_edge(img)
    H=lambda lo,hi: lo<=hue<hi
    is_green=H(70,160); is_blue=H(190,260); is_red=H(0,20) or H(340,360)
    is_brown=H(20,40) and sat<0.5; is_grey=sat<0.12
    bright=val>0.55; dark=val<0.35; colorful=sat>0.40
    portrait=ratio<0.80; landscape=ratio>1.35; square=0.80<=ratio<=1.35
    complex_=edge>38; simple=edge<20
    cat,sub,tags="Abstract","Pattern",["pattern","texture"]
    if   is_green and bright and landscape:                   cat,sub,tags="Nature","Forest",["green","trees","outdoor"]
    elif is_blue  and bright and landscape and val>0.6:       cat,sub,tags="Nature","Sky",["blue","sky","outdoor"]
    elif is_blue  and landscape and sat>0.3:                  cat,sub,tags="Nature","Beach",["water","blue","outdoor"]
    elif is_green and landscape:                              cat,sub,tags="Nature","Landscape",["green","nature","outdoor"]
    elif portrait and complex_ and not dark:                  cat,sub,tags="People","Portrait",["person","portrait","face"]
    elif square   and complex_ and sat<0.3 and bright:        cat,sub,tags="People","Portrait",["person","monochrome"]
    elif (is_red or is_brown) and complex_ and square:        cat,sub,tags="Food","Meal",["food","plate","colorful"]
    elif colorful and complex_ and square and not is_blue:    cat,sub,tags="Food","Dish",["food","colorful"]
    elif is_brown and complex_ and landscape:                 cat,sub,tags="Animals","Wildlife",["animal","brown"]
    elif is_green and complex_ and square:                    cat,sub,tags="Animals","Wildlife",["animal","nature"]
    elif is_grey  and complex_ and portrait:                  cat,sub,tags="Architecture","Building",["building","urban"]
    elif is_grey  and complex_ and landscape:                 cat,sub,tags="Architecture","Cityscape",["city","urban"]
    elif bright   and complex_ and portrait:                  cat,sub,tags="Architecture","Structure",["building","outdoor"]
    elif is_grey  and landscape and simple:                   cat,sub,tags="Vehicles","Car",["vehicle","grey"]
    elif dark     and landscape and complex_:                 cat,sub,tags="Vehicles","Transport",["vehicle","dark"]
    elif dark     and complex_  and is_blue:                  cat,sub,tags="Technology","Electronics",["tech","screen"]
    elif is_grey  and simple    and landscape:                cat,sub,tags="Technology","Device",["device","minimal"]
    elif colorful and complex_  and not bright:               cat,sub,tags="Art","Painting",["art","colorful"]
    elif colorful and simple:                                 cat,sub,tags="Art","Abstract",["art","abstract"]
    elif is_green and landscape and complex_:                 cat,sub,tags="Sports","Outdoor",["sports","field"]
    elif simple:                                              cat,sub,tags="Abstract","Minimal",["minimal","pattern"]
    return {"category":cat,"subcategory":sub,"tags":tags,
            "description":f"{'Bright' if bright else 'Dark'}, {'colorful' if colorful else 'muted'} {sub.lower()}."}

# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def sanitize(n): return re.sub(r'[\\/:*?"<>|\x00-\x1f]','_',n).strip() or "Unknown"

def get_group(info,mode):
    if mode=="category":    return sanitize(info.get("category","Unclassified"))
    if mode=="subcategory": return sanitize(info.get("category","Unk"))+"/"+sanitize(info.get("subcategory","Unk"))
    tags=info.get("tags",[]); return sanitize(tags[0]) if tags else "Untagged"

def r2_key_from_input(url_or_key,bucket):
    if not url_or_key.startswith("http"): return url_or_key.strip().lstrip("/")
    path=urllib.parse.urlparse(url_or_key).path.lstrip("/")
    if bucket and path.startswith(bucket+"/"): path=path[len(bucket)+1:]
    return path

def safe_dest_key(s3,bucket,folder,filename,idx):
    dk=f"{folder}/{filename}"
    try: s3.head_object(Bucket=bucket,Key=dk); stem,ext=Path(filename).stem,Path(filename).suffix; dk=f"{folder}/{stem}_{idx}{ext}"
    except: pass
    return dk

def upload_r2(s3,bucket,local,dest_key):
    ct={".jpg":"image/jpeg",".jpeg":"image/jpeg",".png":"image/png",".gif":"image/gif",
        ".webp":"image/webp",".bmp":"image/bmp",".tiff":"image/tiff",".tif":"image/tiff"
        }.get(Path(local).suffix.lower(),"application/octet-stream")
    s3.upload_file(local,bucket,dest_key,ExtraArgs={"ContentType":ct})

def safe_local_copy(src,dest_dir,idx):
    os.makedirs(dest_dir,exist_ok=True); name=os.path.basename(src); dest=os.path.join(dest_dir,name)
    if os.path.exists(dest):
        stem,ext=Path(name).stem,Path(name).suffix; dest=os.path.join(dest_dir,f"{stem}_{idx}{ext}")
    shutil.copy2(src,dest)

def find_local(folder):
    paths=[]
    for root,_,files in os.walk(folder):
        for f in files:
            if Path(f).suffix.lower() in SUPPORTED: paths.append(os.path.join(root,f))
    return sorted(paths)

def show_summary(summary):
    st.divider(); st.subheader("📊 Results")
    total=sum(len(v) for v in summary.values())
    cols=st.columns(min(len(summary)+1,6))
    cols[0].metric("✅ Total",total)
    for i,(g,files) in enumerate(sorted(summary.items()),1):
        cols[i%len(cols)].metric(f"📁 {g.split('/')[-1]}",len(files))
    st.divider()
    for group,files in sorted(summary.items()):
        with st.expander(f"📁 **{group}** — {len(files)} image(s)"):
            for f in files: st.write(f"• {f}")

# ════════════════════════════════════════════════════════════════════════════════
# SEGREGATION ENGINES — each calls log_session_event with correct extra fields
# ════════════════════════════════════════════════════════════════════════════════
def seg_r2_full(s3,bucket,prefix,out_prefix,mode,bar):
    img_keys,_,_=scan_bucket(s3,bucket,prefix)
    if not img_keys:
        st.error("❌ No images found. Use the **Bucket Scanner** above — click ⚡ Quick Scan to see all keys.")
        return {}
    total=len(img_keys); summary={}; failed=0
    st.success(f"✅ Found **{total}** images — processing now…")
    with tempfile.TemporaryDirectory() as tmp:
        for i,key in enumerate(img_keys,1):
            name=Path(key).name; bar.progress(i/total,text=f"[{i}/{total}] {name}")
            local=os.path.join(tmp,f"{i}_{name}")
            try: s3.download_file(bucket,key,local)
            except Exception as e: st.warning(f"⚠️ Download `{key}`: {e}"); failed+=1; continue
            info=classify_image(local); group=get_group(info,mode)
            folder=f"{out_prefix.rstrip('/')}/{group}"
            dk=safe_dest_key(s3,bucket,folder,name,i)
            try:
                upload_r2(s3,bucket,local,dk)
                st.write(f"✅ `{name}` → 📁 **{group}** `({info['category']} / {info['subcategory']})`")
                summary.setdefault(group,[]).append(name)
            except Exception as e: st.error(f"❌ Upload `{name}`: {e}"); failed+=1
    bar.progress(1.0,text="Complete!")
    moved_n = sum(len(v) for v in summary.values())
    log_session_event(
        "r2_full", bucket=bucket, source_prefix=prefix,
        output_prefix=out_prefix, mode=mode, summary=summary,
        extra={
            "moved":           moved_n,
            "unchanged_count": 0,
            "total_processed": total,
            "failed":          failed,
        }
    )
    return summary

def seg_r2_ref(s3,bucket,ref_input,prefix,out_prefix,bar):
    ref_tmp=None
    try:
        ref_key=r2_key_from_input(ref_input,bucket); suffix=Path(ref_key).suffix or ".jpg"
        f=tempfile.NamedTemporaryFile(suffix=suffix,delete=False); f.close(); ref_tmp=f.name
        s3.download_file(bucket,ref_key,ref_tmp); ref_info=classify_image(ref_tmp)
    except Exception as e: st.error(f"❌ Reference failed: {e}"); return {}
    finally:
        if ref_tmp and os.path.exists(ref_tmp): os.remove(ref_tmp)
    ref_cat=ref_info["category"].lower(); ref_sub=ref_info["subcategory"].lower()
    ref_tags={t.lower() for t in ref_info.get("tags",[])}
    group_nm=sanitize(f"{ref_cat}_{ref_sub}") or "Similar"
    st.info(f"Reference → **{ref_cat} / {ref_sub}**  |  tags: `{', '.join(ref_tags)}`")
    img_keys,_,_=scan_bucket(s3,bucket,prefix)
    if not img_keys: st.error("❌ No images found."); return {}
    total=len(img_keys); summary={}
    with tempfile.TemporaryDirectory() as tmp:
        for i,key in enumerate(img_keys,1):
            name=Path(key).name; bar.progress(i/total,text=f"[{i}/{total}] {name}")
            local=os.path.join(tmp,f"{i}_{name}")
            try: s3.download_file(bucket,key,local)
            except Exception as e: st.warning(f"⚠️ {e}"); continue
            info=classify_image(local); cat=info["category"].lower(); sub=info["subcategory"].lower()
            tags={t.lower() for t in info.get("tags",[])}
            similar=(cat==ref_cat) or (sub==ref_sub) or bool(tags&ref_tags)
            folder=group_nm if similar else "Other"
            dk=safe_dest_key(s3,bucket,f"{out_prefix.rstrip('/')}/{folder}",name,i)
            label="✅ SIMILAR" if similar else "➡️ Other"
            try:
                upload_r2(s3,bucket,local,dk); st.write(f"{label} `{name}` → `{folder}/`")
                summary.setdefault(folder,[]).append(name)
            except Exception as e: st.error(f"❌ `{name}`: {e}")
    bar.progress(1.0,text="Complete!")
    moved_n     = len(summary.get(group_nm, []))
    unchanged_n = len(summary.get("Other",   []))
    log_session_event(
        "r2_reference", bucket=bucket, source_prefix=prefix,
        output_prefix=out_prefix, reference_image=ref_input, summary=summary,
        extra={
            "ref_category":    ref_cat,
            "ref_subcategory": ref_sub,
            "moved":           moved_n,
            "unchanged_count": unchanged_n,
            "total_processed": total,
        }
    )
    return summary

def seg_local_full(source,output,mode,bar):
    images=find_local(source)
    if not images: st.error(f"❌ No images in `{source}`"); return {}
    total=len(images); summary={}
    for i,path in enumerate(images,1):
        name=os.path.basename(path); bar.progress(i/total,text=f"[{i}/{total}] {name}")
        info=classify_image(path); group=get_group(info,mode)
        dest_dir=os.path.join(output,group.replace("/",os.sep))
        try:
            safe_local_copy(path,dest_dir,i); st.write(f"✅ `{name}` → **{group}/**")
            summary.setdefault(group,[]).append(name)
        except Exception as e: st.error(f"❌ `{name}`: {e}")
    bar.progress(1.0,text="Complete!")
    moved_n = sum(len(v) for v in summary.values())
    log_session_event(
        "local_full", source_prefix=source, output_prefix=output,
        mode=mode, summary=summary,
        extra={
            "moved":           moved_n,
            "unchanged_count": total - moved_n,
            "total_processed": total,
        }
    )
    return summary

def seg_local_ref(s3,bucket,ref_input,source,output,bar):
    ref_tmp=None
    try:
        if os.path.isfile(ref_input): ref_info=classify_image(ref_input)
        else:
            ref_key=r2_key_from_input(ref_input,bucket); suffix=Path(ref_key).suffix or ".jpg"
            f=tempfile.NamedTemporaryFile(suffix=suffix,delete=False); f.close(); ref_tmp=f.name
            s3.download_file(bucket,ref_key,ref_tmp); ref_info=classify_image(ref_tmp)
    except Exception as e: st.error(f"❌ Reference failed: {e}"); return {}
    finally:
        if ref_tmp and os.path.exists(ref_tmp): os.remove(ref_tmp)
    ref_cat=ref_info["category"].lower(); ref_sub=ref_info["subcategory"].lower()
    ref_tags={t.lower() for t in ref_info.get("tags",[])}
    group_nm=sanitize(f"{ref_cat}_{ref_sub}") or "Similar"
    st.info(f"Reference → **{ref_cat}/{ref_sub}**")
    images=find_local(source)
    if not images: st.error(f"❌ No images in `{source}`"); return {}
    total=len(images); summary={}
    for i,path in enumerate(images,1):
        name=os.path.basename(path); bar.progress(i/total,text=f"[{i}/{total}] {name}")
        info=classify_image(path); cat=info["category"].lower(); sub=info["subcategory"].lower()
        tags={t.lower() for t in info.get("tags",[])}
        similar=(cat==ref_cat) or (sub==ref_sub) or bool(tags&ref_tags)
        folder=group_nm if similar else "Other"; label="✅ SIMILAR" if similar else "➡️ Other"
        try:
            safe_local_copy(path,os.path.join(output,folder),i)
            st.write(f"{label} `{name}`"); summary.setdefault(folder,[]).append(name)
        except Exception as e: st.error(f"❌ `{name}`: {e}")
    bar.progress(1.0,text="Complete!")
    moved_n     = len(summary.get(group_nm, []))
    unchanged_n = len(summary.get("Other",   []))
    log_session_event(
        "local_reference", source_prefix=source, output_prefix=output,
        reference_image=ref_input, summary=summary,
        extra={
            "moved":           moved_n,
            "unchanged_count": unchanged_n,
            "total_processed": total,
        }
    )
    return summary


# ════════════════════════════════════════════════════════════════════════════════
#  UI — HEADER
# ════════════════════════════════════════════════════════════════════════════════
st.title("🗂️ AI Image Segregator")
st.caption("Cloudflare R2  ·  Pillow AI  ·  Auto-Bucket Detection  ·  JSON Session Logging")

s3 = _s3_client()

# ════════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ════════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ Settings")

    if "buckets" not in st.session_state or st.button("🔄 Refresh Bucket List", use_container_width=True):
        with st.spinner("Fetching buckets…"):
            st.session_state["buckets"] = fetch_buckets(s3)

    all_buckets = st.session_state.get("buckets", [])
    connected   = bool(all_buckets)

    if connected:
        st.success(f"☁️ Connected — **{len(all_buckets)}** bucket(s)")
    else:
        st.error("☁️ R2 connection failed")

    st.divider()
    st.markdown("**🪣 Select Bucket**")
    st.caption("New bucket created? Click 🔄 Refresh above — it will appear here instantly.")

    if all_buckets:
        bucket = st.selectbox("Active bucket:", options=all_buckets, key="bucket_select")
        with st.expander(f"All buckets ({len(all_buckets)})"):
            for b in all_buckets:
                icon = "🟢" if b == bucket else "⚪"
                st.write(f"{icon} `{b}`")
    else:
        bucket = st.text_input("Bucket name", placeholder="my-bucket")

    st.divider()
    st.subheader("Grouping Mode")
    mode = st.radio(
        "Group images by:",
        ["category", "subcategory", "tags"],
        format_func=lambda x: {
            "category":    "📁 Category  (Nature, Food…)",
            "subcategory": "📂 Sub-category  (Nature/Forest…)",
            "tags":        "🏷️ First Tag",
        }[x],
    )
    st.divider()
    st.caption("Supported: JPG PNG GIF WEBP BMP TIFF")

    with st.expander("ℹ️ How new buckets work"):
        st.markdown("""
**When you create a new bucket in Cloudflare R2:**
1. Go to your R2 dashboard
2. Create the bucket
3. Come back here
4. Click **🔄 Refresh Bucket List**
5. Your new bucket appears in the dropdown instantly
        """)

    _render_log_sidebar()

# ════════════════════════════════════════════════════════════════════════════════
#  HOW-TO GUIDE
# ════════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="guide-box">
<h4>📖 Quick Guide — 3 Steps</h4>
<p><b>Step 1 →</b> Select your <b>Bucket</b> in the sidebar. New bucket? Click 🔄 Refresh.</p>
<p><b>Step 2 →</b> Use the <b>Bucket Scanner</b> below — click ⚡ Quick Scan to see all image paths.</p>
<p><b>Step 3 →</b> Copy the folder path shown by scanner → paste into <b>Source Prefix</b> in the tabs below → Run.</p>
<p>⚠️ <b>Bucket name ≠ Source Prefix.</b> If your bucket is <span class="kp">img</span> and images are at the root, leave Source Prefix <b>blank</b>.</p>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════════════
#  BUCKET SCANNER
# ════════════════════════════════════════════════════════════════════════════════
st.subheader("🔍 Bucket Scanner")
st.caption("Scan your bucket to see exact folder paths — then copy-paste into the form below.")

if not bucket:
    st.info("Select a bucket in the sidebar.")
else:
    sc1, sc2, sc3 = st.columns([3, 1, 1])
    scan_pfx  = sc1.text_input("Filter by prefix (leave blank = show everything)",
                                value="", placeholder="images/   or leave blank", key="scan_pfx")
    scan_btn  = sc2.button("🔍 Scan",          key="btn_scan",  type="primary", use_container_width=True)
    quick_btn = sc3.button("⚡ Quick Scan All", key="btn_quick", use_container_width=True)

    prev_bucket = st.session_state.get("_prev_bucket")
    if prev_bucket != bucket:
        st.session_state["_prev_bucket"] = bucket
        st.session_state["_scan_result"] = None

    if scan_btn or quick_btn:
        pfx = "" if quick_btn else scan_pfx
        with st.spinner(f"Scanning `{bucket}` …"):
            result = scan_bucket(s3, bucket, pfx)
        st.session_state["_scan_result"] = result
        st.session_state["_scan_pfx"]    = pfx

    scan_res = st.session_state.get("_scan_result")
    if scan_res:
        img_keys, all_keys, folders = scan_res
        scanned_pfx = st.session_state.get("_scan_pfx", "")
        m1,m2,m3 = st.columns(3)
        m1.metric("🖼️ Images",       len(img_keys))
        m2.metric("📄 Total Objects", len(all_keys))
        m3.metric("📁 Sub-folders",   len(folders))

        if img_keys:
            st.markdown('<div class="ok-box">✅ Images found! Copy a path below → paste into Source Prefix</div>',
                        unsafe_allow_html=True)
            st.markdown("#### 📁 Folder paths — copy and paste into Source Prefix:")

            folder_map: dict = {}
            for k in img_keys:
                parts = k.split("/")
                folder = "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
                folder_map.setdefault(folder, []).append(k)

            for folder, fkeys in sorted(folder_map.items()):
                fa, fb, fc = st.columns([3, 1, 1])
                display = folder if folder else "(root — leave prefix blank)"
                fa.code(display)
                fb.metric("Images", len(fkeys))
                if fc.button("📋 Use this", key=f"use_{folder}", use_container_width=True):
                    st.session_state["_selected_prefix"] = folder
                    st.success(f"✅ Prefix `{folder or '(blank)'}` copied — it will auto-fill below!")

                with st.expander(f"Preview files in `{display}`"):
                    for k in fkeys[:8]: st.code(k)
                    if len(fkeys) > 8: st.caption(f"… and {len(fkeys)-8} more")

                    st.markdown("---")
                    st.markdown("**⬆️ Upload Reference & Sort**")
                    _upl = st.file_uploader("Upload reference", type=["jpg","jpeg","png","gif","webp","bmp","tiff"], key=f"scan_up_{folder}")
                    if _upl:
                        _rb = _upl.read()
                        _rs = Path(_upl.name).suffix or ".jpg"
                        if st.button(f"🚀 Sort {len(fkeys)} images", key=f"scan_sort_{folder}", type="primary"):
                            _b = st.progress(0.0, text="Sorting…")
                            _r = seg_folder_by_upload(s3, bucket, folder, _rb, _rs, _b)
                            if _r:
                                st.success(f"✅ {len(_r.get('moved',[]))} moved · {len(_r.get('unchanged',[]))} stayed")

                    st.markdown("---")
                    st.markdown("**🗑️ Delete Entire Folder**")
                    if st.button(f"🗑️ Delete {len(fkeys)} images", key=f"scan_del_{folder}"):
                        st.session_state[f"scan_del_confirm_{folder}"] = True
                    if st.session_state.get(f"scan_del_confirm_{folder}"):
                        st.warning(f"⚠️ Delete ALL {len(fkeys)} images in `{display}`?")
                        _da, _db = st.columns(2)
                        if _da.button("✅ Yes", key=f"scan_delyes_{folder}", type="primary"):
                            _deleted = 0
                            for _k in fkeys:
                                try: s3.delete_object(Bucket=bucket, Key=_k); _deleted += 1
                                except: pass
                            st.success(f"✅ Deleted {_deleted} images")
                            st.session_state[f"scan_del_confirm_{folder}"] = False
                            del st.session_state["_scan_result"]
                            st.rerun()
                        if _db.button("❌ Cancel", key=f"scan_delno_{folder}"):
                            st.session_state[f"scan_del_confirm_{folder}"] = False
                            st.rerun()
        else:
            st.markdown(f'<div class="err-box">❌ No images under <code>{scanned_pfx or "(root)"}</code></div>',
                        unsafe_allow_html=True)
            if all_keys:
                st.markdown("**All keys found (none are images):**")
                for k in all_keys[:20]: st.code(k)
            else:
                st.warning("Bucket is empty or credentials cannot access it.")

    with st.expander("📝 Examples for YOUR bucket"):
        st.markdown("""
| Scenario | Source Prefix | Output Prefix |
|---|---|---|
| Images in `images/masterImgs/` | `images/masterImgs/` | `images/masterImgs/` |
| Images in `images/uploadBHKImgs/` | `images/uploadBHKImgs/` | `images/uploadBHKImgs/` |
| Images in `images/uploadPropertyImgs/` | `images/uploadPropertyImgs/` | `images/uploadPropertyImgs/` |
| Process entire bucket | *(leave blank)* | `categorized/` |

**Rule:** Source Prefix = folder path shown by scanner above. Segregated images go into sub-folders INSIDE the same folder.
""")

# ════════════════════════════════════════════════════════════════════════════════
#  SEGREGATION TABS
# ════════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("🚀 Run Segregation")

tab_r1, tab_r2, tab_l1, tab_l2 = st.tabs([
    "☁️ R2 — Full Bucket",
    "☁️ R2 — By Reference",
    "📁 Local — Full Folder",
    "🔍 Local — By Reference",
])

_auto_pfx = st.session_state.get("_selected_prefix", "")

with tab_r1:
    st.markdown("""<div class="guide-box"><h4>☁️ Full Bucket Segregation</h4>
<p>Every image → classified → re-uploaded into <span class="kp">sorted/Nature/</span>, <span class="kp">sorted/Food/</span>, etc.</p>
</div>""", unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    pfx1  = c1.text_input("Source Prefix  (paste from scanner above, or blank = all)",
                           value=_auto_pfx, placeholder="masterImgs/   or leave blank", key="pfx1")
    op1   = c2.text_input("Output Prefix  (sorted results go here)", placeholder="sorted/", key="op1")
    if bucket:
        st.info(f"📦 Bucket: **`{bucket}`**  |  📂 Source: **`{pfx1 or '(entire bucket)'}`**  |  📤 Output: **`{op1 or '?'}`**")
    if st.button("🚀 Start R2 Segregation", key="b1", type="primary"):
        errs=[]
        if not connected: errs.append("R2 not connected.")
        if not bucket:    errs.append("Select a bucket in sidebar.")
        if not op1:       errs.append("Enter an Output Prefix (e.g. sorted/)")
        for e in errs: st.error(e)
        if not errs:
            bar=st.progress(0.0,text="Starting…")
            summary=seg_r2_full(s3,bucket,pfx1,op1,mode,bar)
            if summary: show_summary(summary)

with tab_r2:
    st.markdown("""<div class="guide-box"><h4>☁️ Sort by Reference Image</h4>
<p>Upload one reference image → similar images sorted into one folder, rest into "Other".</p>
</div>""", unsafe_allow_html=True)
    ref2 = st.text_input("🖼️ Reference — R2 object key", placeholder="masterImgs/photo.jpg", key="ref2")
    c1,c2=st.columns(2)
    pfx2 =c1.text_input("Source Prefix", value=_auto_pfx, placeholder="masterImgs/  or blank", key="pfx2")
    op2  =c2.text_input("Output Prefix", placeholder="sorted/", key="op2")
    if st.button("🔍 Find & Segregate", key="b2", type="primary"):
        errs=[]
        if not connected: errs.append("R2 not connected.")
        if not bucket:    errs.append("Select a bucket.")
        if not ref2:      errs.append("Enter a reference object key.")
        if not op2:       errs.append("Enter output prefix.")
        for e in errs: st.error(e)
        if not errs:
            bar=st.progress(0.0,text="Fetching reference…")
            summary=seg_r2_ref(s3,bucket,ref2,pfx2,op2,bar)
            if summary: show_summary(summary)

with tab_l1:
    st.markdown("""<div class="guide-box"><h4>📁 Local Folder Segregation</h4>
<p>Reads images from your computer → sorts into sub-folders locally. No R2 needed.</p>
</div>""", unsafe_allow_html=True)
    c1,c2=st.columns(2)
    src3=c1.text_input("Source Folder",placeholder="C:/Users/Name/Pictures",key="s3")
    out3=c2.text_input("Output Folder",placeholder="C:/Users/Name/Sorted",  key="o3")
    if src3 and st.button("🔍 Preview",key="prev3"):
        if not os.path.isdir(src3): st.error(f"❌ Not found: `{src3}`")
        else:
            found=find_local(src3)
            if found: st.success(f"✅ {len(found)} images."); [st.code(p) for p in found[:10]]
            else: st.error("No images found.")
    if st.button("🚀 Start Local Segregation",key="b3",type="primary"):
        errs=[]
        if not src3: errs.append("Enter source folder.")
        if not out3: errs.append("Enter output folder.")
        if src3 and not os.path.isdir(src3): errs.append(f"Not found: `{src3}`")
        for e in errs: st.error(e)
        if not errs:
            bar=st.progress(0.0,text="Starting…")
            summary=seg_local_full(src3,out3,mode,bar)
            if summary: show_summary(summary)

with tab_l2:
    st.markdown("""<div class="guide-box"><h4>🔍 Local Sort by Reference</h4>
<p>Reference = local file path OR R2 object key (fetched automatically).</p>
</div>""", unsafe_allow_html=True)
    ref4=st.text_input("🖼️ Reference",placeholder="C:/imgs/ref.jpg  OR  masterImgs/ref.jpg",key="ref4")
    if ref4 and os.path.isfile(ref4):
        try: st.image(ref4,caption="Preview",width=220)
        except: pass
    c1,c2=st.columns(2)
    src4=c1.text_input("Source Folder",placeholder="C:/Users/Name/Pictures",key="s4")
    out4=c2.text_input("Output Folder",placeholder="C:/Users/Name/Sorted",  key="o4")
    if st.button("🔍 Find & Segregate (Local)",key="b4",type="primary"):
        errs=[]
        if not ref4: errs.append("Enter a reference image.")
        if not src4: errs.append("Enter source folder.")
        if not out4: errs.append("Enter output folder.")
        if src4 and not os.path.isdir(src4): errs.append(f"Not found: `{src4}`")
        if ref4 and not os.path.isfile(ref4) and not bucket:
            errs.append("Select a bucket (needed to fetch R2 reference).")
        for e in errs: st.error(e)
        if not errs:
            bar=st.progress(0.0,text="Starting…")
            summary=seg_local_ref(s3,bucket,ref4,src4,out4,bar)
            if summary: show_summary(summary)


# ════════════════════════════════════════════════════════════════════════════════
#  IMAGE GALLERY helpers
# ════════════════════════════════════════════════════════════════════════════════
def get_presigned_url(s3, bucket: str, key: str, expires: int = 600) -> str:
    try:
        return s3.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires)
    except Exception:
        return ""

_HASH_THRESHOLD_PLACEHOLDER = 12
_HASH_THRESHOLD_REAL        = 8
_PIXEL_THRESHOLD_PLACEHOLDER= 0.82
_PIXEL_THRESHOLD_REAL       = 0.88


# ════════════════════════════════════════════════════════════════════════════════
# PHASE 1: preview_folder_by_upload — scan only, nothing moved
# ════════════════════════════════════════════════════════════════════════════════
def preview_folder_by_upload(s3, bucket: str, folder_prefix: str,
                              ref_bytes: bytes, ref_suffix: str, bar) -> dict:
    with tempfile.NamedTemporaryFile(suffix=ref_suffix, delete=False) as tf:
        tf.write(ref_bytes); tf.flush(); ref_path = tf.name
    try:
        ref_img            = PILImage.open(ref_path).convert("RGB")
        ref_info           = classify_image(ref_path)
        ref_hash           = _perceptual_hash(ref_img)
        ref_is_placeholder = _detect_placeholder_text(ref_img)
    finally:
        os.remove(ref_path)

    ref_cat = ref_info["category"].lower().strip()
    ref_sub = ref_info["subcategory"].lower().strip()

    if ref_is_placeholder:
        dest_sub        = "no_image"
        hash_threshold  = _HASH_THRESHOLD_PLACEHOLDER
        pixel_threshold = _PIXEL_THRESHOLD_PLACEHOLDER
    else:
        dest_sub        = sanitize(f"{ref_cat}_{ref_sub}")
        hash_threshold  = _HASH_THRESHOLD_REAL
        pixel_threshold = _PIXEL_THRESHOLD_REAL

    base_pfx = folder_prefix.rstrip("/") + "/" if folder_prefix else ""
    new_pfx  = base_pfx + dest_sub + "/"

    all_img, _, _ = scan_bucket(s3, bucket, folder_prefix, max_keys=0)
    direct_keys = [
        k for k in all_img
        if "/" not in (k[len(base_pfx):] if base_pfx and k.startswith(base_pfx) else k)
        and (k[len(base_pfx):] if base_pfx and k.startswith(base_pfx) else k)
    ]

    if not direct_keys:
        return {}

    total = len(direct_keys)
    to_move, to_stay = [], []

    with tempfile.TemporaryDirectory() as tmp:
        for i, key in enumerate(direct_keys, 1):
            name = Path(key).name
            bar.progress(i / total, text=f"Analysing [{i}/{total}] {name}")
            local = os.path.join(tmp, f"{i}_{name}")
            try:
                s3.download_file(bucket, key, local)
            except Exception as e:
                st.warning(f"⚠️ Download `{key}`: {e}"); continue
            try:
                img             = PILImage.open(local).convert("RGB")
                img_hash        = _perceptual_hash(img)
                hash_dist       = _hamming_distance(ref_hash, img_hash)
                pixel_sim       = _pixel_similarity(ref_img, img)
                img_placeholder = _detect_placeholder_text(img) if ref_is_placeholder else False
            except Exception:
                hash_dist = 999; pixel_sim = 0.0; img_placeholder = False

            if ref_is_placeholder:
                is_similar = hash_dist <= hash_threshold or pixel_sim >= pixel_threshold or img_placeholder
                reason = ("placeholder_visual" if img_placeholder else
                          "hash_match" if hash_dist <= hash_threshold else "pixel_match")
            else:
                is_similar = hash_dist <= hash_threshold or pixel_sim >= pixel_threshold
                reason = "hash_match" if hash_dist <= hash_threshold else "pixel_match"

            rec = {"key": key, "filename": name,
                   "hash_dist": hash_dist, "pixel_sim": round(pixel_sim, 4)}
            if is_similar:
                rec["reason"] = reason; to_move.append(rec)
            else:
                to_stay.append(rec)

    bar.progress(1.0, text="Analysis complete!")
    return {
        "folder":          folder_prefix or "(root)",
        "dest_folder":     new_pfx,
        "reference_type":  "placeholder" if ref_is_placeholder else "real",
        "reference_class": dest_sub,
        "total_scanned":   total,
        "to_move_count":   len(to_move),
        "to_stay_count":   len(to_stay),
        "to_move":         to_move,
        "to_stay":         to_stay,
    }


# ════════════════════════════════════════════════════════════════════════════════
# PHASE 2: seg_folder_by_upload — execute move (uses preview_data if supplied)
# ════════════════════════════════════════════════════════════════════════════════
def seg_folder_by_upload(s3, bucket: str, folder_prefix: str,
                         ref_bytes: bytes, ref_suffix: str, bar,
                         preview_data: dict = None) -> dict:

    # ── LEGACY path (no preview_data) ────────────────────────────────────────
    if preview_data is None:
        with tempfile.NamedTemporaryFile(suffix=ref_suffix, delete=False) as tf:
            tf.write(ref_bytes); tf.flush(); ref_path = tf.name
        try:
            ref_img            = PILImage.open(ref_path).convert("RGB")
            ref_info           = classify_image(ref_path)
            ref_hash           = _perceptual_hash(ref_img)
            ref_is_placeholder = _detect_placeholder_text(ref_img)
        finally:
            os.remove(ref_path)

        ref_cat  = ref_info["category"].lower().strip()
        ref_sub  = ref_info["subcategory"].lower().strip()
        ref_tags = {t.lower() for t in ref_info.get("tags", [])}

        if ref_is_placeholder:
            dest_sub        = "no_image"
            hash_threshold  = _HASH_THRESHOLD_PLACEHOLDER
            pixel_threshold = _PIXEL_THRESHOLD_PLACEHOLDER
            st.info(f"📎 Reference detected as **placeholder**\n\n"
                    f"✅ Matches → `{folder_prefix.rstrip('/')}/{dest_sub}/`\n\n"
                    f"➡️ All OTHER images stay exactly where they are.")
        else:
            dest_sub        = sanitize(f"{ref_cat}_{ref_sub}")
            hash_threshold  = _HASH_THRESHOLD_REAL
            pixel_threshold = _PIXEL_THRESHOLD_REAL
            st.info(f"📎 Reference → **{ref_info['category']} / {ref_info['subcategory']}**\n\n"
                    f"✅ Similar → `{folder_prefix.rstrip('/')}/{dest_sub}/`\n\n"
                    f"➡️ All OTHER images stay exactly where they are.")

        base_pfx = folder_prefix.rstrip("/") + "/" if folder_prefix else ""
        new_pfx  = base_pfx + dest_sub + "/"

        all_img, _, _ = scan_bucket(s3, bucket, folder_prefix, max_keys=0)
        direct_keys = [
            k for k in all_img
            if "/" not in (k[len(base_pfx):] if base_pfx and k.startswith(base_pfx) else k)
            and (k[len(base_pfx):] if base_pfx and k.startswith(base_pfx) else k)
        ]

        if not direct_keys:
            st.error("❌ No direct images found in this folder.")
            return {}

        total = len(direct_keys)
        st.info(f"Scanning **{total}** images in `{folder_prefix or '(root)'}`…")
        result = {"moved": [], "unchanged": []}

        with tempfile.TemporaryDirectory() as tmp:
            for i, key in enumerate(direct_keys, 1):
                name = Path(key).name
                bar.progress(i / total, text=f"[{i}/{total}] {name}")
                local = os.path.join(tmp, f"{i}_{name}")
                try:
                    s3.download_file(bucket, key, local)
                except Exception as e:
                    st.warning(f"⚠️ Download `{key}`: {e}"); continue
                try:
                    img             = PILImage.open(local).convert("RGB")
                    img_hash        = _perceptual_hash(img)
                    hash_dist       = _hamming_distance(ref_hash, img_hash)
                    pixel_sim       = _pixel_similarity(ref_img, img)
                    img_placeholder = _detect_placeholder_text(img) if ref_is_placeholder else False
                except Exception:
                    hash_dist = 999; pixel_sim = 0.0; img_placeholder = False

                if ref_is_placeholder:
                    is_similar = (hash_dist <= hash_threshold or
                                  pixel_sim >= pixel_threshold or img_placeholder)
                else:
                    is_similar = (hash_dist <= hash_threshold or pixel_sim >= pixel_threshold)

                if is_similar:
                    dest_key = safe_dest_key(s3, bucket, new_pfx.rstrip("/"), name, i)
                    try:
                        upload_r2(s3, bucket, local, dest_key)
                        s3.delete_object(Bucket=bucket, Key=key)
                        label = ("🚫 PLACEHOLDER" if ref_is_placeholder and img_placeholder
                                 else "🔁 SIMILAR-PLACEHOLDER" if ref_is_placeholder
                                 else "✅ MOVED")
                        st.write(f"{label}  `{name}` → `{new_pfx}`")
                        result["moved"].append(name)
                    except Exception as e:
                        st.error(f"❌ Move failed `{name}`: {e}")
                else:
                    st.write(f"➡️ STAYS  `{name}`  (hash dist: {hash_dist} | pixel sim: {pixel_sim:.2f})")
                    result["unchanged"].append(name)

        bar.progress(1.0, text="Complete!")
        moved_n     = len(result.get("moved", []))
        unchanged_n = len(result.get("unchanged", []))
        log_session_event(
            "r2_folder_upload", bucket=bucket, source_prefix=folder_prefix,
            output_prefix=new_pfx, reference_image="<uploaded>",
            summary={dest_sub: result.get("moved", [])},
            extra={
                "is_placeholder":  ref_is_placeholder,
                "dest_sub":        dest_sub,
                "moved":           moved_n,
                "unchanged_count": unchanged_n,
                "total_processed": moved_n + unchanged_n,
            }
        )
        return result

    # ── PHASE 2: execute from preview_data ───────────────────────────────────
    keys_to_move = [rec["key"] for rec in preview_data.get("to_move", [])]
    new_pfx      = preview_data["dest_folder"]
    dest_sub     = preview_data["reference_class"]

    if not keys_to_move:
        st.warning("No images to move based on preview.")
        return {"moved": [], "unchanged": []}

    total  = len(keys_to_move)
    result = {"moved": [], "unchanged": []}
    st.info(f"Moving **{total}** matched images → `{new_pfx}`")

    with tempfile.TemporaryDirectory() as tmp:
        for i, key in enumerate(keys_to_move, 1):
            name = Path(key).name
            bar.progress(i / total, text=f"Moving [{i}/{total}] {name}")
            local = os.path.join(tmp, f"{i}_{name}")
            try:
                s3.download_file(bucket, key, local)
            except Exception as e:
                st.warning(f"⚠️ Download `{key}`: {e}"); continue
            dk = safe_dest_key(s3, bucket, new_pfx.rstrip("/"), name, i)
            try:
                upload_r2(s3, bucket, local, dk)
                s3.delete_object(Bucket=bucket, Key=key)
                st.write(f"✅ MOVED  `{name}` → `{new_pfx}`")
                result["moved"].append(name)
            except Exception as e:
                st.error(f"❌ Move failed `{name}`: {e}")

    result["unchanged"] = [rec["filename"] for rec in preview_data.get("to_stay", [])]
    bar.progress(1.0, text="Segregation complete!")

    moved_n     = len(result.get("moved", []))
    unchanged_n = len(result.get("unchanged", []))
    log_session_event(
        "r2_folder_upload", bucket=bucket,
        source_prefix=preview_data.get("folder", ""),
        output_prefix=new_pfx, reference_image="<uploaded>",
        summary={dest_sub: result.get("moved", [])},
        extra={
            "is_placeholder":  preview_data.get("reference_type") == "placeholder",
            "dest_sub":        dest_sub,
            "moved":           moved_n,
            "unchanged_count": unchanged_n,
            "total_processed": preview_data.get("total_scanned", 0),
        }
    )
    return result


# ════════════════════════════════════════════════════════════════════════════════
#  🖼️  IMAGE GALLERY
# ════════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("🖼️ Image Gallery — Browse & Smart Sort by Folder")
st.caption(
    "Browse any folder in R2 · Upload a reference image per folder → "
    "preview similar images as JSON → confirm to segregate into sub-folders."
)

st.markdown("""<style>
.gal-folder-bar{background:#161b22;border:1px solid #30363d;border-left:4px solid #58a6ff;
                padding:.55rem 1rem;border-radius:4px;margin:.25rem 0;
                font-family:monospace;font-size:.82rem;color:#cdd9e5}
.gal-folder-bar span{color:#58a6ff;font-weight:700}
.gal-img-name{font-size:.68rem;color:#8b949e;margin-top:.3rem;
              overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.gal-stat{background:#161b22;border:1px solid #30363d;border-radius:4px;
          padding:.4rem .9rem;display:inline-block;font-size:.8rem;color:#8b949e;margin:.15rem}
.gal-stat b{color:#58a6ff}
.ref-upload-box{background:#0d1f12;border:1px solid #238636;border-radius:6px;
                padding:.9rem 1.1rem;margin:.4rem 0 .6rem}
.ref-upload-box h5{color:#3fb950;margin:0 0 .5rem;font-size:.88rem}
.ref-upload-box p{color:#8b9e8b;font-size:.78rem;margin:.15rem 0}
.rv-moved{background:#0d1f12;border:2px solid #238636;border-radius:8px;padding:.7rem 1rem;margin:.5rem 0}
.rv-moved h5{color:#3fb950;font-size:.9rem;margin:0 0 .4rem}
.rv-stayed{background:#111827;border:2px solid #4a9eff;border-radius:8px;padding:.7rem 1rem;margin:.5rem 0}
.rv-stayed h5{color:#4a9eff;font-size:.9rem;margin:0 0 .4rem}
.del-sel-bar{background:#2d1116;border:1px solid #da3633;border-radius:6px;
             padding:.6rem 1rem;margin:.4rem 0;color:#f85149;font-size:.83rem}
</style>""", unsafe_allow_html=True)

# ── Predefined paths ─────────────────────────────────────────────────────────
if bucket:
    st.markdown("### 📌 Quick Access — Property Image Folders")

    PREDEFINED_PATHS = [
        ("images/masterImgs/",         "Master Images",   "🏛️"),
        ("images/uploadBHKImgs/",      "BHK Images",      "🏠"),
        ("images/uploadPropertyImgs/", "Property Images", "🏢"),
    ]

    for path_pfx, path_label, path_icon in PREDEFINED_PATHS:
        with st.expander(f"{path_icon} {path_label} — `{path_pfx}`"):
            _img_in_path, _, _ = scan_bucket(s3, bucket, path_pfx, max_keys=0)
            _base = path_pfx.rstrip("/") + "/"
            _direct_in_path = [
                k for k in _img_in_path
                if "/" not in (k[len(_base):] if k.startswith(_base) else k)
                and (k[len(_base):] if k.startswith(_base) else k)
            ]
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Total images", len(_img_in_path))
            mc2.metric("Unsorted (direct)", len(_direct_in_path))
            mc3.metric("Already sub-foldered", len(_img_in_path) - len(_direct_in_path))

            st.markdown("---")
            st.markdown("#### ⬆️ Upload Reference → Preview → Segregate")
            _up = st.file_uploader(
                f"Reference image for `{path_pfx}`",
                type=["jpg","jpeg","png","gif","webp","bmp","tiff"],
                key=f"pred_up_{path_pfx}",
            )

            if _up:
                _ref_b = _up.read()
                _ref_s = Path(_up.name).suffix or ".jpg"

                with tempfile.NamedTemporaryFile(suffix=_ref_s, delete=False) as _tf:
                    _tf.write(_ref_b); _tf.flush(); _tf_path = _tf.name
                try:
                    _prev_info        = classify_image(_tf_path)
                    _prev_img         = PILImage.open(_tf_path).convert("RGB")
                    _prev_placeholder = _detect_placeholder_text(_prev_img)
                finally:
                    os.remove(_tf_path)

                if _prev_placeholder:
                    _dest_sub   = "no_image"
                    _type_label = "🚫 Placeholder"
                else:
                    _dest_sub   = sanitize(f"{_prev_info['category'].lower()}_{_prev_info['subcategory'].lower()}")
                    _type_label = f"{_prev_info['category']} / {_prev_info['subcategory']}"

                _dest_pfx = path_pfx.rstrip("/") + "/" + _dest_sub + "/"
                _col_prev, _col_info = st.columns([1, 2])
                _col_prev.image(_ref_b, caption="Your reference", width=200)
                _col_info.markdown(f"**Classified as:** {_type_label}\n\n📁 Destination: `{_dest_pfx}`")

                _prev_state_key = f"pred_preview_data_{path_pfx}"

                if st.button(f"🔍 Find Similar Images — {path_label}  ({len(_direct_in_path)} unsorted)",
                             key=f"pred_prev_btn_{path_pfx}"):
                    _pb = st.progress(0.0, text="Analysing images…")
                    _pv = preview_folder_by_upload(s3, bucket, path_pfx, _ref_b, _ref_s, _pb)
                    st.session_state[_prev_state_key] = _pv if _pv else None
                    if not _pv:
                        st.error("❌ No images found in this folder.")

                _stored_pv = st.session_state.get(_prev_state_key)
                if _stored_pv:
                    st.markdown("---")
                    st.markdown("#### 📋 Preview")
                    _pc1, _pc2, _pc3 = st.columns(3)
                    _pc1.metric("🔍 Total Scanned", _stored_pv.get("total_scanned", 0))
                    _pc2.metric("✅ Will Move",      _stored_pv.get("to_move_count", 0))
                    _pc3.metric("➡️ Will Stay",      _stored_pv.get("to_stay_count", 0))

                    _json_display = {
                        "folder":          _stored_pv.get("folder"),
                        "dest_folder":     _stored_pv.get("dest_folder"),
                        "reference_type":  _stored_pv.get("reference_type"),
                        "reference_class": _stored_pv.get("reference_class"),
                        "total_scanned":   _stored_pv.get("total_scanned"),
                        "to_move_count":   _stored_pv.get("to_move_count"),
                        "to_stay_count":   _stored_pv.get("to_stay_count"),
                        "to_move": [
                            {"r2_key": r["key"], "filename": r["filename"],
                             "hash_dist": r["hash_dist"],
                             "pixel_sim": r["pixel_sim"], "reason": r.get("reason", "")}
                            for r in _stored_pv.get("to_move", [])
                        ],
                    }
                    with st.expander(f"📄 JSON Preview — {_stored_pv.get('to_move_count',0)} image(s) will be moved", expanded=True):
                        st.json(_json_display)

                    st.download_button(
                        label="⬇️ Download Preview JSON",
                        data=json.dumps(_stored_pv, indent=2, ensure_ascii=False, default=str),
                        file_name=f"preview_{path_pfx.replace('/','_')}{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json", key=f"pred_dl_{path_pfx}",
                    )

                    if _stored_pv.get("to_move_count", 0) > 0:
                        if st.button(
                            f"🚀 Confirm Segregation — Move {_stored_pv['to_move_count']} images to `{_stored_pv['dest_folder']}`",
                            key=f"pred_sort_{path_pfx}", type="primary",
                        ):
                            _bar = st.progress(0.0, text="Moving images…")
                            _res = seg_folder_by_upload(s3, bucket, path_pfx, _ref_b, _ref_s, _bar, preview_data=_stored_pv)
                            if _res:
                                _moved_n = len(_res.get("moved", []))
                                _unch_n  = len(_res.get("unchanged", []))
                                # build result JSON for download
                                _seg_json = {
                                    "event":           "segregation_complete",
                                    "timestamp":       datetime.datetime.now().isoformat(),
                                    "bucket":          bucket,
                                    "source_folder":   path_pfx,
                                    "dest_folder":     _stored_pv.get("dest_folder"),
                                    "reference_type":  _stored_pv.get("reference_type"),
                                    "reference_class": _stored_pv.get("reference_class"),
                                    "moved_count":     _moved_n,
                                    "unchanged_count": _unch_n,
                                    "total_scanned":   _stored_pv.get("total_scanned", 0),
                                    "moved_files": [
                                        {"r2_key": _stored_pv["dest_folder"] + fn, "filename": fn}
                                        for fn in _res.get("moved", [])
                                    ],
                                    "unchanged_files": [
                                        {"r2_key": path_pfx + fn, "filename": fn}
                                        for fn in _res.get("unchanged", [])
                                    ],
                                }
                                st.session_state[f"pred_seg_result_{path_pfx}"] = _seg_json
                                st.session_state[f"pred_seg_moved_keys_{path_pfx}"] = [
                                    _stored_pv["dest_folder"] + fn for fn in _res.get("moved", [])
                                ]
                                st.session_state[_prev_state_key] = None
                                if "gal_all_folders" in st.session_state:
                                    del st.session_state["gal_all_folders"]
                                st.rerun()
                    else:
                        st.info("ℹ️ No similar images found — nothing to segregate.")

                # ── Post-segregation: Download + Delete ───────────────────────
                _seg_done = st.session_state.get(f"pred_seg_result_{path_pfx}")
                if _seg_done:
                    st.success(
                        f"✅ **Segregation complete!** "
                        f"**{_seg_done['moved_count']}** moved → `{_seg_done['dest_folder']}` · "
                        f"**{_seg_done['unchanged_count']}** unchanged"
                    )
                    _dl1, _dl2 = st.columns(2)
                    _dl1.download_button(
                        label="⬇️ Download Segregation Result JSON",
                        data=json.dumps(_seg_done, indent=2, ensure_ascii=False, default=str),
                        file_name=f"seg_result_{path_pfx.replace('/','_')}{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        key=f"pred_seg_dl_{path_pfx}",
                        use_container_width=True,
                    )
                    if _dl2.button(
                        f"🗑️ Delete {_seg_done['moved_count']} moved files from `{_seg_done['dest_folder']}`",
                        key=f"pred_seg_delbtn_{path_pfx}", use_container_width=True,
                    ):
                        st.session_state[f"pred_seg_del_confirm_{path_pfx}"] = True

                    if st.session_state.get(f"pred_seg_del_confirm_{path_pfx}"):
                        st.warning(f"⚠️ This will permanently delete **{_seg_done['moved_count']}** images from `{_seg_done['dest_folder']}`.")
                        _dca, _dcb = st.columns(2)
                        if _dca.button("✅ Yes, delete all moved files", key=f"pred_seg_delyes_{path_pfx}", type="primary"):
                            _moved_keys_to_del = st.session_state.get(f"pred_seg_moved_keys_{path_pfx}", [])
                            _del_ok = 0
                            _dbar2 = st.progress(0.0, text="Deleting…")
                            for _di, _dk in enumerate(_moved_keys_to_del, 1):
                                _dbar2.progress(_di / max(len(_moved_keys_to_del), 1),
                                               text=f"Deleting {_di}/{len(_moved_keys_to_del)}")
                                try: s3.delete_object(Bucket=bucket, Key=_dk); _del_ok += 1
                                except: pass
                            _dbar2.empty()
                            st.success(f"✅ Deleted {_del_ok} files.")
                            st.session_state[f"pred_seg_result_{path_pfx}"]      = None
                            st.session_state[f"pred_seg_moved_keys_{path_pfx}"]  = []
                            st.session_state[f"pred_seg_del_confirm_{path_pfx}"] = False
                            if "gal_all_folders" in st.session_state:
                                del st.session_state["gal_all_folders"]
                            st.rerun()
                        if _dcb.button("❌ Cancel", key=f"pred_seg_delno_{path_pfx}"):
                            st.session_state[f"pred_seg_del_confirm_{path_pfx}"] = False
                            st.rerun()

                    if st.button("✖️ Clear result", key=f"pred_seg_clear_{path_pfx}"):
                        st.session_state[f"pred_seg_result_{path_pfx}"]     = None
                        st.session_state[f"pred_seg_moved_keys_{path_pfx}"] = []
                        st.rerun()

            st.markdown("---")
            _col_del, _ = st.columns([1, 3])
            if _col_del.button(f"🗑️ Delete ALL in `{path_pfx}`", key=f"pred_del_{path_pfx}"):
                st.session_state[f"pred_del_confirm_{path_pfx}"] = True
            if st.session_state.get(f"pred_del_confirm_{path_pfx}"):
                st.warning(f"⚠️ Delete ALL {len(_img_in_path)} images in `{path_pfx}`?")
                _da, _db = st.columns(2)
                if _da.button("✅ Yes, delete all", key=f"pred_delyes_{path_pfx}", type="primary"):
                    _deleted = 0
                    for _k in _img_in_path:
                        try: s3.delete_object(Bucket=bucket, Key=_k); _deleted += 1
                        except: pass
                    st.success(f"✅ Deleted {_deleted} images")
                    st.session_state[f"pred_del_confirm_{path_pfx}"] = False
                    st.rerun()
                if _db.button("❌ Cancel", key=f"pred_delno_{path_pfx}"):
                    st.session_state[f"pred_del_confirm_{path_pfx}"] = False
                    st.rerun()

            if st.button(f"👁️ View Images in `{path_pfx}`", key=f"pred_view_{path_pfx}"):
                st.session_state["gal_folder"] = path_pfx
                st.session_state["gal_keys"]   = _img_in_path
                st.rerun()

    st.divider()

# ── Session state init ───────────────────────────────────────────────────────
for _k, _v in [("gal_folder", None), ("gal_keys", []), ("gal_upref_open", None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

if not bucket:
    st.info("Select a bucket in the sidebar to use the gallery.")
else:
    gal_col1, gal_col2 = st.columns([4, 1])
    gal_col1.markdown("#### Step 1 — Choose a folder to browse or sort")
    refresh_gal = gal_col2.button("🔄 Refresh Folders", key="gal_refresh", use_container_width=True)

    if refresh_gal or "gal_all_folders" not in st.session_state:
        with st.spinner("Loading folder list…"):
            _gal_imgs, _, _ = scan_bucket(s3, bucket, "", max_keys=5000)
            _fmap: dict = {}
            for k in _gal_imgs:
                parts  = k.split("/")
                folder = "/".join(parts[:-1]) + "/" if len(parts) > 1 else "(root)"
                _fmap[folder] = _fmap.get(folder, 0) + 1
            st.session_state["gal_all_folders"] = _fmap

    folder_map_gal: dict = st.session_state.get("gal_all_folders", {})

    if not folder_map_gal:
        st.warning("No images found in this bucket.")
    else:
        for folder_path, img_count in sorted(folder_map_gal.items()):
            fa, fb, fc, fd = st.columns([4, 1, 2, 1])
            display = folder_path if folder_path != "(root)" else "📦 (root)"
            fa.markdown(f'<div class="gal-folder-bar">📁 <span>{display}</span> — {img_count} image(s)</div>',
                        unsafe_allow_html=True)

            if fb.button("📂 Open", key=f"gal_open_{folder_path}", use_container_width=True):
                with st.spinner(f"Loading `{folder_path}`…"):
                    pfx_use = "" if folder_path == "(root)" else folder_path
                    _fkeys, _, _ = scan_bucket(s3, bucket, pfx_use, max_keys=2000)
                    pfx_str = pfx_use.rstrip("/") + "/" if pfx_use else ""
                    direct  = [k for k in _fkeys
                                if (k[len(pfx_str):] if pfx_str and k.startswith(pfx_str) else k).find("/") == -1]
                    st.session_state["gal_folder"]     = folder_path
                    st.session_state["gal_keys"]       = direct if direct else _fkeys
                    st.session_state["gal_upref_open"] = None

            btn_label = "✅ Close" if st.session_state["gal_upref_open"] == folder_path else "⬆️ Upload & Sort"
            if fc.button(btn_label, key=f"gal_upbtn_{folder_path}", use_container_width=True):
                st.session_state["gal_upref_open"] = (
                    None if st.session_state["gal_upref_open"] == folder_path else folder_path
                )

            if fd.button("🗑️ Delete", key=f"gal_delbtn_{folder_path}", use_container_width=True):
                st.session_state[f"gal_del_confirm_{folder_path}"] = True

            if st.session_state.get(f"gal_del_confirm_{folder_path}"):
                st.warning(f"⚠️ Delete ALL {img_count} images in `{display}`?")
                _da, _db = st.columns(2)
                if _da.button("✅ Yes", key=f"gal_delyes_{folder_path}", type="primary"):
                    pfx_use = "" if folder_path == "(root)" else folder_path
                    _all_keys, _, _ = scan_bucket(s3, bucket, pfx_use, max_keys=5000)
                    _deleted = 0
                    for _k in _all_keys:
                        try: s3.delete_object(Bucket=bucket, Key=_k); _deleted += 1
                        except: pass
                    st.success(f"✅ Deleted {_deleted} images")
                    st.session_state[f"gal_del_confirm_{folder_path}"] = False
                    if "gal_all_folders" in st.session_state:
                        del st.session_state["gal_all_folders"]
                    st.rerun()
                if _db.button("❌ Cancel", key=f"gal_delno_{folder_path}"):
                    st.session_state[f"gal_del_confirm_{folder_path}"] = False
                    st.rerun()

            # ── Upload & Sort Panel ──────────────────────────────────────────
            if st.session_state["gal_upref_open"] == folder_path:
                pfx_use = "" if folder_path == "(root)" else folder_path
                st.markdown(f"""<div class="ref-upload-box">
<h5>⬆️ Upload Reference — <code>{display}</code></h5>
<p>Step 1: Upload → click <b>🔍 Find Similar Images</b> to preview JSON.</p>
<p>Step 2: Review JSON → click <b>🚀 Confirm Segregation</b> to move.</p>
</div>""", unsafe_allow_html=True)

                up_file = st.file_uploader(
                    "Choose your reference image",
                    type=["jpg","jpeg","png","gif","webp","bmp","tiff"],
                    key=f"upfile_{folder_path}",
                )

                if up_file is not None:
                    ref_bytes  = up_file.read()
                    ref_suffix = Path(up_file.name).suffix or ".jpg"

                    prev_col, info_col = st.columns([1, 2])
                    prev_col.image(ref_bytes, caption="Your reference", width=200)

                    with tempfile.NamedTemporaryFile(suffix=ref_suffix, delete=False) as _tf:
                        _tf.write(ref_bytes); _tf.flush(); _tf_path = _tf.name
                    try:
                        _prev             = classify_image(_tf_path)
                        _prev_img         = PILImage.open(_tf_path).convert("RGB")
                        _prev_placeholder = _detect_placeholder_text(_prev_img)
                    finally:
                        os.remove(_tf_path)

                    if _prev_placeholder:
                        _new_sub    = "no_image"
                        _type_label = "🚫 Placeholder"
                    else:
                        _new_sub    = sanitize(f"{_prev['category'].lower()}_{_prev['subcategory'].lower()}")
                        _type_label = f"{_prev['category']} / {_prev['subcategory']}"

                    _new_pfx = (pfx_use.rstrip("/") + "/" if pfx_use else "") + _new_sub + "/"
                    info_col.markdown(f"**Classified as:** {_type_label}\n\n📁 Destination: `{_new_pfx}`")

                    _gal_prev_key = f"gal_preview_data_{folder_path}"

                    if st.button(f"🔍 Find Similar Images  ({img_count} images in folder)",
                                 key=f"gal_prev_btn_{folder_path}"):
                        _pb = st.progress(0.0, text="Analysing images…")
                        _pv = preview_folder_by_upload(s3, bucket, pfx_use, ref_bytes, ref_suffix, _pb)
                        st.session_state[_gal_prev_key] = _pv if _pv else None
                        if not _pv:
                            st.error("❌ No images found in this folder.")

                    _gal_stored_pv = st.session_state.get(_gal_prev_key)
                    if _gal_stored_pv:
                        st.markdown("---")
                        st.markdown("#### 📋 Preview")
                        _gc1, _gc2, _gc3 = st.columns(3)
                        _gc1.metric("🔍 Total Scanned", _gal_stored_pv.get("total_scanned", 0))
                        _gc2.metric("✅ Will Move",      _gal_stored_pv.get("to_move_count", 0))
                        _gc3.metric("➡️ Will Stay",      _gal_stored_pv.get("to_stay_count", 0))

                        _gal_json = {
                            "folder":          _gal_stored_pv.get("folder"),
                            "dest_folder":     _gal_stored_pv.get("dest_folder"),
                            "reference_type":  _gal_stored_pv.get("reference_type"),
                            "reference_class": _gal_stored_pv.get("reference_class"),
                            "total_scanned":   _gal_stored_pv.get("total_scanned"),
                            "to_move_count":   _gal_stored_pv.get("to_move_count"),
                            "to_stay_count":   _gal_stored_pv.get("to_stay_count"),
                            "to_move": [
                                {"r2_key": r["key"], "filename": r["filename"],
                                 "hash_dist": r["hash_dist"],
                                 "pixel_sim": r["pixel_sim"], "reason": r.get("reason", "")}
                                for r in _gal_stored_pv.get("to_move", [])
                            ],
                        }
                        with st.expander(f"📄 JSON Preview — {_gal_stored_pv.get('to_move_count',0)} image(s) will be moved", expanded=True):
                            st.json(_gal_json)

                        st.download_button(
                            label="⬇️ Download Preview JSON",
                            data=json.dumps(_gal_stored_pv, indent=2, ensure_ascii=False, default=str),
                            file_name=f"preview_{folder_path.replace('/','_')}{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json", key=f"gal_dl_{folder_path}",
                        )

                        if _gal_stored_pv.get("to_move_count", 0) > 0:
                            if st.button(
                                f"🚀 Confirm Segregation — Move {_gal_stored_pv['to_move_count']} images to `{_gal_stored_pv['dest_folder']}`",
                                key=f"gal_run_{folder_path}", type="primary",
                            ):
                                run_bar = st.progress(0.0, text="Moving images…")
                                res = seg_folder_by_upload(s3, bucket, pfx_use, ref_bytes, ref_suffix,
                                                           run_bar, preview_data=_gal_stored_pv)
                                if res:
                                    moved_n     = len(res.get("moved", []))
                                    unchanged_n = len(res.get("unchanged", []))
                                    _gal_seg_json = {
                                        "event":           "segregation_complete",
                                        "timestamp":       datetime.datetime.now().isoformat(),
                                        "bucket":          bucket,
                                        "source_folder":   folder_path,
                                        "dest_folder":     _gal_stored_pv.get("dest_folder"),
                                        "reference_type":  _gal_stored_pv.get("reference_type"),
                                        "reference_class": _gal_stored_pv.get("reference_class"),
                                        "moved_count":     moved_n,
                                        "unchanged_count": unchanged_n,
                                        "total_scanned":   _gal_stored_pv.get("total_scanned", 0),
                                        "moved_files": [
                                            {"r2_key": _gal_stored_pv["dest_folder"] + fn, "filename": fn}
                                            for fn in res.get("moved", [])
                                        ],
                                        "unchanged_files": [
                                            {"r2_key": (pfx_use.rstrip("/") + "/" if pfx_use else "") + fn, "filename": fn}
                                            for fn in res.get("unchanged", [])
                                        ],
                                    }
                                    st.session_state[f"gal_seg_result_{folder_path}"] = _gal_seg_json
                                    st.session_state[f"gal_seg_moved_keys_{folder_path}"] = [
                                        _gal_stored_pv["dest_folder"] + fn for fn in res.get("moved", [])
                                    ]
                                    st.session_state[_gal_prev_key]   = None
                                    if "gal_all_folders" in st.session_state:
                                        del st.session_state["gal_all_folders"]
                                    st.session_state["gal_upref_open"] = None
                                    st.rerun()
                        else:
                            st.info("ℹ️ No similar images found — nothing to segregate.")

                # ── Gallery post-segregation: Download + Delete ───────────────
                _gal_seg_done = st.session_state.get(f"gal_seg_result_{folder_path}")
                if _gal_seg_done:
                    st.success(
                        f"✅ **Segregation complete!** "
                        f"**{_gal_seg_done['moved_count']}** moved → `{_gal_seg_done['dest_folder']}` · "
                        f"**{_gal_seg_done['unchanged_count']}** unchanged"
                    )
                    _gdl1, _gdl2 = st.columns(2)
                    _gdl1.download_button(
                        label="⬇️ Download Segregation Result JSON",
                        data=json.dumps(_gal_seg_done, indent=2, ensure_ascii=False, default=str),
                        file_name=f"seg_result_{folder_path.replace('/','_')}{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        key=f"gal_seg_dl_{folder_path}",
                        use_container_width=True,
                    )
                    if _gdl2.button(
                        f"🗑️ Delete {_gal_seg_done['moved_count']} moved files",
                        key=f"gal_seg_delbtn_{folder_path}", use_container_width=True,
                    ):
                        st.session_state[f"gal_seg_del_confirm_{folder_path}"] = True

                    if st.session_state.get(f"gal_seg_del_confirm_{folder_path}"):
                        st.warning(f"⚠️ Permanently delete **{_gal_seg_done['moved_count']}** images from `{_gal_seg_done['dest_folder']}`?")
                        _gdca, _gdcb = st.columns(2)
                        if _gdca.button("✅ Yes, delete", key=f"gal_seg_delyes_{folder_path}", type="primary"):
                            _gal_moved_del = st.session_state.get(f"gal_seg_moved_keys_{folder_path}", [])
                            _gdel_ok = 0
                            _gdbar = st.progress(0.0, text="Deleting…")
                            for _gdi, _gdk in enumerate(_gal_moved_del, 1):
                                _gdbar.progress(_gdi / max(len(_gal_moved_del), 1),
                                               text=f"Deleting {_gdi}/{len(_gal_moved_del)}")
                                try: s3.delete_object(Bucket=bucket, Key=_gdk); _gdel_ok += 1
                                except: pass
                            _gdbar.empty()
                            st.success(f"✅ Deleted {_gdel_ok} files.")
                            st.session_state[f"gal_seg_result_{folder_path}"]      = None
                            st.session_state[f"gal_seg_moved_keys_{folder_path}"]  = []
                            st.session_state[f"gal_seg_del_confirm_{folder_path}"] = False
                            if "gal_all_folders" in st.session_state:
                                del st.session_state["gal_all_folders"]
                            st.rerun()
                        if _gdcb.button("❌ Cancel", key=f"gal_seg_delno_{folder_path}"):
                            st.session_state[f"gal_seg_del_confirm_{folder_path}"] = False
                            st.rerun()

                    if st.button("✖️ Clear result", key=f"gal_seg_clear_{folder_path}"):
                        st.session_state[f"gal_seg_result_{folder_path}"]     = None
                        st.session_state[f"gal_seg_moved_keys_{folder_path}"] = []
                        st.rerun()

        # ── Sort Result Viewer ───────────────────────────────────────────────
        _sort_res = st.session_state.get("sort_result")
        if _sort_res:
            _mv_keys = _sort_res.get("moved_keys",  [])
            _st_keys = _sort_res.get("stayed_keys", [])
            _mv_pfx  = _sort_res.get("moved_pfx",  "")
            _st_pfx  = _sort_res.get("stayed_pfx", "")
            if "sort_del_sel" not in st.session_state:
                st.session_state["sort_del_sel"] = set()

            st.divider()
            st.subheader("📊 Sort Results — Review & Delete")

            def _render_thumb_grid(keys, section_tag, ncols=5, thumb_w=160):
                if not keys: st.info("No images in this group."); return
                for _rs in range(0, len(keys), ncols):
                    _row_keys = keys[_rs: _rs + ncols]
                    _cols = st.columns(ncols)
                    for _col, _rk in zip(_cols, _row_keys):
                        _fname = Path(_rk).name
                        with _col:
                            _url = get_presigned_url(s3, bucket, _rk, expires=600)
                            if _url:
                                try: st.image(_url, width=thumb_w)
                                except: st.markdown("🖼️ *error*")
                            else:
                                st.markdown("🖼️ *no preview*")
                            st.markdown(f'<div class="gal-img-name" title="{_rk}">{_fname}</div>',
                                        unsafe_allow_html=True)
                            _chk = st.checkbox("🗑️ Select",
                                               value=_rk in st.session_state["sort_del_sel"],
                                               key=f"delchk_{section_tag}_{_rk}")
                            if _chk: st.session_state["sort_del_sel"].add(_rk)
                            else:    st.session_state["sort_del_sel"].discard(_rk)

            st.markdown(f'<div class="rv-moved"><h5>✅ Moved ({len(_mv_keys)}) — now in <code>{_mv_pfx}</code></h5></div>',
                        unsafe_allow_html=True)
            _render_thumb_grid(_mv_keys, "mv")
            st.markdown("---")
            st.markdown(f'<div class="rv-stayed"><h5>➡️ Stayed ({len(_st_keys)}) — still in <code>{_st_pfx}</code></h5></div>',
                        unsafe_allow_html=True)
            _render_thumb_grid(_st_keys, "st")
            st.markdown("---")

            _sel = st.session_state.get("sort_del_sel", set())
            _sel_list = list(_sel)
            if _sel:
                st.markdown(f'<div class="del-sel-bar">🗑️ <b>{len(_sel)}</b> image(s) selected</div>',
                            unsafe_allow_html=True)
                _ba, _bb, _ = st.columns([2, 2, 4])
                if _ba.button(f"🗑️ Delete {len(_sel)} selected", key="sort_del_btn",
                              type="primary", use_container_width=True):
                    _dbar = st.progress(0.0, text="Deleting…")
                    _ok = 0
                    for _di, _dk in enumerate(_sel_list, 1):
                        _dbar.progress(_di / len(_sel_list), text=f"Deleting {_di}/{len(_sel_list)}")
                        try: s3.delete_object(Bucket=bucket, Key=_dk); _ok += 1
                        except Exception as _de: st.error(f"Failed `{_dk}`: {_de}")
                    _dbar.empty()
                    st.success(f"✅ Deleted {_ok} / {len(_sel_list)} images.")
                    st.session_state["sort_result"]["moved_keys"]  = [k for k in _mv_keys if k not in _sel]
                    st.session_state["sort_result"]["stayed_keys"] = [k for k in _st_keys if k not in _sel]
                    st.session_state["sort_del_sel"] = set()
                    if "gal_all_folders" in st.session_state:
                        del st.session_state["gal_all_folders"]
                    st.rerun()
                if _bb.button("⬜ Deselect All", key="sort_desel_btn", use_container_width=True):
                    st.session_state["sort_del_sel"] = set()
                    st.rerun()
            else:
                st.info("☝️ Tick any image above to mark it for deletion.")

            if st.button("❌ Close Result Viewer", key="sort_close_btn"):
                st.session_state["sort_result"]  = None
                st.session_state["sort_del_sel"] = set()
                st.rerun()

        # ── Folder Viewer (thumbnail grid) ───────────────────────────────────
        active_folder = st.session_state.get("gal_folder")
        active_keys   = st.session_state.get("gal_keys", [])

        if active_folder and active_keys:
            st.divider()
            st.markdown(f"### 📂 Viewing: `{active_folder}`")
            st.markdown(f'<span class="gal-stat">🖼️ <b>{len(active_keys)}</b> images</span>'
                        f'<span class="gal-stat">📦 Bucket: <b>{bucket}</b></span>',
                        unsafe_allow_html=True)

            ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([2, 2, 2, 2])
            thumb_size   = ctrl1.select_slider("Thumbnail size", options=[100,140,180,220,260], value=180, key="gal_thumb")
            cols_per_row = ctrl2.slider("Columns", 2, 8, 4, key="gal_cols")
            show_names   = ctrl3.checkbox("Show filenames", value=True, key="gal_names")
            if ctrl4.button("❌ Close Gallery", key="gal_close", use_container_width=True):
                st.session_state["gal_folder"] = None
                st.session_state["gal_keys"]   = []
                st.rerun()

            PAGE_SIZE  = 48
            total_imgs = len(active_keys)
            page_count = max(1, (total_imgs + PAGE_SIZE - 1) // PAGE_SIZE)
            page_num   = 0
            if page_count > 1:
                page_num = st.number_input(f"Page (1–{page_count})", min_value=1,
                                           max_value=page_count, value=1, key="gal_page") - 1

            page_keys = active_keys[page_num * PAGE_SIZE: (page_num + 1) * PAGE_SIZE]
            for row_start in range(0, len(page_keys), cols_per_row):
                row_keys = page_keys[row_start: row_start + cols_per_row]
                cols     = st.columns(cols_per_row)
                for col, key in zip(cols, row_keys):
                    filename = Path(key).name
                    with col:
                        url = get_presigned_url(s3, bucket, key, expires=600)
                        if url:
                            try: st.image(url, width=thumb_size)
                            except: st.markdown("🖼️ *preview error*")
                        else:
                            st.markdown("🖼️ *no preview*")
                        if show_names:
                            st.markdown(f'<div class="gal-img-name" title="{key}">{filename}</div>',
                                        unsafe_allow_html=True)
            if page_count > 1:
                st.caption(f"Showing {page_num*PAGE_SIZE+1}–{min((page_num+1)*PAGE_SIZE, total_imgs)} "
                           f"of {total_imgs}  |  Page {page_num+1}/{page_count}")

        elif active_folder and not active_keys:
            st.info(f"No images found directly inside `{active_folder}`. "
                    "It may contain sub-folders — open those individually.")